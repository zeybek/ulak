# Protocol: NATS (JetStream)

The NATS dispatcher publishes messages to NATS subjects using [cnats](https://github.com/nats-io/nats.c) (the official C client). It is an optional protocol -- build with `ENABLE_NATS=1` to include it. The dispatcher supports both Core NATS (at-most-once, fire-and-forget) and JetStream (at-least-once with stream persistence). It provides four authentication methods, static and per-message header propagation, JetStream deduplication via `Nats-Msg-Id`, automatic reconnection, and batch delivery with async publish tracking.

---

## Quick Example

```sql
-- Create a NATS endpoint (JetStream enabled by default)
SELECT ulak.create_endpoint('bus', 'nats', '{
  "url": "nats://nats:4222",
  "subject": "orders.created"
}'::jsonb);

-- Send a message
SELECT ulak.send('bus', '{"order_id": 123}'::jsonb);
```

The background worker publishes the message to the `orders.created` subject. With JetStream enabled (default), the worker waits for a `jsPubAck` from the server before marking the message as delivered. The message is persisted in the matching JetStream stream.

---

## Endpoint Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `url` | string | NATS server URL. Supports `nats://` scheme and comma-separated URLs for cluster connections (e.g., `nats://n1:4222,nats://n2:4222`). |
| `subject` | string | Target publish subject. Must not contain wildcard characters (`*` or `>`). Hierarchical dot-separated naming (e.g., `orders.created.eu`). |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `jetstream` | boolean | `true` | Enable JetStream mode. When `true`, publishes use `js_PublishMsg` with acknowledgment. When `false`, uses Core NATS fire-and-forget. |
| `stream` | string | -- | JetStream stream name. The subject determines routing to the stream -- this field is informational and does not create or bind streams. |
| `token` | string | -- | Token authentication. |
| `username` | string | -- | Username for user/password authentication. |
| `password` | string | -- | Password for user/password authentication. |
| `nkey_seed` | string | -- | NKey seed for NKey authentication. |
| `credentials_file` | string | -- | Path to JWT `.creds` file. Must be an absolute path with `.creds` extension. Path traversal (`..`) is rejected. |
| `tls` | boolean | `false` | Enable TLS encryption. |
| `tls_ca_cert` | string | -- | Path to CA certificate for verifying the server. |
| `tls_cert` | string | -- | Path to client certificate for mutual TLS (mTLS). |
| `tls_key` | string | -- | Path to client private key for mTLS. |
| `headers` | object | -- | Static headers attached to every message. String key-value pairs. |
| `options` | object | -- | Advanced connection options. See [Advanced Options](#advanced-options). |

### Full Configuration Example

```json
{
  "url": "nats://nats1:4222,nats://nats2:4222",
  "subject": "orders.created",
  "jetstream": true,
  "stream": "ORDERS",
  "username": "producer",
  "password": "secret",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/nats-ca.pem",
  "tls_cert": "/etc/ssl/client/cert.pem",
  "tls_key": "/etc/ssl/client/key.pem",
  "headers": {
    "source": "ulak",
    "content-type": "application/json"
  },
  "options": {
    "max_reconnect": "60",
    "reconnect_wait": "2000",
    "ping_interval": "120000",
    "max_pings_out": "2",
    "io_buf_size": "32768"
  }
}
```

---

## Core NATS vs JetStream

The NATS dispatcher operates in one of two modes depending on the `jetstream` config field (default: `true`).

| Feature | Core NATS | JetStream |
|---------|-----------|-----------|
| Delivery guarantee | At-most-once | At-least-once |
| Persistence | No -- messages exist only in transit | Yes -- messages stored in stream |
| Publisher ACK | No -- fire-and-forget | Yes -- `jsPubAck` with sequence number |
| Deduplication | No | Yes -- via `Nats-Msg-Id` header |
| Batch flush | `natsConnection_FlushTimeout` | `js_PublishAsyncComplete` |
| Failed message tracking | No per-message tracking | `js_PublishAsyncGetPendingList` |

### Core NATS Mode

```json
{
  "url": "nats://nats:4222",
  "subject": "events.raw",
  "jetstream": false
}
```

Core NATS uses `natsConnection_PublishMsg()`. Messages are delivered to all current subscribers but are not persisted. If no subscriber is connected, the message is silently dropped.

### JetStream Mode (Default)

```json
{
  "url": "nats://nats:4222",
  "subject": "orders.created",
  "jetstream": true,
  "stream": "ORDERS"
}
```

JetStream uses `js_PublishMsg()` for synchronous dispatch and `js_PublishMsgAsync()` for batch operations. The server acknowledges with a `jsPubAck` containing the stream name, sequence number, and a duplicate flag.

---

## Authentication (4 Methods)

The NATS dispatcher supports four authentication methods. They are mutually exclusive -- configure only one per endpoint.

### Token Authentication

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "token": "secret-token"
}
```

### User/Password Authentication

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "username": "producer",
  "password": "secret"
}
```

### NKey Authentication

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "nkey_seed": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
}
```

NKey authentication uses Ed25519 key pairs. The `nkey_seed` is the private seed used to sign the server challenge. The public key must be configured on the NATS server.

### Credentials File (JWT)

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "credentials_file": "/etc/nats/user.creds"
}
```

Security constraints on `credentials_file`:
- Must be an absolute path (starts with `/`)
- Must have `.creds` extension
- Must not contain `..` (path traversal protection)

These constraints prevent the PostgreSQL server OS user from being exploited to read arbitrary files.

### Authentication Priority

When multiple authentication fields are present, the dispatcher uses this priority order:

1. `credentials_file` (JWT)
2. `nkey_seed` (NKey)
3. `token` (Token)
4. `username` + `password` (User/Pass)

Only the highest-priority method is applied.

---

## Subject Wildcards

NATS wildcard characters `*` (single-token) and `>` (full wildcard) are valid for subscribe operations but are protocol violations when used in publish subjects. The NATS dispatcher rejects any subject containing these characters during configuration validation:

```sql
-- This will fail validation
SELECT ulak.create_endpoint('bad', 'nats', '{
  "url": "nats://nats:4222",
  "subject": "orders.>"
}'::jsonb);
-- ERROR: NATS 'subject' must not contain wildcard characters ('*' or '>')
```

NATS subjects are hierarchical with dot-separated tokens (e.g., `orders.created.eu`). Use specific subjects for publishing and wildcards only on the subscriber side.

---

## Headers

The NATS dispatcher supports two sources of message headers:

### Static Headers

Defined in the endpoint config `headers` field. Attached to every message published through the endpoint.

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "headers": {
    "source": "ulak",
    "environment": "production",
    "content-type": "application/json"
  }
}
```

### Per-Message Headers

Passed via `produce_ex()` or `dispatch_ex()` at publish time. Per-message headers override static headers when the same key is present.

### Automatic Headers

For JetStream mode, the dispatcher automatically sets:

| Header | Value | Purpose |
|--------|-------|---------|
| `Nats-Msg-Id` | Queue message row ID (as string) | JetStream server-side deduplication |

The `Nats-Msg-Id` header is only set when `jetstream` is `true` and the message ID is greater than 0.

---

## JetStream Deduplication

When publishing to JetStream, the dispatcher sets the `Nats-Msg-Id` header to the PostgreSQL queue row ID. The JetStream server uses this header for server-side deduplication within the stream's deduplication window.

### How It Works

1. The dispatcher sets `Nats-Msg-Id` to the message queue ID (e.g., `"42"`).
2. If the same message is retried (e.g., after a timeout), it carries the same `Nats-Msg-Id`.
3. The JetStream server detects the duplicate and returns a `jsPubAck` with `Duplicate = true`.
4. The dispatcher captures the duplicate flag in the `DispatchResult` (`nats_js_duplicate`).

This provides exactly-once semantics for message delivery when combined with ulak's retry mechanism. Even if a message is published twice due to a transient failure, the stream stores only one copy.

---

## Advanced Options

The `options` object provides fine-grained control over NATS connection behavior. All values must be strings (parsed to integers internally).

```json
{
  "url": "nats://nats:4222",
  "subject": "events",
  "options": {
    "max_reconnect": "60",
    "reconnect_wait": "2000",
    "ping_interval": "120000",
    "max_pings_out": "2",
    "io_buf_size": "32768"
  }
}
```

| Option | Description |
|--------|-------------|
| `max_reconnect` | Maximum number of reconnection attempts. `-1` for infinite. |
| `reconnect_wait` | Time in milliseconds between reconnection attempts. |
| `ping_interval` | Interval in milliseconds between PING messages to the server. |
| `max_pings_out` | Maximum outstanding PINGs before the connection is considered lost. |
| `io_buf_size` | Size in bytes of the I/O read/write buffers. |

These options are applied via `nats_apply_options()` before the connection is established. Only the five keys listed above are accepted -- unknown keys cause a validation error.

---

## Batch Delivery

The NATS dispatcher supports batch operations through the produce/flush pattern.

### JetStream Batch

1. **produce()** -- Calls `js_PublishMsgAsync()` to enqueue the message without waiting for acknowledgment. Message ownership transfers to the NATS library.

2. **flush()** -- Calls `js_PublishAsyncComplete()` with the configured timeout. Then retrieves failed messages via `js_PublishAsyncGetPendingList()`. Failed messages are matched back to the pending array by `Nats-Msg-Id` header.

### Core NATS Batch

1. **produce()** -- Calls `natsConnection_PublishMsg()`. Since Core NATS is fire-and-forget, messages are marked as successful immediately in the pending array.

2. **flush()** -- Calls `natsConnection_FlushTimeout()` to flush the connection write buffer. No per-message tracking is available -- if the flush fails, all unflushed messages are marked as failed.

### Delivery Flow (JetStream)

```
Worker                          NATS Server
  |                                |
  |-- produce(msg1) ------------->|  (js_PublishMsgAsync)
  |-- produce(msg2) ------------->|  (js_PublishMsgAsync)
  |-- produce(msg3) ------------->|  (js_PublishMsgAsync)
  |                                |
  |-- flush(timeout) ------------>|  (js_PublishAsyncComplete)
  |                                |-- stream ACKs
  |<-- completion ----------------|
  |                                |
  |  js_PublishAsyncGetPendingList |  (retrieve any failures)
  |  (match failed by Nats-Msg-Id)|
```

---

## Error Classification

The NATS dispatcher classifies cnats status codes and JetStream error codes into permanent and retryable categories. Permanent errors skip retry and move the message directly to the failed state (and eventually the DLQ).

### Permanent NATS Errors

| Status | Meaning |
|--------|---------|
| `NATS_CONNECTION_AUTH_FAILED` | Authentication failure |
| `NATS_NOT_PERMITTED` | Permission denied for subject |
| `NATS_INVALID_ARG` | Invalid argument to NATS function |
| `NATS_MAX_PAYLOAD` | Message exceeds server `max_payload` |
| `NATS_PROTOCOL_ERROR` | NATS protocol violation |

### Permanent JetStream Errors

| Error Code | Name | Meaning |
|------------|------|---------|
| 10059 | `JSStreamNotFoundErr` | Target stream does not exist |
| 10076 | `JSNotEnabledErr` | JetStream is not enabled on the server |
| 10003 | `JSBadRequestErr` | Malformed JetStream request |
| 10025 | `JSStreamNameExistErr` | Stream name conflict |

### Retryable Errors

All other errors are treated as retryable, including:

- `NATS_TIMEOUT` -- Operation timed out
- `NATS_CONNECTION_CLOSED` -- Connection permanently closed
- `NATS_CONNECTION_DISCONNECTED` -- Temporary disconnect
- `NATS_NO_SERVER` -- No server available
- `NATS_IO_ERROR` -- Network I/O error
- `NATS_SLOW_CONSUMER` -- Consumer cannot keep up
- `JSInsufficientResourcesErr` (10023) -- Server resource limits
- `JSClusterNoPeersErr` (10074) -- JetStream cluster has no peers
- `JSStreamStoreFailedErr` (10077) -- Stream storage failure

Retryable errors cause the message to return to `pending` status with an incremented `retry_count` and a backoff delay calculated from the endpoint retry policy.

---

## GUC Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.nats_delivery_timeout` | 5000 ms | 1000 -- 60000 | Maximum time to wait for JetStream publish acknowledgment on synchronous dispatch. Also used as the connection timeout. |
| `ulak.nats_flush_timeout` | 10000 ms | 1000 -- 300000 | Maximum time for async publish completion during batch flush (`js_PublishAsyncComplete` or `natsConnection_FlushTimeout`). |
| `ulak.nats_batch_capacity` | 64 | 16 -- 1024 | Initial capacity for the pending messages array. Grows dynamically via `repalloc` when exceeded. Also sets `PublishAsync.MaxPending` for JetStream. |
| `ulak.nats_reconnect_wait` | 2000 ms | 100 -- 60000 | Time between automatic reconnection attempts. Applied via `natsOptions_SetReconnectWait()`. |
| `ulak.nats_max_reconnect` | 60 | -1 -- 10000 | Maximum number of reconnection attempts. Set to `-1` for infinite reconnection. Applied via `natsOptions_SetMaxReconnect()`. |

All parameters have `PGC_SIGHUP` context -- they can be changed with `ALTER SYSTEM SET` and applied with `SELECT pg_reload_conf()` without restarting PostgreSQL. Workers pick up the new values on their next SIGHUP processing cycle.

---

## See Also

- [Getting Started](Getting-Started) -- Installation with `ENABLE_NATS=1` and first NATS message
- [Architecture](Architecture) -- Dispatcher factory, batch delivery model, dispatcher cache
- [Security](Security) -- Credential zeroing, TLS configuration
- [Reliability](Reliability) -- Circuit breaker, retry backoff, DLQ
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- `send()`, `send_with_options()`, `create_endpoint()`
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-Kafka](Protocol-Kafka) | [Protocol-MQTT](Protocol-MQTT) | [Protocol-Redis](Protocol-Redis) | [Protocol-AMQP](Protocol-AMQP)
