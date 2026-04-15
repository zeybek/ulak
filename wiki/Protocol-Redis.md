# Protocol: Redis Streams

The Redis dispatcher publishes messages to [Redis Streams](https://redis.io/docs/data-types/streams/) using `XADD`. It is an optional protocol -- build with `ENABLE_REDIS=1` to include it. The dispatcher uses [hiredis](https://github.com/redis/hiredis) and supports ACL authentication (Redis 6+), TLS, stream trimming (`MAXLEN` / `MINID`), consumer group auto-creation, and pipelined batch delivery.

---

## Quick Example

```sql
-- Create a Redis Streams endpoint
SELECT ulak.create_endpoint('stream', 'redis', '{
  "host": "redis",
  "stream_key": "order-events"
}'::jsonb);

-- Send a message
SELECT ulak.send('stream', '{"order_id": 123}'::jsonb);
```

The background worker will execute `XADD order-events * payload '{"order_id": 123}' ts '2026-04-14T12:00:00Z'` on the specified Redis instance. The stream is auto-created by Redis if it does not already exist (unless `nomkstream` is set).

---

## Endpoint Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `host` | string | Redis hostname or IP address. |
| `stream_key` | string | Target stream name for `XADD`. |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | number (1 -- 65535) | 6379 | Redis port. |
| `db` | number (0 -- 15) | 0 | Database index. Sent as `SELECT db` after connection. |
| `password` | string | -- | Redis AUTH password. |
| `username` | string | -- | ACL username (Redis 6+). When provided with `password`, sends `AUTH username password`. |
| `connect_timeout` | number (1 -- 3600) | 5 | Connection timeout in seconds. |
| `command_timeout` | number (1 -- 3600) | 30 | Command execution timeout in seconds. |
| `maxlen` | number (>= 0) | -- | Stream maximum length. Appended as `MAXLEN` to `XADD`. `0` means unlimited (no trimming). |
| `maxlen_approximate` | boolean | true | Use `~` for approximate trimming. Faster because Redis avoids scanning the entire radix tree. |
| `minid` | number (>= 0) | -- | Minimum ID threshold for trimming. Appended as `MINID` to `XADD`. Trims entries with IDs lower than this value. |
| `nomkstream` | boolean | false | When `true`, the `XADD` command fails if the stream does not already exist instead of auto-creating it. |
| `consumer_group` | string | -- | Consumer group name. Auto-created on connect via `XGROUP CREATE`. |
| `tls` | boolean | false | Enable TLS for the connection. |
| `tls_ca_cert` | string | -- | Path to CA certificate file (PEM) for server verification. |
| `tls_cert` | string | -- | Path to client certificate file (PEM) for mutual TLS. |
| `tls_key` | string | -- | Path to client private key file (PEM) for mutual TLS. |

### Full Configuration Example

```json
{
  "host": "redis.example.com",
  "port": 6380,
  "stream_key": "order-events",
  "db": 2,
  "username": "producer",
  "password": "secret",
  "connect_timeout": 10,
  "command_timeout": 30,
  "maxlen": 100000,
  "maxlen_approximate": true,
  "nomkstream": false,
  "consumer_group": "order-processors",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/redis-ca.pem",
  "tls_cert": "/etc/ssl/client/redis-cert.pem",
  "tls_key": "/etc/ssl/client/redis-key.pem"
}
```

---

## Stream Management

Redis Streams grow unbounded by default. ulak supports three trimming strategies via the `XADD` command options. All trimming happens atomically with message insertion -- no separate cleanup commands are needed.

### MAXLEN

Cap the stream to a fixed number of entries. Oldest entries are evicted when the limit is exceeded.

**Approximate (default, fast):**

```json
{
  "host": "redis",
  "stream_key": "events",
  "maxlen": 10000,
  "maxlen_approximate": true
}
```

Produces: `XADD events MAXLEN ~ 10000 * payload ... ts ...`

The `~` operator allows Redis to trim in batches aligned with the internal radix tree nodes, which is significantly faster than exact trimming. The actual stream length may temporarily exceed the limit by a small margin.

**Exact (slower):**

```json
{
  "host": "redis",
  "stream_key": "events",
  "maxlen": 10000,
  "maxlen_approximate": false
}
```

Produces: `XADD events MAXLEN 10000 * payload ... ts ...`

Guarantees the stream never exceeds exactly 10000 entries, but requires Redis to walk the radix tree on every insertion.

### MINID

Trim entries with IDs older than a threshold. Useful for time-based retention where you want to keep entries from a specific point forward.

```json
{
  "host": "redis",
  "stream_key": "events",
  "minid": 1713091234567
}
```

Produces: `XADD events MINID ~ 1713091234567 * payload ... ts ...`

When `maxlen_approximate` is `true` (default), the `~` operator is also applied to `MINID` trimming.

### NOMKSTREAM

By default, `XADD` creates the stream if it does not exist. Set `nomkstream` to `true` to fail instead:

```json
{
  "host": "redis",
  "stream_key": "events",
  "nomkstream": true
}
```

Produces: `XADD events NOMKSTREAM * payload ... ts ...`

This is useful when the stream must be pre-created (e.g., with specific consumer groups and configuration) and accidental auto-creation would indicate a misconfiguration.

### Priority

When multiple trimming options are specified, they are applied with the following priority:

| Priority | Combination | Behavior |
|----------|-------------|----------|
| 1 (highest) | `nomkstream` + `minid` | Fail if stream missing; trim by minimum ID |
| 2 | `nomkstream` + `maxlen` | Fail if stream missing; trim by max length |
| 3 | `minid` (alone) | Auto-create stream; trim by minimum ID |
| 4 (lowest) | `maxlen` (alone) | Auto-create stream; trim by max length |

`MINID` takes precedence over `MAXLEN` when both are specified, because ID-based trimming provides more predictable time-based retention.

---

## Authentication

### ACL Authentication (Redis 6+)

Redis 6 introduced the ACL system with per-user permissions. When both `username` and `password` are provided, ulak sends the two-argument AUTH command:

```json
{
  "host": "redis.example.com",
  "stream_key": "events",
  "username": "ulak_producer",
  "password": "secret"
}
```

The connection handshake sends: `AUTH ulak_producer secret`

Ensure the ACL user has at minimum `+xadd`, `+xgroup`, and `+select` permissions on the target stream key.

### Legacy Authentication (Redis 5 and Earlier)

When only `password` is provided (no `username`), ulak sends the single-argument AUTH command:

```json
{
  "host": "redis.example.com",
  "stream_key": "events",
  "password": "requirepass-value"
}
```

The connection handshake sends: `AUTH requirepass-value`

### Error Handling

Authentication failures are classified as **permanent errors** and are not retried. A wrong password or missing ACL user will move the message directly to the failed state. This prevents retry storms against a Redis instance with invalid credentials.

---

## TLS

Enable TLS by setting `"tls": true`. For server-only verification (one-way TLS):

```json
{
  "host": "redis.example.com",
  "port": 6380,
  "stream_key": "events",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/redis-ca.pem"
}
```

For mutual TLS (mTLS), add the client certificate and key:

```json
{
  "host": "redis.example.com",
  "port": 6380,
  "stream_key": "events",
  "password": "secret",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/redis-ca.pem",
  "tls_cert": "/etc/ssl/client/redis-cert.pem",
  "tls_key": "/etc/ssl/client/redis-key.pem"
}
```

The TLS private key is zeroed with `explicit_bzero()` during dispatcher cleanup. See [Security](Security) for the full credential lifecycle.

---

## Stream Fields

Every `XADD` command produced by ulak adds exactly two fields to the stream entry:

| Field | Description |
|-------|-------------|
| `payload` | The message body as a JSON string. This is the value passed to `ulak.send()`. |
| `ts` | ISO 8601 UTC timestamp of when the message was dispatched. Format: `2026-04-14T12:00:00Z`. |

A consumer reading the stream will see entries like:

```
> XREAD COUNT 1 STREAMS order-events 0
1) 1) "order-events"
   2) 1) 1) "1713091234567-0"
         2) 1) "payload"
            2) "{\"order_id\": 123}"
            3) "ts"
            4) "2026-04-14T12:00:00Z"
```

---

## Batch Delivery

The Redis dispatcher uses pipelined batch delivery for throughput:

1. **produce()** -- Appends a `XADD` command to the hiredis output buffer via `redisAppendCommand()` without performing a network round-trip. This is non-blocking.

2. **flush()** -- Reads all reply objects from the connection via `redisGetReply()` in sequence, matching each reply to the corresponding message. Returns the set of failed message IDs to the worker.

### Internal Details

- Delivery is single-threaded -- no locks or synchronization are needed. The hiredis connection is owned exclusively by one worker (via the dispatcher cache).
- If the connection drops mid-flush, all remaining unread replies are marked as failed. The dispatcher is evicted from the cache, and a new connection is established on the next batch.
- Pipeline depth is bounded by the worker batch size (`ulak.batch_size`, default 100).

### Delivery Flow

```
Worker                          Redis
  |                                |
  |-- XADD (append to buffer) --->|  (no round-trip)
  |-- XADD (append to buffer) --->|  (no round-trip)
  |-- XADD (append to buffer) --->|  (no round-trip)
  |                                |
  |-- flush (send buffer) ------->|
  |                                |-- execute XADD pipeline
  |<-- reply 1 (stream ID) -------|
  |<-- reply 2 (stream ID) -------|
  |<-- reply 3 (error) -----------|
  |                                |
  |  (match failures to batch)     |
```

---

## Consumer Group

When the `consumer_group` field is specified, ulak auto-creates the consumer group on connection:

```
XGROUP CREATE order-events order-processors $ MKSTREAM
```

- `$` means the group starts reading from new messages only (not historical).
- `MKSTREAM` creates the stream if it does not exist (regardless of the `nomkstream` setting, since group creation happens at connection time, not at message delivery time).
- The command is **idempotent** -- if the group already exists, the `BUSYGROUP` error is silently ignored.

This is a convenience for bootstrapping consumer infrastructure. Once the group exists, consumers can read with `XREADGROUP GROUP order-processors consumer-1 COUNT 10 BLOCK 5000 STREAMS order-events >`.

```json
{
  "host": "redis",
  "stream_key": "order-events",
  "consumer_group": "order-processors"
}
```

---

## Error Classification

The Redis dispatcher classifies hiredis error responses into permanent and retryable categories.

### Permanent Errors

| Error | Meaning |
|-------|---------|
| `NOAUTH` | Authentication required but no credentials provided |
| `NOPERM` | ACL permission denied for the command or key |
| `WRONGTYPE` | Key exists but is not a stream type |
| `OOM` | Redis out-of-memory (maxmemory reached, no eviction policy allows freeing) |
| `READONLY` | Write command sent to a read-only replica |

### Retryable Errors

All other errors are treated as retryable, including:

- Connection refused or reset
- Network timeouts
- `LOADING` (Redis is loading the dataset from disk)
- `CLUSTERDOWN` (cluster is unavailable)
- Temporary I/O errors

Retryable errors cause the message to return to `pending` status with an incremented `retry_count` and a backoff delay calculated from the endpoint retry policy.

---

## GUC Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.redis_connect_timeout` | 5 s | 1 -- 60 | Connection timeout. Applied when establishing a new hiredis connection. |
| `ulak.redis_command_timeout` | 30 s | 1 -- 300 | Command execution timeout. Applied to each `XADD` and pipeline flush operation. |
| `ulak.redis_default_db` | 0 | 0 -- 15 | Default database index when not specified in endpoint config. |
| `ulak.redis_default_port` | 6379 | 1 -- 65535 | Default port when not specified in endpoint config. |

All parameters have `PGC_SIGHUP` context -- they can be changed with `ALTER SYSTEM SET` and applied with `SELECT pg_reload_conf()` without restarting PostgreSQL. Workers pick up the new values on their next SIGHUP processing cycle.

---

## See Also

- [Getting Started](Getting-Started) -- Installation with `ENABLE_REDIS=1` and first Redis message
- [Architecture](Architecture) -- Dispatcher factory, batch delivery model, dispatcher cache
- [Security](Security) -- Credential zeroing, TLS configuration
- [Reliability](Reliability) -- Circuit breaker, retry backoff, DLQ
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- `send()`, `send_with_options()`, `create_endpoint()`
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-Kafka](Protocol-Kafka) | [Protocol-MQTT](Protocol-MQTT) | [Protocol-AMQP](Protocol-AMQP) | [Protocol-NATS](Protocol-NATS)
