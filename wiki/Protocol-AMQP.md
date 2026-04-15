# Protocol: AMQP (RabbitMQ)

The AMQP dispatcher publishes messages to RabbitMQ using [librabbitmq](https://github.com/alanxz/rabbitmq-c) over the AMQP 0-9-1 protocol. It is an optional protocol -- build with `ENABLE_AMQP=1` to include it. The dispatcher supports exchange types (direct, fanout, topic, headers), routing keys, publisher confirms with per-message ACK/NACK tracking, virtual hosts, TLS/mTLS, persistent delivery, and batch operations with delivery tag sequencing.

---

## Quick Example

```sql
-- Create an AMQP endpoint (default exchange, direct routing to queue)
SELECT ulak.create_endpoint('queue', 'amqp', '{
  "host": "rabbitmq",
  "exchange": "",
  "routing_key": "order-queue",
  "username": "guest",
  "password": "guest"
}'::jsonb);

-- Send a message
SELECT ulak.send('queue', '{"order_id": 123}'::jsonb);
```

The background worker publishes the message to the default exchange with routing key `order-queue`. RabbitMQ routes it to the queue named `order-queue`. The worker waits for a publisher confirm (ACK) before marking the message as delivered.

---

## Endpoint Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `host` | string | RabbitMQ broker hostname or IP address. |
| `exchange` | string | Target exchange name. Use empty string `""` for the RabbitMQ default exchange (direct routing to queue by name). |
| `routing_key` | string | Routing key for message delivery. For the default exchange, this is the target queue name. |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | number | 5672 | Broker port (1--65535). Auto-adjusts to 5671 when `tls` is `true` and no explicit port is set. |
| `vhost` | string | `"/"` | AMQP virtual host. |
| `username` | string | `"guest"` | SASL PLAIN authentication username. |
| `password` | string | `"guest"` | SASL PLAIN authentication password. |
| `exchange_type` | string | -- | Exchange type hint for documentation/validation: `direct`, `fanout`, `topic`, or `headers`. Does not declare the exchange -- it must already exist on the broker. |
| `persistent` | boolean | `true` | Set `delivery_mode = 2` (persistent). Messages survive broker restarts when published to a durable queue. |
| `mandatory` | boolean | `false` | Require the message to be routed to at least one queue. Unroutable messages trigger a `basic.return` from the broker. |
| `heartbeat` | number | 60 | AMQP heartbeat interval in seconds (0--600). Set to `0` to disable heartbeats. |
| `frame_max` | number | 131072 | Maximum AMQP frame size in bytes (4096--2097152). Default is 128 KB. |
| `tls` | boolean | `false` | Enable TLS encryption. |
| `tls_ca_cert` | string | -- | Path to CA certificate for verifying the broker. |
| `tls_cert` | string | -- | Path to client certificate for mutual TLS (mTLS). |
| `tls_key` | string | -- | Path to client private key for mTLS. |
| `tls_verify_peer` | boolean | `true` | Verify broker certificate against the CA. When `true` without a `tls_ca_cert`, a warning is emitted. |

### Full Configuration Example

```json
{
  "host": "rabbitmq.example.com",
  "port": 5671,
  "vhost": "/production",
  "username": "producer",
  "password": "secret",
  "exchange": "order-events",
  "routing_key": "orders.created",
  "exchange_type": "topic",
  "persistent": true,
  "mandatory": false,
  "heartbeat": 60,
  "frame_max": 131072,
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/rabbitmq-ca.pem",
  "tls_cert": "/etc/ssl/client/cert.pem",
  "tls_key": "/etc/ssl/client/key.pem",
  "tls_verify_peer": true
}
```

---

## Exchange Types

AMQP 0-9-1 defines four exchange types. The `exchange_type` config field is validated but does not declare the exchange -- it must already exist on the broker.

| Type | Routing Behavior | Use Case |
|------|-----------------|----------|
| `direct` | Exact match between routing key and queue binding key. | Point-to-point delivery. Route `orders.created` to a queue bound with key `orders.created`. |
| `fanout` | Ignores routing key entirely. Delivers to all bound queues. | Broadcast. Publish once, deliver to every consumer queue. |
| `topic` | Pattern matching with `.` separators. `*` matches one word, `#` matches zero or more words. | Selective fan-out. Routing key `orders.created.us` matches bindings `orders.created.*` and `orders.#`. |
| `headers` | Matches on message headers instead of routing key. | Attribute-based routing when the routing key is insufficient. |

### Routing Examples

```sql
-- Direct exchange: message goes to queue bound with exact key "invoices"
SELECT ulak.create_endpoint('direct_ep', 'amqp', '{
  "host": "rabbitmq", "exchange": "billing", "routing_key": "invoices",
  "exchange_type": "direct"
}'::jsonb);

-- Fanout exchange: routing_key is ignored, all bound queues receive the message
SELECT ulak.create_endpoint('fanout_ep', 'amqp', '{
  "host": "rabbitmq", "exchange": "notifications", "routing_key": "",
  "exchange_type": "fanout"
}'::jsonb);

-- Topic exchange: message routed to queues matching the pattern
SELECT ulak.create_endpoint('topic_ep', 'amqp', '{
  "host": "rabbitmq", "exchange": "events", "routing_key": "orders.created.eu",
  "exchange_type": "topic"
}'::jsonb);
```

---

## Publisher Confirms

Publisher confirms are auto-enabled on every AMQP connection via `amqp_confirm_select()`. This provides reliable delivery tracking at the protocol level.

### How It Works

1. After opening a channel, the dispatcher enables confirm mode. Delivery tags start at 1 and increment for each published message.
2. Each `amqp_basic_publish()` call assigns a monotonically increasing delivery tag.
3. The broker sends `basic.ack` (success) or `basic.nack` (failure) for each delivery tag.
4. The `multiple` flag on ACK/NACK indicates that all messages up to and including the delivery tag are confirmed.

### Confirm Processing

The dispatcher reads confirm frames via `amqp_simple_wait_frame_noblock()` with a 10ms poll interval. Each frame is matched against the pending message array by delivery tag. NACK'd messages are marked as failed with the error `"Message NACK'd by broker"`.

If the connection or channel closes unexpectedly during confirm processing, all unconfirmed messages are treated as retryable failures.

---

## Message Properties

The AMQP dispatcher automatically sets these properties on every published message:

| Property | Value | Description |
|----------|-------|-------------|
| `content_type` | `application/json` | Payload MIME type. |
| `delivery_mode` | `2` (persistent) or `1` (transient) | Controlled by the `persistent` config field (default: `true`). |
| `timestamp` | Current Unix timestamp | Set at publish time via `time(NULL)`. |
| `app_id` | `ulak` | Identifies the publishing application. |
| `message_id` | `pgx-{delivery_tag}` | Unique per-message identifier derived from the AMQP delivery tag. |

These properties are set in the `amqp_basic_properties_t` struct before each `amqp_basic_publish()` call. They cannot be overridden via endpoint configuration.

---

## Authentication and TLS

### SASL PLAIN Authentication

The AMQP dispatcher uses SASL PLAIN for authentication (the only mechanism supported by librabbitmq). Credentials are passed to `amqp_login()` at connection time.

```json
{
  "host": "rabbitmq",
  "exchange": "",
  "routing_key": "my-queue",
  "username": "producer",
  "password": "secret"
}
```

When `username` and `password` are omitted, they default to `"guest"/"guest"` -- the RabbitMQ default credentials that only work for localhost connections.

### TLS (Server Verification)

```json
{
  "host": "rabbitmq.example.com",
  "exchange": "events",
  "routing_key": "orders",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/rabbitmq-ca.pem"
}
```

When `tls` is `true` and no explicit `port` is provided, the port auto-adjusts from 5672 to 5671.

### Mutual TLS (mTLS)

```json
{
  "host": "rabbitmq.example.com",
  "exchange": "events",
  "routing_key": "orders",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/rabbitmq-ca.pem",
  "tls_cert": "/etc/ssl/client/cert.pem",
  "tls_key": "/etc/ssl/client/key.pem",
  "tls_verify_peer": true
}
```

The dispatcher calls `amqp_ssl_socket_set_cacert()`, `amqp_ssl_socket_set_key()`, and `amqp_ssl_socket_set_verify_peer()` to configure the TLS socket. Both peer certificate and hostname verification are controlled by `tls_verify_peer`.

> **Warning:** Setting `tls_verify_peer` to `false` disables certificate and hostname verification. The connection is still encrypted but vulnerable to man-in-the-middle attacks. Use only in development environments.

---

## Batch Delivery

The AMQP dispatcher supports high-throughput batch delivery via the produce/flush pattern with publisher confirms.

### Batch Flow

1. **produce()** -- Calls `amqp_basic_publish()` for each message and assigns a delivery tag. The message is tracked in the pending array but no confirm is waited for. This is non-blocking.

2. **flush()** -- Polls for publisher confirm frames via `amqp_simple_wait_frame_noblock()` with a 10ms poll interval until all pending messages are confirmed or the timeout expires. Returns the set of failed message IDs.

### Delivery Tag Sequencing

Delivery tags are monotonically increasing integers starting from 1 per channel. The `multiple` flag on `basic.ack` allows the broker to confirm all messages up to a given tag in a single frame, reducing protocol overhead.

### Delivery Flow

```
Worker                          RabbitMQ
  |                                |
  |-- produce(msg1, tag=1) ------>|  (basic.publish)
  |-- produce(msg2, tag=2) ------>|  (basic.publish)
  |-- produce(msg3, tag=3) ------>|  (basic.publish)
  |                                |
  |-- flush(timeout) ------------>|  (wait_frame_noblock, 10ms poll)
  |                                |-- basic.ack(tag=3, multiple=true)
  |<-- confirm frame -------------|
  |                                |
  |  (all 3 messages confirmed)    |
```

Unconfirmed messages after timeout expiry are marked as retryable failures (`"AMQP confirm timed out"`). NACK'd messages are marked with `"AMQP message NACK'd by broker"`.

---

## Error Classification

The AMQP dispatcher classifies broker reply codes into permanent and retryable categories. Permanent errors skip retry and move the message directly to the failed state (and eventually the DLQ).

### Permanent Errors

| Reply Code | Meaning |
|------------|---------|
| 402 | `INVALID_PATH` -- Invalid virtual host |
| 403 | `ACCESS_REFUSED` -- Authentication failure or permission denied |
| 404 | `NOT_FOUND` -- Queue or exchange does not exist |
| 530 | `NOT_ALLOWED` -- Virtual host access denied |

### Retryable Errors

All other errors are treated as retryable, including:

- Connection timeouts
- Socket errors
- Broker temporarily unavailable
- Channel closed unexpectedly
- Library-level exceptions

Retryable errors cause the message to return to `pending` status with an incremented `retry_count` and a backoff delay calculated from the endpoint retry policy.

---

## GUC Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.amqp_connection_timeout` | 10 s | 1 -- 300 | Maximum time to wait for AMQP connection establishment. |
| `ulak.amqp_heartbeat` | 60 s | 0 -- 600 | Default AMQP heartbeat interval. Set to `0` to disable heartbeats. Overridden by per-endpoint `heartbeat` config. |
| `ulak.amqp_frame_max` | 131072 | 4096 -- 2097152 | Default maximum AMQP frame size in bytes (128 KB). Overridden by per-endpoint `frame_max` config. |
| `ulak.amqp_delivery_timeout` | 30000 ms | 5000 -- 300000 | Maximum time to wait for publisher confirm on synchronous dispatch and batch flush. |
| `ulak.amqp_batch_capacity` | 64 | 16 -- 1024 | Initial capacity for the pending messages array. Grows dynamically via `repalloc` when exceeded. |
| `ulak.amqp_ssl_verify_peer` | `true` | -- | Default SSL/TLS peer certificate verification. Overridden by per-endpoint `tls_verify_peer` config. |

All parameters have `PGC_SIGHUP` context -- they can be changed with `ALTER SYSTEM SET` and applied with `SELECT pg_reload_conf()` without restarting PostgreSQL. Workers pick up the new values on their next SIGHUP processing cycle.

---

## See Also

- [Getting Started](Getting-Started) -- Installation with `ENABLE_AMQP=1` and first AMQP message
- [Architecture](Architecture) -- Dispatcher factory, batch delivery model, dispatcher cache
- [Security](Security) -- Credential zeroing, TLS configuration
- [Reliability](Reliability) -- Circuit breaker, retry backoff, DLQ
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- `send()`, `send_with_options()`, `create_endpoint()`
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-Kafka](Protocol-Kafka) | [Protocol-MQTT](Protocol-MQTT) | [Protocol-Redis](Protocol-Redis) | [Protocol-NATS](Protocol-NATS)
