# Protocol: Apache Kafka

The Kafka dispatcher publishes messages to Apache Kafka topics using [librdkafka](https://github.com/confluentinc/librdkafka). It is an optional protocol -- build with `ENABLE_KAFKA=1` to include it. The dispatcher supports async batch delivery with delivery reports, SASL authentication, SSL/TLS, message keys, headers, idempotent producer, and configurable compression.

---

## Quick Example

```sql
-- Create a Kafka endpoint
SELECT ulak.create_endpoint('events', 'kafka', '{
  "broker": "kafka:9092",
  "topic": "order-events"
}'::jsonb);

-- Send a message
SELECT ulak.send('events', '{"order_id": 123}'::jsonb);
```

The background worker will produce the message to the `order-events` topic on the specified broker. Delivery is confirmed asynchronously via librdkafka delivery reports.

---

## Endpoint Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `broker` | string | Bootstrap servers. Comma-separated for multiple brokers. |
| `topic` | string | Target Kafka topic. |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `key` | string | -- | Static message key for partitioning. Can be overridden per-message via `metadata.key`. |
| `partition` | number | -1 | Target partition. `-1` means automatic partitioning (murmur2 hash of key, or round-robin if no key). |
| `headers` | object | -- | Static headers attached to every message. String key-value pairs. |
| `options` | object | -- | librdkafka configuration options passed through directly. See [librdkafka Options](#librdkafka-options). |

### Full Configuration Example

```json
{
  "broker": "broker1:9092,broker2:9092,broker3:9092",
  "topic": "order-events",
  "key": "order-service",
  "partition": -1,
  "headers": {
    "source": "ulak",
    "environment": "production",
    "content-type": "application/json"
  },
  "options": {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "producer",
    "sasl.password": "secret",
    "ssl.ca.location": "/etc/ssl/certs/kafka-ca.pem",
    "client.id": "ulak-producer",
    "acks": "all",
    "retries": "3",
    "batch.size": "16384",
    "linger.ms": "5",
    "compression.type": "snappy"
  }
}
```

---

## librdkafka Options

The `options` object passes any valid [librdkafka configuration property](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) directly to the producer. All values must be strings (librdkafka parses them internally).

### Common Options

| Option | Description |
|--------|-------------|
| `security.protocol` | Protocol used to communicate with brokers: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl.mechanism` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `sasl.username` | SASL username |
| `sasl.password` | SASL password |
| `ssl.ca.location` | Path to CA certificate for verifying the broker |
| `ssl.certificate.location` | Path to client certificate for mTLS |
| `ssl.key.location` | Path to client private key for mTLS |
| `client.id` | Client identifier sent to the broker |
| `acks` | Acknowledgment level: `0`, `1`, `-1` (or `all`) |
| `retries` | Number of retries for transient produce failures |
| `batch.size` | Maximum size of a message batch in bytes |
| `linger.ms` | Delay to wait for additional messages before sending a batch |
| `compression.type` | Compression codec: `none`, `gzip`, `snappy`, `lz4`, `zstd` |

### Defaults Applied Automatically

When not specified in the `options` object, ulak applies these defaults to the librdkafka producer:

| Option | Default Value | Rationale |
|--------|---------------|-----------|
| `acks` | `all` | Wait for all in-sync replicas to acknowledge |
| `retries` | `3` | Retry transient broker errors |
| `batch.size` | `16384` | 16 KB batch size for throughput |
| `linger.ms` | `5` | Small delay to accumulate batches |
| `compression.type` | `snappy` | Good compression ratio with low CPU |
| `enable.idempotence` | `true` | Prevent duplicate messages on retry |

These defaults prioritize durability and correctness. Override them in the `options` object when you need different trade-offs (e.g., `"acks": "1"` for lower latency at the cost of durability).

---

## Authentication and Security

### SASL + SSL (Recommended for Production)

```json
{
  "broker": "kafka.example.com:9093",
  "topic": "events",
  "options": {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "producer",
    "sasl.password": "secret",
    "ssl.ca.location": "/etc/ssl/certs/kafka-ca.pem"
  }
}
```

### SASL + PLAINTEXT (Development Only)

```json
{
  "broker": "kafka:9092",
  "topic": "events",
  "options": {
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "producer",
    "sasl.password": "secret"
  }
}
```

> **Warning:** SASL_PLAINTEXT transmits credentials in cleartext. Use only in isolated development environments.

### SSL Without SASL (Certificate Authentication)

```json
{
  "broker": "kafka.example.com:9093",
  "topic": "events",
  "options": {
    "security.protocol": "SSL",
    "ssl.ca.location": "/etc/ssl/certs/kafka-ca.pem",
    "ssl.certificate.location": "/etc/ssl/client/cert.pem",
    "ssl.key.location": "/etc/ssl/client/key.pem"
  }
}
```

This uses mutual TLS (mTLS) for authentication. The broker verifies the client certificate, and the client verifies the broker certificate against the CA.

### PLAINTEXT (No Security)

```json
{
  "broker": "kafka:9092",
  "topic": "events"
}
```

When no `options.security.protocol` is specified, librdkafka defaults to `PLAINTEXT`. Suitable for local development with Docker Compose.

---

## Headers

Kafka headers are key-value string pairs attached to each message. ulak supports headers from two sources:

1. **Static headers** from the endpoint `headers` config field -- attached to every message sent through the endpoint.
2. **Per-message headers** via `dispatch_ex` -- passed through the message `metadata` at dispatch time.

When both sources provide the same header key, the per-message header takes precedence and overrides the static value.

```json
{
  "broker": "kafka:9092",
  "topic": "events",
  "headers": {
    "source": "ulak",
    "content-type": "application/json"
  }
}
```

---

## Batch Delivery

The Kafka dispatcher uses asynchronous batch delivery for high throughput:

1. **produce()** -- Enqueues each message via `rd_kafka_producev()` without waiting for broker acknowledgment. This is non-blocking; the message is placed in the librdkafka internal queue.

2. **flush()** -- Polls the librdkafka producer for delivery reports until all pending messages are confirmed or the flush timeout expires. Returns the set of failed message IDs to the worker.

### Internal Details

- A spinlock-protected pending array tracks in-flight messages between produce and flush phases.
- Delivery report callbacks run on the librdkafka background thread, not the PostgreSQL worker process. These callbacks use fixed 256-byte error buffers (stack-allocated) and never call `palloc` -- they are safe to execute outside the PostgreSQL memory context.
- The pending array capacity is controlled by `ulak.kafka_batch_capacity`.

### Delivery Flow

```
Worker                          librdkafka
  |                                 |
  |-- produce(msg1) -------------->|  (enqueue, non-blocking)
  |-- produce(msg2) -------------->|  (enqueue, non-blocking)
  |-- produce(msg3) -------------->|  (enqueue, non-blocking)
  |                                 |
  |-- flush(timeout) ------------->|  (poll delivery reports)
  |                                 |-- send batch to broker -->
  |                                 |<-- ack/nack from broker --
  |<-- delivery reports -----------|
  |                                 |
  |  (match failed_ids to batch)    |
```

---

## Error Classification

The Kafka dispatcher classifies librdkafka error codes into permanent and retryable categories. Permanent errors skip retry and move the message directly to the failed state (and eventually the DLQ).

### Permanent Errors

| Error Code | Meaning |
|------------|---------|
| `MSG_SIZE_TOO_LARGE` | Message exceeds broker `message.max.bytes` |
| `TOPIC_AUTHORIZATION_FAILED` | Producer lacks permission for the topic |
| `CLUSTER_AUTHORIZATION_FAILED` | Producer lacks cluster-level permission |
| `UNSUPPORTED_SASL_MECHANISM` | Broker does not support the configured SASL mechanism |
| `SASL_AUTHENTICATION_FAILED` | Invalid credentials |

### Retryable Errors

All other errors are treated as retryable, including:

- Broker unavailable or not responding
- Network timeouts
- Leader not available (partition leader election in progress)
- Request timed out

Retryable errors cause the message to return to `pending` status with an incremented `retry_count` and a backoff delay calculated from the endpoint retry policy.

---

## GUC Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.kafka_delivery_timeout` | 30000 ms | 5000 -- 300000 | Maximum time to wait for broker delivery confirmation per flush cycle. |
| `ulak.kafka_poll_interval` | 100 ms | 10 -- 1000 | Interval between `rd_kafka_poll()` calls during flush. Controls how frequently delivery reports are collected. |
| `ulak.kafka_flush_timeout` | 5000 ms | 1000 -- 60000 | Maximum time for the flush phase to complete. If exceeded, unconfirmed messages are marked as failed. |
| `ulak.kafka_batch_capacity` | 64 | 16 -- 1024 | Maximum number of pending messages tracked between produce and flush phases. Determines the size of the spinlock-protected pending array. |
| `ulak.kafka_acks` | `"all"` | `0`, `1`, `-1`, `all` | Default acknowledgment level applied when not specified in the endpoint `options`. |
| `ulak.kafka_compression` | `"snappy"` | `none`, `gzip`, `snappy`, `lz4`, `zstd` | Default compression codec applied when not specified in the endpoint `options`. |

All parameters have `PGC_SIGHUP` context -- they can be changed with `ALTER SYSTEM SET` and applied with `SELECT pg_reload_conf()` without restarting PostgreSQL. Workers pick up the new values on their next SIGHUP processing cycle.

---

## See Also

- [Getting Started](Getting-Started) -- Installation with `ENABLE_KAFKA=1` and first Kafka message
- [Architecture](Architecture) -- Dispatcher factory, batch delivery model, dispatcher cache
- [Security](Security) -- Credential zeroing, TLS configuration
- [Reliability](Reliability) -- Circuit breaker, retry backoff, DLQ
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- `send()`, `send_with_options()`, `create_endpoint()`
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-MQTT](Protocol-MQTT) | [Protocol-Redis](Protocol-Redis) | [Protocol-AMQP](Protocol-AMQP) | [Protocol-NATS](Protocol-NATS)
