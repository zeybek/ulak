# ulak

**Transactional Outbox Pattern for PostgreSQL**

ulak is a PostgreSQL extension that writes messages to a transactional outbox table atomically with your business data, then delivers them reliably via background workers to HTTP, Apache Kafka, MQTT, Redis Streams, AMQP (RabbitMQ), and NATS (JetStream). Workers use `FOR UPDATE SKIP LOCKED` with modulo partitioning for zero-contention parallel processing. The extension ships with a built-in circuit breaker, exponential backoff retry, dead letter queue with redrive, role-based access control, SSRF protection, and pub/sub fan-out -- giving your application exactly-once semantics for writes and at-least-once delivery guarantees.

## Quick Example

```sql
-- Create an HTTP endpoint
SELECT ulak.create_endpoint('my-webhook', 'http',
  '{"url": "https://httpbin.org/post", "method": "POST"}'::jsonb);

-- Send a message atomically with your business transaction
BEGIN;
  INSERT INTO orders (id, total) VALUES (1, 99.99);
  SELECT ulak.send('my-webhook', '{"order_id": 1, "total": 99.99}'::jsonb);
COMMIT;
-- The message is now queued and will be delivered by a background worker
```

## Key Features

- **6 Protocols** -- HTTP (built-in), Apache Kafka, MQTT, Redis Streams, AMQP (RabbitMQ), NATS (JetStream). Each protocol is an optional compile-time module except HTTP which is always included.

- **Multi-Worker** -- 1 to 32 parallel background workers with modulo partitioning (`id % N`). Each worker processes a disjoint slice of the queue using `FOR UPDATE SKIP LOCKED`, eliminating row contention entirely.

- **Reliability** -- Per-endpoint circuit breaker (closed/open/half-open state machine), configurable retry with exponential, linear, or fixed backoff, and a dead letter queue with single-message, per-endpoint, and bulk redrive.

- **Security** -- Three RBAC roles (`ulak_admin`, `ulak_application`, `ulak_monitor`), SSRF protection that blocks private/loopback addresses by default, TLS/mTLS certificate verification, OAuth2 bearer tokens, AWS Signature V4, and webhook HMAC signing.

- **Pub/Sub** -- Event types with JSONB containment filters and multi-endpoint fan-out. Publish once, deliver to every matching subscriber.

- **Message Control** -- Priority levels 0-10 (higher first), ordering keys for per-key FIFO delivery, idempotency keys with payload hash conflict detection, TTL with automatic expiration, and scheduled delivery with future timestamps.

- **Operational** -- Backpressure via configurable `max_queue_size` (rejects new messages when the queue is full), monthly archive partitions with automatic rotation, health checks, worker status monitoring, endpoint health dashboard, and optional response body capture.

- **CloudEvents** -- Binary and structured mode support with configurable source attribute, enabling interoperability with CloudEvents-aware systems.

## Supported PostgreSQL Versions

| Version | Status |
|---------|--------|
| PostgreSQL 14 | Supported |
| PostgreSQL 15 | Supported |
| PostgreSQL 16 | Supported |
| PostgreSQL 17 | Supported |
| PostgreSQL 18 | Supported (default) |

## Documentation

### Getting Started

| Page | Description |
|------|-------------|
| [Quick Start](Getting-Started) | Prerequisites, installation, Docker setup, and your first message |

### Architecture

| Page | Description |
|------|-------------|
| [System Architecture](Architecture) | Worker model, queue design, dispatcher factory, and data flow |

### Protocols

| Page | Description |
|------|-------------|
| [HTTP](Protocol-HTTP) | Webhook delivery, batching, TLS, OAuth2, AWS SigV4, HMAC signing |
| [Apache Kafka](Protocol-Kafka) | Topic publishing, acks, compression, delivery guarantees |
| [MQTT](Protocol-MQTT) | Broker connections, QoS levels, topic publishing |
| [Redis Streams](Protocol-Redis) | Stream appends, XADD, maxlen trimming |
| [AMQP / RabbitMQ](Protocol-AMQP) | Exchange routing, publisher confirms, TLS |
| [NATS / JetStream](Protocol-NATS) | Subject publishing, JetStream acknowledgments, reconnection |

### Features

| Page | Description |
|------|-------------|
| [Pub/Sub Events](Pub-Sub) | Event types, subscriptions, JSONB filters, fan-out publishing |
| [Message Features](Message-Features) | Priority, ordering keys, idempotency, TTL, scheduled delivery |

### Operations

| Page | Description |
|------|-------------|
| [Reliability](Reliability) | Circuit breaker, retry backoff strategies, DLQ management, redrive |
| [Monitoring](Monitoring) | Health checks, worker status, endpoint health, metrics, DLQ summary |
| [Security](Security) | RBAC roles, SSRF protection, TLS configuration, authentication |

### Reference

| Page | Description |
|------|-------------|
| [Configuration (57 GUCs)](Configuration-Reference) | All `ulak.*` GUC parameters with defaults, ranges, and reload semantics |
| [SQL API (40 Functions)](SQL-API-Reference) | Complete function reference: send, publish, monitor, DLQ, archive |

### Development

| Page | Description |
|------|-------------|
| [Building & Testing](Building-and-Testing) | Build flags, regression tests, code formatting, CI pipeline |

## License

ulak is licensed under the [Apache License 2.0](https://github.com/zeybek/ulak/blob/main/LICENSE.md). You may use, modify, and distribute the project in commercial and non-commercial settings.
