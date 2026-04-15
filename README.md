# ulak

[![CI](https://github.com/zeybek/ulak/actions/workflows/ci.yml/badge.svg)](https://github.com/zeybek/ulak/actions/workflows/ci.yml)
[![PostgreSQL 14-18](https://img.shields.io/badge/PostgreSQL-14--18-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE.md)
<!-- x-release-please-start-version -->
[![Version](https://img.shields.io/badge/version-0.0.0)](CHANGELOG.md)
<!-- x-release-please-end -->

**Transactional Outbox Pattern for PostgreSQL** — messages committed atomically with your business transactions, delivered reliably via background workers.

ulak writes messages to a PostgreSQL table **inside your transaction**. Background workers pick them up and deliver asynchronously with retries, circuit breaking, and dead letter queues. Your application gets **exactly-once semantics for writes** and **at-least-once delivery**.

## Features

- **6 Protocols** — [HTTP](https://github.com/zeybek/ulak/wiki/Protocol-HTTP) (built-in), [Kafka](https://github.com/zeybek/ulak/wiki/Protocol-Kafka), [MQTT](https://github.com/zeybek/ulak/wiki/Protocol-MQTT), [Redis Streams](https://github.com/zeybek/ulak/wiki/Protocol-Redis), [AMQP](https://github.com/zeybek/ulak/wiki/Protocol-AMQP), [NATS](https://github.com/zeybek/ulak/wiki/Protocol-NATS)
- **Multi-Worker** — 1–32 parallel workers with `FOR UPDATE SKIP LOCKED` and [modulo partitioning](https://github.com/zeybek/ulak/wiki/Architecture)
- **Reliability** — [Circuit breaker, exponential backoff retry, DLQ with redrive](https://github.com/zeybek/ulak/wiki/Reliability)
- **Security** — [RBAC, SSRF protection, TLS/mTLS, OAuth2, AWS SigV4, webhook HMAC signing](https://github.com/zeybek/ulak/wiki/Security)
- **Pub/Sub** — [Event types with JSONB containment filters and multi-endpoint fan-out](https://github.com/zeybek/ulak/wiki/Pub-Sub)
- **Message Control** — [Priority, ordering keys, idempotency, TTL, scheduled delivery](https://github.com/zeybek/ulak/wiki/Message-Features)
- **CloudEvents** — Binary and structured mode support
- **Operational** — [Backpressure, monthly archive partitions, health checks, response capture](https://github.com/zeybek/ulak/wiki/Monitoring)

## Quick Start

```bash
# Start PostgreSQL + all protocol services
git clone https://github.com/zeybek/ulak.git
cd ulak
docker compose up -d

# Build with all protocols and install
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make clean && make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && make install"

# Configure and restart
docker exec ulak-postgres-1 psql -U postgres -c \
  "ALTER SYSTEM SET shared_preload_libraries = 'ulak';
   ALTER SYSTEM SET ulak.database = 'ulak_test';"
docker restart ulak-postgres-1

# Create extension
docker exec ulak-postgres-1 psql -U postgres -d ulak_test -c \
  "CREATE EXTENSION ulak;"
```

Send your first message:

```sql
-- Create an HTTP endpoint
SELECT ulak.create_endpoint('my-webhook', 'http',
  '{"url": "https://httpbin.org/post", "method": "POST"}'::jsonb);

-- Send a message (inside your transaction)
BEGIN;
  INSERT INTO orders (id, total) VALUES (1, 99.99);
  SELECT ulak.send('my-webhook', '{"order_id": 1, "total": 99.99}'::jsonb);
COMMIT;
-- Message is now queued and will be delivered by a background worker
```

Other protocols work the same way:

```sql
-- Kafka
SELECT ulak.create_endpoint('events', 'kafka',
  '{"broker": "kafka:9092", "topic": "order-events"}'::jsonb);

-- Redis Streams
SELECT ulak.create_endpoint('stream', 'redis',
  '{"host": "redis", "stream_key": "my-events"}'::jsonb);

-- MQTT
SELECT ulak.create_endpoint('sensor', 'mqtt',
  '{"broker": "mosquitto", "topic": "sensors/temp", "qos": 1}'::jsonb);

-- AMQP / RabbitMQ
SELECT ulak.create_endpoint('queue', 'amqp',
  '{"host": "rabbitmq", "exchange": "", "routing_key": "my-queue",
    "username": "guest", "password": "guest"}'::jsonb);

-- NATS
SELECT ulak.create_endpoint('bus', 'nats',
  '{"url": "nats://nats:4222", "subject": "orders.created"}'::jsonb);
```

## Installation

### Prerequisites

| Dependency | Required | Build Flag |
|------------|----------|------------|
| PostgreSQL 14–18 | Yes | — |
| libcurl | Yes | — |
| librdkafka | Optional | `ENABLE_KAFKA=1` |
| libmosquitto | Optional | `ENABLE_MQTT=1` |
| hiredis | Optional | `ENABLE_REDIS=1` |
| librabbitmq | Optional | `ENABLE_AMQP=1` |
| libnats / cnats | Optional | `ENABLE_NATS=1` |

### Build from Source

```bash
# HTTP only (default)
make && make install

# With all protocols
make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && make install
```

Add to `postgresql.conf` and restart:

```
shared_preload_libraries = 'ulak'
```

```sql
CREATE EXTENSION ulak;
```

### Docker

The Dockerfile accepts a `PG_MAJOR` build argument (default: 18):

```bash
# Default (PostgreSQL 18)
docker compose up -d

# Specific version
PG_MAJOR=15 docker compose up -d
```

The compose file includes PostgreSQL, Kafka, Redis, Mosquitto (MQTT), RabbitMQ (AMQP), and NATS.

## Usage

### Sending Messages

```sql
-- Simple send
SELECT ulak.send('my-webhook', '{"event": "order.created"}'::jsonb);

-- With options (priority, idempotency, scheduling)
SELECT ulak.send_with_options(
  'my-webhook',
  '{"event": "order.created"}'::jsonb,
  5,                                    -- priority (0-10, higher = first)
  NOW() + INTERVAL '10 minutes',        -- scheduled delivery
  'order-123-created',                  -- idempotency key
  '550e8400-e29b-41d4-a716-446655440000'::uuid, -- correlation ID (UUID)
  NOW() + INTERVAL '1 hour',           -- TTL / expires at
  'order-123'                           -- ordering key (FIFO per key)
);

-- Batch send
SELECT ulak.send_batch('my-webhook', ARRAY[
  '{"id": 1}'::jsonb,
  '{"id": 2}'::jsonb,
  '{"id": 3}'::jsonb
]);
```

### Pub/Sub

```sql
-- Create event types and subscribe endpoints
SELECT ulak.create_event_type('order.created', 'New order placed');
SELECT ulak.subscribe('order.created', 'my-webhook');
SELECT ulak.subscribe('order.created', 'events');  -- fan-out

-- Publish to all subscribers
SELECT ulak.publish('order.created', '{"order_id": 123}'::jsonb);
```

### Monitoring

```sql
SELECT * FROM ulak.health_check();
SELECT * FROM ulak.get_worker_status();
SELECT * FROM ulak.get_endpoint_health();
SELECT * FROM ulak.dlq_summary();
```

### DLQ Management

```sql
-- Redrive failed messages
SELECT ulak.redrive_message(42);           -- single message
SELECT ulak.redrive_endpoint('my-webhook'); -- all for endpoint
SELECT ulak.redrive_all();                  -- everything

-- Replay from archive
SELECT ulak.replay_message(100);
SELECT ulak.replay_range(
  1,
  date_trunc('month', now()) - interval '1 month',
  date_trunc('month', now())
);
```

## Configuration

All parameters use the `ulak.` prefix. Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `workers` | 4 | Background workers (1–32) |
| `poll_interval` | 500ms | Queue polling frequency |
| `batch_size` | 200 | Messages per batch cycle |
| `max_queue_size` | 1,000,000 | Backpressure threshold |
| `circuit_breaker_threshold` | 10 | Failures before circuit opens |
| `circuit_breaker_cooldown` | 30s | Wait before half-open probe |
| `http_timeout` | 10s | HTTP request timeout |
| `dlq_retention_days` | 30 | DLQ message retention |

See the [Configuration Reference](https://github.com/zeybek/ulak/wiki/Configuration-Reference) for all 57 parameters with types, ranges, and restart/reload semantics.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Application Transaction                                  │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ BEGIN;                                                │ │
│  │   INSERT INTO orders ...                              │ │
│  │   SELECT ulak.send('webhook', '{...}');         │ │
│  │ COMMIT;                    ▲                          │ │
│  └────────────────────────────│──────────────────────────┘ │
│                               │ SPI INSERT (atomic)        │
│  ┌────────────────────────────▼──────────────────────────┐ │
│  │ ulak.queue          (pending messages)          │ │
│  └───────┬───────────┬───────────┬───────────────────────┘ │
│          │           │           │                          │
│   Worker 0    Worker 1    Worker N                          │
│   (id%N=0)    (id%N=1)    (id%N=N)                         │
│      │           │           │                              │
│      ▼           ▼           ▼                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Dispatcher Factory                                   │   │
│  │  HTTP │ Kafka │ MQTT │ Redis │ AMQP │ NATS           │   │
│  └──┬──────┬──────┬──────┬──────┬──────┬───────────────┘   │
└─────│──────│──────│──────│──────│───────────────────────────┘
      ▼      ▼      ▼      ▼      ▼      ▼
   Endpoints (HTTP APIs, Kafka topics, MQTT brokers, message buses, ...)
```

See the [Architecture](https://github.com/zeybek/ulak/wiki/Architecture) wiki page for the full technical deep-dive.

## Testing

```bash
# Regression and isolation tests
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make installcheck"

# Code quality
make tools-install  # install clang-format, cppcheck, lefthook locally
make tools-versions # print local tool versions
make format    # clang-format
make lint      # cppcheck
make hooks-install  # install local git hooks (auto-installs lefthook via Homebrew or Go when available)
make hooks-run      # run local pre-commit checks manually
```

With `lefthook` installed, the `pre-commit` hook auto-formats staged C/H files under `src/` and `include/`, re-stages them, and then runs the remaining checks.

CI uses `clang-format-22` and `clang-tidy-22`. For the closest local parity, install tools with `make tools-install` and verify with `make tools-versions`.

Conventional commit headers are enforced locally through the `commit-msg` hook:

```text
feat(scope): subject
fix: subject
chore(ci)!: subject
```

## Documentation

Full documentation is available in the **[Wiki](https://github.com/zeybek/ulak/wiki)**.

| Category | Pages |
|----------|-------|
| **Getting Started** | [Quick Start](https://github.com/zeybek/ulak/wiki/Getting-Started) |
| **Architecture** | [System Architecture](https://github.com/zeybek/ulak/wiki/Architecture) |
| **Protocols** | [HTTP](https://github.com/zeybek/ulak/wiki/Protocol-HTTP) · [Kafka](https://github.com/zeybek/ulak/wiki/Protocol-Kafka) · [MQTT](https://github.com/zeybek/ulak/wiki/Protocol-MQTT) · [Redis](https://github.com/zeybek/ulak/wiki/Protocol-Redis) · [AMQP](https://github.com/zeybek/ulak/wiki/Protocol-AMQP) · [NATS](https://github.com/zeybek/ulak/wiki/Protocol-NATS) |
| **Features** | [Pub/Sub Events](https://github.com/zeybek/ulak/wiki/Pub-Sub) · [Message Features](https://github.com/zeybek/ulak/wiki/Message-Features) |
| **Operations** | [Reliability](https://github.com/zeybek/ulak/wiki/Reliability) · [Monitoring](https://github.com/zeybek/ulak/wiki/Monitoring) · [Security](https://github.com/zeybek/ulak/wiki/Security) |
| **Reference** | [Configuration (57 GUCs)](https://github.com/zeybek/ulak/wiki/Configuration-Reference) · [SQL API (40+ Functions)](https://github.com/zeybek/ulak/wiki/SQL-API-Reference) |
| **Development** | [Building & Testing](https://github.com/zeybek/ulak/wiki/Building-and-Testing) · [Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md) |

## License

`ulak` is licensed under [Apache License 2.0](LICENSE.md).

You may use, modify, and distribute the project in commercial and
non-commercial settings under Apache 2.0.
