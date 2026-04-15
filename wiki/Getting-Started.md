# Getting Started

This guide walks you through installing ulak, sending your first message, and trying out each supported protocol.

## Prerequisites

| Dependency | Required | Build Flag | Notes |
|------------|----------|------------|-------|
| PostgreSQL 14-18 | Yes | -- | Development headers needed (`postgresql-server-dev-*`) |
| libcurl | Yes | -- | HTTP dispatcher (always built) |
| librdkafka | Optional | `ENABLE_KAFKA=1` | Apache Kafka |
| libmosquitto | Optional | `ENABLE_MQTT=1` | MQTT |
| hiredis | Optional | `ENABLE_REDIS=1` | Redis Streams |
| librabbitmq | Optional | `ENABLE_AMQP=1` | RabbitMQ / AMQP |
| libnats (cnats) | Optional | `ENABLE_NATS=1` | NATS / JetStream |

Only PostgreSQL and libcurl are required. Each protocol library is optional and enabled at compile time with its corresponding build flag. If a protocol library is not linked, its `create_endpoint` call will return an error at runtime.

## Docker Quick Start (Recommended)

The fastest way to get a fully working environment with all protocol services:

```bash
git clone https://github.com/zeybek/ulak.git && cd ulak
docker compose up -d
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make clean && make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && make install"
docker exec ulak-postgres-1 psql -U postgres -c \
  "ALTER SYSTEM SET shared_preload_libraries = 'ulak'; ALTER SYSTEM SET ulak.database = 'ulak_test';"
docker restart ulak-postgres-1
docker exec ulak-postgres-1 psql -U postgres -d ulak_test -c "CREATE EXTENSION ulak;"
```

The docker-compose file includes PostgreSQL, Kafka, Redis, Mosquitto (MQTT), RabbitMQ (AMQP), and NATS -- everything you need to test all six protocols.

Use the `PG_MAJOR` environment variable to select a specific PostgreSQL version (default is 18):

```bash
PG_MAJOR=15 docker compose up -d
```

## Build from Source

For HTTP-only (minimal dependencies):

```bash
make && sudo make install
```

For all protocols:

```bash
make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && sudo make install
```

After building, add ulak to `postgresql.conf` and restart the server:

```
shared_preload_libraries = 'ulak'
ulak.database = 'your_database'
```

Then connect to your database and create the extension:

```sql
CREATE EXTENSION ulak;
```

## Your First Message

### Step 1: Create an HTTP Endpoint

```sql
SELECT ulak.create_endpoint(
  'my-webhook',
  'http',
  '{"url": "https://httpbin.org/post", "method": "POST"}'::jsonb
);
```

This registers an HTTP POST endpoint named `my-webhook`. The background workers will use this configuration to deliver messages.

### Step 2: Send a Message Atomically

```sql
BEGIN;
  INSERT INTO orders (id, total) VALUES (1, 99.99);
  SELECT ulak.send('my-webhook', '{"order_id": 1, "total": 99.99}'::jsonb);
COMMIT;
```

The `send()` call inserts a row into `ulak.queue` inside your transaction. If the transaction rolls back, the message is never queued. If it commits, a background worker will pick it up and deliver it.

### Step 3: Check Message Status

```sql
SELECT id, status, retry_count, last_error, completed_at
FROM ulak.queue
WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'my-webhook')
ORDER BY id DESC
LIMIT 5;
```

Messages move through these states: `pending` -> `processing` -> `completed` (or `failed` -> DLQ).

### Step 4: Monitor with Health Check

```sql
SELECT * FROM ulak.health_check();
```

This returns an overview of system health including worker status, queue depth, and endpoint circuit breaker states.

You can also inspect individual components:

```sql
-- Worker status
SELECT * FROM ulak.get_worker_status();

-- Per-endpoint health (circuit breaker state, success/failure counts)
SELECT * FROM ulak.get_endpoint_health();

-- Dead letter queue summary
SELECT * FROM ulak.dlq_summary();
```

## Protocol Quick Examples

Each protocol follows the same pattern: create an endpoint, then send messages to it.

### Apache Kafka

```sql
SELECT ulak.create_endpoint('events', 'kafka',
  '{"broker": "kafka:9092", "topic": "order-events"}'::jsonb);

SELECT ulak.send('events', '{"event": "order.created", "order_id": 42}'::jsonb);
```

### Redis Streams

```sql
SELECT ulak.create_endpoint('stream', 'redis',
  '{"host": "redis", "stream_key": "my-events"}'::jsonb);

SELECT ulak.send('stream', '{"event": "user.signup", "user_id": 7}'::jsonb);
```

### MQTT

```sql
SELECT ulak.create_endpoint('sensor', 'mqtt',
  '{"broker": "mosquitto", "topic": "sensors/temp", "qos": 1}'::jsonb);

SELECT ulak.send('sensor', '{"temperature": 22.5, "unit": "celsius"}'::jsonb);
```

### AMQP / RabbitMQ

```sql
SELECT ulak.create_endpoint('queue', 'amqp',
  '{"host": "rabbitmq", "exchange": "", "routing_key": "my-queue",
    "username": "guest", "password": "guest"}'::jsonb);

SELECT ulak.send('queue', '{"task": "send_email", "to": "user@example.com"}'::jsonb);
```

### NATS / JetStream

```sql
SELECT ulak.create_endpoint('bus', 'nats',
  '{"url": "nats://nats:4222", "subject": "orders.created"}'::jsonb);

SELECT ulak.send('bus', '{"order_id": 123, "status": "confirmed"}'::jsonb);
```

## Pub/Sub Quick Example

Pub/Sub lets you decouple publishers from specific endpoints. Create an event type, subscribe one or more endpoints, then publish -- ulak fans out the message to all subscribers.

```sql
-- Create an event type
SELECT ulak.create_event_type('order.created', 'Fired when a new order is placed');

-- Subscribe multiple endpoints to the same event
SELECT ulak.subscribe('order.created', 'my-webhook');
SELECT ulak.subscribe('order.created', 'events');  -- Kafka endpoint

-- Publish once, deliver to all subscribers
SELECT ulak.publish('order.created', '{"order_id": 123, "total": 49.99}'::jsonb);
```

The `publish()` call creates one message per active subscription. Each message is delivered independently with its own retry policy and circuit breaker state.

## Advanced Send Options

ulak supports priority, scheduling, idempotency, TTL, and ordering keys via `send_with_options()`:

```sql
SELECT ulak.send_with_options(
  'my-webhook',                                  -- endpoint name
  '{"event": "order.created"}'::jsonb,           -- payload
  5,                                              -- priority (0-10, higher = first)
  NOW() + INTERVAL '10 minutes',                 -- scheduled delivery
  'order-123-created',                           -- idempotency key
  '550e8400-e29b-41d4-a716-446655440000'::uuid,  -- correlation ID
  NOW() + INTERVAL '1 hour',                     -- TTL (expires_at)
  'order-123'                                    -- ordering key (FIFO per key)
);
```

For batch sending:

```sql
SELECT ulak.send_batch('my-webhook', ARRAY[
  '{"id": 1}'::jsonb,
  '{"id": 2}'::jsonb,
  '{"id": 3}'::jsonb
]);
```

## What's Next

- [System Architecture](Architecture) -- Understand the worker model, queue design, and dispatcher factory
- [Protocol-HTTP](Protocol-HTTP), [Protocol-Kafka](Protocol-Kafka), [Protocol-MQTT](Protocol-MQTT), [Protocol-Redis](Protocol-Redis), [Protocol-AMQP](Protocol-AMQP), [Protocol-NATS](Protocol-NATS) -- Deep dive into each protocol's configuration options
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters with defaults, ranges, and reload semantics
- [Security](Security) -- RBAC roles, SSRF protection, TLS, and authentication methods
- [Reliability](Reliability) -- Circuit breaker, retry strategies, and DLQ management
- [Monitoring](Monitoring) -- Health checks, metrics, and operational dashboards
