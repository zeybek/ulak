# SQL API Reference

All functions reside in the `ulak` schema. Access is controlled via RBAC roles:

| Abbreviation | Role | Purpose |
|:---:|------|---------|
| **A** | `ulak_admin` | Full administrative access |
| **P** | `ulak_application` | Send messages and view worker status |
| **M** | `ulak_monitor` | Read-only monitoring access |

---

## Core Sending Functions

### ulak.send

Enqueue a single message for delivery to an endpoint.

```sql
ulak.send(endpoint_name text, payload jsonb) → boolean
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `endpoint_name` | `text` | Name of the target endpoint |
| `payload` | `jsonb` | Message payload |

**Implementation:** SECURITY DEFINER, C function
**Roles:** A, P

```sql
SELECT ulak.send('my-webhook', '{"event": "user.created", "user_id": 42}'::jsonb);
```

---

### ulak.send_with_options

Enqueue a message with advanced delivery options such as priority, scheduling, idempotency, and ordering.

```sql
ulak.send_with_options(
    p_endpoint_name   text,
    p_payload         jsonb,
    p_priority        smallint    DEFAULT 0,
    p_scheduled_at    timestamptz DEFAULT NULL,
    p_idempotency_key text        DEFAULT NULL,
    p_correlation_id  uuid        DEFAULT NULL,
    p_expires_at      timestamptz DEFAULT NULL,
    p_ordering_key    text        DEFAULT NULL
) → bigint
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_endpoint_name` | `text` | -- | Target endpoint name |
| `p_payload` | `jsonb` | -- | Message payload |
| `p_priority` | `smallint` | `0` | Higher values are processed first |
| `p_scheduled_at` | `timestamptz` | `NULL` | Defer delivery until this time |
| `p_idempotency_key` | `text` | `NULL` | Prevents duplicate sends within a window |
| `p_correlation_id` | `uuid` | `NULL` | Correlation identifier for tracing |
| `p_expires_at` | `timestamptz` | `NULL` | Message expiration time |
| `p_ordering_key` | `text` | `NULL` | Ensures FIFO delivery within key group |

**Returns:** Message ID (`bigint`)
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A, P

```sql
SELECT ulak.send_with_options(
    'order-service',
    '{"order_id": 1001}'::jsonb,
    p_priority := 5,
    p_scheduled_at := now() + interval '10 minutes',
    p_idempotency_key := 'order-1001-confirm',
    p_ordering_key := 'customer-42'
);
```

---

### ulak.send_batch

Enqueue multiple messages to the same endpoint in a single call.

```sql
ulak.send_batch(endpoint_name text, payloads jsonb[]) → bigint[]
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `endpoint_name` | `text` | Target endpoint name |
| `payloads` | `jsonb[]` | Array of message payloads |

**Returns:** Array of message IDs
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A, P

```sql
SELECT ulak.send_batch(
    'analytics',
    ARRAY[
        '{"event": "page_view", "page": "/home"}'::jsonb,
        '{"event": "page_view", "page": "/about"}'::jsonb
    ]
);
```

---

### ulak.send_batch_with_priority

Enqueue multiple messages with a shared priority level.

```sql
ulak.send_batch_with_priority(endpoint_name text, payloads jsonb[], priority smallint DEFAULT 0) → bigint[]
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpoint_name` | `text` | -- | Target endpoint name |
| `payloads` | `jsonb[]` | -- | Array of message payloads |
| `priority` | `smallint` | `0` | Priority level for all messages in the batch |

**Returns:** Array of message IDs
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A, P

```sql
SELECT ulak.send_batch_with_priority(
    'alerts',
    ARRAY['{"level": "critical"}'::jsonb, '{"level": "critical"}'::jsonb],
    10
);
```

---

## Endpoint Management

### ulak.create_endpoint

Create a new endpoint configuration.

```sql
ulak.create_endpoint(name text, protocol text, config jsonb) → bigint
```

**Returns:** Endpoint ID
**Implementation:** C function
**Roles:** A

```sql
SELECT ulak.create_endpoint(
    'payment-webhook',
    'http',
    '{
        "url": "https://api.example.com/webhooks/payment",
        "method": "POST",
        "headers": {"Authorization": "Bearer token123"}
    }'::jsonb
);
```

---

### ulak.drop_endpoint

Remove an endpoint and cancel its pending messages.

```sql
ulak.drop_endpoint(endpoint_name text) → boolean
```

**Implementation:** C function
**Roles:** A

```sql
SELECT ulak.drop_endpoint('old-webhook');
```

---

### ulak.alter_endpoint

Update the configuration of an existing endpoint.

```sql
ulak.alter_endpoint(endpoint_name text, new_config jsonb) → boolean
```

**Implementation:** C function
**Roles:** A

```sql
SELECT ulak.alter_endpoint(
    'payment-webhook',
    '{"url": "https://api-v2.example.com/webhooks/payment"}'::jsonb
);
```

---

### ulak.validate_endpoint_config

Validate an endpoint configuration without creating it. Useful for dry-run checks.

```sql
ulak.validate_endpoint_config(protocol text, config jsonb) → boolean
```

**Implementation:** C function
**Roles:** A

```sql
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "https://example.com/hook", "method": "POST"}'::jsonb
);
```

---

### ulak.enable_endpoint

Re-enable a previously disabled endpoint.

```sql
ulak.enable_endpoint(p_endpoint_name text) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.enable_endpoint('payment-webhook');
```

---

### ulak.disable_endpoint

Disable an endpoint. Messages will queue but not be dispatched.

```sql
ulak.disable_endpoint(p_endpoint_name text) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.disable_endpoint('payment-webhook');
```

---

## Pub/Sub

### ulak.create_event_type

Define a new event type with an optional JSON Schema for payload validation.

```sql
ulak.create_event_type(p_name text, p_description text, p_schema jsonb) → bigint
```

**Returns:** Event type ID
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.create_event_type(
    'user.created',
    'Fired when a new user registers',
    '{"type": "object", "required": ["user_id"]}'::jsonb
);
```

---

### ulak.drop_event_type

Remove an event type and all its subscriptions.

```sql
ulak.drop_event_type(p_name text) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.drop_event_type('user.created');
```

---

### ulak.subscribe

Subscribe an endpoint to an event type with an optional filter.

```sql
ulak.subscribe(p_event_type text, p_endpoint_name text, p_filter jsonb) → bigint
```

**Returns:** Subscription ID
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.subscribe(
    'order.completed',
    'analytics-webhook',
    '{"region": "eu"}'::jsonb
);
```

---

### ulak.unsubscribe

Remove a subscription by its ID.

```sql
ulak.unsubscribe(p_subscription_id bigint) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.unsubscribe(42);
```

---

### ulak.publish

Publish an event to all matching subscribers.

```sql
ulak.publish(p_event_type text, p_payload jsonb) → integer
```

**Returns:** Number of messages enqueued
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A, P

```sql
SELECT ulak.publish('user.created', '{"user_id": 42, "email": "user@example.com"}'::jsonb);
```

---

### ulak.publish_batch

Publish multiple events in a single call.

```sql
ulak.publish_batch(p_events jsonb) → integer
```

The `p_events` parameter is a JSON array of objects, each with `event_type` and `payload` keys.

**Returns:** Total number of messages enqueued
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A, P

```sql
SELECT ulak.publish_batch('[
    {"event_type": "order.created",   "payload": {"order_id": 1}},
    {"event_type": "order.completed", "payload": {"order_id": 2}}
]'::jsonb);
```

---

## Circuit Breaker

### ulak.reset_circuit_breaker

Manually reset a tripped circuit breaker to the closed state.

```sql
ulak.reset_circuit_breaker(p_endpoint_name text) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.reset_circuit_breaker('payment-webhook');
```

---

### ulak.update_circuit_breaker

Internal function to update the circuit breaker state after a dispatch attempt. Not intended for direct use.

```sql
ulak.update_circuit_breaker(p_endpoint_id bigint, p_success boolean) → void
```

**Implementation:** PL/pgSQL
**Roles:** A (internal)

---

## DLQ Management

### ulak.redrive_message

Re-enqueue a single message from the dead letter queue back to the main queue.

```sql
ulak.redrive_message(p_dlq_id bigint) → bigint
```

**Returns:** New message ID in the queue
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.redrive_message(101);
```

---

### ulak.redrive_endpoint

Re-enqueue all failed messages for a specific endpoint.

```sql
ulak.redrive_endpoint(p_endpoint_name text) → integer
```

**Returns:** Number of messages redriven
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.redrive_endpoint('payment-webhook');
```

---

### ulak.redrive_all

Re-enqueue all messages in the dead letter queue.

```sql
ulak.redrive_all() → integer
```

**Returns:** Number of messages redriven
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.redrive_all();
```

---

### ulak.cleanup_dlq

Delete expired messages from the dead letter queue based on `ulak.dlq_retention_days`.

```sql
ulak.cleanup_dlq() → integer
```

**Returns:** Number of messages deleted
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.cleanup_dlq();
```

---

### ulak.dlq_summary

Get a summary of dead letter queue contents grouped by endpoint.

```sql
ulak.dlq_summary() → TABLE(
    endpoint_name    text,
    failed_count     bigint,
    redriven_count   bigint,
    oldest_failed_at timestamptz,
    newest_failed_at timestamptz
)
```

**Implementation:** PL/pgSQL
**Roles:** A, M

```sql
SELECT * FROM ulak.dlq_summary();
```

---

## Archive & Replay

### ulak.archive_completed_messages

Move completed messages from the queue to the monthly-partitioned archive.

```sql
ulak.archive_completed_messages(
    p_older_than_seconds int DEFAULT 3600,
    p_batch_size         int DEFAULT 1000
) → integer
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_older_than_seconds` | `int` | `3600` | Only archive messages older than this many seconds |
| `p_batch_size` | `int` | `1000` | Maximum messages to archive per call |

**Returns:** Number of messages archived
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.archive_completed_messages(7200, 5000);
```

---

### ulak.replay_message

Re-enqueue a single message from the archive back to the queue.

```sql
ulak.replay_message(p_archive_message_id bigint) → bigint
```

**Returns:** New message ID
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.replay_message(50001);
```

---

### ulak.replay_range

Replay all archived messages for an endpoint within a time range, optionally filtered by status.

```sql
ulak.replay_range(
    p_endpoint_id bigint,
    p_from_ts     timestamptz,
    p_to_ts       timestamptz,
    p_status      text DEFAULT NULL
) → integer
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_endpoint_id` | `bigint` | -- | Target endpoint ID |
| `p_from_ts` | `timestamptz` | -- | Start of replay window |
| `p_to_ts` | `timestamptz` | -- | End of replay window |
| `p_status` | `text` | `NULL` | Filter by status (e.g., `'delivered'`, `'failed'`); NULL replays all |

**Returns:** Number of messages replayed
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.replay_range(
    1,
    '2026-04-01 00:00:00+00',
    '2026-04-14 00:00:00+00',
    'failed'
);
```

---

### ulak.maintain_archive_partitions

Create future monthly partitions ahead of time to prevent insertion failures.

```sql
ulak.maintain_archive_partitions(p_months_ahead int DEFAULT 3) → integer
```

**Returns:** Number of partitions created
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A

```sql
SELECT ulak.maintain_archive_partitions(6);
```

---

### ulak.cleanup_old_archive_partitions

Drop archive partitions older than the retention period.

```sql
ulak.cleanup_old_archive_partitions(p_retention_months int DEFAULT 6) → integer
```

**Returns:** Number of partitions dropped
**Implementation:** SECURITY DEFINER, PL/pgSQL
**Roles:** A

```sql
SELECT ulak.cleanup_old_archive_partitions(12);
```

---

## Monitoring

### ulak.health_check

Run a comprehensive system health check across all components.

```sql
ulak.health_check() → TABLE(
    component  text,
    status     text,
    details    jsonb,
    checked_at timestamptz
)
```

**Implementation:** C function
**Roles:** A, M

```sql
SELECT * FROM ulak.health_check();
```

---

### ulak.get_worker_status

Get the current status of all background worker processes.

```sql
ulak.get_worker_status() → TABLE(
    pid                int,
    state              text,
    started_at         timestamptz,
    messages_processed bigint,
    last_activity      timestamptz,
    error_count        bigint,
    last_error         text
)
```

**Implementation:** C function
**Roles:** A, P, M

```sql
SELECT * FROM ulak.get_worker_status();
```

---

### ulak.get_endpoint_health

Get health and status information for one or all endpoints.

```sql
ulak.get_endpoint_health(p_endpoint_name text DEFAULT NULL) → TABLE(
    endpoint_name          text,
    protocol               text,
    enabled                boolean,
    circuit_state          text,
    circuit_failure_count  int,
    last_success_at        timestamptz,
    last_failure_at        timestamptz,
    pending_messages       bigint,
    oldest_pending_message timestamptz
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_endpoint_name` | `text` | `NULL` | Endpoint name; NULL returns all endpoints |

**Implementation:** PL/pgSQL
**Roles:** M

```sql
-- All endpoints
SELECT * FROM ulak.get_endpoint_health();

-- Single endpoint
SELECT * FROM ulak.get_endpoint_health('payment-webhook');
```

---

### ulak.metrics

Return Prometheus-compatible metrics for the extension.

```sql
ulak.metrics() → TABLE(
    metric_name  text,
    metric_value double precision,
    labels       jsonb,
    metric_type  text
)
```

**Implementation:** PL/pgSQL
**Roles:** A, P, M

```sql
SELECT * FROM ulak.metrics();
```

---

### ulak._shmem_metrics

Return metrics from shared memory counters. Lower-level than `ulak.metrics()` and includes per-worker statistics.

```sql
ulak._shmem_metrics() → TABLE(
    metric_name  text,
    metric_value double precision,
    labels       jsonb,
    metric_type  text
)
```

**Implementation:** C function
**Roles:** A, P, M

```sql
SELECT * FROM ulak._shmem_metrics();
```

---

## Lifecycle & Utility

### ulak.mark_expired_messages

Scan the queue for messages past their `expires_at` timestamp and move them to the DLQ.

```sql
ulak.mark_expired_messages() → integer
```

**Returns:** Number of messages expired
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.mark_expired_messages();
```

---

### ulak.archive_single_to_dlq

Move a specific message from the queue to the dead letter queue.

```sql
ulak.archive_single_to_dlq(p_message_id bigint) → boolean
```

**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.archive_single_to_dlq(12345);
```

---

### ulak.cleanup_event_log

Delete event log entries older than `ulak.event_log_retention_days`.

```sql
ulak.cleanup_event_log() → integer
```

**Returns:** Number of entries deleted
**Implementation:** PL/pgSQL
**Roles:** A

```sql
SELECT ulak.cleanup_event_log();
```

---

### ulak._check_backpressure

Internal function that raises an error if the queue size exceeds `ulak.max_queue_size`. Called automatically by send functions.

```sql
ulak._check_backpressure(p_projected_additional bigint DEFAULT 0) → void
```

**Implementation:** PL/pgSQL
**Roles:** A (internal)

---

### ulak.enable_fast_mode

Set `synchronous_commit = off` for the current session to improve throughput at the cost of durability guarantees.

```sql
ulak.enable_fast_mode() → void
```

**Implementation:** PL/pgSQL
**Roles:** Any

```sql
SELECT ulak.enable_fast_mode();
-- Subsequent sends in this session use async commit
SELECT ulak.send('my-webhook', '{"fast": true}'::jsonb);
```

---

## Tables Reference

| Table | Columns | Purpose |
|-------|:-------:|---------|
| `ulak.endpoints` | 15 | Endpoint configurations including circuit breaker state |
| `ulak.queue` | 22 | Transactional message queue (pending and in-flight) |
| `ulak.dlq` | 22 | Dead letter queue for failed messages |
| `ulak.archive` | 23 | Monthly-partitioned archive of completed messages |
| `ulak.event_log` | 8 | System event audit log |
| `ulak.event_types` | 5 | Pub/sub event type definitions |
| `ulak.subscriptions` | 7 | Event type to endpoint subscription mappings |

---

## See Also

- [Getting Started](Getting-Started) -- Installation and first steps
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [Architecture](Architecture) -- Internal design and worker model
- [Security](Security) -- RBAC roles and access control
- [Protocol: HTTP](Protocol-HTTP) | [Kafka](Protocol-Kafka) | [MQTT](Protocol-MQTT) | [Redis](Protocol-Redis) | [AMQP](Protocol-AMQP) | [NATS](Protocol-NATS)
