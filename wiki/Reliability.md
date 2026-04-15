# Reliability & Operations

ulak provides multiple reliability mechanisms that work together to ensure messages are delivered even when downstream systems are unreliable: circuit breaker, retry with configurable backoff, dead letter queue, backpressure, stale message recovery, and archive partitioning with replay.

---

## Circuit Breaker

Each endpoint has an independent circuit breaker that prevents workers from wasting resources on endpoints that are known to be down. The circuit breaker is a state machine with three states:

```
    ┌─────────────────────────────────────────┐
    │                                         │
    ▼                                         │
 CLOSED ──(failures >= threshold)──► OPEN     │
    ▲                                 │       │
    │                                 │       │
    │              (cooldown expires) │       │
    │                                 ▼       │
    └──(probe succeeds)──── HALF_OPEN ────────┘
                            (probe fails)
```

### States

| State | Behavior |
|-------|----------|
| **CLOSED** | Normal operation. Messages are dispatched to the endpoint. The failure counter increments on each dispatch failure and resets to 0 on any success. When `circuit_failure_count >= ulak.circuit_breaker_threshold`, the circuit transitions to OPEN. |
| **OPEN** | All messages for this endpoint are deferred back to `pending` status with a 10-second retry delay. No dispatch attempts are made. After the cooldown period elapses (tracked via `circuit_half_open_at = opened_at + cooldown`), the next worker to claim messages transitions the circuit to HALF_OPEN. |
| **HALF_OPEN** | A single probe message is dispatched while all other messages for the endpoint are deferred with a 5-second retry delay. If the probe succeeds, the circuit returns to CLOSED with failure count reset. If it fails, the circuit returns to OPEN with a fresh cooldown timer. |

### CAS-Style Transition

The OPEN-to-HALF_OPEN transition uses a compare-and-swap UPDATE:

```sql
UPDATE ulak.endpoints SET circuit_state = 'half_open'
WHERE id = $1 AND circuit_state = 'open';
```

Only the worker whose UPDATE affects a row wins the transition and sends the probe. All other workers see `SPI_processed == 0` and defer their messages.

### Concurrency Control

The `update_circuit_breaker()` function uses an advisory lock to serialize updates per endpoint:

```sql
PERFORM pg_advisory_xact_lock(hashtext('ulak_cb'), p_endpoint_id::int);
```

The lock is released automatically at transaction end, preventing race conditions when multiple workers concurrently update the same endpoint's circuit breaker state.

### Configuration

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.circuit_breaker_threshold` | 10 | 1 -- 1000 | Consecutive failures before circuit opens |
| `ulak.circuit_breaker_cooldown` | 30s | 5 -- 3600 | Seconds in OPEN state before attempting HALF_OPEN probe |

### Manual Reset

Force an endpoint's circuit breaker back to CLOSED regardless of current state:

```sql
SELECT ulak.reset_circuit_breaker('my-endpoint');
```

This clears the failure count, timestamps, and logs a `circuit.reset` event to `ulak.event_log`.

### Monitoring

Check circuit breaker state for all endpoints:

```sql
SELECT * FROM ulak.get_endpoint_health();
```

Circuit state is also exposed through `ulak.metrics()` as the `endpoint_circuit_state` gauge (0 = closed, 1 = half_open, 2 = open).

---

## Retry Strategies

Failed messages are retried with configurable backoff until they either succeed or exhaust their retry budget. Three backoff strategies are available, configured per-endpoint via the `retry_policy` JSONB column.

### Exponential (Default)

```
delay = base_delay * 2^retry_count, capped at max_delay
```

With default GUCs (`base_delay=10s`, `max_delay=300s`):

| Attempt | Delay |
|---------|-------|
| 0 | 10s |
| 1 | 20s |
| 2 | 40s |
| 3 | 80s |
| 4 | 160s |
| 5+ | 300s (cap) |

Overflow protection: retry counts above 10 return `max_delay` directly to prevent integer overflow from the bit shift.

### Linear

```
delay = base_delay + (retry_count * increment), capped at max_delay
```

With `base_delay=10s`, `increment=30s`, `max_delay=300s`:

| Attempt | Delay |
|---------|-------|
| 0 | 10s |
| 1 | 40s |
| 2 | 70s |
| 3 | 100s |
| 4 | 130s |
| 9+ | 300s (cap) |

### Fixed

```
delay = base_delay (constant for every retry)
```

### Per-Endpoint Configuration

```sql
-- Create endpoint with default retry policy (exponential, max_retries=10)
SELECT ulak.create_endpoint('my-api', 'http', '{
  "url": "https://api.example.com/webhook"
}'::jsonb);

-- Customize retry policy to linear with 20 retries
UPDATE ulak.endpoints SET retry_policy = '{
  "max_retries": 20,
  "backoff": "linear",
  "base_delay_seconds": 5,
  "max_delay_seconds": 600,
  "increment_seconds": 15
}'::jsonb WHERE name = 'my-api';
```

### GUC Defaults

These GUCs provide system-wide defaults. Per-endpoint `retry_policy` JSON overrides the backoff strategy, but the delay calculation still uses GUC values for `base_delay`, `max_delay`, and `increment`.

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.default_max_retries` | 10 | 0 -- 1000 | Default max retries before moving to DLQ |
| `ulak.retry_base_delay` | 10s | 1 -- 3600 | Base delay for backoff calculation |
| `ulak.retry_max_delay` | 300s | 1 -- 86400 | Maximum delay cap |
| `ulak.retry_increment` | 30s | 1 -- 3600 | Increment for linear backoff |

### HTTP Retry-After Header

When an HTTP endpoint returns a `429 Too Many Requests` or `503 Service Unavailable` response with a `Retry-After` header, the server-specified delay overrides the calculated backoff delay. The value is capped at 86400 seconds (24 hours) to prevent abuse. This applies to sync dispatch mode only; batch mode does not parse per-request `Retry-After` headers.

---

## Error Classification

Dispatch errors are classified by prefix in the `last_error` field, which determines what happens to the message:

| Prefix | Meaning | Action |
|--------|---------|--------|
| `[RETRYABLE]` | Temporary failure (network timeout, 5xx, 429) | Retry with backoff delay |
| `[PERMANENT]` | Non-recoverable error (4xx except 429/410) | Move to DLQ immediately, skip remaining retries |
| `[DISABLE]` | Endpoint gone (HTTP 410) | Move to DLQ; optionally auto-disable endpoint if `auto_disable_on_gone=true` in endpoint config |

The error classification is set by the protocol dispatcher during dispatch. Worker logic checks the prefix with `strncmp` for PERMANENT errors and `strstr` for DISABLE markers.

---

## Dead Letter Queue (DLQ)

Messages land in the DLQ (`ulak.dlq` table) when:

- `retry_count >= max_retries` (exhausted retry budget), OR
- The error is classified as `[PERMANENT]`

The DLQ preserves the complete message including payload, headers, metadata, correlation ID, and the full error history.

### DLQ Operations

| Function | Description |
|----------|-------------|
| `ulak.redrive_message(dlq_id)` | Re-queue a single DLQ message. Resets `retry_count` to 0 and clears `expires_at` and `idempotency_key` to avoid conflicts. |
| `ulak.redrive_endpoint('name')` | Re-queue all failed messages for a specific endpoint, in original creation order. |
| `ulak.redrive_all()` | Re-queue all failed messages across all endpoints. |
| `ulak.dlq_summary()` | Overview by endpoint: failed count, redriven count, time range. |
| `ulak.cleanup_dlq()` | Delete DLQ entries older than `ulak.dlq_retention_days`. |

### Audit Trail

Redriven messages are marked with `status = 'redriven'` in the DLQ rather than deleted. This preserves the audit trail. The `cleanup_dlq()` function removes entries older than the retention period (default 30 days).

```sql
-- Check DLQ health
SELECT * FROM ulak.dlq_summary();

-- Redrive a specific message
SELECT ulak.redrive_message(42);

-- Redrive all failures for an endpoint
SELECT ulak.redrive_endpoint('my-webhook');
```

### Configuration

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.dlq_retention_days` | 30 | 1 -- 3650 | Days before cleanup_dlq() deletes old entries |

---

## Backpressure

ulak enforces a configurable queue size limit to prevent unbounded memory and disk growth. The check is performed in every `send()`, `send_with_options()`, `send_batch()`, `publish()`, and `publish_batch()` call.

### How It Works

The `_check_backpressure()` function uses an `OFFSET` probe instead of `COUNT(*)` to achieve O(1) cost regardless of queue size:

```sql
PERFORM 1
FROM ulak.queue
WHERE status IN ('pending', 'processing')
OFFSET v_threshold_offset
LIMIT 1;
```

If a row exists at that offset, the queue has exceeded the limit. This avoids a full table scan.

For `publish()` and `publish_batch()`, the projected fan-out count (number of matching subscriptions) is calculated first and passed to `_check_backpressure()` so that a single publish that would create millions of fan-out messages is rejected before any inserts.

### Behavior When Exceeded

When the queue is full, the function raises:

```
SQLSTATE 53400 (configuration_limit_exceeded)
[ulak] Queue backpressure: pending/processing queue at current workload
would exceed limit ...
```

Applications should catch this error and implement client-side backoff.

### Configuration

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.max_queue_size` | 1,000,000 | 0 -- INT_MAX | Maximum pending + processing messages. Set to 0 to disable backpressure (unlimited). |

---

## Stale Message Recovery

If a worker process crashes or a database connection drops while messages are in `processing` status, those messages become "stale" -- they will never be completed because no worker is working on them.

ulak recovers stale messages in two ways:

1. **Startup recovery (one-time):** When worker 0 starts, it resets all messages that have been in `processing` status longer than `stale_recovery_timeout`. This handles the case where the entire server restarted.

2. **Continuous recovery (periodic):** Worker 0 periodically checks for stale `processing` messages and reverts them to `pending` with `next_retry_at = NOW()` for immediate re-processing. The `retry_count` is incremented and the `last_error` is set to indicate recovery.

Recovery SQL:

```sql
UPDATE ulak.queue SET status = 'pending',
  retry_count = retry_count + 1,
  last_error = 'Recovered from stale processing state',
  next_retry_at = NOW()
WHERE status = 'processing'
  AND processing_started_at < NOW() - (timeout || ' seconds')::interval;
```

### Configuration

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.stale_recovery_timeout` | 300s | 60 -- 3600 | Seconds before a `processing` message is considered stale |

---

## Archive Partitioning

Completed, failed, and expired messages are periodically moved from `ulak.queue` to `ulak.archive` by the background worker's maintenance cycle. This keeps the active queue table small for fast worker queries.

### Partition Scheme

The archive table is partitioned by `RANGE (created_at)` with monthly partitions named `archive_YYYY_MM`. A default partition (`archive_default`) acts as a safety net for inserts without a matching partition.

At extension creation, ulak pre-creates partitions for the current month plus 3 months ahead. The `maintain_archive_partitions()` function should be called periodically (via worker maintenance or `pg_cron`) to create future partitions.

### BRIN Index

The archive uses a BRIN index on `created_at` instead of a B-tree, which is approximately 48x smaller (benchmarked at 240 KB vs 11.5 MB for 500K rows across 10 partitions) with nearly identical range query performance. Configuration: `pages_per_range = 64, autosummarize = on`.

### Archive Functions

| Function | Description |
|----------|-------------|
| `ulak.archive_completed_messages(older_than_seconds, batch_size)` | Move completed/expired messages to archive. Default: older than 1 hour, 1000 per batch. |
| `ulak.maintain_archive_partitions(months_ahead)` | Pre-create monthly partitions (default 3 months ahead). |
| `ulak.cleanup_old_archive_partitions(retention_months)` | Drop partitions older than retention period. |

### Message Replay

Archived messages can be replayed back into the queue:

```sql
-- Replay a single message by archive ID
SELECT ulak.replay_message(12345);

-- Replay all messages for an endpoint within a time range
SELECT ulak.replay_range(
  p_endpoint_id := 1,
  p_from_ts := '2025-01-01'::timestamptz,
  p_to_ts := '2025-01-31'::timestamptz
);

-- Optionally filter by original status
SELECT ulak.replay_range(1, '2025-01-01', '2025-01-31', 'failed');
```

Replayed messages are inserted as new `pending` messages with `idempotency_key` and `expires_at` cleared to avoid conflicts. Replay events are logged to `ulak.event_log`.

### Configuration

| GUC | Default | Range | Description |
|-----|---------|-------|-------------|
| `ulak.archive_retention_months` | 6 | 1 -- 120 | Months before old archive partitions are dropped |

---

## Operational Summary

| Mechanism | Purpose | Key GUC |
|-----------|---------|---------|
| Circuit breaker | Stop dispatching to broken endpoints | `ulak.circuit_breaker_threshold`, `ulak.circuit_breaker_cooldown` |
| Retry with backoff | Recover from transient failures | `ulak.default_max_retries`, `ulak.retry_base_delay` |
| Dead letter queue | Preserve permanently failed messages | `ulak.dlq_retention_days` |
| Backpressure | Prevent unbounded queue growth | `ulak.max_queue_size` |
| Stale recovery | Recover crashed worker messages | `ulak.stale_recovery_timeout` |
| Archive partitioning | Keep queue table small, enable replay | `ulak.archive_retention_months` |

---

## See Also

- [Message Features](Message-Features) -- priority, ordering, idempotency, TTL
- [Pub/Sub Events](Pub-Sub) -- fan-out with backpressure projection
- [Configuration Reference](Configuration-Reference) -- complete GUC listing
- [System Architecture](Architecture) -- worker loop and dispatch pipeline
- [Security](Security) -- RBAC roles and permissions
