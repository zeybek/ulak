# Monitoring & Metrics

ulak exposes health checks, per-worker status, per-endpoint health, and Prometheus-compatible metrics via SQL functions. All monitoring data is available without external agents -- query it directly from PostgreSQL.

---

## Health Check

```sql
SELECT * FROM ulak.health_check();
```

Returns overall system health as a table:

| Column | Type | Description |
|--------|------|-------------|
| `component` | `TEXT` | Component being checked (`shared_memory`, `workers`) |
| `status` | `TEXT` | `healthy`, `degraded`, or `unhealthy` |
| `details` | `TEXT` | Human-readable explanation |
| `checked_at` | `TIMESTAMPTZ` | Timestamp of the check |

The function inspects shared memory initialization and background worker state. A `degraded` status means some workers are not running; `unhealthy` means shared memory is not available or no workers are active.

---

## Worker Status

```sql
SELECT * FROM ulak.get_worker_status();
```

Returns one row per configured worker slot from shared memory:

| Column | Type | Description |
|--------|------|-------------|
| `pid` | `INTEGER` | OS process ID of the worker (0 if slot is vacant) |
| `state` | `TEXT` | Worker state (`running`, `idle`, `stopped`, etc.) |
| `started_at` | `TIMESTAMPTZ` | When the worker was launched |
| `messages_processed` | `BIGINT` | Total messages this worker has dispatched |
| `last_activity` | `TIMESTAMPTZ` | Timestamp of the most recent dispatch attempt |
| `error_count` | `INTEGER` | Total dispatch errors for this worker |
| `last_error` | `TEXT` | Most recent error message (NULL if none) |

This function reads directly from shared memory and does not touch any tables.

---

## Endpoint Health

```sql
-- All endpoints
SELECT * FROM ulak.get_endpoint_health();

-- Specific endpoint
SELECT * FROM ulak.get_endpoint_health('my-webhook');
```

Returns per-endpoint health information by joining `ulak.endpoints` with pending message counts from `ulak.queue`:

| Column | Type | Description |
|--------|------|-------------|
| `endpoint_name` | `TEXT` | Unique endpoint name |
| `protocol` | `TEXT` | `http`, `kafka`, `mqtt`, `redis`, `amqp`, or `nats` |
| `enabled` | `BOOLEAN` | Whether the endpoint is active |
| `circuit_state` | `TEXT` | Circuit breaker state: `closed`, `open`, or `half_open` |
| `circuit_failure_count` | `INTEGER` | Consecutive dispatch failure count |
| `last_success_at` | `TIMESTAMPTZ` | Last successful dispatch |
| `last_failure_at` | `TIMESTAMPTZ` | Last failed dispatch |
| `pending_messages` | `BIGINT` | Number of messages with `status = 'pending'` |
| `oldest_pending_message` | `TIMESTAMPTZ` | `created_at` of the oldest pending message |

Pass `NULL` (the default) for all endpoints, or a specific name to filter.

---

## DLQ Summary

```sql
SELECT * FROM ulak.dlq_summary();
```

Returns one row per endpoint that has dead letter queue entries:

| Column | Type | Description |
|--------|------|-------------|
| `endpoint_name` | `TEXT` | Endpoint that failed delivery |
| `failed_count` | `BIGINT` | Messages with `status = 'failed'` |
| `redriven_count` | `BIGINT` | Messages with `status = 'redriven'` |
| `oldest_failed_at` | `TIMESTAMPTZ` | Earliest failure timestamp |
| `newest_failed_at` | `TIMESTAMPTZ` | Most recent failure timestamp |

Results are ordered by `failed_count` descending, so the noisiest endpoints appear first.

---

## Unified Metrics

```sql
SELECT * FROM ulak.metrics();
```

Returns all key metrics as key-value rows designed for Prometheus/Grafana integration via `sql_exporter` or similar:

| Column | Type | Description |
|--------|------|-------------|
| `metric_name` | `TEXT` | Metric identifier |
| `metric_value` | `DOUBLE PRECISION` | Numeric value |
| `labels` | `JSONB` | Dimensional labels (e.g. `{"status": "pending"}`) |
| `metric_type` | `TEXT` | `gauge` or `counter` |

### Available Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `queue_depth` | gauge | `status` | Message count by status (`pending`, `processing`, `completed`, `failed`, `expired`) |
| `queue_depth_by_endpoint` | gauge | `endpoint`, `status` | Message count by endpoint name and status |
| `oldest_message_age_seconds` | gauge | `status` | Age in seconds of the oldest `pending` or `processing` message |
| `dlq_depth` | gauge | `endpoint`, `status` | Dead letter queue entries by endpoint and status |
| `endpoint_circuit_state` | gauge | `endpoint` | Circuit breaker state: `0` = closed, `1` = half_open, `2` = open |
| `messages_processed_total` | counter | `worker_id` (or `{}` for aggregate) | Total messages dispatched, per-worker and aggregate |
| `errors_total` | counter | `worker_id` (or `{}` for aggregate) | Total dispatch errors, per-worker and aggregate |
| `spawns_total` | counter | `{}` | Total worker spawn count |
| `spawn_failures_total` | counter | `{}` | Total worker spawn failure count |
| `restarts_total` | counter | `{}` | Total worker restart count |

The SQL-sourced metrics (`queue_depth`, `queue_depth_by_endpoint`, `oldest_message_age_seconds`, `dlq_depth`, `endpoint_circuit_state`) are computed from table queries. The shared memory metrics (`messages_processed_total`, `errors_total`, `spawns_total`, `spawn_failures_total`, `restarts_total`) are read from the internal `_shmem_metrics()` C function.

---

## Grafana Dashboard Queries

These SQL queries can be used directly as data sources in Grafana panels via a PostgreSQL data source or `sql_exporter`.

### Queue Depth Over Time

```sql
SELECT metric_value, labels->>'status' AS status
FROM ulak.metrics()
WHERE metric_name = 'queue_depth';
```

### Queue Depth by Endpoint

```sql
SELECT metric_value,
       labels->>'endpoint' AS endpoint,
       labels->>'status' AS status
FROM ulak.metrics()
WHERE metric_name = 'queue_depth_by_endpoint';
```

### Circuit Breaker States

```sql
SELECT endpoint_name, circuit_state, circuit_failure_count
FROM ulak.get_endpoint_health();
```

### Worker Performance

```sql
SELECT pid, messages_processed, error_count, last_activity
FROM ulak.get_worker_status();
```

### DLQ Alerts

```sql
SELECT endpoint_name, failed_count
FROM ulak.dlq_summary()
WHERE failed_count > 0;
```

### Message Processing Rate (Shared Memory Counters)

```sql
SELECT metric_value, labels->>'worker_id' AS worker_id
FROM ulak.metrics()
WHERE metric_name = 'messages_processed_total'
  AND labels != '{}'::jsonb;
```

---

## Alerting Recommendations

| Alert | Query / Metric | Threshold | Severity |
|-------|---------------|-----------|----------|
| Queue depth high | `queue_depth` WHERE `status = 'pending'` | > 10000 | Warning |
| Circuit breaker open | `endpoint_circuit_state` | = 2 | Critical |
| Worker down | `get_worker_status()` | `state != 'running'` | Critical |
| DLQ growing | `dlq_summary()` `failed_count` | > 100 | Warning |
| Message age high | `oldest_message_age_seconds` | > 300 seconds | Warning |
| Error rate spike | `errors_total` per worker | Sudden increase | Warning |
| Spawn failures | `spawn_failures_total` | > 0 | Warning |
| All workers idle | `get_worker_status()` | All `last_activity` stale | Warning |

---

## Role Access

Each monitoring function is granted to specific RBAC roles. The table below shows which roles can call each function:

| Function | `ulak_admin` | `ulak_application` | `ulak_monitor` |
|----------|:---:|:---:|:---:|
| `health_check()` | yes | no | yes |
| `get_worker_status()` | yes | yes | yes |
| `get_endpoint_health()` | no | no | yes |
| `dlq_summary()` | yes | no | yes |
| `_shmem_metrics()` | yes | yes | yes |
| `metrics()` | yes | yes | yes |

Assign the `ulak_monitor` role to your monitoring user for full read-only access to all monitoring functions:

```sql
GRANT ulak_monitor TO grafana_user;
```

---

## See Also

- [Security](Security) -- RBAC role definitions and permission model
- [Configuration Reference](Configuration-Reference) -- GUC parameters for worker count, circuit breaker thresholds, and queue limits
- [Architecture](Architecture) -- Shared memory layout and worker lifecycle
- [SQL API Reference](SQL-API-Reference) -- Complete function signatures and examples
- [Reliability](Reliability) -- Circuit breaker, retry backoff, and DLQ management
