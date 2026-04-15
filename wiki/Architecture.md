# System Architecture

ulak is a PostgreSQL extension that turns the database into a message dispatch engine. Application code inserts rows into `ulak.queue` via SQL; background workers dequeue messages, route them through a protocol-agnostic dispatcher layer, and deliver to external endpoints over HTTP, Kafka, MQTT, Redis Streams, AMQP, or NATS. All state lives in PostgreSQL -- no external broker or coordinator is needed.

---

## High-Level Overview

```
                          PostgreSQL Backend
 +---------------------------------------------------------------+
 |                                                                |
 |  Application Transaction                                       |
 |  +--------------------------+                                  |
 |  | INSERT INTO ulak.queue   |                                  |
 |  | (endpoint_id, payload)   |--+                               |
 |  +--------------------------+  |                               |
 |                                |  NOTIFY ulak_new_msg          |
 |                                v                               |
 |  ulak.queue table                                              |
 |  +----------------------------------------------------------+  |
 |  | id | endpoint_id | payload | status  | priority | ...    |  |
 |  |  1 |          10 | {...}   | pending |        5 |        |  |
 |  |  2 |          10 | {...}   | pending |        3 |        |  |
 |  |  3 |          20 | {...}   | pending |        5 |        |  |
 |  +----------------------------------------------------------+  |
 |         |              |              |                         |
 |         | id % 3 = 0   | id % 3 = 1   | id % 3 = 2            |
 |         v              v              v                        |
 |  +-----------+  +-----------+  +-----------+                   |
 |  | Worker 0  |  | Worker 1  |  | Worker 2  |   (BGWorkers)    |
 |  +-----------+  +-----------+  +-----------+                   |
 |         |              |              |                         |
 |         +-------+------+------+-------+                        |
 |                 |             |                                 |
 |                 v             v                                 |
 |       Dispatcher Factory (vtable dispatch)                     |
 |       +---------------------------------------------+          |
 |       | dispatcher_create(protocol, config)         |          |
 |       |   +------+-------+------+-------+-------+   |          |
 |       |   | HTTP | Kafka | MQTT | Redis | AMQP  |   |          |
 |       |   |      |       |      |Streams|       |   |          |
 |       |   +------+-------+------+-------+-------+   |          |
 |       |   | NATS |                               |   |          |
 |       |   +------+                               |   |          |
 |       +---------------------------------------------+          |
 |                 |             |                                 |
 +---------------------------------------------------------------+
                   |             |
                   v             v
           +-----------+  +-----------+
           | Webhook   |  | Kafka     |   External Endpoints
           | Endpoint  |  | Cluster   |
           +-----------+  +-----------+
```

---

## Background Worker Lifecycle

Each worker is registered at `_PG_init()` time via `RegisterBackgroundWorker()`. The entry point `ulak_worker_main()` receives a packed 32-bit argument encoding both identity and fleet size: `(total_workers << 16) | worker_id`.

### Startup Sequence

```
_PG_init()
  |
  +-- ulak_shmem_init()              Hook into shmem_request + shmem_startup
  +-- ulak_register_worker()         Register N static BGWorkers
        |
        +-- for i in 0..N-1:
              bgw_main_arg = (N << 16) | i
              RegisterBackgroundWorker(&worker)

ulak_worker_main(Datum main_arg)
  |
  +-- Unpack: total_workers = (arg >> 16), worker_id = (arg & 0xFFFF)
  +-- ulak_worker_init_libs()        curl_global_init, mosquitto_lib_init, rd_kafka_*
  +-- BackgroundWorkerInitializeConnection(ulak.database, NULL, 0)
  +-- ulak_register_database()       Register in shmem registry
  +-- ulak_set_target_workers()      Write total_workers to shmem
  +-- ulak_add_worker_pid()          Claim slot, increment generation counter
  +-- pqsignal(SIGTERM/SIGHUP)      Install signal handlers
  +-- BackgroundWorkerUnblockSignals()
  +-- ulak_set_worker_started()
  +-- ulak_worker_loop()             >>> Enter main loop <<<
  +-- ulak_remove_worker_pid()       Release shmem slot
  +-- ulak_clear_worker()
  +-- ulak_worker_cleanup_libs()     curl_global_cleanup, mosquitto_lib_cleanup
  +-- proc_exit(0)
```

### Main Loop (`ulak_worker_loop`)

```
dispatcher_cache_init()

while (!got_sigterm):
    |
    +-- Check SIGHUP --> ProcessConfigFile, flush dispatcher cache
    |
    +-- WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
    |             poll_interval)
    |
    +-- Check WL_POSTMASTER_DEATH --> proc_exit(1)
    |
    +-- Extension check (pg_cron pattern):
    |     get_extension_oid("ulak", missing_ok=true)
    |     Skip iteration if not installed
    |
    +-- One-time: Async_Listen("ulak_new_msg")
    +-- One-time: Recover stale processing messages (worker_id=0 only)
    |
    +-- PG_TRY:
    |     +-- Worker 0 maintenance:
    |     |     mark_expired_messages()            every iteration
    |     |     archive_completed_messages()        every 10 iterations
    |     |     run_periodic_maintenance()          every 60 iterations
    |     |       (DLQ cleanup, event_log cleanup)
    |     |
    |     +-- process_pending_messages_batch()      all workers
    |     +-- ulak_update_worker_activity()
    |
    +-- PG_CATCH:
          AbortCurrentTransaction()
          MemoryContextDelete(worker_batch_context)   clean orphaned batch
          dispatcher_cache_destroy() + reinit          purge corrupted state
          Backoff: 1s normal, 30s after 10 consecutive errors

dispatcher_cache_destroy()
```

Worker 0 is the designated maintenance worker. It handles expiry marking, archival, and cleanup on staggered counters to avoid spending every iteration on housekeeping. All workers -- including worker 0 -- process messages every iteration.

---

## Message Processing Pipeline

The core of ulak is `process_pending_messages_batch()`. Each call executes within a single transaction: dequeue, dispatch, and status update are atomic.

### Step-by-Step

**1. Transaction setup**

```sql
SET LOCAL synchronous_commit = off;    -- 2-3x faster, safe for at-least-once
```

The isolation level is READ COMMITTED (the PostgreSQL default). REPEATABLE READ was explicitly avoided because it causes `SQLSTATE 40001` serialization failures when multiple workers compete for `FOR UPDATE SKIP LOCKED` on the same rows.

**2. Dequeue (SELECT ... FOR UPDATE SKIP LOCKED)**

```sql
SELECT q.id, q.endpoint_id, q.payload, q.retry_count,
       e.protocol, e.config, e.retry_policy,
       q.priority, q.scheduled_at, q.expires_at, q.correlation_id,
       e.enabled, e.circuit_failure_count, e.circuit_state,
       e.circuit_opened_at, e.circuit_half_open_at,
       q.headers, q.metadata
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE q.status = 'pending'
  AND e.enabled = true
  AND (q.next_retry_at IS NULL OR q.next_retry_at <= NOW())
  AND (q.scheduled_at IS NULL OR q.scheduled_at <= NOW())
  AND (q.expires_at IS NULL OR q.expires_at > NOW())
  AND (q.ordering_key IS NULL
       OR (NOT EXISTS (SELECT 1 FROM ulak.queue q2
                       WHERE q2.ordering_key = q.ordering_key
                         AND q2.status = 'processing')
           AND NOT EXISTS (SELECT 1 FROM ulak.queue q2
                       WHERE q2.ordering_key = q.ordering_key
                         AND q2.status = 'pending'
                         AND q2.id < q.id)))
  AND (q.id % total_workers) = worker_id       -- modulo partition (multi-worker only)
ORDER BY q.priority DESC, q.endpoint_id, q.created_at ASC
LIMIT batch_size
FOR UPDATE OF q SKIP LOCKED
```

Key points:
- The `ordering_key` constraint guarantees FIFO within a key: no sibling is in `processing`, and no earlier message is still `pending`.
- The modulo clause `(q.id % N) = W` assigns disjoint ID ranges to each worker. Single-worker mode omits it entirely.
- `SKIP LOCKED` provides a second layer of safety: even without modulo partitioning, workers never block on each other.

**3. Copy to batch_context**

All SPI tuple data is deep-copied (payload, config, headers, metadata Jsonb) into a dedicated `batch_context` MemoryContext, child of `TopMemoryContext`. This decouples message data from SPI memory lifecycle.

**4. Batch mark as processing**

```sql
UPDATE ulak.queue SET status = 'processing',
       processing_started_at = NOW()
WHERE id = ANY($1::bigint[]);
```

A single batch UPDATE atomically transitions all dequeued messages.

**5. Group by endpoint**

Messages arrive pre-sorted by `endpoint_id` from the ORDER BY clause. The worker scans linearly to identify contiguous endpoint groups.

**6. Per-endpoint dispatch**

For each endpoint group:

```
Circuit breaker check
  |
  +-- open? --> revert to pending with retry delay, skip dispatch
  |               (cooldown elapsed? --> CAS transition to half_open,
  |                send one probe message, defer the rest)
  |
  +-- closed/half_open? --> continue
        |
        +-- get_or_create_dispatcher(endpoint_id, protocol, config)
        |     (hash table lookup, config invalidation via hash_any)
        |
        +-- rate_limit_acquire() per message
        |     (token bucket in shared memory)
        |
        +-- Batch mode? (supports_batch && !capture_response)
        |     |
        |     +-- Phase 1: produce() or produce_ex() all messages (non-blocking)
        |     +-- Phase 2: flush(timeout_ms) --> wait for delivery confirmations
        |     +-- Phase 3: Match failed_ids to message batch
        |
        +-- Sync mode?
              |
              +-- Per message: dispatch_ex() or dispatch() --> immediate result
```

**7. Status update (batch UPDATEs)**

Messages are categorized into four groups, each handled by a single batch UPDATE:

| Outcome | Status | Action |
|---------|--------|--------|
| Success | `completed` | Batch UPDATE + optional response JSONB |
| Retryable error | `pending` | Increment retry_count, set next_retry_at with backoff |
| Permanent error / max retries | `failed` | Batch UPDATE + DLQ archive |
| Rate limited | `pending` | Revert to pending, no retry_count increment |

**8. Circuit breaker update**

One `ulak.update_circuit_breaker(endpoint_id, success)` call per endpoint (not per message), using the last result in the group.

**9. Stats flush**

After `CommitTransactionCommand()`, accumulated batch statistics are flushed to shared memory via `worker_flush_stats_to_shmem()`. Stats are never flushed before commit to avoid phantom metrics from rolled-back transactions.

**10. Cleanup**

`MemoryContextDelete(batch_context)` frees all batch allocations in one operation.

---

## Dispatcher Architecture

### Factory Pattern

`dispatcher_create(ProtocolType, Jsonb *config)` switches on the protocol enum and delegates to a protocol-specific constructor:

```
dispatcher_create(protocol, config)
    |
    +-- PROTOCOL_TYPE_HTTP  --> http_dispatcher_create(config)
    +-- PROTOCOL_TYPE_KAFKA --> kafka_dispatcher_create(config)
    +-- PROTOCOL_TYPE_MQTT  --> mqtt_dispatcher_create(config)
    +-- PROTOCOL_TYPE_REDIS --> redis_dispatcher_create(config)
    +-- PROTOCOL_TYPE_AMQP  --> amqp_dispatcher_create(config)
    +-- PROTOCOL_TYPE_NATS  --> nats_dispatcher_create(config)
```

Protocols are compiled in conditionally via `ENABLE_KAFKA`, `ENABLE_MQTT`, etc. HTTP is always available.

### Vtable (DispatcherOperations)

Every dispatcher carries a pointer to a `DispatcherOperations` struct -- a C vtable:

```c
struct DispatcherOperations {
    /* Core (required) */
    bool (*dispatch)(self, payload, &error_msg);        // sync single message
    bool (*validate_config)(config);                    // config validation
    void (*cleanup)(self);                              // free resources

    /* Batch (optional -- NULL if unsupported) */
    bool (*produce)(self, payload, msg_id, &error_msg); // enqueue without waiting
    int  (*flush)(self, timeout_ms, &failed_ids,        // flush pending,
                  &failed_count, &failed_errors);       //   return success count
    bool (*supports_batch)(self);                       // capability check

    /* Extended (optional -- falls back to core) */
    bool (*dispatch_ex)(self, payload, headers,          // dispatch with headers,
                        metadata, &result);              //   metadata, response capture
    bool (*produce_ex)(self, payload, msg_id, headers,   // batch produce with
                       metadata, &error_msg);            //   per-message headers
};
```

### Dispatcher Base Struct

```c
struct Dispatcher {
    ProtocolType       protocol;      // enum: HTTP, Kafka, MQTT, ...
    Jsonb             *config;        // original endpoint JSONB config
    DispatcherOperations *ops;        // vtable
    void              *private_data;  // protocol-specific opaque state
};
```

Protocol implementations embed this as their first field and store connection handles (curl_easy, rd_kafka_t, mosquitto, etc.) in `private_data`.

### DispatchResult

Every dispatch operation can produce a `DispatchResult` containing common fields plus protocol-specific response data:

```
DispatchResult
  +-- success, error_msg, response_time_ms       (common)
  +-- http_status_code, http_response_body, ...   (HTTP)
  +-- kafka_partition, kafka_offset, ...           (Kafka)
  +-- mqtt_mid                                     (MQTT)
  +-- redis_stream_id                              (Redis)
  +-- amqp_delivery_tag, amqp_confirmed            (AMQP)
  +-- nats_js_sequence, nats_js_stream, ...        (NATS)
```

`dispatch_result_to_jsonb()` converts this to a JSONB value for the `queue.response` column, including only the fields relevant to the protocol used.

### Dispatcher Cache

Workers maintain a per-process hash table that caches live dispatchers across batch cycles:

```
dispatcher_cache (HTAB, in DispatcherCacheContext)
  key: endpoint_id (int64)
  val: DispatcherCacheEntry
         +-- dispatcher     Dispatcher*   (preserves TCP/TLS connections)
         +-- config_hash    uint32        (hash_any on Jsonb binary)
         +-- last_used      TimestampTz
```

- **Lookup**: O(1) hash table lookup by endpoint_id.
- **Invalidation**: `hash_any()` on the raw Jsonb binary. If the config changes (even just key reordering), the old dispatcher is freed and a new one created. This is a harmless false invalidation -- correctness over efficiency.
- **Eviction**: Idle entries older than 60 seconds are evicted every 60 seconds.
- **SIGHUP**: The entire cache is destroyed and reinitialized on config reload.
- **Error recovery**: The PG_CATCH block destroys and reinitializes the cache to avoid reusing corrupted state.
- **Cleanup**: `before_shmem_exit` hook guarantees cleanup on worker termination.
- **Memory**: Dispatchers are allocated in `DispatcherCacheContext` (persistent), not in `batch_context`, so they survive batch boundaries. Config Jsonb is deep-copied into this context because the original lives in `batch_context` which is deleted after each batch.

---

## Concurrency Model

### Modulo Partitioning

With N workers, worker W processes only messages where `(id % N) = W`:

```
Messages:  [1] [2] [3] [4] [5] [6] [7] [8] [9]
Worker 0:   *           *           *
Worker 1:       *           *           *
Worker 2:           *           *           *
```

This gives each worker a deterministic, disjoint partition of the queue. Sequential IDs distribute evenly. Workers never contend for the same rows.

### SKIP LOCKED

Even with modulo partitioning, `FOR UPDATE ... SKIP LOCKED` provides a safety net:
- During scaling events (N changes), there may be brief overlap.
- If a message is somehow locked by another process (manual intervention, admin query), workers skip it rather than blocking.

### READ COMMITTED Isolation

The worker explicitly uses READ COMMITTED (not REPEATABLE READ). With REPEATABLE READ, PostgreSQL raises `SQLSTATE 40001` (serialization failure) when two transactions both attempt `SELECT FOR UPDATE SKIP LOCKED` and their snapshots conflict. READ COMMITTED avoids this entirely: each statement sees the latest committed data.

### synchronous_commit = off

```sql
SET LOCAL synchronous_commit = off;
```

Worker transactions do not wait for WAL flush. This yields 2-3x faster writes. It is safe because:
- ulak provides **at-least-once** delivery semantics.
- If the server crashes before WAL flush, messages revert to `pending` and are re-dispatched.
- The worst case is a duplicate delivery, which consumers should handle idempotently.

---

## Shared Memory

### Layout

ulak requests a single contiguous shared memory segment (`UlakShmemState`) at server startup:

```
UlakShmemState
+------------------------------------------------------------------+
| LWLock *lock                    (1 tranche, named "ulak")        |
+------------------------------------------------------------------+
| Aggregate worker state                                           |
|   worker_pid, worker_started, worker_started_at                  |
|   atomic_messages_processed   (pg_atomic_uint64 -- lock-free)    |
|   atomic_error_count          (pg_atomic_uint32 -- lock-free)    |
|   messages_processed, error_count, last_activity                 |
|   last_error_msg[256]                                            |
+------------------------------------------------------------------+
| Multi-database registry                                          |
|   database_count                                                 |
|   databases[64]  (UlakDatabaseEntry)                             |
|     +------------------------------------------------------------+
|     | dboid, dbname, active, target_workers, active_workers      |
|     | worker_pids[32]           PID per slot                     |
|     | generation[32]            ABA protection counter           |
|     | worker_started_at[32]                                      |
|     | metrics_mutex (slock_t)   spinlock for hot counters         |
|     | messages_processed[32]    per-worker counters               |
|     | error_count[32]           per-worker counters               |
|     | last_error_at[32], last_error_msg[32][256]                 |
|     | last_activity[32]                                          |
|     | Circuit breaker: consecutive_spawn_failures,               |
|     |   circuit_open_until, total_spawn_failures                 |
|     +------------------------------------------------------------+
+------------------------------------------------------------------+
| Launcher bookkeeping                                             |
|   launcher_started, launcher_pid, launcher_started_at            |
|   total_spawns, total_spawn_failures, total_restarts             |
|   last_check_cycle                                               |
+------------------------------------------------------------------+
| Rate limit buckets[256]  (RateLimitShmemBucket)                  |
|     +------------------------------------------------------------+
|     | mutex (slock_t)           per-bucket spinlock               |
|     | endpoint_id, active                                        |
|     | tokens (double), max_tokens, refill_rate                   |
|     | last_refill (TimestampTz)                                  |
|     +------------------------------------------------------------+
+------------------------------------------------------------------+
```

### Coordination Primitives

| Primitive | Scope | Purpose |
|-----------|-------|---------|
| `LWLock` (1 tranche) | Global | Protects database registry, worker slots, timestamps |
| `slock_t metrics_mutex` | Per-database | Hot path: per-worker counter updates |
| `slock_t mutex` | Per-rate-limit-bucket | Token bucket acquire/refill |
| `pg_atomic_uint64` | Global | Lock-free `messages_processed` counter |
| `pg_atomic_uint32` | Global | Lock-free `error_count` counter |

Atomic counters are used for the aggregate message/error counts because they are updated by every worker on every batch. The LWLock is only taken for timestamp writes (`last_activity` is a `TimestampTz`, which is not atomically writable on all platforms).

### ABA Protection

Each worker slot has a `generation` counter that increments when a new worker claims the slot. This prevents stale references: if a monitoring query reads slot 5 with generation 3, and the worker dies and a new one takes slot 5 with generation 4, the monitoring query can detect the change.

### Version Compatibility

- **PostgreSQL 15+**: `shmem_request_hook` for requesting shared memory before allocation.
- **PostgreSQL 14**: Direct call to `RequestAddinShmemSpace()` and `RequestNamedLWLockTranche()` during `_PG_init()`.

---

## Message State Machine

```
                         +------------------+
                         |                  |
                         v                  |
 INSERT -----------> pending ----+          |
                         ^       |          |
                         |       | SELECT FOR UPDATE SKIP LOCKED
                         |       v          |
                         |   processing     |
                         |       |          |
              +----------+-------+----------+----------+
              |          |       |          |          |
              |  (rate   | (success)  (retryable  (permanent
              |  limit)  |              error)      error /
              |          v              |          max retries)
              |      completed          |              |
              |                         |              v
              +--- pending              +--- pending  failed
                  (no retry_count          (retry_count++,    |
                   increment)               next_retry_at     |
                                            with backoff)     v
                                                            DLQ
                                                     (dead_letter_queue)

  Circuit breaker open:
    processing --> pending (deferred, retry delay, no dispatch attempted)
```

### Transition Details

| From | To | Condition |
|------|----|-----------|
| (new) | `pending` | `INSERT INTO ulak.queue` |
| `pending` | `processing` | Dequeued by worker (FOR UPDATE SKIP LOCKED) |
| `processing` | `completed` | Dispatch succeeded |
| `processing` | `pending` | Retryable error, retries remaining. `retry_count++`, `next_retry_at = NOW() + backoff` |
| `processing` | `pending` | Rate limited. No `retry_count` increment, no backoff |
| `processing` | `pending` | Circuit breaker open. Deferred with retry delay |
| `processing` | `failed` | Permanent error (`[PERMANENT]` prefix) or `retry_count >= max_retries` |
| `failed` | DLQ | `ulak.archive_single_to_dlq(id)` moves to `ulak.dead_letter_queue` |

### Backoff Strategies

Configured via endpoint `retry_policy` JSONB or global GUCs:

- **Exponential** (default): `base_delay * 2^retry_count`, capped at `max_delay`
- **Linear**: `base_delay + (retry_count * increment)`, capped at `max_delay`
- **Fixed**: constant `base_delay` on every retry
- **Server override**: HTTP `Retry-After` header overrides calculated delay

---

## Memory Management

### MemoryContext Hierarchy

```
TopMemoryContext
|
+-- DispatcherCacheContext (persistent, survives batches)
|     |
|     +-- dispatcher_cache HTAB
|     |     +-- DispatcherCacheEntry (endpoint_id -> Dispatcher*)
|     |           +-- Dispatcher instances with connection handles
|     |           +-- Deep-copied config Jsonb (outlives batch_context)
|     |
|     +-- curl_easy handles, TLS sessions, DNS cache (libcurl-managed)
|
+-- batch_context (per-batch, created/destroyed each iteration)
      |
      +-- MessageBatchInfo[] array
      +-- payload_str copies (pstrdup from SPI tuples)
      +-- config Jsonb copies (memcpy from SPI tuples)
      +-- headers, metadata Jsonb copies
      +-- retry_policy Jsonb copies
      +-- DispatchResult instances (palloc0)
```

### batch_context Lifecycle

1. **Created** at the start of `process_pending_messages_batch()` as a child of `TopMemoryContext`.
2. **Active** during SPI tuple extraction -- all message field copies go here.
3. **Survived** through dispatch -- dispatcher cache lives in its own context, but message payloads are read from batch_context.
4. **Destroyed** after `CommitTransactionCommand()` -- a single `MemoryContextDelete()` frees all batch allocations at once, avoiding per-field pfree calls.

### PG_TRY / PG_CATCH Cleanup

The main worker loop wraps each iteration in `PG_TRY/PG_CATCH`. On error:

```
PG_CATCH:
    CopyErrorData() --> stack buffers (before AbortCurrentTransaction destroys them)
    FreeErrorData(), FlushErrorState()
    AbortCurrentTransaction()
    MemoryContextDelete(worker_batch_context)    // clean orphaned batch
    dispatcher_cache_destroy() + reinit          // purge corrupted dispatchers
    Reset local stats (uncommitted batch)
    Backoff sleep
```

The `worker_batch_context` static pointer ensures the batch context is always cleaned up, even if the error occurs deep inside SPI or dispatcher code. The dispatcher cache is fully rebuilt because partial state after an error is not trustworthy.

---

## See Also

- [Getting Started](Getting-Started) -- Quick setup and first message
- [Configuration Reference](Configuration-Reference) -- All 57 GUCs
- [SQL API Reference](SQL-API-Reference) -- All 40 SQL functions
- [Reliability](Reliability) -- Circuit breakers, DLQ, retry policies
- [Monitoring](Monitoring) -- Shared memory stats, health checks
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-Kafka](Protocol-Kafka) | [Protocol-MQTT](Protocol-MQTT) | [Protocol-Redis](Protocol-Redis) | [Protocol-AMQP](Protocol-AMQP) | [Protocol-NATS](Protocol-NATS)
