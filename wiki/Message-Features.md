# Message Features

ulak provides fine-grained control over message delivery through priority scheduling, ordering guarantees, idempotency, deferred delivery, TTL, distributed tracing correlation, and high-throughput batch operations. All of these features are accessed through the `send_with_options()` function or specialized batch APIs.

---

## Priority

Messages have a priority from 0 to 10 (higher value = processed first). The default priority is 0. Workers fetch messages ordered by `priority DESC, endpoint_id, created_at ASC`, so high-priority messages are always dispatched before lower-priority ones.

```sql
-- Send a high-priority message (priority 10 = highest)
SELECT ulak.send_with_options('webhook', '{"urgent": true}'::jsonb,
  p_priority := 10);

-- Send a normal message (default priority 0)
SELECT ulak.send('webhook', '{"routine": true}'::jsonb);
```

The worker fetch query uses a composite index optimized for this ordering:

```sql
-- Index definition (created automatically by the extension)
CREATE INDEX idx_queue_worker_fetch
  ON ulak.queue(priority DESC, endpoint_id, created_at ASC)
  WHERE status = 'pending';
```

Priority is enforced within each worker's partition. With multiple workers, each worker independently fetches its highest-priority messages, but cross-worker global ordering is not guaranteed.

### Batch Send with Priority

```sql
SELECT ulak.send_batch_with_priority('webhook',
  ARRAY['{"id": 1}'::jsonb, '{"id": 2}'::jsonb, '{"id": 3}'::jsonb],
  5  -- priority for all messages in the batch
);
```

---

## Ordering Keys

Messages with the same `ordering_key` are processed strictly in FIFO order. No two messages with the same ordering key can be in `processing` status simultaneously, and a pending message will not be dispatched if an earlier message with the same key is still pending.

```sql
-- These messages are always delivered in order: step 1, then step 2, then step 3
SELECT ulak.send_with_options('webhook', '{"step": 1}'::jsonb,
  p_ordering_key := 'order-123');
SELECT ulak.send_with_options('webhook', '{"step": 2}'::jsonb,
  p_ordering_key := 'order-123');
SELECT ulak.send_with_options('webhook', '{"step": 3}'::jsonb,
  p_ordering_key := 'order-123');
```

### How It Works

The worker fetch query includes two NOT EXISTS subqueries for ordering key enforcement:

```sql
AND (q.ordering_key IS NULL
     OR (NOT EXISTS (
             SELECT 1 FROM ulak.queue q2
             WHERE q2.ordering_key = q.ordering_key
               AND q2.status = 'processing')
         AND NOT EXISTS (
             SELECT 1 FROM ulak.queue q2
             WHERE q2.ordering_key = q.ordering_key
               AND q2.status = 'pending'
               AND q2.id < q.id)))
```

This enforces two rules:
1. No sibling message with the same ordering key is currently being processed.
2. No earlier pending message with the same ordering key exists (FIFO within the key).

Messages with `NULL` ordering key (the default) are processed in parallel with no ordering constraint.

### Supporting Indexes

```sql
CREATE INDEX idx_queue_ordering_processing ON ulak.queue(ordering_key)
  WHERE ordering_key IS NOT NULL AND status = 'processing';

CREATE INDEX idx_queue_ordering_pending ON ulak.queue(ordering_key, created_at ASC)
  WHERE ordering_key IS NOT NULL AND status = 'pending';
```

### Performance Consideration

Ordering keys introduce serialization. Messages sharing the same key are processed one at a time. Use distinct ordering keys for independent sequences (e.g., `order-123`, `order-456`) to maintain parallelism.

---

## Idempotency

The `idempotency_key` parameter prevents duplicate message delivery. A partial unique index ensures that no two active (pending or processing) messages share the same idempotency key.

```sql
SELECT ulak.send_with_options('webhook', '{"order_id": 1}'::jsonb,
  p_idempotency_key := 'order-1-created');

-- Second call with same key and same payload: returns existing message ID (no duplicate)
SELECT ulak.send_with_options('webhook', '{"order_id": 1}'::jsonb,
  p_idempotency_key := 'order-1-created');

-- Same key but DIFFERENT payload: raises an error
SELECT ulak.send_with_options('webhook', '{"order_id": 2}'::jsonb,
  p_idempotency_key := 'order-1-created');
-- ERROR: Idempotency key 'order-1-created' conflict: same key with different payload
```

### How It Works

1. When `idempotency_key` is provided, `send_with_options()` computes `payload_hash = md5(payload::text)`.
2. The INSERT is attempted. If it violates the unique index, the EXCEPTION handler fires.
3. The handler retrieves the existing message's `payload_hash` and compares.
4. If hashes match (same payload), the existing message ID is returned silently.
5. If hashes differ (different payload), an error is raised.

### Unique Index

```sql
CREATE UNIQUE INDEX idx_queue_idempotency_key ON ulak.queue(idempotency_key)
  WHERE idempotency_key IS NOT NULL AND status IN ('pending', 'processing');
```

The partial index only covers active messages. Once a message completes, fails, or expires, its idempotency key is freed for reuse.

---

## Scheduled Delivery

Messages can be held until a specified time using `scheduled_at`. The worker skips messages whose `scheduled_at` is in the future.

```sql
-- Deliver 30 minutes from now
SELECT ulak.send_with_options('webhook', '{"reminder": true}'::jsonb,
  p_scheduled_at := NOW() + INTERVAL '30 minutes');

-- Deliver at a specific time
SELECT ulak.send_with_options('webhook', '{"report": "daily"}'::jsonb,
  p_scheduled_at := '2025-04-15 09:00:00+00'::timestamptz);
```

The worker query includes:

```sql
AND (q.scheduled_at IS NULL OR q.scheduled_at <= NOW())
```

Messages with `NULL` `scheduled_at` are eligible for immediate dispatch.

---

## TTL / Message Expiry

Messages with an `expires_at` timestamp are automatically skipped by workers after that time. The periodic `mark_expired_messages()` maintenance function transitions them to `expired` status.

```sql
-- Message expires in 1 hour (not delivered after that)
SELECT ulak.send_with_options('webhook', '{"flash_sale": true}'::jsonb,
  p_expires_at := NOW() + INTERVAL '1 hour');

-- Message expires at a specific time
SELECT ulak.send_with_options('webhook', '{"offer": "limited"}'::jsonb,
  p_expires_at := '2025-04-14 23:59:59+00'::timestamptz);
```

### Expiry Flow

1. Worker fetch query filters out expired messages: `AND (q.expires_at IS NULL OR q.expires_at > NOW())`
2. Periodically, `mark_expired_messages()` runs: `UPDATE ulak.queue SET status = 'expired' WHERE status = 'pending' AND expires_at < NOW()`
3. Expired messages are eventually archived to `ulak.archive` by the maintenance cycle.

---

## Correlation ID

A UUID for distributed tracing. The correlation ID is stored on the message, passed through to the dispatch result, and captured in the response when `ulak.capture_response` is enabled.

```sql
SELECT ulak.send_with_options('webhook', '{"data": 1}'::jsonb,
  p_correlation_id := '550e8400-e29b-41d4-a716-446655440000'::uuid);
```

Use correlation IDs to trace a message across your application, ulak queue, and the downstream endpoint. The ID is available in:

- `ulak.queue.correlation_id` while the message is in the queue
- `ulak.dlq.correlation_id` if the message moves to the dead letter queue
- `ulak.archive.correlation_id` after archival
- The `response` JSONB when response capture is enabled

---

## Batch Operations

For high-throughput scenarios, use `send_batch()` to insert multiple messages in a single SQL statement. This is 10-15x faster than individual `send()` calls because it uses a single `INSERT ... SELECT unnest(...)` and suppresses per-row notify triggers.

```sql
-- Batch send (all messages get default priority 0)
SELECT ulak.send_batch('webhook', ARRAY[
  '{"id": 1}'::jsonb,
  '{"id": 2}'::jsonb,
  '{"id": 3}'::jsonb
]);
-- Returns: bigint[] array of created message IDs

-- Batch send with uniform priority
SELECT ulak.send_batch_with_priority('webhook',
  ARRAY['{"id": 1}'::jsonb, '{"id": 2}'::jsonb],
  5  -- all messages get priority 5
);
```

### What Makes It Fast

1. **Single INSERT:** All payloads are inserted via `unnest()` in one statement, avoiding per-row overhead.
2. **Suppressed NOTIFY:** Per-row insert triggers are suppressed via `SET LOCAL ulak.suppress_notify = 'on'`. A single `pg_notify('ulak_new_msg', '')` is sent after the batch completes.
3. **Reduced WAL:** One transaction commit instead of N.

### Backpressure

Batch operations check backpressure before inserting. The entire batch is rejected if the queue would exceed `ulak.max_queue_size`.

---

## Fast Mode

Enable fast mode for the current transaction to get approximately 2-3x faster writes at the cost of up to 600ms of WAL data loss on a server crash. This is safe for ulak's at-least-once delivery model because messages are idempotent.

```sql
SELECT ulak.enable_fast_mode();
-- All subsequent sends in this transaction are faster
SELECT ulak.send('webhook', '{"data": 1}'::jsonb);
SELECT ulak.send('webhook', '{"data": 2}'::jsonb);
```

Under the hood, this sets `SET LOCAL synchronous_commit = off`, which is scoped to the current transaction. Background workers already use this setting internally.

---

## `send_with_options` Reference

The full signature of `send_with_options()`:

```sql
ulak.send_with_options(
  p_endpoint_name   text,           -- Required: target endpoint name
  p_payload         jsonb,          -- Required: message payload
  p_priority        smallint  DEFAULT 0,      -- 0-10, higher = first
  p_scheduled_at    timestamptz DEFAULT NULL,  -- Delay until this time
  p_idempotency_key text      DEFAULT NULL,    -- Dedup key
  p_correlation_id  uuid      DEFAULT NULL,    -- Distributed tracing ID
  p_expires_at      timestamptz DEFAULT NULL,  -- TTL deadline
  p_ordering_key    text      DEFAULT NULL     -- FIFO ordering group
) RETURNS bigint  -- message ID
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_endpoint_name` | text | (required) | Name of the target endpoint (must exist and be enabled) |
| `p_payload` | jsonb | (required) | Message payload delivered to the endpoint |
| `p_priority` | smallint | 0 | Dispatch priority 0-10. Higher values are processed first. |
| `p_scheduled_at` | timestamptz | NULL | Hold message until this time. NULL = dispatch immediately. |
| `p_idempotency_key` | text | NULL | Unique deduplication key across active messages. |
| `p_correlation_id` | uuid | NULL | UUID for distributed tracing. Passed through to response. |
| `p_expires_at` | timestamptz | NULL | Message TTL. Automatically marked expired after this time. |
| `p_ordering_key` | text | NULL | FIFO ordering group. Same key = strict serial processing. |

The function is declared `SECURITY DEFINER` with `search_path = pg_catalog, ulak`, so it runs with the extension owner's privileges regardless of the caller's role. Both `ulak_admin` and `ulak_application` roles can execute it.

### Combined Example

```sql
SELECT ulak.send_with_options(
  'payment-webhook',
  '{"order_id": 456, "amount": 99.99}'::jsonb,
  p_priority := 8,
  p_ordering_key := 'customer-789',
  p_idempotency_key := 'payment-456-v1',
  p_correlation_id := gen_random_uuid(),
  p_scheduled_at := NOW() + INTERVAL '5 minutes',
  p_expires_at := NOW() + INTERVAL '2 hours'
);
```

This creates a high-priority message that:
- Will not be dispatched until 5 minutes from now
- Will expire if not delivered within 2 hours
- Is deduplicated by the key `payment-456-v1`
- Maintains FIFO ordering within customer 789's messages
- Can be traced across systems via the correlation ID

---

## See Also

- [Pub/Sub Events](Pub-Sub) -- publish/subscribe with fan-out and filters
- [Reliability](Reliability) -- circuit breaker, retry, DLQ, backpressure
- [System Architecture](Architecture) -- worker fetch query and dispatch pipeline
- [Configuration Reference](Configuration-Reference) -- `ulak.max_queue_size`, `ulak.max_payload_size`, and other GUCs
- [Getting Started](Getting-Started) -- basic send examples
