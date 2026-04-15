# Pub/Sub Events

ulak supports publish/subscribe messaging with named event types, subscription-based routing, JSONB containment filters, and multi-endpoint fan-out. One `publish()` call can create messages for any number of subscribers in a single transaction.

---

## Event Types

Event types are named categories that publishers emit and subscribers listen to. Each event type is a row in `ulak.event_types` with an optional description and JSON schema.

```sql
-- Create event types
SELECT ulak.create_event_type('order.created', 'New order placed');
SELECT ulak.create_event_type('order.shipped', 'Order shipped to customer');
SELECT ulak.create_event_type('user.registered', 'New user account created');

-- Optional: include a JSON schema for documentation
SELECT ulak.create_event_type('payment.completed', 'Payment processed', '{
  "type": "object",
  "properties": {
    "order_id": {"type": "integer"},
    "amount": {"type": "number"},
    "currency": {"type": "string"}
  }
}'::jsonb);

-- Remove an event type (cascades to subscriptions)
SELECT ulak.drop_event_type('user.registered');
```

Event types are referenced by name in `subscribe()` and `publish()` calls. Dropping an event type cascades to all its subscriptions (via `ON DELETE CASCADE` on `ulak.subscriptions.event_type_id`).

---

## Subscriptions

A subscription links an event type to an endpoint. When an event is published, messages are created for all active subscriptions matching that event type. Each subscription can optionally include a JSONB filter.

```sql
-- Subscribe an endpoint to an event type
SELECT ulak.subscribe('order.created', 'webhook-endpoint');

-- Fan-out: multiple endpoints subscribe to the same event
SELECT ulak.subscribe('order.created', 'kafka-endpoint');
SELECT ulak.subscribe('order.created', 'slack-notifier');

-- Subscribe with a JSONB containment filter
SELECT ulak.subscribe('order.created', 'high-value-webhook',
  '{"region": "EU"}'::jsonb);

-- Unsubscribe by subscription ID
SELECT ulak.unsubscribe(42);
```

### Subscription Table Structure

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigserial | Subscription ID (used in `unsubscribe()`) |
| `event_type_id` | bigint | FK to `ulak.event_types` (CASCADE on delete) |
| `endpoint_id` | bigint | FK to `ulak.endpoints` (CASCADE on delete) |
| `filter` | jsonb | Optional JSONB containment filter |
| `enabled` | boolean | Whether this subscription is active (default `true`) |
| `description` | text | Optional description |

A unique constraint on `(event_type_id, endpoint_id)` prevents duplicate subscriptions. To change a subscription's filter, update the row directly:

```sql
UPDATE ulak.subscriptions
SET filter = '{"region": "US", "priority": "high"}'::jsonb
WHERE id = 42;
```

---

## Publishing

### Single Event

```sql
-- Publish to all subscribers of 'order.created'
SELECT ulak.publish('order.created', '{
  "order_id": 123,
  "total": 99.99,
  "region": "EU"
}'::jsonb);
-- Returns: integer count of messages created (one per matching subscription)
```

### Batch Publish

Publish multiple events of different types in a single call. All inserts happen in one transaction with a single `NOTIFY` at the end.

```sql
SELECT ulak.publish_batch('[
  {"event_type": "order.created", "payload": {"order_id": 1, "region": "EU"}},
  {"event_type": "order.shipped", "payload": {"order_id": 2, "tracking": "XYZ123"}},
  {"event_type": "order.created", "payload": {"order_id": 3, "region": "US"}}
]'::jsonb);
-- Returns: total messages created across all events
```

### What Happens Internally

1. The event type name is resolved to an `event_type_id`.
2. All active subscriptions for that event type are queried, joined with enabled endpoints.
3. For each subscription, the JSONB filter (if set) is tested against the payload.
4. Matching subscriptions produce one `INSERT INTO ulak.queue` row each, via a single `INSERT ... SELECT`.
5. A single `pg_notify('ulak_new_msg', '')` is sent after all inserts complete.

Per-row insert triggers are suppressed during fan-out (same pattern as `send_batch()`), and re-enabled after the batch completes.

---

## Filter Matching

Subscription filters use the PostgreSQL JSONB containment operator (`@>`). A subscription matches when the published payload contains all the key-value pairs in the filter.

```sql
-- Filter definition (on subscription)
'{"region": "EU"}'::jsonb

-- This payload MATCHES (contains region=EU plus other fields):
'{"order_id": 1, "region": "EU", "total": 50.00}'::jsonb

-- This payload DOES NOT match (region is US):
'{"order_id": 2, "region": "US", "total": 75.00}'::jsonb
```

### Filter Examples

| Filter | Matches | Does Not Match |
|--------|---------|----------------|
| `{"region": "EU"}` | `{"region": "EU", "id": 1}` | `{"region": "US", "id": 1}` |
| `{"priority": "high"}` | `{"priority": "high", "region": "EU"}` | `{"priority": "low"}` |
| `{"tags": ["urgent"]}` | `{"tags": ["urgent", "billing"]}` | `{"tags": ["billing"]}` |
| `NULL` (no filter) | Any payload | -- |

Filters are evaluated in SQL as part of the subscription query, so PostgreSQL's GIN indexes on JSONB apply. A `NULL` filter matches every payload.

### Nested Filters

JSONB containment works at any depth:

```sql
SELECT ulak.subscribe('order.created', 'premium-webhook',
  '{"customer": {"tier": "premium"}}'::jsonb);

-- Matches:
SELECT ulak.publish('order.created', '{
  "order_id": 1,
  "customer": {"tier": "premium", "name": "Acme"}
}'::jsonb);
```

---

## Fan-Out Architecture

One `publish()` call creates N messages, where N is the number of active subscriptions with matching filters and enabled endpoints.

```
publish('order.created', payload)
        │
        ├──► Subscription 1 (webhook-endpoint)    → queue message #101
        ├──► Subscription 2 (kafka-endpoint)       → queue message #102
        ├──► Subscription 3 (slack-notifier)       → queue message #103
        └──► Subscription 4 (high-value, filtered) → SKIPPED (filter mismatch)
```

### Backpressure with Fan-Out

Fan-out can amplify queue growth rapidly. ulak accounts for this by computing the exact projected fan-out count before inserting any rows:

```sql
-- Internal: count matching subscriptions before insert
SELECT count(*) INTO v_projected_count
FROM ulak.subscriptions s
JOIN ulak.endpoints e ON e.id = s.endpoint_id
WHERE s.event_type_id = v_event_type_id
  AND s.enabled = true
  AND e.enabled = true
  AND (s.filter IS NULL OR p_payload @> s.filter);

-- Pass projected count to backpressure check
PERFORM ulak._check_backpressure(v_projected_count);
```

If the current queue size plus the projected fan-out would exceed `ulak.max_queue_size`, the publish is rejected before any messages are inserted. This prevents a single high-fan-out publish from overwhelming the system.

For `publish_batch()`, the projected count is calculated across all events in the batch at once.

---

## Pub/Sub Function Reference

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `create_event_type` | `(name, description?, schema?)` | `bigint` | Create a named event type |
| `drop_event_type` | `(name)` | `boolean` | Delete an event type and cascade subscriptions |
| `subscribe` | `(event_type, endpoint_name, filter?)` | `bigint` | Create a subscription (returns subscription ID) |
| `unsubscribe` | `(subscription_id)` | `boolean` | Remove a subscription |
| `publish` | `(event_type, payload)` | `integer` | Fan-out to all matching subscribers |
| `publish_batch` | `(events_jsonb)` | `integer` | Batch publish multiple events |

### RBAC Permissions

| Role | Allowed |
|------|---------|
| `ulak_admin` | All pub/sub functions |
| `ulak_application` | `publish()`, `publish_batch()`, `SELECT` on event_types and subscriptions |
| `ulak_monitor` | `SELECT` on event_types and subscriptions |

Subscription management (`subscribe`, `unsubscribe`, `create_event_type`, `drop_event_type`) is restricted to `ulak_admin`.

---

## See Also

- [Message Features](Message-Features) -- priority, ordering, idempotency, TTL for individual sends
- [Reliability](Reliability) -- backpressure, circuit breaker, DLQ, retry
- [System Architecture](Architecture) -- worker loop and dispatch pipeline
- [Configuration Reference](Configuration-Reference) -- `ulak.max_queue_size` and other GUCs
- [Security](Security) -- RBAC roles and permissions
