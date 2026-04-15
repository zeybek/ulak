# Test: Ordering key serialization across concurrent workers
#
# Messages with the same ordering_key must be processed in order.
# When one message with key "order-A" is processing, no other
# "order-A" message should be fetchable by any worker.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_order_ep', 'http', jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- 3 messages with ordering_key "order-A"
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'A1'), 'order-A'
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'A2'), 'order-A'
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'A3'), 'order-A'
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';

    -- 2 messages with ordering_key "order-B"
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'B1'), 'order-B'
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'B2'), 'order-B'
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';

    -- 1 message without ordering_key
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'free'), NULL
    FROM ulak.endpoints e WHERE e.name = 'iso_order_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_order_ep';
}

# Session 1: marks first "order-A" message as processing
session "w1"
setup { BEGIN; }
step "w1_mark_processing"
{
    UPDATE ulak.queue
    SET status = 'processing'
    WHERE id = (
        SELECT id FROM ulak.queue
        WHERE ordering_key = 'order-A' AND status = 'pending'
        ORDER BY id LIMIT 1
    )
    RETURNING payload::text, ordering_key;
}
step "w1_commit" { COMMIT; }

# Session 2: fetches with ordering key blocking logic
session "w2"
setup { BEGIN; }
step "w2_fetch_with_ordering"
{
    WITH blocked_keys AS (
        SELECT DISTINCT ordering_key FROM ulak.queue
        WHERE ordering_key IS NOT NULL AND status = 'processing'
    )
    SELECT payload::text, ordering_key
    FROM ulak.queue q
    WHERE q.status = 'pending'
      AND (q.ordering_key IS NULL
           OR (q.ordering_key NOT IN (SELECT ordering_key FROM blocked_keys)
               AND NOT EXISTS (
                   SELECT 1 FROM ulak.queue q2
                   WHERE q2.ordering_key = q.ordering_key
                     AND q2.status = 'pending'
                     AND q2.id < q.id)))
    ORDER BY q.id
    FOR UPDATE SKIP LOCKED;
}
step "w2_commit" { COMMIT; }

# w1 marks A1 as processing -> w2 must see B1 and "free" only (not A2, A3, B2)
permutation "w1_mark_processing" "w2_fetch_with_ordering" "w1_commit" "w2_commit"
