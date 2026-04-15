# Test: Completing a processing message unblocks next in ordering key group
#
# When a message with ordering_key is completed, the next pending message
# with the same key becomes fetchable.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_okc_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- 3 messages with ordering_key='X': first is processing, others pending
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key, status)
    SELECT e.id, jsonb_build_object('key', 'X1'), 'order-X', 'processing'
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'X2'), 'order-X'
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'X3'), 'order-X'
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';

    -- 2 messages with ordering_key='Y': both pending
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'Y1'), 'order-Y'
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'Y2'), 'order-Y'
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';

    -- 1 message without ordering key
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('key', 'free'), NULL
    FROM ulak.endpoints e WHERE e.name = 'iso_okc_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_okc_ep';
}

# w1: complete the processing X1 message
session "w1"
step "w1_complete"
{
    UPDATE ulak.queue
    SET status = 'completed', completed_at = NOW()
    WHERE ordering_key = 'order-X'
      AND status = 'processing';
}

# w2: fetch with ordering key logic after w1 commits
session "w2"
setup { BEGIN; }
step "w2_fetch"
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

# w1 completes X1, then w2 should see: X2 (unblocked), Y1 (first of Y), free
# Should NOT see: X3 (blocked by X2), Y2 (blocked by Y1)
permutation "w1_complete" "w2_fetch" "w2_commit"
