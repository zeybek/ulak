# Test: Batch mark-as-processing atomicity
#
# Worker 1 fetches and marks messages as processing in a batch.
# Worker 2 must not see those messages even mid-transaction.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_batch_ep', 'http', jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    INSERT INTO ulak.queue (endpoint_id, payload)
    SELECT e.id, jsonb_build_object('seq', g)
    FROM ulak.endpoints e, generate_series(1, 6) g
    WHERE e.name = 'iso_batch_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_batch_ep';
}

# Worker 1: fetch + mark as processing
session "w1"
setup { BEGIN; }
step "w1_fetch_and_mark"
{
    WITH fetched AS (
        SELECT id FROM ulak.queue
        WHERE status = 'pending'
        ORDER BY id LIMIT 3
        FOR UPDATE SKIP LOCKED
    ),
    updated AS (
        UPDATE ulak.queue
        SET status = 'processing', processing_started_at = NOW()
        WHERE id IN (SELECT id FROM fetched)
        RETURNING payload::text, status
    )
    SELECT * FROM updated ORDER BY payload;
}
step "w1_commit" { COMMIT; }

# Worker 2: fetch remaining pending messages
session "w2"
setup { BEGIN; }
step "w2_fetch_pending"
{
    SELECT payload::text, status
    FROM ulak.queue
    WHERE status = 'pending'
    ORDER BY id
    FOR UPDATE SKIP LOCKED;
}
step "w2_commit" { COMMIT; }

# w1 marks 3 as processing, w2 sees only remaining 3 pending
permutation "w1_fetch_and_mark" "w2_fetch_pending" "w1_commit" "w2_commit"
# w2 fetches before w1 commits — w2 should still skip locked rows
permutation "w1_fetch_and_mark" "w2_fetch_pending" "w2_commit" "w1_commit"
