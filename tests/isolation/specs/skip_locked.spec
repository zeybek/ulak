# Test: FOR UPDATE SKIP LOCKED ensures non-blocking concurrent access
#
# Two sessions compete for the same pending messages.
# Session 1 locks rows; session 2 must skip them, not block.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_skip_ep', 'http', jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    INSERT INTO ulak.queue (endpoint_id, payload)
    SELECT e.id, jsonb_build_object('seq', g)
    FROM ulak.endpoints e, generate_series(1, 4) g
    WHERE e.name = 'iso_skip_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_skip_ep';
}

session "w1"
setup { BEGIN; }
step "w1_fetch"
{
    SELECT payload::text
    FROM ulak.queue
    WHERE status = 'pending'
    ORDER BY id
    LIMIT 2
    FOR UPDATE SKIP LOCKED;
}
step "w1_commit" { COMMIT; }

session "w2"
setup { BEGIN; }
step "w2_fetch"
{
    SELECT payload::text
    FROM ulak.queue
    WHERE status = 'pending'
    ORDER BY id
    LIMIT 2
    FOR UPDATE SKIP LOCKED;
}
step "w2_commit" { COMMIT; }

# w1 locks first 2 rows, w2 must skip them and get rows 3-4
permutation "w1_fetch" "w2_fetch" "w1_commit" "w2_commit"
