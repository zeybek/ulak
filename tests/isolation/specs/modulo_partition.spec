# Test: Modulo partitioning ensures non-overlapping message distribution
#
# Two workers use (id % 2) = worker_id to partition messages.
# Each worker must only see its own partition — zero overlap.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_modulo_ep', 'http', jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    INSERT INTO ulak.queue (endpoint_id, payload)
    SELECT e.id, jsonb_build_object('seq', g)
    FROM ulak.endpoints e, generate_series(1, 8) g
    WHERE e.name = 'iso_modulo_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_modulo_ep';
}

# Worker 0: fetches even-id messages
session "w0"
setup { BEGIN; }
step "w0_fetch"
{
    SELECT (q.id % 2) AS partition, payload::text
    FROM ulak.queue q
    WHERE q.status = 'pending'
      AND (q.id % 2) = 0
    ORDER BY q.id
    LIMIT 4
    FOR UPDATE SKIP LOCKED;
}
step "w0_commit" { COMMIT; }

# Worker 1: fetches odd-id messages
session "w1"
setup { BEGIN; }
step "w1_fetch"
{
    SELECT (q.id % 2) AS partition, payload::text
    FROM ulak.queue q
    WHERE q.status = 'pending'
      AND (q.id % 2) = 1
    ORDER BY q.id
    LIMIT 4
    FOR UPDATE SKIP LOCKED;
}
step "w1_commit" { COMMIT; }

# Both workers fetch concurrently — must see different partitions
permutation "w0_fetch" "w1_fetch" "w0_commit" "w1_commit"
permutation "w1_fetch" "w0_fetch" "w1_commit" "w0_commit"
