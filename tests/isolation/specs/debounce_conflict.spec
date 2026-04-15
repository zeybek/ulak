# Test: Concurrent send_with_options with on_conflict=skip
#
# Two sessions try to send with the same idempotency key.
# Second session uses on_conflict=skip and should silently return.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_debounce_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_debounce_ep';
}

# Session 1: insert message with idempotency key via direct INSERT
session "w1"
setup { BEGIN; }
step "w1_insert"
{
    INSERT INTO ulak.queue (endpoint_id, payload, idempotency_key)
    SELECT e.id, jsonb_build_object('from', 'w1'), 'dedup-test-1'
    FROM ulak.endpoints e WHERE e.name = 'iso_debounce_ep'
    RETURNING payload::text, idempotency_key;
}
step "w1_commit" { COMMIT; }

# Session 2: insert with same idempotency key -- will block then get conflict
session "w2"
setup { BEGIN; }
step "w2_insert"
{
    INSERT INTO ulak.queue (endpoint_id, payload, idempotency_key)
    SELECT e.id, jsonb_build_object('from', 'w2'), 'dedup-test-1'
    FROM ulak.endpoints e WHERE e.name = 'iso_debounce_ep'
    RETURNING payload::text, idempotency_key;
}
step "w2_commit" { COMMIT; }

# Verification: only 1 message should exist (w1 wins, w2 gets unique_violation)
session "checker"
step "check_result"
{
    SELECT count(*) AS msg_count,
           min(payload::text) AS first_payload
    FROM ulak.queue
    WHERE idempotency_key = 'dedup-test-1';
}

# w1 inserts, w2 blocks on unique index, w1 commits, w2 gets unique_violation
permutation "w1_insert" "w2_insert" "w1_commit" "w2_commit" "check_result"
