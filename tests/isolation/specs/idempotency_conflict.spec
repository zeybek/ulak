# Test: Concurrent inserts with same idempotency_key
#
# Partial unique index on idempotency_key WHERE status IN ('pending','processing')
# prevents duplicate active messages. Second INSERT blocks until first commits,
# then gets unique_violation.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_idem_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_idem_ep';
}

# Session 1: insert message with idempotency key
session "w1"
setup { BEGIN; }
step "w1_insert"
{
    INSERT INTO ulak.queue (endpoint_id, payload, idempotency_key)
    SELECT e.id, jsonb_build_object('from', 'w1'), 'idem-test-1'
    FROM ulak.endpoints e WHERE e.name = 'iso_idem_ep'
    RETURNING payload::text, idempotency_key;
}
step "w1_commit" { COMMIT; }

# Session 2: insert with same idempotency key — will block then fail
session "w2"
setup { BEGIN; }
step "w2_insert"
{
    INSERT INTO ulak.queue (endpoint_id, payload, idempotency_key)
    SELECT e.id, jsonb_build_object('from', 'w2'), 'idem-test-1'
    FROM ulak.endpoints e WHERE e.name = 'iso_idem_ep'
    RETURNING payload::text, idempotency_key;
}
step "w2_commit" { COMMIT; }

# Verification: only 1 message should exist
session "checker"
step "check_count"
{
    SELECT count(*) AS msg_count, min(payload::text) AS payload
    FROM ulak.queue
    WHERE idempotency_key = 'idem-test-1';
}

# w1 inserts, w2 blocks on unique index, w1 commits, w2 gets unique_violation
permutation "w1_insert" "w2_insert" "w1_commit" "w2_commit" "check_count"
