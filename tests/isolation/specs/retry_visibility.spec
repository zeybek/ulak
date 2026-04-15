# Test: Messages with future next_retry_at are invisible to fetch
#
# The worker query filters: (next_retry_at IS NULL OR next_retry_at <= NOW())
# Messages scheduled for future retry must not be fetched.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_retry_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- msg 1: next_retry_at = NOW() (visible)
    INSERT INTO ulak.queue (endpoint_id, payload, next_retry_at)
    SELECT e.id, jsonb_build_object('vis', 'now'), NOW()
    FROM ulak.endpoints e WHERE e.name = 'iso_retry_ep';

    -- msg 2: next_retry_at = future (INVISIBLE)
    INSERT INTO ulak.queue (endpoint_id, payload, next_retry_at)
    SELECT e.id, jsonb_build_object('vis', 'future'), NOW() + interval '1 hour'
    FROM ulak.endpoints e WHERE e.name = 'iso_retry_ep';

    -- msg 3: next_retry_at IS NULL (visible)
    INSERT INTO ulak.queue (endpoint_id, payload, next_retry_at)
    SELECT e.id, jsonb_build_object('vis', 'null'), NULL
    FROM ulak.endpoints e WHERE e.name = 'iso_retry_ep';

    -- msg 4: next_retry_at = past (visible)
    INSERT INTO ulak.queue (endpoint_id, payload, next_retry_at)
    SELECT e.id, jsonb_build_object('vis', 'past'), NOW() - interval '1 second'
    FROM ulak.endpoints e WHERE e.name = 'iso_retry_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_retry_ep';
}

session "w1"
setup { BEGIN; }
step "w1_fetch"
{
    SELECT payload::text
    FROM ulak.queue
    WHERE status = 'pending'
      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY id
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
      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
    ORDER BY id
    FOR UPDATE SKIP LOCKED;
}
step "w2_commit" { COMMIT; }

# w1 gets all 3 visible messages, w2 gets 0 (all locked)
# The "future" message is never returned by either worker
permutation "w1_fetch" "w2_fetch" "w1_commit" "w2_commit"
