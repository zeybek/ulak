# Test: Priority ordering under concurrent access
#
# Higher-priority messages must be fetched first, matching the worker's
# ORDER BY priority DESC, created_at ASC.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_prio_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- Insert messages with different priorities (inserted in ascending order)
    INSERT INTO ulak.queue (endpoint_id, payload, priority)
    SELECT e.id, jsonb_build_object('prio', 0, 'seq', 1), 0
    FROM ulak.endpoints e WHERE e.name = 'iso_prio_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, priority)
    SELECT e.id, jsonb_build_object('prio', 0, 'seq', 2), 0
    FROM ulak.endpoints e WHERE e.name = 'iso_prio_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, priority)
    SELECT e.id, jsonb_build_object('prio', 5, 'seq', 3), 5
    FROM ulak.endpoints e WHERE e.name = 'iso_prio_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, priority)
    SELECT e.id, jsonb_build_object('prio', 5, 'seq', 4), 5
    FROM ulak.endpoints e WHERE e.name = 'iso_prio_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, priority)
    SELECT e.id, jsonb_build_object('prio', 10, 'seq', 5), 10
    FROM ulak.endpoints e WHERE e.name = 'iso_prio_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_prio_ep';
}

# Worker 1: fetches top 2 by priority
session "w1"
setup { BEGIN; }
step "w1_fetch"
{
    SELECT payload::text, priority
    FROM ulak.queue
    WHERE status = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT 2
    FOR UPDATE SKIP LOCKED;
}
step "w1_commit" { COMMIT; }

# Worker 2: fetches next 2 by priority (w1's rows are locked)
session "w2"
setup { BEGIN; }
step "w2_fetch"
{
    SELECT payload::text, priority
    FROM ulak.queue
    WHERE status = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT 2
    FOR UPDATE SKIP LOCKED;
}
step "w2_commit" { COMMIT; }

# w1 gets prio 10 + first prio 5, w2 gets second prio 5 + first prio 0
permutation "w1_fetch" "w2_fetch" "w1_commit" "w2_commit"
