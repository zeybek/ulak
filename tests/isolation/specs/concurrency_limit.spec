# Test: Per-endpoint concurrency limit
#
# When concurrency_limit is set on an endpoint, workers should not claim
# more messages than the limit. We simulate this by checking that the
# worker fetch query respects the limit.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled, concurrency_limit)
    VALUES ('iso_conc_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false, 2);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_conc_ep';
}

# Session 1: insert 5 messages, mark 2 as processing (at limit)
session "setup_session"
step "insert_messages"
{
    INSERT INTO ulak.queue (endpoint_id, payload, status)
    SELECT e.id, jsonb_build_object('msg', g), 'pending'
    FROM ulak.endpoints e, generate_series(1, 5) g
    WHERE e.name = 'iso_conc_ep';
}
step "mark_two_processing"
{
    UPDATE ulak.queue SET status = 'processing', processing_started_at = NOW()
    WHERE id IN (
        SELECT id FROM ulak.queue
        WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'iso_conc_ep')
          AND status = 'pending'
        ORDER BY id LIMIT 2
    );
}

# Session 2: try to claim messages — should get 0 because limit=2 and 2 already processing
session "worker"
step "try_claim"
{
    SELECT count(*) AS claimable
    FROM ulak.queue q
    JOIN ulak.endpoints e ON q.endpoint_id = e.id
    WHERE q.status = 'pending'
      AND e.enabled = false
      AND e.name = 'iso_conc_ep'
      AND (e.concurrency_limit IS NULL OR e.concurrency_limit > (
          SELECT count(*) FROM ulak.queue q3
          WHERE q3.endpoint_id = e.id AND q3.status = 'processing'
      ));
}

# Verification
session "checker"
step "check_counts"
{
    SELECT
        (SELECT count(*) FROM ulak.queue WHERE status = 'pending'
            AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'iso_conc_ep')) AS pending,
        (SELECT count(*) FROM ulak.queue WHERE status = 'processing'
            AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'iso_conc_ep')) AS processing;
}

permutation "insert_messages" "mark_two_processing" "try_claim" "check_counts"
