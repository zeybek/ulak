# Test: Concurrent redrive of the same DLQ message
#
# Documents a known race condition: redrive_message() lacks locking,
# so two concurrent calls on the same DLQ message both succeed,
# creating duplicate queue entries.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.dlq;
    DELETE FROM ulak.endpoints;

    -- Reset sequences for deterministic IDs in output
    ALTER SEQUENCE ulak.dlq_id_seq RESTART WITH 1;
    ALTER SEQUENCE ulak.queue_id_seq RESTART WITH 1;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_dlq_ep', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- Manually insert a DLQ entry (simulating a permanently failed message)
    INSERT INTO ulak.dlq (
        original_message_id, endpoint_id, endpoint_name, protocol,
        payload, retry_count, last_error,
        original_created_at, failed_at
    )
    SELECT 999999, e.id, e.name, e.protocol,
           jsonb_build_object('dlq', 'test'), 10, 'max retries exceeded',
           NOW() - interval '1 hour', NOW() - interval '30 minutes'
    FROM ulak.endpoints e WHERE e.name = 'iso_dlq_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.dlq;
    DELETE FROM ulak.endpoints WHERE name = 'iso_dlq_ep';
}

# Worker 1: redrive the DLQ message
session "w1"
step "w1_redrive"
{
    SELECT (ulak.redrive_message(
        (SELECT id FROM ulak.dlq WHERE endpoint_name = 'iso_dlq_ep' LIMIT 1)
    ) IS NOT NULL) AS redrove;
}

# Worker 2: redrive the same DLQ message
session "w2"
step "w2_redrive"
{
    SELECT (ulak.redrive_message(
        (SELECT id FROM ulak.dlq WHERE endpoint_name = 'iso_dlq_ep' LIMIT 1)
    ) IS NOT NULL) AS redrove;
}

# Verification
session "checker"
step "check_queue_count"
{
    SELECT count(*) AS queue_messages
    FROM ulak.queue q
    JOIN ulak.endpoints e ON q.endpoint_id = e.id
    WHERE e.name = 'iso_dlq_ep';
}
step "check_dlq_status"
{
    SELECT status, payload::text
    FROM ulak.dlq WHERE endpoint_name = 'iso_dlq_ep';
}

# Sequential redrives: both succeed, creating 2 queue entries (race condition)
permutation "w1_redrive" "w2_redrive" "check_queue_count" "check_dlq_status"
