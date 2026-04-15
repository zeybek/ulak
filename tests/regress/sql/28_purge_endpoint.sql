-- 28_purge_endpoint.sql: purge_endpoint function
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint(
    'purge_test_ep',
    'http',
    '{"url": "http://localhost:9999/purge", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- Insert several pending messages
SELECT ulak.send('purge_test_ep', '{"msg": 1}'::jsonb);
SELECT ulak.send('purge_test_ep', '{"msg": 2}'::jsonb);
SELECT ulak.send('purge_test_ep', '{"msg": 3}'::jsonb);

-- Verify 3 pending messages
SELECT count(*) = 3 AS three_pending
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'purge_test_ep' AND q.status = 'pending';

-- ============================================================================
-- PURGE ENDPOINT
-- ============================================================================

-- Purge should return count of deleted messages
SELECT ulak.purge_endpoint('purge_test_ep') = 3 AS purged_three;

-- No pending messages should remain
SELECT count(*) = 0 AS no_pending_after_purge
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'purge_test_ep' AND q.status = 'pending';

-- Event log should record the purge
SELECT event_type = 'purge' AS purge_logged
FROM ulak.event_log
WHERE entity_type = 'endpoint'
  AND metadata->>'endpoint_name' = 'purge_test_ep'
ORDER BY created_at DESC LIMIT 1;

-- ============================================================================
-- PURGE EMPTY ENDPOINT (no messages)
-- ============================================================================

SELECT ulak.purge_endpoint('purge_test_ep') = 0 AS purge_empty_returns_zero;

-- ============================================================================
-- PURGE NONEXISTENT ENDPOINT
-- ============================================================================

DO $$
BEGIN
    PERFORM ulak.purge_endpoint('nonexistent_endpoint');
    RAISE EXCEPTION 'Should have raised endpoint not found';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%does not exist%' THEN
        RAISE NOTICE 'purge_nonexistent_error: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.event_log WHERE metadata->>'endpoint_name' = 'purge_test_ep';
SELECT ulak.drop_endpoint('purge_test_ep');
