-- 04_queue_operations.sql: Queue operations — send() inserts, status defaults, queue state
-- pg_regress test for ulak

-- ============================================================================
-- SEND MESSAGE
-- ============================================================================

-- send() should insert a message into the queue
SELECT ulak.send('test_http_endpoint', '{"event": "user.created", "id": 1}'::jsonb);

-- Verify message was enqueued with correct defaults
SELECT
    endpoint_id IS NOT NULL AS has_endpoint_id,
    payload::text AS payload,
    status,
    retry_count,
    priority
FROM ulak.queue
WHERE payload @> '{"event": "user.created"}'::jsonb
LIMIT 1;

-- ============================================================================
-- SEND MULTIPLE MESSAGES
-- ============================================================================

-- Send a second message
SELECT ulak.send('test_http_endpoint', '{"event": "user.updated", "id": 2}'::jsonb);

-- Verify queue count
SELECT count(*) AS queue_count
FROM ulak.queue
WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test_http_endpoint');

-- ============================================================================
-- SEND WITH NULL ARGS (STRICT function returns NULL)
-- ============================================================================

-- send() is STRICT: NULL args return NULL, no error, no queue insert
SELECT ulak.send(NULL, '{"test": true}'::jsonb) IS NULL AS null_endpoint_returns_null;
SELECT ulak.send('test_http_endpoint', NULL) IS NULL AS null_payload_returns_null;

-- ============================================================================
-- DISABLED ENDPOINT REJECTION
-- ============================================================================

SELECT ulak.disable_endpoint('test_http_endpoint');

DO $$
BEGIN
    PERFORM ulak.send('test_http_endpoint', '{"event": "disabled"}'::jsonb);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'send disabled endpoint: rejected';
END $$;

SELECT count(*) AS disabled_send_queue_count
FROM ulak.queue
WHERE payload @> '{"event": "disabled"}'::jsonb;

SELECT ulak.enable_endpoint('test_http_endpoint');

-- ============================================================================
-- QUEUE STATE VERIFICATION
-- ============================================================================

-- All messages should default to 'pending' status
SELECT DISTINCT status FROM ulak.queue;

-- All messages should have retry_count = 0
SELECT count(*) = count(*) FILTER (WHERE retry_count = 0) AS all_zero_retries
FROM ulak.queue;

-- All messages should have next_retry_at set
SELECT count(*) = count(*) FILTER (WHERE next_retry_at IS NOT NULL) AS all_have_retry_at
FROM ulak.queue;
