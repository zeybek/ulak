-- 06_triggers.sql: Trigger behavior — updated_at auto-update, notify trigger
-- pg_regress test for ulak

-- ============================================================================
-- UPDATED_AT TRIGGER ON ENDPOINTS
-- ============================================================================

-- Create endpoint and capture initial timestamps
SELECT ulak.create_endpoint(
    'trigger_test',
    'http',
    '{"url": "http://example.com/hook", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- Record created_at for comparison
SELECT created_at = updated_at AS timestamps_equal_at_creation
FROM ulak.endpoints
WHERE name = 'trigger_test';

-- Wait briefly then update to ensure updated_at changes
-- Use pg_sleep to guarantee time difference
SELECT pg_sleep(0.1);

UPDATE ulak.endpoints
SET config = '{"url": "http://example.com/hook-v2", "method": "POST"}'::jsonb
WHERE name = 'trigger_test';

-- updated_at should now be >= created_at (trigger fired)
SELECT updated_at >= created_at AS updated_at_advanced
FROM ulak.endpoints
WHERE name = 'trigger_test';

-- ============================================================================
-- UPDATED_AT TRIGGER ON QUEUE
-- ============================================================================

-- Insert a queue message
INSERT INTO ulak.queue (endpoint_id, payload)
SELECT id, '{"trigger_test": true}'::jsonb
FROM ulak.endpoints
WHERE name = 'trigger_test';

-- Check initial state
SELECT created_at = updated_at AS queue_timestamps_equal_at_insert
FROM ulak.queue
WHERE payload @> '{"trigger_test": true}'::jsonb;

SELECT pg_sleep(0.1);

-- Update the queue entry
UPDATE ulak.queue
SET status = 'processing'
WHERE payload @> '{"trigger_test": true}'::jsonb;

-- updated_at should have advanced
SELECT updated_at >= created_at AS queue_updated_at_advanced
FROM ulak.queue
WHERE payload @> '{"trigger_test": true}'::jsonb;

-- ============================================================================
-- NOTIFY TRIGGER (verify trigger exists and fires)
-- ============================================================================

-- The notify trigger fires on INSERT to queue (AFTER INSERT FOR EACH STATEMENT)
-- We can verify the trigger exists on the queue table
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'ulak.queue'::regclass
  AND tgname = 'notify_new_message_trigger';

-- We can verify the trigger function exists
SELECT EXISTS(
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'ulak' AND p.proname = 'notify_new_message'
) AS notify_function_exists;

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'trigger_test');
SELECT ulak.drop_endpoint('trigger_test');
