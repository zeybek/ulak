-- 32_notify_config.sql: NOTIFY GUC + config change trigger
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- ENABLE_NOTIFY GUC EXISTS AND HAS DEFAULT
-- ============================================================================

-- GUC should exist and default to 'on'
SELECT name, setting
FROM pg_settings
WHERE name = 'ulak.enable_notify';

-- ============================================================================
-- ARCHIVE_PREMAKE_MONTHS GUC EXISTS AND HAS DEFAULT
-- ============================================================================

-- GUC should exist and default to '3'
SELECT name, setting
FROM pg_settings
WHERE name = 'ulak.archive_premake_months';

-- ============================================================================
-- CONFIG CHANGE TRIGGER EXISTS
-- ============================================================================

SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'ulak.endpoints'::regclass
  AND tgname = 'notify_endpoint_config_change';

-- Verify trigger function exists
SELECT EXISTS(
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'ulak' AND p.proname = 'notify_config_change'
) AS config_change_func_exists;

-- ============================================================================
-- NOTIFY TRIGGER CHECKS ulak.enable_notify
-- ============================================================================

-- Verify the trigger function body references ulak.enable_notify
SELECT prosrc LIKE '%ulak.enable_notify%' AS trigger_checks_enable_notify
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'ulak' AND p.proname = 'notify_new_message';

-- ============================================================================
-- NOTIFY TRIGGER CHECKS ulak.suppress_notify
-- ============================================================================

-- Verify the trigger function body references ulak.suppress_notify
SELECT prosrc LIKE '%ulak.suppress_notify%' AS trigger_checks_suppress_notify
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'ulak' AND p.proname = 'notify_new_message';

-- ============================================================================
-- SEND STILL WORKS (basic sanity with NOTIFY path)
-- ============================================================================

SELECT ulak.create_endpoint(
    'notify_test_ep',
    'http',
    '{"url": "http://localhost:9999/notify", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

SELECT ulak.send('notify_test_ep', '{"notify_test": true}'::jsonb);

-- Verify message was queued
SELECT count(*) = 1 AS message_queued
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'notify_test_ep';

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'notify_test_ep');
SELECT ulak.drop_endpoint('notify_test_ep');
