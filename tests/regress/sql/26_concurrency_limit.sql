-- 26_concurrency_limit.sql: Per-endpoint concurrency limits
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint(
    'conc_test_ep',
    'http',
    '{"url": "http://localhost:9999/conc", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- ============================================================================
-- CONCURRENCY_LIMIT COLUMN EXISTS
-- ============================================================================

-- Default is NULL (unlimited)
SELECT concurrency_limit IS NULL AS default_is_null
FROM ulak.endpoints WHERE name = 'conc_test_ep';

-- ============================================================================
-- SET CONCURRENCY LIMIT
-- ============================================================================

UPDATE ulak.endpoints SET concurrency_limit = 5 WHERE name = 'conc_test_ep';

SELECT concurrency_limit = 5 AS limit_set
FROM ulak.endpoints WHERE name = 'conc_test_ep';

-- ============================================================================
-- CONSTRAINT: must be positive or NULL
-- ============================================================================

-- Zero should fail
DO $$
BEGIN
    UPDATE ulak.endpoints SET concurrency_limit = 0 WHERE name = 'conc_test_ep';
    RAISE EXCEPTION 'Should have raised check constraint violation';
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'zero_rejected: true';
END $$;

-- Negative should fail
DO $$
BEGIN
    UPDATE ulak.endpoints SET concurrency_limit = -1 WHERE name = 'conc_test_ep';
    RAISE EXCEPTION 'Should have raised check constraint violation';
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'negative_rejected: true';
END $$;

-- NULL is valid (unlimited)
UPDATE ulak.endpoints SET concurrency_limit = NULL WHERE name = 'conc_test_ep';

SELECT concurrency_limit IS NULL AS reset_to_null
FROM ulak.endpoints WHERE name = 'conc_test_ep';

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'conc_test_ep');
SELECT ulak.drop_endpoint('conc_test_ep');
