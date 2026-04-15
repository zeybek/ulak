-- 10_error_handling.sql: Error paths — null args, nonexistent endpoints, invalid inputs
-- pg_regress test for ulak

-- ============================================================================
-- SEND WITH NULL ARGS (STRICT C function returns NULL)
-- ============================================================================

-- send() is STRICT: NULL endpoint_name returns NULL without error
SELECT ulak.send(NULL, '{"test": true}'::jsonb) IS NULL AS send_null_endpoint;

-- send() is STRICT: NULL payload returns NULL without error
SELECT ulak.send('test_http_endpoint', NULL) IS NULL AS send_null_payload;

-- send() is STRICT: both NULL returns NULL without error
SELECT ulak.send(NULL, NULL) IS NULL AS send_both_null;

-- ============================================================================
-- DROP NONEXISTENT ENDPOINT (STRICT: NULL returns NULL)
-- ============================================================================

-- drop_endpoint is STRICT: NULL name returns NULL
SELECT ulak.drop_endpoint(NULL) IS NULL AS drop_null_returns_null;

-- ============================================================================
-- ENABLE/DISABLE NONEXISTENT ENDPOINT
-- ============================================================================

-- enable_endpoint raises exception for nonexistent endpoint
SELECT ulak.enable_endpoint('nonexistent_endpoint_xyz');

-- disable_endpoint raises exception for nonexistent endpoint
SELECT ulak.disable_endpoint('nonexistent_endpoint_xyz');

-- ============================================================================
-- INVALID CONSTRAINT VIOLATIONS
-- ============================================================================

-- NULL endpoint name should fail NOT NULL constraint
DO $$
BEGIN
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES (NULL, 'http', '{"url":"http://localhost"}'::jsonb);
EXCEPTION WHEN not_null_violation THEN
    RAISE NOTICE 'NOT NULL violation: %', SQLERRM;
END
$$;

-- NULL protocol should fail NOT NULL constraint
DO $$
BEGIN
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('err_test_ep', NULL, '{"url":"http://localhost"}'::jsonb);
EXCEPTION WHEN not_null_violation THEN
    RAISE NOTICE 'NOT NULL violation: %', SQLERRM;
END
$$;

-- NULL config should fail NOT NULL constraint
DO $$
BEGIN
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('err_test_ep', 'http', NULL);
EXCEPTION WHEN not_null_violation THEN
    RAISE NOTICE 'NOT NULL violation: %', SQLERRM;
END
$$;

-- NULL payload in queue should fail NOT NULL constraint
DO $$
DECLARE
    v_eid bigint;
BEGIN
    SELECT id INTO v_eid FROM ulak.endpoints WHERE name = 'test_http_endpoint';
    INSERT INTO ulak.queue (endpoint_id, payload)
    VALUES (v_eid, NULL);
EXCEPTION WHEN not_null_violation THEN
    RAISE NOTICE 'NOT NULL violation: %', SQLERRM;
END
$$;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG WITH NULL (STRICT: returns NULL)
-- ============================================================================

SELECT ulak.validate_endpoint_config(NULL, '{"url":"http://example.com"}'::jsonb) IS NULL AS validate_null_protocol;
SELECT ulak.validate_endpoint_config('http', NULL) IS NULL AS validate_null_config;

-- ============================================================================
-- QUEUE FOREIGN KEY VIOLATION
-- ============================================================================

-- Insert into queue with non-existent endpoint_id
DO $$
BEGIN
    INSERT INTO ulak.queue (endpoint_id, payload)
    VALUES (999999, '{"error_test": true}'::jsonb);
EXCEPTION WHEN foreign_key_violation THEN
    RAISE NOTICE 'FK violation: %', SQLERRM;
END
$$;
