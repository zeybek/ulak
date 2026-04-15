-- 03_endpoints_validation.sql: Validation — invalid protocol, validate_endpoint_config
-- pg_regress test for ulak

-- ============================================================================
-- INVALID PROTOCOL
-- ============================================================================

-- Attempt to insert an endpoint with invalid protocol directly (should fail CHECK constraint)
-- Wrap in DO block to avoid non-deterministic DETAIL in output
DO $$
BEGIN
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('bad_proto', 'ftp', '{"host":"localhost"}'::jsonb);
EXCEPTION WHEN check_violation THEN
    RAISE NOTICE 'CHECK constraint violation: %', SQLERRM;
END
$$;

-- ============================================================================
-- DUPLICATE NAME
-- ============================================================================

-- Create a valid endpoint
SELECT ulak.create_endpoint(
    'validation_test',
    'http',
    '{"url": "http://example.com/hook", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- Attempt to create endpoint with same name (should fail unique constraint)
DO $$
BEGIN
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('validation_test', 'http', '{"url": "http://other.com"}'::jsonb);
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'UNIQUE constraint violation: %', SQLERRM;
END
$$;

-- ============================================================================
-- VALIDATE ENDPOINT CONFIG
-- ============================================================================

-- Valid HTTP config
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "method": "POST"}'::jsonb
) AS valid_http;

-- validate_endpoint_config is STRICT — NULL args return NULL (no error)
SELECT ulak.validate_endpoint_config(NULL, NULL) IS NULL AS null_returns_null;

-- ============================================================================
-- AUTO_DISABLE_ON_GONE CONFIG VALIDATION
-- ============================================================================

-- Valid: auto_disable_on_gone as boolean true
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "auto_disable_on_gone": true}'::jsonb
) AS valid_auto_disable;

-- Valid: auto_disable_on_gone as boolean false
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "auto_disable_on_gone": false}'::jsonb
) AS valid_auto_disable_false;

-- Invalid: auto_disable_on_gone as string (must be boolean)
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "auto_disable_on_gone": "yes"}'::jsonb
) AS invalid_auto_disable_string;

-- ============================================================================
-- ENABLE/DISABLE NONEXISTENT ENDPOINT
-- ============================================================================

-- enable_endpoint on nonexistent name should raise error
SELECT ulak.enable_endpoint('does_not_exist');

-- disable_endpoint on nonexistent name should raise error
SELECT ulak.disable_endpoint('does_not_exist');

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT ulak.drop_endpoint('validation_test');
