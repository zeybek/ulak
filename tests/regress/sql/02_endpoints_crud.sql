-- 02_endpoints_crud.sql: Endpoint CRUD — create, alter, drop, enable/disable
-- pg_regress test for ulak

-- ============================================================================
-- CREATE ENDPOINT
-- ============================================================================

-- Create an HTTP endpoint
SELECT ulak.create_endpoint(
    'crud_test_http',
    'http',
    '{"url": "http://example.com/hook", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- Verify it exists
SELECT name, protocol, enabled
FROM ulak.endpoints
WHERE name = 'crud_test_http';

-- Create a Kafka endpoint
SELECT ulak.create_endpoint(
    'crud_test_kafka',
    'kafka',
    '{"broker": "localhost:9092", "topic": "test-topic"}'::jsonb
) IS NOT NULL AS created;

-- ============================================================================
-- ALTER ENDPOINT
-- ============================================================================

-- Alter the HTTP endpoint config
SELECT ulak.alter_endpoint(
    'crud_test_http',
    '{"url": "http://example.com/hook-v2", "method": "PUT"}'::jsonb
);

-- Verify the config was updated
SELECT config->>'url' AS url, config->>'method' AS method
FROM ulak.endpoints
WHERE name = 'crud_test_http';

-- ============================================================================
-- DISABLE / ENABLE ENDPOINT
-- ============================================================================

-- Disable the endpoint
SELECT ulak.disable_endpoint('crud_test_http');

-- Verify disabled
SELECT name, enabled
FROM ulak.endpoints
WHERE name = 'crud_test_http';

-- Enable the endpoint
SELECT ulak.enable_endpoint('crud_test_http');

-- Verify enabled
SELECT name, enabled
FROM ulak.endpoints
WHERE name = 'crud_test_http';

-- ============================================================================
-- DROP ENDPOINT
-- ============================================================================

-- Drop the Kafka endpoint
SELECT ulak.drop_endpoint('crud_test_kafka');

-- Verify it's gone
SELECT EXISTS(
    SELECT 1 FROM ulak.endpoints WHERE name = 'crud_test_kafka'
) AS still_exists;

-- Clean up
SELECT ulak.drop_endpoint('crud_test_http');
