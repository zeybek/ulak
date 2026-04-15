-- 07_http_config.sql: HTTP endpoint configuration validation
-- pg_regress test for ulak

-- ============================================================================
-- VALID HTTP CONFIGS
-- ============================================================================

-- Minimal valid HTTP config
SELECT ulak.create_endpoint(
    'http_minimal',
    'http',
    '{"url": "http://example.com/webhook", "method": "POST"}'::jsonb
) IS NOT NULL AS minimal_created;

-- HTTP config with all optional fields
SELECT ulak.create_endpoint(
    'http_full',
    'http',
    '{"url": "https://api.example.com/events", "method": "POST", "timeout": 30, "headers": {"Authorization": "Bearer token123"}, "ssl_verify": true}'::jsonb
) IS NOT NULL AS full_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'url' AS url, config->>'method' AS method
FROM ulak.endpoints
WHERE name IN ('http_minimal', 'http_full')
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR HTTP
-- ============================================================================

-- Valid HTTP config passes validation
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "method": "POST"}'::jsonb
) AS valid_post;

-- Different HTTP methods
SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "method": "GET"}'::jsonb
) AS valid_get;

SELECT ulak.validate_endpoint_config(
    'http',
    '{"url": "http://example.com", "method": "PUT"}'::jsonb
) AS valid_put;

-- ============================================================================
-- HTTP ENDPOINT ALTER
-- ============================================================================

-- Alter HTTP endpoint config
SELECT ulak.alter_endpoint(
    'http_minimal',
    '{"url": "http://example.com/webhook-v2", "method": "PUT", "timeout": 60}'::jsonb
);

-- Verify altered config
SELECT config->>'url' AS url, config->>'method' AS method, config->>'timeout' AS timeout
FROM ulak.endpoints
WHERE name = 'http_minimal';

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT ulak.drop_endpoint('http_minimal');
SELECT ulak.drop_endpoint('http_full');
