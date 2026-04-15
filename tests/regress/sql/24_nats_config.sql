-- 24_nats_config.sql: NATS endpoint configuration validation
-- pg_regress test for ulak
-- NOTE: Only runs when built with ENABLE_NATS=1

-- ============================================================================
-- VALID NATS CONFIGS
-- ============================================================================

-- Minimal valid NATS config (JetStream default)
SELECT ulak.create_endpoint(
    'nats_minimal',
    'nats',
    '{"url": "nats://localhost:4222", "subject": "orders.created"}'::jsonb
) IS NOT NULL AS minimal_created;

-- Core NATS (fire-and-forget)
SELECT ulak.create_endpoint(
    'nats_core',
    'nats',
    '{"url": "nats://localhost:4222", "subject": "events.raw", "jetstream": false}'::jsonb
) IS NOT NULL AS core_created;

-- Full JetStream config
SELECT ulak.create_endpoint(
    'nats_full',
    'nats',
    '{"url": "nats://nats:4222", "subject": "orders.new", "jetstream": true, "stream": "ORDERS", "username": "user", "password": "pass", "headers": {"X-Source": "ulak", "X-Version": "1"}}'::jsonb
) IS NOT NULL AS full_created;

-- Multi-server URL
SELECT ulak.create_endpoint(
    'nats_cluster',
    'nats',
    '{"url": "nats://s1:4222,nats://s2:4222,nats://s3:4222", "subject": "cluster.test"}'::jsonb
) IS NOT NULL AS cluster_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'url' AS url, config->>'subject' AS subject
FROM ulak.endpoints
WHERE name LIKE 'nats_%'
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR NATS
-- ============================================================================

-- Valid minimal
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test"}'::jsonb
) AS valid_minimal;

-- Valid with all options
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "jetstream": true, "stream": "TEST", "token": "secret", "tls": true, "headers": {"X-Key": "val"}, "options": {"max_reconnect": "60"}}'::jsonb
) AS valid_full;

-- Valid core NATS
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "jetstream": false}'::jsonb
) AS valid_core;

-- ============================================================================
-- INVALID NATS CONFIGS
-- ============================================================================

-- Subject with token wildcard (invalid for publish)
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "orders.*.created"}'::jsonb
) AS wildcard_star;

-- Subject with full wildcard (invalid for publish)
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "orders.>"}'::jsonb
) AS wildcard_gt;

-- Credentials file with directory traversal
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "credentials_file": "/tmp/../etc/passwd"}'::jsonb
) AS creds_traversal;

-- Credentials file with relative path
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "credentials_file": "my.creds"}'::jsonb
) AS creds_relative;

-- Credentials file with wrong extension
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "credentials_file": "/etc/passwd"}'::jsonb
) AS creds_wrong_ext;

-- Valid credentials file
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "credentials_file": "/opt/nats/user.creds"}'::jsonb
) AS creds_valid;

-- Missing url
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"subject": "test"}'::jsonb
) AS missing_url;

-- Missing subject
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222"}'::jsonb
) AS missing_subject;

-- Empty subject
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": ""}'::jsonb
) AS empty_subject;

-- Invalid jetstream type
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "jetstream": "true"}'::jsonb
) AS invalid_jetstream_type;

-- Invalid tls type
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "tls": "true"}'::jsonb
) AS invalid_tls_type;

-- Invalid headers type
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "headers": "bad"}'::jsonb
) AS invalid_headers_type;

-- Invalid options type
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "options": "bad"}'::jsonb
) AS invalid_options_type;

-- Unsupported option key
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "options": {"bad_option": "1"}}'::jsonb
) AS invalid_option_key;

-- Unsupported option value type
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "options": {"max_reconnect": 10}}'::jsonb
) AS invalid_option_value_type;

-- Invalid option integer string
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "options": {"max_reconnect": "abc"}}'::jsonb
) AS invalid_option_integer_string;

-- Unknown config key
SELECT ulak.validate_endpoint_config(
    'nats',
    '{"url": "nats://localhost:4222", "subject": "test", "unknown_key": "value"}'::jsonb
) AS unknown_key;

-- ============================================================================
-- ENDPOINT MANAGEMENT
-- ============================================================================

-- Alter endpoint
SELECT ulak.alter_endpoint(
    'nats_minimal',
    '{"url": "nats://newhost:4222", "subject": "orders.updated"}'::jsonb
) AS altered;

SELECT config->>'url' AS new_url, config->>'subject' AS new_subject
FROM ulak.endpoints WHERE name = 'nats_minimal';

-- Disable/Enable
SELECT ulak.disable_endpoint('nats_minimal') AS disabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'nats_minimal';

SELECT ulak.enable_endpoint('nats_minimal') AS enabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'nats_minimal';

-- Send message (will be pending — no actual NATS server for regression tests)
SELECT ulak.send('nats_minimal', '{"event": "test.nats"}'::jsonb) AS sent;

SELECT count(*) AS nats_queue_count
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'nats_minimal';

-- ============================================================================
-- CLEANUP
-- ============================================================================
DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name LIKE 'nats_%'
);
SELECT ulak.drop_endpoint('nats_minimal') AS drop1;
SELECT ulak.drop_endpoint('nats_core') AS drop2;
SELECT ulak.drop_endpoint('nats_full') AS drop3;
SELECT ulak.drop_endpoint('nats_cluster') AS drop4;
