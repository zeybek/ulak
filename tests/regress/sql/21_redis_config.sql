-- 21_redis_config.sql: Redis Streams endpoint configuration validation
-- pg_regress test for ulak
-- NOTE: Only runs when built with ENABLE_REDIS=1

-- ============================================================================
-- VALID REDIS CONFIGS
-- ============================================================================

-- Minimal valid Redis config (host + stream_key required)
SELECT ulak.create_endpoint(
    'redis_minimal',
    'redis',
    '{"host": "localhost", "stream_key": "test-stream"}'::jsonb
) IS NOT NULL AS minimal_created;

-- Redis config with all optional fields
SELECT ulak.create_endpoint(
    'redis_full',
    'redis',
    '{"host": "redis", "port": 6380, "db": 2, "stream_key": "events", "password": "secret", "connect_timeout": 10, "command_timeout": 60, "maxlen": 10000, "maxlen_approximate": true, "nomkstream": false}'::jsonb
) IS NOT NULL AS full_created;

-- Redis config with TLS
SELECT ulak.create_endpoint(
    'redis_tls',
    'redis',
    '{"host": "redis-tls", "stream_key": "secure-stream", "tls": true, "tls_ca_cert": "/certs/ca.pem", "tls_cert": "/certs/client.pem", "tls_key": "/certs/client-key.pem"}'::jsonb
) IS NOT NULL AS tls_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'host' AS host, config->>'stream_key' AS stream_key
FROM ulak.endpoints
WHERE name LIKE 'redis_%'
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR REDIS
-- ============================================================================

-- Valid minimal config
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost", "stream_key": "test"}'::jsonb
) AS valid_minimal;

-- Valid with port and db
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "redis", "port": 6379, "db": 0, "stream_key": "events"}'::jsonb
) AS valid_with_port;

-- ============================================================================
-- INVALID REDIS CONFIGS
-- ============================================================================

-- Missing host
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"stream_key": "no-host"}'::jsonb
) AS missing_host;

-- Missing stream_key
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost"}'::jsonb
) AS missing_stream_key;

-- Empty host
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "", "stream_key": "test"}'::jsonb
) AS empty_host;

-- Empty stream_key
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost", "stream_key": ""}'::jsonb
) AS empty_stream_key;

-- Invalid port (out of range)
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost", "stream_key": "test", "port": 99999}'::jsonb
) AS invalid_port;

-- Invalid db (out of range 0-15)
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost", "stream_key": "test", "db": 16}'::jsonb
) AS invalid_db;

-- Unknown config key
SELECT ulak.validate_endpoint_config(
    'redis',
    '{"host": "localhost", "stream_key": "test", "unknown_key": "value"}'::jsonb
) AS unknown_key;

-- ============================================================================
-- REDIS ENDPOINT CRUD OPERATIONS
-- ============================================================================

-- Alter endpoint config
SELECT ulak.alter_endpoint(
    'redis_minimal',
    '{"host": "new-redis", "stream_key": "new-stream", "port": 6380}'::jsonb
) AS altered;

-- Verify alter took effect
SELECT config->>'host' AS new_host, config->>'stream_key' AS new_stream
FROM ulak.endpoints
WHERE name = 'redis_minimal';

-- Enable/disable
SELECT ulak.disable_endpoint('redis_minimal') AS disabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'redis_minimal';

SELECT ulak.enable_endpoint('redis_minimal') AS enabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'redis_minimal';

-- ============================================================================
-- REDIS MESSAGE SEND (queue insertion)
-- ============================================================================

SELECT ulak.send('redis_full', '{"event": "order.created", "order_id": 42}'::jsonb) AS sent;

SELECT ulak.send_with_options(
    'redis_full',
    '{"event": "order.updated"}'::jsonb,
    p_priority := 5::smallint,
    p_ordering_key := 'order-42'
) IS NOT NULL AS sent_with_options;

SELECT count(*) AS redis_queue_count
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.protocol = 'redis';

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name LIKE 'redis_%'
);
SELECT ulak.drop_endpoint('redis_minimal');
SELECT ulak.drop_endpoint('redis_full');
SELECT ulak.drop_endpoint('redis_tls');
