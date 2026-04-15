-- 23_amqp_config.sql: AMQP endpoint configuration validation
-- pg_regress test for ulak
-- NOTE: Only runs when built with ENABLE_AMQP=1

-- ============================================================================
-- VALID AMQP CONFIGS
-- ============================================================================

-- Minimal valid AMQP config
SELECT ulak.create_endpoint(
    'amqp_minimal',
    'amqp',
    '{"host": "localhost", "exchange": "events", "routing_key": "orders"}'::jsonb
) IS NOT NULL AS minimal_created;

-- Default exchange (empty string)
SELECT ulak.create_endpoint(
    'amqp_default_exchange',
    'amqp',
    '{"host": "localhost", "exchange": "", "routing_key": "my-queue"}'::jsonb
) IS NOT NULL AS default_exchange_created;

-- Full config
SELECT ulak.create_endpoint(
    'amqp_full',
    'amqp',
    '{"host": "rabbitmq", "port": 5672, "vhost": "/", "exchange": "events", "exchange_type": "topic", "routing_key": "orders.created", "username": "user", "password": "pass", "persistent": true, "mandatory": false}'::jsonb
) IS NOT NULL AS full_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'host' AS host, config->>'exchange' AS exchange
FROM ulak.endpoints
WHERE name LIKE 'amqp_%'
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR AMQP
-- ============================================================================

-- Valid config
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "events", "routing_key": "test"}'::jsonb
) AS valid_minimal;

-- Valid exchange types
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "e", "routing_key": "r", "exchange_type": "direct"}'::jsonb
) AS valid_direct;

SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "e", "routing_key": "r", "exchange_type": "fanout"}'::jsonb
) AS valid_fanout;

SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "e", "routing_key": "r", "exchange_type": "topic"}'::jsonb
) AS valid_topic;

-- ============================================================================
-- INVALID AMQP CONFIGS
-- ============================================================================

-- Missing host
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"exchange": "events", "routing_key": "test"}'::jsonb
) AS missing_host;

-- Missing exchange
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "routing_key": "test"}'::jsonb
) AS missing_exchange;

-- Missing routing_key
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "events"}'::jsonb
) AS missing_routing_key;

-- Invalid exchange_type
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "e", "routing_key": "r", "exchange_type": "invalid"}'::jsonb
) AS invalid_exchange_type;

-- Unknown config key
SELECT ulak.validate_endpoint_config(
    'amqp',
    '{"host": "localhost", "exchange": "e", "routing_key": "r", "unknown": "value"}'::jsonb
) AS unknown_key;

-- ============================================================================
-- AMQP ENDPOINT CRUD
-- ============================================================================

SELECT ulak.alter_endpoint(
    'amqp_minimal',
    '{"host": "new-host", "exchange": "new-exchange", "routing_key": "new-key"}'::jsonb
) AS altered;

SELECT config->>'host' AS new_host, config->>'exchange' AS new_exchange
FROM ulak.endpoints
WHERE name = 'amqp_minimal';

SELECT ulak.disable_endpoint('amqp_minimal') AS disabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'amqp_minimal';

SELECT ulak.enable_endpoint('amqp_minimal') AS enabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'amqp_minimal';

-- ============================================================================
-- AMQP MESSAGE SEND (queue insertion)
-- ============================================================================

SELECT ulak.send('amqp_full', '{"event": "order.created"}'::jsonb) AS sent;

SELECT count(*) AS amqp_queue_count
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.protocol = 'amqp';

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name LIKE 'amqp_%'
);
SELECT ulak.drop_endpoint('amqp_minimal');
SELECT ulak.drop_endpoint('amqp_default_exchange');
SELECT ulak.drop_endpoint('amqp_full');
