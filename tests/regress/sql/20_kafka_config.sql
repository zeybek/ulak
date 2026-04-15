-- 20_kafka_config.sql: Kafka endpoint configuration validation
-- pg_regress test for ulak
-- NOTE: Only runs when built with ENABLE_KAFKA=1

-- ============================================================================
-- VALID KAFKA CONFIGS
-- ============================================================================

-- Minimal valid Kafka config (broker + topic required)
SELECT ulak.create_endpoint(
    'kafka_minimal',
    'kafka',
    '{"broker": "localhost:9092", "topic": "test-topic"}'::jsonb
) IS NOT NULL AS minimal_created;

-- Kafka config with all optional fields
SELECT ulak.create_endpoint(
    'kafka_full',
    'kafka',
    '{"broker": "broker1:9092,broker2:9092", "topic": "events", "key": "static-key", "partition": 0, "headers": {"ce_type": "test.event", "x-source": "ulak"}}'::jsonb
) IS NOT NULL AS full_created;

-- Kafka config with librdkafka options
SELECT ulak.create_endpoint(
    'kafka_options',
    'kafka',
    '{"broker": "localhost:9092", "topic": "opts-topic", "options": {"compression.codec": "lz4", "linger.ms": "10", "batch.num.messages": "1000"}}'::jsonb
) IS NOT NULL AS options_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'broker' AS broker, config->>'topic' AS topic
FROM ulak.endpoints
WHERE name LIKE 'kafka_%'
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR KAFKA
-- ============================================================================

-- Valid Kafka config passes validation
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "localhost:9092", "topic": "test"}'::jsonb
) AS valid_minimal;

-- Multiple brokers
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "b1:9092,b2:9092,b3:9092", "topic": "multi-broker"}'::jsonb
) AS valid_multi_broker;

-- With partition and key
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "localhost:9092", "topic": "keyed", "key": "my-key", "partition": 2}'::jsonb
) AS valid_with_key;

-- ============================================================================
-- INVALID KAFKA CONFIGS
-- ============================================================================

-- Missing broker
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"topic": "no-broker"}'::jsonb
) AS missing_broker;

-- Missing topic
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "localhost:9092"}'::jsonb
) AS missing_topic;

-- Empty broker
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "", "topic": "test"}'::jsonb
) AS empty_broker;

-- Empty topic
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "localhost:9092", "topic": ""}'::jsonb
) AS empty_topic;

-- Unknown config key (strict validation)
SELECT ulak.validate_endpoint_config(
    'kafka',
    '{"broker": "localhost:9092", "topic": "test", "unknown_key": "value"}'::jsonb
) AS unknown_key;

-- ============================================================================
-- KAFKA ENDPOINT CRUD OPERATIONS
-- ============================================================================

-- Alter endpoint config
SELECT ulak.alter_endpoint(
    'kafka_minimal',
    '{"broker": "new-broker:9092", "topic": "new-topic"}'::jsonb
) AS altered;

-- Verify alter took effect
SELECT config->>'broker' AS new_broker, config->>'topic' AS new_topic
FROM ulak.endpoints
WHERE name = 'kafka_minimal';

-- Enable/disable
SELECT ulak.disable_endpoint('kafka_minimal') AS disabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'kafka_minimal';

SELECT ulak.enable_endpoint('kafka_minimal') AS enabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'kafka_minimal';

-- ============================================================================
-- KAFKA MESSAGE SEND (queue insertion only — actual dispatch needs running broker)
-- ============================================================================

-- Send single message to Kafka endpoint
SELECT ulak.send('kafka_full', '{"event": "order.created", "order_id": 42}'::jsonb) AS sent;

-- Send with options (priority, ordering key)
SELECT ulak.send_with_options(
    'kafka_full',
    '{"event": "order.updated", "order_id": 42}'::jsonb,
    p_priority := 5::smallint,
    p_ordering_key := 'order-42'
) IS NOT NULL AS sent_with_options;

-- Verify messages in queue
SELECT count(*) AS kafka_queue_count
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.protocol = 'kafka';

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name LIKE 'kafka_%'
);
SELECT ulak.drop_endpoint('kafka_minimal');
SELECT ulak.drop_endpoint('kafka_full');
SELECT ulak.drop_endpoint('kafka_options');
