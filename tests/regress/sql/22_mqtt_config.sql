-- 22_mqtt_config.sql: MQTT endpoint configuration validation
-- pg_regress test for ulak
-- NOTE: Only runs when built with ENABLE_MQTT=1

-- ============================================================================
-- VALID MQTT CONFIGS
-- ============================================================================

-- Minimal valid MQTT config (broker + topic required)
SELECT ulak.create_endpoint(
    'mqtt_minimal',
    'mqtt',
    '{"broker": "localhost", "topic": "test/topic"}'::jsonb
) IS NOT NULL AS minimal_created;

-- MQTT config with all optional fields
SELECT ulak.create_endpoint(
    'mqtt_full',
    'mqtt',
    '{"broker": "mqtt.example.com", "port": 8883, "topic": "events/orders", "qos": 1, "retain": true, "client_id": "pgx-test-01", "username": "user", "password": "secret", "clean_session": false}'::jsonb
) IS NOT NULL AS full_created;

-- MQTT config with TLS
SELECT ulak.create_endpoint(
    'mqtt_tls',
    'mqtt',
    '{"broker": "mqtt-tls", "topic": "secure/topic", "tls": true, "tls_ca_cert": "/certs/ca.pem", "tls_cert": "/certs/client.pem", "tls_key": "/certs/client-key.pem"}'::jsonb
) IS NOT NULL AS tls_created;

-- MQTT config with Last Will and Testament
SELECT ulak.create_endpoint(
    'mqtt_lwt',
    'mqtt',
    '{"broker": "localhost", "topic": "events/data", "will_topic": "events/status", "will_payload": "{\"status\": \"offline\"}", "will_qos": 1, "will_retain": true}'::jsonb
) IS NOT NULL AS lwt_created;

-- Verify configs stored correctly
SELECT name, protocol, config->>'broker' AS broker, config->>'topic' AS topic
FROM ulak.endpoints
WHERE name LIKE 'mqtt_%'
ORDER BY name;

-- ============================================================================
-- VALIDATE_ENDPOINT_CONFIG FOR MQTT
-- ============================================================================

-- Valid minimal config
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test"}'::jsonb
) AS valid_minimal;

-- Valid with QoS levels
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "qos": 0}'::jsonb
) AS valid_qos0;

SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "qos": 1}'::jsonb
) AS valid_qos1;

SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "qos": 2}'::jsonb
) AS valid_qos2;

-- Valid with nested options object
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "options": {"keepalive": 60}}'::jsonb
) AS valid_options_object;

-- ============================================================================
-- INVALID MQTT CONFIGS
-- ============================================================================

-- Topic with single-level wildcard (invalid for publish)
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "sensor/+/data"}'::jsonb
) AS wildcard_plus;

-- Topic with multi-level wildcard (invalid for publish)
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "sensor/#"}'::jsonb
) AS wildcard_hash;

-- Missing broker
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"topic": "no-broker"}'::jsonb
) AS missing_broker;

-- Missing topic
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost"}'::jsonb
) AS missing_topic;

-- Empty broker
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "", "topic": "test"}'::jsonb
) AS empty_broker;

-- Invalid qos range
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "qos": 3}'::jsonb
) AS invalid_qos_range;

-- Invalid qos type
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "qos": "1"}'::jsonb
) AS invalid_qos_type;

-- Invalid port range
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "port": 70000}'::jsonb
) AS invalid_port;

-- Invalid tls type
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "tls": "true"}'::jsonb
) AS invalid_tls_type;

-- Invalid options type
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "options": "bad"}'::jsonb
) AS invalid_options_type;

-- Invalid will_qos range
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "will_qos": 9}'::jsonb
) AS invalid_will_qos;

-- Unknown config key
SELECT ulak.validate_endpoint_config(
    'mqtt',
    '{"broker": "localhost", "topic": "test", "unknown_key": "value"}'::jsonb
) AS unknown_key;

-- ============================================================================
-- MQTT ENDPOINT CRUD OPERATIONS
-- ============================================================================

-- Alter endpoint config
SELECT ulak.alter_endpoint(
    'mqtt_minimal',
    '{"broker": "new-broker", "topic": "new/topic", "qos": 1}'::jsonb
) AS altered;

SELECT config->>'broker' AS new_broker, config->>'topic' AS new_topic
FROM ulak.endpoints
WHERE name = 'mqtt_minimal';

-- Enable/disable
SELECT ulak.disable_endpoint('mqtt_minimal') AS disabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'mqtt_minimal';

SELECT ulak.enable_endpoint('mqtt_minimal') AS enabled;
SELECT enabled FROM ulak.endpoints WHERE name = 'mqtt_minimal';

-- ============================================================================
-- MQTT MESSAGE SEND (queue insertion)
-- ============================================================================

SELECT ulak.send('mqtt_full', '{"event": "temperature", "value": 23.5}'::jsonb) AS sent;

SELECT count(*) AS mqtt_queue_count
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.protocol = 'mqtt';

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name LIKE 'mqtt_%'
);
SELECT ulak.drop_endpoint('mqtt_minimal');
SELECT ulak.drop_endpoint('mqtt_full');
SELECT ulak.drop_endpoint('mqtt_tls');
SELECT ulak.drop_endpoint('mqtt_lwt');
