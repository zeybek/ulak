-- 17_cloudevents.sql: CloudEvents endpoint configuration validation

-- ============================================================================
-- BINARY MODE
-- ============================================================================

SELECT ulak.create_endpoint('ce_binary', 'http', '{
    "url": "http://localhost:9999/ce-bin",
    "cloudevents": true,
    "cloudevents_mode": "binary"
}'::jsonb) IS NOT NULL AS binary_ok;

SELECT config->>'cloudevents' AS ce, config->>'cloudevents_mode' AS mode
FROM ulak.endpoints WHERE name = 'ce_binary';

-- ============================================================================
-- STRUCTURED MODE
-- ============================================================================

SELECT ulak.create_endpoint('ce_structured', 'http', '{
    "url": "http://localhost:9999/ce-struct",
    "cloudevents": true,
    "cloudevents_mode": "structured"
}'::jsonb) IS NOT NULL AS structured_ok;

-- ============================================================================
-- CUSTOM TYPE
-- ============================================================================

SELECT ulak.create_endpoint('ce_typed', 'http', '{
    "url": "http://localhost:9999/ce-typed",
    "cloudevents": true,
    "cloudevents_mode": "binary",
    "cloudevents_type": "com.myapp.order.created"
}'::jsonb) IS NOT NULL AS typed_ok;

SELECT config->>'cloudevents_type' AS ce_type
FROM ulak.endpoints WHERE name = 'ce_typed';

-- ============================================================================
-- CLOUDEVENTS DISABLED
-- ============================================================================

SELECT ulak.create_endpoint('ce_disabled', 'http', '{
    "url": "http://localhost:9999/ce-off",
    "cloudevents": false
}'::jsonb) IS NOT NULL AS disabled_ok;

-- ============================================================================
-- COMBINED: CE + SIGNING
-- ============================================================================

SELECT ulak.create_endpoint('ce_sign', 'http', '{
    "url": "http://localhost:9999/ce-sign",
    "cloudevents": true,
    "cloudevents_mode": "structured",
    "signing_secret": "whsec_test"
}'::jsonb) IS NOT NULL AS ce_signing_ok;

-- ============================================================================
-- COMBINED: CE + AUTH
-- ============================================================================

SELECT ulak.create_endpoint('ce_auth', 'http', '{
    "url": "http://localhost:9999/ce-auth",
    "cloudevents": true,
    "cloudevents_mode": "binary",
    "auth": {"type": "bearer", "token": "ce-token-123"}
}'::jsonb) IS NOT NULL AS ce_auth_ok;

-- ============================================================================
-- COMBINED: CE + SIGNING + AUTH (all three)
-- ============================================================================

SELECT ulak.create_endpoint('ce_all', 'http', '{
    "url": "http://localhost:9999/ce-all",
    "cloudevents": true,
    "cloudevents_mode": "binary",
    "cloudevents_type": "com.myapp.event",
    "signing_secret": "whsec_all",
    "auth": {"type": "basic", "username": "user", "password": "pass"}
}'::jsonb) IS NOT NULL AS ce_all_ok;

-- ============================================================================
-- VALIDATE
-- ============================================================================

SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/v",
    "cloudevents": true,
    "cloudevents_mode": "binary"
}'::jsonb) AS validate_binary;

SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/v",
    "cloudevents": true,
    "cloudevents_mode": "structured"
}'::jsonb) AS validate_structured;

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT ulak.drop_endpoint('ce_binary');
SELECT ulak.drop_endpoint('ce_structured');
SELECT ulak.drop_endpoint('ce_typed');
SELECT ulak.drop_endpoint('ce_disabled');
SELECT ulak.drop_endpoint('ce_sign');
SELECT ulak.drop_endpoint('ce_auth');
SELECT ulak.drop_endpoint('ce_all');
