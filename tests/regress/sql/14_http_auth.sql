-- 14_http_auth.sql: HTTP authentication configuration validation
-- pg_regress test for ulak

-- ============================================================================
-- BEARER AUTH: VALID CONFIG
-- ============================================================================

SELECT ulak.create_endpoint('auth_bearer', 'http', '{
    "url": "http://localhost:9999/bearer",
    "auth": {"type": "bearer", "token": "eyJhbGciOiJSUzI1NiJ9.test"}
}'::jsonb) IS NOT NULL AS bearer_ok;

-- ============================================================================
-- BASIC AUTH: VALID CONFIG
-- ============================================================================

SELECT ulak.create_endpoint('auth_basic', 'http', '{
    "url": "http://localhost:9999/basic",
    "auth": {"type": "basic", "username": "admin", "password": "secret123"}
}'::jsonb) IS NOT NULL AS basic_ok;

-- ============================================================================
-- OAUTH2: VALID CONFIG
-- ============================================================================

SELECT ulak.create_endpoint('auth_oauth2', 'http', '{
    "url": "http://localhost:9999/oauth",
    "auth": {
        "type": "oauth2",
        "token_url": "https://auth.example.com/oauth/token",
        "client_id": "my-client-id",
        "client_secret": "my-client-secret",
        "scope": "messages:write"
    }
}'::jsonb) IS NOT NULL AS oauth2_ok;

-- ============================================================================
-- AWS SIGV4: VALID CONFIG
-- ============================================================================

SELECT ulak.create_endpoint('auth_sigv4', 'http', '{
    "url": "http://localhost:9999/aws",
    "auth": {
        "type": "aws_sigv4",
        "access_key": "AKIAIOSFODNN7EXAMPLE",
        "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "us-east-1",
        "service": "sqs"
    }
}'::jsonb) IS NOT NULL AS sigv4_ok;

-- ============================================================================
-- AWS SIGV4 WITH SESSION TOKEN
-- ============================================================================

SELECT ulak.create_endpoint('auth_sigv4_sts', 'http', '{
    "url": "http://localhost:9999/aws-sts",
    "auth": {
        "type": "aws_sigv4",
        "access_key": "ASIATEMPORARY",
        "secret_key": "tempSecret",
        "region": "eu-west-1",
        "service": "lambda",
        "session_token": "FwoGZXIv..."
    }
}'::jsonb) IS NOT NULL AS sigv4_sts_ok;

-- ============================================================================
-- INVALID: MISSING REQUIRED FIELDS
-- ============================================================================

-- Bearer without token
SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"type": "bearer"}
}'::jsonb) AS bearer_no_token;

-- Basic without password
SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"type": "basic", "username": "admin"}
}'::jsonb) AS basic_no_password;

-- OAuth2 without client_secret
SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"type": "oauth2", "token_url": "https://auth.example.com/token", "client_id": "id"}
}'::jsonb) AS oauth2_no_secret;

-- SigV4 without region
SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"type": "aws_sigv4", "access_key": "AK", "secret_key": "SK", "service": "sqs"}
}'::jsonb) AS sigv4_no_region;

-- ============================================================================
-- INVALID: UNKNOWN AUTH TYPE
-- ============================================================================

SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"type": "kerberos"}
}'::jsonb) AS unknown_type;

-- ============================================================================
-- INVALID: MISSING AUTH TYPE
-- ============================================================================

SELECT ulak.validate_endpoint_config('http', '{
    "url": "http://localhost:9999/test",
    "auth": {"token": "something"}
}'::jsonb) AS missing_type;

-- ============================================================================
-- BACKWARD COMPATIBLE: NO AUTH KEY
-- ============================================================================

SELECT ulak.create_endpoint('no_auth_ep', 'http', '{
    "url": "http://localhost:9999/noauth"
}'::jsonb) IS NOT NULL AS no_auth_ok;

-- ============================================================================
-- AUTH + SIGNING SECRET (both work together)
-- ============================================================================

SELECT ulak.create_endpoint('auth_plus_signing', 'http', '{
    "url": "http://localhost:9999/both",
    "signing_secret": "whsec_test123",
    "auth": {"type": "bearer", "token": "my-token"}
}'::jsonb) IS NOT NULL AS auth_plus_signing_ok;

-- ============================================================================
-- AUTH + CLOUDEVENTS + RATE LIMIT (all features combined)
-- ============================================================================

SELECT ulak.create_endpoint('auth_all_features', 'http', '{
    "url": "http://localhost:9999/all",
    "auth": {"type": "basic", "username": "user", "password": "pass"},
    "cloudevents": true,
    "cloudevents_mode": "binary",
    "rate_limit": {"limit": 10, "interval": "second", "burst": 20}
}'::jsonb) IS NOT NULL AS all_features_ok;

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT ulak.drop_endpoint('auth_bearer');
SELECT ulak.drop_endpoint('auth_basic');
SELECT ulak.drop_endpoint('auth_oauth2');
SELECT ulak.drop_endpoint('auth_sigv4');
SELECT ulak.drop_endpoint('auth_sigv4_sts');
SELECT ulak.drop_endpoint('no_auth_ep');
SELECT ulak.drop_endpoint('auth_plus_signing');
SELECT ulak.drop_endpoint('auth_all_features');
