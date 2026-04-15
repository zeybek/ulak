-- 19_http_proxy.sql: HTTP proxy configuration validation
-- Tests proxy config parsing and validation in endpoint creation

-- ============================================================================
-- VALID PROXY CONFIGURATIONS
-- ============================================================================

-- Proxy with URL only (minimal config)
SELECT ulak.create_endpoint('proxy_basic', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://proxy.corp.com:8080"}
}'::jsonb) IS NOT NULL AS proxy_basic_ok;

-- Proxy with all options
SELECT ulak.create_endpoint('proxy_full', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {
        "url": "http://proxy.corp.com:8080",
        "type": "http",
        "username": "proxyuser",
        "password": "proxypass",
        "no_proxy": "*.internal,127.0.0.1",
        "ca_bundle": "/etc/ssl/proxy-ca.pem",
        "ssl_verify": true
    }
}'::jsonb) IS NOT NULL AS proxy_full_ok;

-- SOCKS5 proxy
SELECT ulak.create_endpoint('proxy_socks5', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "socks5://socks.corp.com:1080", "type": "socks5"}
}'::jsonb) IS NOT NULL AS proxy_socks5_ok;

-- HTTPS proxy with CA bundle
SELECT ulak.create_endpoint('proxy_https', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {
        "url": "https://secure-proxy.corp.com:8443",
        "type": "https",
        "ca_bundle": "/etc/ssl/proxy-ca.pem",
        "ssl_verify": true
    }
}'::jsonb) IS NOT NULL AS proxy_https_ok;

-- Proxy with ssl_verify disabled (for dev/test environments)
SELECT ulak.create_endpoint('proxy_noverify', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "https://dev-proxy.local:8080", "ssl_verify": false}
}'::jsonb) IS NOT NULL AS proxy_noverify_ok;

-- Proxy combined with auth and CloudEvents
SELECT ulak.create_endpoint('proxy_combined', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://proxy.corp.com:8080"},
    "auth": {"type": "bearer", "token": "my-token"},
    "cloudevents": true,
    "cloudevents_mode": "binary"
}'::jsonb) IS NOT NULL AS proxy_combined_ok;

-- ============================================================================
-- INVALID PROXY CONFIGURATIONS
-- ============================================================================

-- Proxy missing url (should fail)
SELECT ulak.create_endpoint('proxy_no_url', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"type": "http"}
}'::jsonb) IS NOT NULL AS proxy_no_url;

-- Proxy with bad scheme (ftp://)
SELECT ulak.create_endpoint('proxy_bad_scheme', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "ftp://proxy.com:8080"}
}'::jsonb) IS NOT NULL AS proxy_bad_scheme;

-- Proxy with username but no password
SELECT ulak.create_endpoint('proxy_missing_pass', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://proxy.com:8080", "username": "user"}
}'::jsonb) IS NOT NULL AS proxy_missing_pass;

-- Proxy with password but no username
SELECT ulak.create_endpoint('proxy_missing_user', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://proxy.com:8080", "password": "pass"}
}'::jsonb) IS NOT NULL AS proxy_missing_user;

-- Proxy with bad type
SELECT ulak.create_endpoint('proxy_bad_type', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://proxy.com:8080", "type": "ftp"}
}'::jsonb) IS NOT NULL AS proxy_bad_type;

-- Proxy with embedded credentials in URL (should fail)
SELECT ulak.create_endpoint('proxy_embedded_creds', 'http', '{
    "url": "http://localhost:9999/webhook",
    "proxy": {"url": "http://user:pass@proxy.com:8080"}
}'::jsonb) IS NOT NULL AS proxy_embedded_creds;

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT ulak.drop_endpoint('proxy_basic');
SELECT ulak.drop_endpoint('proxy_full');
SELECT ulak.drop_endpoint('proxy_socks5');
SELECT ulak.drop_endpoint('proxy_https');
SELECT ulak.drop_endpoint('proxy_noverify');
SELECT ulak.drop_endpoint('proxy_combined');
