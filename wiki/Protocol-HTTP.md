# Protocol: HTTP

HTTP is the built-in protocol dispatcher -- always available with no compile-time flags required. It is the most feature-rich dispatcher in ulak, supporting synchronous and batch dispatch, five authentication methods, TLS/mTLS with certificate pinning, proxy tunneling, CloudEvents envelopes, Standard Webhooks HMAC signing, response body capture, and comprehensive SSRF protection.

---

## Quick Example

```sql
-- Create an HTTP endpoint
SELECT ulak.create_endpoint('my-webhook', 'http', '{
  "url": "https://api.example.com/webhooks",
  "method": "POST",
  "headers": {"X-Source": "ulak"}
}'::jsonb);

-- Send a message (atomically with your business transaction)
BEGIN;
  INSERT INTO orders (id, total) VALUES (1, 99.99);
  SELECT ulak.send('my-webhook', '{"event": "order.created", "order_id": 1}'::jsonb);
COMMIT;
```

The background worker picks up the message, dispatches it to the configured URL, and manages retries, circuit breaking, and dead-letter queueing automatically.

---

## Endpoint Configuration

The second argument to `create_endpoint` is the protocol name (`'http'`), and the third is a JSONB object containing all endpoint settings. Here is a comprehensive example showing every available field:

```json
{
  "url": "https://api.example.com/webhooks",
  "method": "POST",
  "headers": {
    "X-Source": "ulak",
    "X-Environment": "production"
  },
  "timeout": 10,
  "connect_timeout": 5,
  "signing_secret": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
  "cloudevents": true,
  "cloudevents_mode": "binary",
  "cloudevents_type": "com.myapp.order.created",
  "auth": {
    "type": "oauth2",
    "token_url": "https://auth.example.com/oauth/token",
    "client_id": "my_client_id",
    "client_secret": "my_client_secret",
    "scope": "events:write"
  },
  "tls_client_cert": "/etc/ssl/client/cert.pem",
  "tls_client_key": "/etc/ssl/client/key.pem",
  "tls_ca_bundle": "/etc/ssl/certs/custom-ca.pem",
  "tls_pinned_public_key": "sha256//YhKJG3Wk3ZSlFz3Oqb2HBKZG89bBIxSjDBG/A+2xNFQ=",
  "proxy": {
    "url": "http://proxy.example.com:8080",
    "type": "http",
    "username": "proxy_user",
    "password": "proxy_pass",
    "no_proxy": "localhost,127.0.0.1",
    "ca_bundle": "/etc/ssl/certs/proxy-ca.pem",
    "ssl_verify": true
  },
  "auto_disable_on_gone": true,
  "rate_limit": 100
}
```

### Field Reference

#### Required

| Field | Type | Description |
|-------|------|-------------|
| `url` | string | Target URL. Must use `http://` or `https://` scheme. Validated against SSRF blocklist at creation time. |

#### Optional

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `method` | string | `"POST"` | HTTP method: `GET`, `POST`, `PUT`, `PATCH`, or `DELETE`. |
| `headers` | object | `{"Content-Type": "application/json"}` | Static headers added to every request. Header names and values are validated against CR/LF injection. |
| `timeout` | number (1-300) | GUC `ulak.http_timeout` (10) | Request timeout in seconds. Overrides the global GUC for this endpoint. |
| `connect_timeout` | number (1-60) | GUC `ulak.http_connect_timeout` (5) | Connection timeout in seconds. Overrides the global GUC for this endpoint. |
| `signing_secret` | string | -- | HMAC key for [Standard Webhooks](https://www.standardwebhooks.com/) signing. When set, every request includes `webhook-id`, `webhook-timestamp`, and `webhook-signature` headers. |
| `cloudevents` | boolean | `false` | Enable CloudEvents v1.0 envelope. |
| `cloudevents_mode` | string | `"binary"` | `"binary"` (ce-* headers) or `"structured"` (JSON envelope). Only used when `cloudevents` is `true`. |
| `cloudevents_type` | string | `"ulak.message"` | CloudEvents `type` attribute value. |
| `auth` | object | -- | Authentication configuration. See [Authentication](#authentication). |
| `tls_client_cert` | string | -- | Path to client certificate PEM file for mutual TLS. |
| `tls_client_key` | string | -- | Path to client private key PEM file for mutual TLS. |
| `tls_ca_bundle` | string | -- | Path to custom CA bundle PEM file. Overrides the system CA store. |
| `tls_pinned_public_key` | string | -- | Certificate pin in `sha256//base64hash=` format. Rejects connections if the server certificate does not match. |
| `proxy` | object | -- | Proxy configuration. See [Proxy Support](#proxy-support). |
| `auto_disable_on_gone` | boolean | `false` | Automatically disable the endpoint when an HTTP 410 Gone response is received. |
| `rate_limit` | number | -- | Maximum requests per second for this endpoint (token bucket in shared memory). |

Unknown configuration keys are rejected with an error listing all valid keys. This prevents silent misconfiguration from typos.

---

## Authentication

ulak supports five authentication methods for HTTP endpoints. The first four are configured via the `auth` object in endpoint config. The fifth (API Key) uses static headers.

### Basic Authentication

```sql
SELECT ulak.create_endpoint('basic-api', 'http', '{
  "url": "https://api.example.com/events",
  "auth": {
    "type": "basic",
    "username": "api_user",
    "password": "api_secret"
  }
}'::jsonb);
```

Sends credentials via `CURLOPT_USERPWD` with `CURLAUTH_BASIC`. The `username:password` string is zeroed with `explicit_bzero()` immediately after libcurl copies it internally.

**Required fields:** `type`, `username`, `password`.

### Bearer Token

```sql
SELECT ulak.create_endpoint('bearer-api', 'http', '{
  "url": "https://api.example.com/events",
  "auth": {
    "type": "bearer",
    "token": "eyJhbGciOiJSUzI1NiIs..."
  }
}'::jsonb);
```

Appends an `Authorization: Bearer <token>` header. The header string is zeroed after being passed to libcurl.

**Required fields:** `type`, `token`.

### OAuth2 Client Credentials

```sql
SELECT ulak.create_endpoint('oauth2-api', 'http', '{
  "url": "https://api.example.com/events",
  "auth": {
    "type": "oauth2",
    "token_url": "https://auth.example.com/oauth/token",
    "client_id": "my_client_id",
    "client_secret": "my_client_secret",
    "scope": "events:write"
  }
}'::jsonb);
```

Implements the OAuth2 client-credentials flow. Tokens are fetched synchronously during dispatch and cached per-worker in memory.

| Behavior | Detail |
|----------|--------|
| Token caching | Cached per-worker with a 60-second safety buffer before expiry |
| Token URL validation | SSRF check applied to `token_url` |
| Token URL TLS | Always `SSL_VERIFYPEER=1`, `SSL_VERIFYHOST=2` |
| Token URL redirects | Disabled |
| Token URL timeout | 10 seconds |
| Response size cap | 64 KB maximum |
| Token invalidation | Automatic on HTTP 401; token zeroed with `explicit_bzero()` and re-fetched |
| POST body | Zeroed immediately after token request completes |
| Response data | Zeroed before freeing regardless of outcome |

**Required fields:** `type`, `token_url`, `client_id`, `client_secret`.
**Optional fields:** `scope`.

### AWS Signature V4

```sql
SELECT ulak.create_endpoint('aws-api', 'http', '{
  "url": "https://execute-api.us-east-1.amazonaws.com/prod/events",
  "auth": {
    "type": "aws_sigv4",
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "region": "us-east-1",
    "service": "execute-api",
    "session_token": "FwoGZXIvYXdzE..."
  }
}'::jsonb);
```

Uses libcurl native AWS SigV4 support. The `CURLOPT_AWS_SIGV4` option is set to `"aws:amz:{region}:{service}"`.

> **Note:** Requires libcurl >= 7.75.0. The optional `session_token` supports AWS STS temporary credentials via the `x-amz-security-token` header. For production workloads, prefer STS temporary credentials over long-lived access keys.

**Required fields:** `type`, `access_key`, `secret_key`, `region`, `service`.
**Optional fields:** `session_token`.

### API Key (via Headers)

For services that authenticate via a custom header, use the `headers` field directly:

```sql
SELECT ulak.create_endpoint('apikey-endpoint', 'http', '{
  "url": "https://api.example.com/events",
  "headers": {
    "X-API-Key": "your-api-key-here"
  }
}'::jsonb);
```

No `auth` object is needed. The header is included on every request.

---

## TLS/SSL Configuration

ulak defaults to secure TLS settings. Both peer certificate verification and hostname verification are enabled out of the box.

| Feature | GUC / Config | Default | Description |
|---------|-------------|---------|-------------|
| Peer verification | `ulak.http_ssl_verify_peer` | `true` | Validates server certificate against CA bundle |
| Host verification | `ulak.http_ssl_verify_host` | `true` | Checks certificate CN/SAN matches hostname |
| Mutual TLS (mTLS) | `tls_client_cert` + `tls_client_key` | -- | Client cert + key for two-way authentication |
| Custom CA bundle | `tls_ca_bundle` | -- | Path to PEM file, overrides system CA store |
| Certificate pinning | `tls_pinned_public_key` | -- | `sha256//base64hash=` format, rejects mismatched certs |
| Redirect restriction | -- | HTTPS only | Redirects (when enabled) only follow HTTPS destinations |

### mTLS Example

```sql
SELECT ulak.create_endpoint('mtls-webhook', 'http', '{
  "url": "https://secure.example.com/webhook",
  "tls_client_cert": "/etc/ssl/client/cert.pem",
  "tls_client_key": "/etc/ssl/client/key.pem",
  "tls_ca_bundle": "/etc/ssl/certs/custom-ca.pem"
}'::jsonb);
```

### Certificate Pinning Example

```sql
SELECT ulak.create_endpoint('pinned-webhook', 'http', '{
  "url": "https://critical.example.com/webhook",
  "tls_pinned_public_key": "sha256//YhKJG3Wk3ZSlFz3Oqb2HBKZG89bBIxSjDBG/A+2xNFQ="
}'::jsonb);
```

Certificate pinning defends against CA compromise by rejecting any server certificate whose public key hash does not match the configured pin.

> **Warning:** Never set `ulak.http_ssl_verify_peer` or `ulak.http_ssl_verify_host` to `false` in production. TLS private key files should have restrictive permissions (`chmod 600`) and be readable only by the `postgres` OS user.

---

## Proxy Support

HTTP endpoints can be routed through a proxy server by adding a `proxy` object to the endpoint configuration.

```sql
SELECT ulak.create_endpoint('proxied-webhook', 'http', '{
  "url": "https://api.example.com/events",
  "proxy": {
    "url": "http://proxy.example.com:8080",
    "type": "http",
    "username": "proxy_user",
    "password": "proxy_pass",
    "no_proxy": "localhost,127.0.0.1",
    "ca_bundle": "/etc/ssl/certs/proxy-ca.pem",
    "ssl_verify": true
  }
}'::jsonb);
```

### Proxy Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | Yes | -- | Proxy URL. Must not contain embedded credentials (use separate `username`/`password` fields). |
| `type` | string | No | `"http"` | Proxy type: `http`, `https`, or `socks5`. |
| `username` | string | No | -- | Proxy auth username. Must be paired with `password`. |
| `password` | string | No | -- | Proxy auth password. Must be paired with `username`. Zeroed with `explicit_bzero()` on cleanup. |
| `no_proxy` | string | No | -- | Comma-separated hostnames to bypass the proxy (maps to `CURLOPT_NOPROXY`). |
| `ca_bundle` | string | No | -- | CA bundle path for verifying the proxy TLS certificate. |
| `ssl_verify` | boolean | No | `true` | Verify the proxy server TLS certificate. |

Proxy authentication supports both Basic and Digest methods. The `username` and `password` fields must be both present or both absent.

---

## CloudEvents

ulak supports [CloudEvents v1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md) in both binary and structured content modes. Enable CloudEvents by setting `"cloudevents": true` in the endpoint config.

### Binary Content Mode (Default)

In binary mode, CloudEvents attributes are added as HTTP headers prefixed with `ce-`. The message payload is sent unchanged.

```sql
SELECT ulak.create_endpoint('ce-binary', 'http', '{
  "url": "https://api.example.com/events",
  "cloudevents": true,
  "cloudevents_mode": "binary",
  "cloudevents_type": "com.myapp.order.created"
}'::jsonb);
```

The following headers are added to every request:

| Header | Example Value |
|--------|---------------|
| `ce-specversion` | `1.0` |
| `ce-id` | `msg_12345` |
| `ce-type` | `com.myapp.order.created` |
| `ce-source` | `/ulak` (configurable via GUC `ulak.cloudevents_source`) |
| `ce-time` | `2026-04-14T10:30:00Z` |

The payload body is sent as-is:

```json
{"order_id": 123, "total": 49.99}
```

### Structured Content Mode

In structured mode, the payload is wrapped in a CloudEvents JSON envelope. The HTTP body becomes:

```sql
SELECT ulak.create_endpoint('ce-structured', 'http', '{
  "url": "https://api.example.com/events",
  "cloudevents": true,
  "cloudevents_mode": "structured",
  "cloudevents_type": "com.myapp.order.created"
}'::jsonb);
```

Resulting HTTP body:

```json
{
  "specversion": "1.0",
  "id": "msg_12345",
  "type": "com.myapp.order.created",
  "source": "/ulak",
  "time": "2026-04-14T10:30:00Z",
  "datacontenttype": "application/json",
  "data": {"order_id": 123, "total": 49.99}
}
```

> **Note:** When both CloudEvents and webhook signing are enabled, the payload is wrapped first (structured mode), then the signature is computed over the final payload. This ensures the receiver can verify the signature against the body it receives.

---

## Webhook Signing (Standard Webhooks)

ulak implements the [Standard Webhooks](https://www.standardwebhooks.com/) specification for HMAC-SHA256 webhook signing. When a `signing_secret` is configured, every outgoing request includes three headers:

| Header | Value |
|--------|-------|
| `webhook-id` | `msg_{message_id}` |
| `webhook-timestamp` | Unix epoch seconds |
| `webhook-signature` | `v1,{base64(HMAC-SHA256(secret, "{id}.{ts}.{body}"))}` |

### Configuration

```sql
SELECT ulak.create_endpoint('signed-webhook', 'http', '{
  "url": "https://api.example.com/webhook",
  "signing_secret": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
}'::jsonb);
```

The HMAC result is zeroed with `explicit_bzero()` immediately after base64 encoding.

### Receiver-Side Verification

Consumers should verify the signature to confirm message authenticity and integrity. Here is a Python example:

```python
import hmac, hashlib, base64, time

TOLERANCE_SECONDS = 300  # 5-minute replay window

def verify_webhook(payload: bytes, headers: dict, secret: bytes) -> bool:
    msg_id = headers["webhook-id"]
    timestamp = headers["webhook-timestamp"]
    signatures = headers["webhook-signature"]

    # 1. Check timestamp tolerance (replay protection)
    ts = int(timestamp)
    if abs(time.time() - ts) > TOLERANCE_SECONDS:
        return False  # Possible replay attack

    # 2. Reconstruct the signed content
    sign_content = f"{msg_id}.{timestamp}.{payload.decode()}".encode()

    # 3. Compute expected HMAC-SHA256
    expected = hmac.new(secret, sign_content, hashlib.sha256).digest()
    expected_b64 = base64.b64encode(expected).decode()

    # 4. Compare against each provided signature (v1 prefix)
    for sig in signatures.split(" "):
        if sig.startswith("v1,"):
            received_b64 = sig[3:]
            if hmac.compare_digest(expected_b64, received_b64):
                return True

    return False
```

Key verification steps:

1. **Timestamp tolerance** -- reject messages older than 5 minutes to prevent replay attacks.
2. **Reconstruct signed content** -- concatenate `{msg_id}.{timestamp}.{body}` exactly as ulak signs it.
3. **HMAC-SHA256** -- compute with the shared secret.
4. **Constant-time comparison** -- use `hmac.compare_digest()` (or equivalent) to prevent timing attacks.

---

## Dispatch Modes

The HTTP dispatcher supports two modes of operation. The worker selects the mode automatically based on endpoint capabilities and configuration.

### Synchronous Dispatch

Single-message dispatch that blocks until the HTTP response is received. Used when response capture is enabled (`ulak.capture_response = true`) or as a fallback.

- Reuses a persistent `curl_easy` handle across requests.
- `curl_easy_reset()` preserves the connection cache, DNS cache, and TLS session cache between requests, avoiding repeated TCP/TLS handshakes.
- HTTP/2 over TLS is negotiated automatically when the server supports it.

### Batch Dispatch

High-throughput mode that enqueues messages with `produce()` and executes them concurrently with `flush()` using `curl_multi`.

- **Phase 1 (produce):** Each message is added to the batch buffer with its own curl easy handle. No network I/O occurs.
- **Phase 2 (flush):** All pending requests are executed concurrently via `curl_multi_perform()` and `curl_multi_poll()`. The flush blocks until all requests complete or the timeout expires.
- **Phase 3 (collect):** Failed message IDs and error strings are collected for retry classification.

Batch mode features:

| Feature | Detail |
|---------|--------|
| HTTP/2 multiplexing | Enabled via `ulak.http_enable_pipelining` (default `true`). Multiple requests share a single TCP connection. |
| Connection pooling | Per-host and total connection limits prevent resource exhaustion. |
| Capacity | Configurable via `ulak.http_batch_capacity` (default 200, max 1000). |
| Timeout | Configurable via `ulak.http_flush_timeout` (default 30000ms). Uses `CLOCK_MONOTONIC` for accurate wall-clock timing. |
| Retry-After | Not parsed in batch mode. Batch prioritizes throughput; per-request `Retry-After` is only available in sync mode. |

---

## Response Capture

When enabled, ulak captures the HTTP response from each dispatch and stores it alongside the message in the queue.

### Enabling Response Capture

```sql
ALTER SYSTEM SET ulak.capture_response = true;
SELECT pg_reload_conf();
```

When `capture_response` is `true`, the worker uses synchronous dispatch (`dispatch_ex`) instead of batch mode so that each response can be individually captured.

### Captured Data

| Field | Description |
|-------|-------------|
| HTTP status code | The response status (e.g., 200, 404, 500). |
| Response body | Capped at `ulak.response_body_max_size` (default 64 KB). Data beyond the limit is consumed but not stored. |
| Content type | The `Content-Type` header from the response. |

### Querying Responses

```sql
SELECT id, status, response
FROM ulak.queue
WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'my-webhook')
ORDER BY id DESC
LIMIT 5;
```

The `response` column is a JSONB value containing `http_status_code`, `http_response_body`, `http_content_type`, and `http_response_size`.

---

## HTTP Status Code Handling

The HTTP dispatcher classifies response status codes into distinct categories that determine retry behavior:

| Status | Behavior | Error Prefix | Notes |
|--------|----------|--------------|-------|
| 200-299 | Success | -- | Message marked as `completed`. |
| 429 | Rate limited | `[RETRYABLE]` | `Retry-After` header parsed (sync mode only). |
| 410 | Gone | `[PERMANENT][DISABLE]` | Endpoint auto-disabled if `auto_disable_on_gone` is set. Message sent to DLQ. |
| 400-499 (other) | Client error | `[PERMANENT]` | No retry. Message sent to DLQ after max retries. |
| 500-599 | Server error | `[RETRYABLE]` | `Retry-After` header parsed for 503 (sync mode only). |
| Network/DNS errors | Connection failure | `[RETRYABLE]` | Includes timeouts, DNS failures, TLS errors. |
| Proxy errors | Proxy failure | `[RETRYABLE]` or `[PERMANENT]` | DNS resolution failures are retryable; handshake/auth failures are permanent. |

The `[RETRYABLE]` prefix causes the message to return to `pending` status with `retry_count` incremented and `next_retry_at` calculated using the endpoint's backoff strategy. The `[PERMANENT]` prefix causes the message to fail immediately (no further retries). See [Reliability](Reliability) for backoff strategy details.

---

## Per-Message Overrides (dispatch_ex)

The extended dispatch path (`dispatch_ex`) supports per-message header overrides and metadata. These are set via the `headers` and `metadata` columns on the `ulak.queue` table, typically populated through `send_with_options()`.

### Header Overrides

Per-message headers are provided as a JSONB object. They override endpoint-level headers with the same key name:

```sql
SELECT ulak.send_with_options(
  'my-webhook',
  '{"event": "order.created"}'::jsonb,
  5,           -- priority
  NULL,        -- scheduled_at
  NULL,        -- idempotency_key
  NULL,        -- correlation_id
  NULL,        -- expires_at
  NULL         -- ordering_key
);
```

When per-message headers are present, the merge logic is:

1. Start with default `Content-Type: application/json`.
2. Add endpoint-level headers, skipping any that are overridden by per-message headers.
3. Add per-message headers (these win on conflicts).

All header names and values are validated against CR/LF injection. Headers containing `\r` or `\n` characters are silently skipped with a warning.

### Metadata Overrides

The `metadata` JSONB column supports two special keys:

| Key | Type | Description |
|-----|------|-------------|
| `timeout` | number | Override the request timeout (in seconds) for this specific message. |
| `url_suffix` | string | Append a path suffix to the endpoint's base URL. SSRF-protected: suffixes containing `://` or `@` are rejected, and the concatenated URL is validated against the internal IP blocklist. |

Example metadata usage:

```json
{
  "timeout": 30,
  "url_suffix": "/v2/orders/123"
}
```

If the endpoint URL is `https://api.example.com`, the final request URL becomes `https://api.example.com/v2/orders/123`.

---

## SSRF Protection

Server-Side Request Forgery (SSRF) is the primary network-level threat for any extension that makes outbound HTTP requests from within a database. ulak blocks requests to internal and private network addresses by default.

### Blocked IP Ranges

| Range | Type | Notable Targets |
|-------|------|-----------------|
| `127.0.0.0/8` | Loopback | localhost services |
| `10.0.0.0/8` | RFC 1918 Private | Internal VPC hosts |
| `172.16.0.0/12` | RFC 1918 Private | Docker default networks |
| `192.168.0.0/16` | RFC 1918 Private | Local network hosts |
| `169.254.0.0/16` | Link-Local | AWS metadata (`169.254.169.254`), Azure IMDS |
| `0.0.0.0` | Unspecified | Bind-all listeners |
| `::1` | IPv6 Loopback | localhost (IPv6) |
| `fe80::/10` | IPv6 Link-Local | Neighbor discovery |
| `fc00::/7` | IPv6 Unique Local | ULA private networks |

### DNS Rebinding Protection

URL string checks alone are not sufficient. An attacker can register a domain that resolves to a private IP address, bypassing hostname pattern matching. ulak resolves every hostname via `getaddrinfo()` and validates the **resolved IP addresses** against the blocked ranges. If DNS resolution fails entirely, the request is blocked.

```
URL: https://evil.example.com/steal-metadata
  |
  v
[1] Scheme validation --> only http:// and https:// allowed
  |
  v
[2] Hostname string check --> block known private patterns
  |
  v
[3] DNS resolution (getaddrinfo) --> resolve to actual IP(s)
  |
  v
[4] Resolved IP validation --> block if any IP is in private ranges
  |
  v
[5] Request proceeds (all checks passed)
```

### Where SSRF Checks Are Applied

- Endpoint URL (`config.url`)
- URL with suffix from metadata (`config.url` + `metadata.url_suffix`)
- OAuth2 token endpoint (`config.auth.token_url`)

### Scheme Validation

Only `http://` and `https://` schemes are permitted. Dangerous schemes (`file://`, `gopher://`, `dict://`, `ftp://`, `ldap://`) are implicitly blocked.

### Superuser Override

For development and testing environments where endpoints run on `localhost`:

```sql
-- Superuser only (PGC_SUSET)
ALTER SYSTEM SET ulak.http_allow_internal_urls = true;
SELECT pg_reload_conf();
```

> **Warning:** Never enable this in production. It disables all SSRF protection.

---

## Configuration Reference (GUC Parameters)

All HTTP-related GUC parameters. Changes take effect after `SELECT pg_reload_conf()` unless noted otherwise.

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.http_timeout` | `10` | 1-300 | Request timeout in seconds. |
| `ulak.http_connect_timeout` | `5` | 1-60 | Connection timeout in seconds. |
| `ulak.http_max_redirects` | `0` | 0-20 | Maximum redirects to follow. `0` disables redirects entirely. |
| `ulak.http_ssl_verify_peer` | `true` | -- | Verify the server SSL/TLS certificate against the CA bundle. |
| `ulak.http_ssl_verify_host` | `true` | -- | Verify that the certificate CN/SAN matches the target hostname. |
| `ulak.http_allow_internal_urls` | `false` | -- | Allow requests to private/loopback addresses. **Superuser only** (`PGC_SUSET`). Disables SSRF protection. |
| `ulak.http_batch_capacity` | `200` | 1-1000 | Maximum pending requests in the batch buffer per endpoint. |
| `ulak.http_max_connections_per_host` | `10` | 1-100 | Maximum concurrent connections to a single host (`CURLMOPT_MAX_HOST_CONNECTIONS`). |
| `ulak.http_max_total_connections` | `25` | 1-200 | Maximum total concurrent connections across all hosts (`CURLMOPT_MAX_TOTAL_CONNECTIONS`). |
| `ulak.http_flush_timeout` | `30000` | 1000-300000 | Maximum time in milliseconds to wait for batch flush completion. |
| `ulak.http_enable_pipelining` | `true` | -- | Enable HTTP/2 multiplexing for batch operations (`CURLPIPE_MULTIPLEX`). |
| `ulak.capture_response` | `false` | -- | Capture HTTP response status, body, and content type. Forces sync dispatch when enabled. |
| `ulak.response_body_max_size` | `65536` | 0-10485760 | Maximum bytes of response body to capture per dispatch. `0` disables body capture while still recording status code. |

---

## See Also

- [Apache Kafka](Protocol-Kafka) | [MQTT](Protocol-MQTT) | [Redis Streams](Protocol-Redis) | [AMQP / RabbitMQ](Protocol-AMQP) | [NATS / JetStream](Protocol-NATS) -- Other protocol dispatchers
- [Security](Security) -- RBAC, SSRF protection, credential lifecycle, TLS hardening
- [Reliability](Reliability) -- Circuit breaker, retry strategies, DLQ management
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- Complete function reference
- [System Architecture](Architecture) -- Worker model, dispatcher cache, batch processing pipeline
