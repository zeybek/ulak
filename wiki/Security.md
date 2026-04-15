# Security

ulak is designed with defense-in-depth security for running inside PostgreSQL, where it has direct access to your data and network. Every outbound request, credential, and privilege is governed by explicit controls. This page documents the full security model: role-based access control, SSRF protection, credential lifecycle management, TLS configuration, authentication mechanisms, input validation, webhook signing, and recommended hardening practices.

---

## Role-Based Access Control (RBAC)

ulak creates three `NOLOGIN` roles during extension installation. All permissions on `PUBLIC` are revoked -- no function or table is accessible without an explicit grant.

```sql
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ulak FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ulak FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA ulak FROM PUBLIC;
```

Assign these roles to your application users:

```sql
GRANT ulak_application TO myapp_user;
GRANT ulak_monitor TO grafana_user;
GRANT ulak_admin TO ops_user;
```

### Role Descriptions

| Role | Purpose | Table Access | Function Access |
|------|---------|-------------|-----------------|
| **ulak_admin** | Operations and administration | Full CRUD (SELECT, INSERT, UPDATE, DELETE) on all tables | All functions |
| **ulak_application** | Application message sending | SELECT-only on all tables | send, send_with_options, send_batch, send_batch_with_priority, publish, publish_batch, get_worker_status, metrics, _shmem_metrics |
| **ulak_monitor** | Read-only monitoring and observability | SELECT-only on all tables | health_check, get_worker_status, get_endpoint_health, dlq_summary, metrics, _shmem_metrics |

### Permission Matrix -- Tables

| Table | ulak_admin | ulak_application | ulak_monitor | PUBLIC |
|-------|-----------|-----------------|-------------|--------|
| endpoints | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| queue | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| dlq | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| archive | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| event_log | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| event_types | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |
| subscriptions | SELECT, INSERT, UPDATE, DELETE | SELECT | SELECT | -- |

### Permission Matrix -- Functions

| Function | ulak_admin | ulak_application | ulak_monitor |
|----------|-----------|-----------------|-------------|
| **Messaging** | | | |
| send(text, jsonb) | -- | EXECUTE | -- |
| send_with_options(...) | -- | EXECUTE | -- |
| send_batch(text, jsonb[]) | -- | EXECUTE | -- |
| send_batch_with_priority(text, jsonb[], smallint) | -- | EXECUTE | -- |
| publish(text, jsonb) | EXECUTE | EXECUTE | -- |
| publish_batch(jsonb) | EXECUTE | EXECUTE | -- |
| **Endpoint Management** | | | |
| create_endpoint(text, text, jsonb) | EXECUTE | -- | -- |
| drop_endpoint(text) | EXECUTE | -- | -- |
| alter_endpoint(text, jsonb) | EXECUTE | -- | -- |
| validate_endpoint_config(text, jsonb) | EXECUTE | -- | -- |
| enable_endpoint(text) | EXECUTE | -- | -- |
| disable_endpoint(text) | EXECUTE | -- | -- |
| **Circuit Breaker** | | | |
| update_circuit_breaker(bigint, boolean) | EXECUTE | -- | -- |
| reset_circuit_breaker(text) | EXECUTE | -- | -- |
| **DLQ & Archive** | | | |
| archive_single_to_dlq(bigint) | EXECUTE | -- | -- |
| archive_completed_messages(integer, integer) | EXECUTE | -- | -- |
| cleanup_old_archive_partitions(integer) | EXECUTE | -- | -- |
| cleanup_dlq() | EXECUTE | -- | -- |
| replay_message(bigint) | EXECUTE | -- | -- |
| replay_range(bigint, timestamptz, timestamptz, text) | EXECUTE | -- | -- |
| redrive_message(bigint) | EXECUTE | -- | -- |
| redrive_endpoint(text) | EXECUTE | -- | -- |
| redrive_all() | EXECUTE | -- | -- |
| dlq_summary() | EXECUTE | -- | EXECUTE |
| **Pub/Sub** | | | |
| create_event_type(text, text, jsonb) | EXECUTE | -- | -- |
| drop_event_type(text) | EXECUTE | -- | -- |
| subscribe(text, text, jsonb) | EXECUTE | -- | -- |
| unsubscribe(bigint) | EXECUTE | -- | -- |
| **Maintenance** | | | |
| maintain_archive_partitions(integer) | EXECUTE | -- | -- |
| mark_expired_messages() | EXECUTE | -- | -- |
| cleanup_event_log() | EXECUTE | -- | -- |
| **Monitoring** | | | |
| health_check() | EXECUTE | -- | EXECUTE |
| get_worker_status() | EXECUTE | EXECUTE | EXECUTE |
| get_endpoint_health(text) | -- | -- | EXECUTE |
| metrics() | EXECUTE | EXECUTE | EXECUTE |
| _shmem_metrics() | EXECUTE | EXECUTE | EXECUTE |

### SECURITY DEFINER Functions

Eight functions use `SECURITY DEFINER` to execute with the privileges of the extension owner rather than the calling user. This is the mechanism that allows `ulak_application` to insert messages into the queue without having direct `INSERT` permission on the `queue` table.

All eight functions set an explicit search path to prevent search-path hijacking:

```sql
SECURITY DEFINER
SET search_path = pg_catalog, ulak
```

| SECURITY DEFINER Function | Purpose |
|--------------------------|---------|
| send | Single message enqueue |
| send_with_options | Enqueue with priority, scheduling, idempotency |
| send_batch | Batch enqueue |
| send_batch_with_priority | Batch enqueue with priority |
| publish | Pub/Sub event publish |
| publish_batch | Batch pub/sub publish |
| maintain_archive_partitions | Create time-based archive partitions |
| cleanup_old_archive_partitions | Drop expired archive partitions |

> **Design rationale:** Applications call `send()` or `publish()` -- they never `INSERT INTO ulak.queue` directly. The SECURITY DEFINER layer enforces endpoint validation, backpressure checks, and payload constraints before any row is written.

---

## SSRF Protection

Server-Side Request Forgery (SSRF) is the primary network-level threat for any extension that makes outbound HTTP requests from within a database. ulak blocks requests to internal and private network addresses by default.

Implementation: `src/dispatchers/http/http_security.c` -- `http_is_internal_url()`

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
| `fd00::/8` | IPv6 Unique Local | ULA private networks |

### DNS Rebinding Protection

String-based URL checks alone are insufficient. An attacker can register a domain (e.g., `evil.169-254-169-254.nip.io`) that resolves to a private IP address, bypassing hostname pattern matching.

ulak resolves every hostname via `getaddrinfo()` and validates the **resolved IP addresses** against the same blocked ranges. If DNS resolution fails entirely, the request is blocked as suspicious.

```
URL submitted: https://evil.example.com/steal-metadata
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

- Webhook destination URL (`config.url`)
- URL with suffix metadata (`config.url` + `metadata.url_suffix`)
- OAuth2 token endpoint (`config.auth.token_url`)

### Scheme Validation

Only `http://` and `https://` schemes are permitted. The following dangerous schemes are implicitly blocked:

- `file://` -- local filesystem access
- `gopher://` -- protocol smuggling
- `dict://` -- service probing
- `ftp://` -- unintended file transfers
- `ldap://` -- LDAP injection

### Superuser Override

For development and testing environments where endpoints run on `localhost`:

```sql
-- Superuser only (PGC_SUSET)
ALTER SYSTEM SET ulak.http_allow_internal_urls = true;
SELECT pg_reload_conf();
```

> **Warning:** Never enable this in production. It disables all SSRF protection.

---

## Credential Security

Credentials (passwords, tokens, secret keys, TLS private keys) are treated as sensitive memory throughout their lifecycle. ulak uses `explicit_bzero()` to zero credential memory before freeing it. Unlike `memset()`, `explicit_bzero()` is guaranteed not to be optimized away by the compiler, ensuring secrets do not persist in freed memory.

### Credential Lifecycle

```
 [1] Parse from JSONB config (palloc)
          |
          v
 [2] Use during connection/auth
          |
          v
 [3] explicit_bzero() -- zero the memory
          |
          v
 [4] pfree() -- return memory to allocator
```

### Coverage by Protocol

| Protocol | Fields Zeroed with explicit_bzero |
|----------|----------------------------------|
| **HTTP** | Basic auth password, Bearer token, OAuth2 client_secret, OAuth2 cached access_token, OAuth2 auth header, AWS SigV4 userpwd, AWS SigV4 session_token, HMAC signing result, proxy password, proxy userpwd, TLS client cert, TLS client key |
| **Redis** | password, username, TLS private key |
| **AMQP** | username, password, TLS cert, TLS private key |
| **NATS** | token, password, nkey_seed, credentials_file path, TLS private key |
| **MQTT** | username, password, TLS private key |
| **Kafka** | password fields during config parsing |

The AMQP dispatcher additionally provides a reusable `amqp_secure_zero_memory()` wrapper around `explicit_bzero()`.

OAuth2 tokens are zeroed both during normal cleanup and during token invalidation (triggered by HTTP 401 responses). The OAuth2 POST body containing `client_secret` is zeroed immediately after the token request completes, and the raw HTTP response is zeroed before freeing regardless of success or failure.

---

## TLS/SSL Configuration

All protocols support TLS with configurable verification. Defaults are secure -- peer and hostname verification are enabled out of the box.

### HTTP (libcurl)

```sql
SELECT ulak.create_endpoint('secure_webhook', 'http', '{
  "url": "https://api.example.com/webhook",
  "tls_ca_cert": "/etc/ssl/certs/custom-ca.pem",
  "tls_client_cert": "/etc/ssl/client/cert.pem",
  "tls_client_key": "/etc/ssl/client/key.pem",
  "tls_pinned_public_key": "sha256//YhKJG3Wk3ZSlFz3Oqb2HBKZG89bBIxSjDBG/A+2xNFQ=",
  "headers": {"Content-Type": "application/json"}
}'::jsonb);
```

| Feature | Default | Details |
|---------|---------|---------|
| Peer certificate verification | Enabled (`CURLOPT_SSL_VERIFYPEER = 1`) | Validates server certificate against CA bundle |
| Hostname verification | Enabled (`CURLOPT_SSL_VERIFYHOST = 2`) | Checks certificate CN/SAN matches hostname |
| Mutual TLS (mTLS) | Optional | Client cert + key for two-way authentication |
| Custom CA bundle | Optional | `tls_ca_cert` path to PEM file |
| Certificate pinning | Optional | `tls_pinned_public_key` with `sha256//...` hash |
| Redirect restriction | HTTPS only | Redirects are only followed to HTTPS destinations |
| OAuth2 token endpoint | Hardened | Always `SSL_VERIFYPEER=1`, `SSL_VERIFYHOST=2`, no redirects, 10s timeout |
| Proxy TLS | Supported | Separate TLS configuration for proxy connections |

### Redis (hiredis)

```sql
SELECT ulak.create_endpoint('secure_redis', 'redis', '{
  "host": "redis.example.com",
  "port": 6380,
  "stream_key": "events",
  "password": "secret",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/redis-ca.pem",
  "tls_cert": "/etc/ssl/client/redis-cert.pem",
  "tls_key": "/etc/ssl/client/redis-key.pem"
}'::jsonb);
```

### AMQP / RabbitMQ (rabbitmq-c)

```sql
SELECT ulak.create_endpoint('secure_amqp', 'amqp', '{
  "host": "rabbitmq.example.com",
  "port": 5671,
  "vhost": "/",
  "exchange": "events",
  "routing_key": "audit",
  "username": "producer",
  "password": "secret",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/rabbitmq-ca.pem",
  "tls_cert": "/etc/ssl/client/amqp-cert.pem",
  "tls_key": "/etc/ssl/client/amqp-key.pem",
  "verify_peer": true,
  "verify_hostname": true
}'::jsonb);
```

| Feature | Default |
|---------|---------|
| `verify_peer` | `true` (controlled by `ulak.amqp_ssl_verify_peer`) |
| `verify_hostname` | Enabled |

### NATS (nats.c)

```sql
SELECT ulak.create_endpoint('secure_nats', 'nats', '{
  "url": "tls://nats.example.com:4222",
  "subject": "events.audit",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/nats-ca.pem",
  "tls_cert": "/etc/ssl/client/nats-cert.pem",
  "tls_key": "/etc/ssl/client/nats-key.pem"
}'::jsonb);
```

### MQTT (libmosquitto)

```sql
SELECT ulak.create_endpoint('secure_mqtt', 'mqtt', '{
  "host": "mqtt.example.com",
  "port": 8883,
  "topic": "devices/telemetry",
  "qos": 1,
  "username": "producer",
  "password": "secret",
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/mqtt-ca.pem",
  "tls_cert": "/etc/ssl/client/mqtt-cert.pem",
  "tls_key": "/etc/ssl/client/mqtt-key.pem",
  "tls_insecure": false
}'::jsonb);
```

> **Note:** The `tls_insecure` flag disables hostname verification only. Peer certificate verification remains active. Set to `true` only for testing with self-signed certificates.

---

## Authentication Mechanisms (HTTP)

ulak supports five authentication methods for HTTP endpoints, configured via the `auth` object in endpoint config.

### Basic Authentication

```sql
SELECT ulak.create_endpoint('basic_api', 'http', '{
  "url": "https://api.example.com/events",
  "auth": {
    "type": "basic",
    "username": "api_user",
    "password": "api_secret"
  }
}'::jsonb);
```

Sends credentials via `CURLOPT_USERPWD` with `CURLAUTH_BASIC`. The `username:password` string is zeroed with `explicit_bzero()` immediately after libcurl copies it internally.

### Bearer Token

```sql
SELECT ulak.create_endpoint('bearer_api', 'http', '{
  "url": "https://api.example.com/events",
  "auth": {
    "type": "bearer",
    "token": "eyJhbGciOiJSUzI1NiIs..."
  }
}'::jsonb);
```

Appends `Authorization: Bearer <token>` header. The header string is zeroed after being passed to libcurl.

### OAuth2 Client Credentials

```sql
SELECT ulak.create_endpoint('oauth2_api', 'http', '{
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

| Security Feature | Detail |
|-----------------|--------|
| Token caching | Cached in memory with 60-second expiry buffer |
| Token URL validation | SSRF check applied to `token_url` |
| Token URL TLS | Always `SSL_VERIFYPEER=1`, `SSL_VERIFYHOST=2` |
| Token URL redirects | Disabled |
| Token URL timeout | 10 seconds |
| Response size cap | 64 KB maximum |
| Token invalidation | Automatic on HTTP 401; token zeroed and re-fetched |
| POST body | Zeroed with `explicit_bzero()` immediately after request |
| Response data | Zeroed before freeing regardless of outcome |

### AWS Signature V4

```sql
SELECT ulak.create_endpoint('aws_api', 'http', '{
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

Uses libcurl native AWS SigV4 support (requires libcurl 7.75.0+). The `CURLOPT_AWS_SIGV4` parameter is set to `"aws:amz:{region}:{service}"`. The optional `session_token` supports AWS STS temporary credentials via the `x-amz-security-token` header.

### Webhook HMAC Signing (Standard Webhooks)

```sql
SELECT ulak.create_endpoint('signed_webhook', 'http', '{
  "url": "https://api.example.com/webhook",
  "signing_secret": "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
}'::jsonb);
```

ulak implements the [Standard Webhooks](https://www.standardwebhooks.com/) specification. When a `signing_secret` is configured, every outgoing request includes three headers:

| Header | Value |
|--------|-------|
| `webhook-id` | `msg_{message_id}` -- unique message identifier |
| `webhook-timestamp` | Unix epoch seconds |
| `webhook-signature` | `v1,{base64(HMAC-SHA256(secret, "{id}.{ts}.{body}"))}` |

The HMAC result is zeroed with `explicit_bzero()` immediately after base64 encoding.

#### Receiver-Side Verification

Consumers should verify the signature to confirm message authenticity and integrity:

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
1. **Timestamp tolerance** -- reject messages older than 5 minutes to prevent replay attacks
2. **Reconstruct signed content** -- concatenate `{msg_id}.{timestamp}.{body}` exactly as ulak signs it
3. **HMAC-SHA256** -- compute with the shared secret
4. **Constant-time comparison** -- use `hmac.compare_digest()` (or equivalent) to prevent timing attacks

---

## Input Validation

ulak validates all user-supplied input at the boundary before it reaches protocol libraries.

### HTTP Header Injection

CR (`\r`) and LF (`\n`) characters are rejected in both header names and header values. Headers containing these characters are silently skipped with a warning log. This prevents HTTP response splitting and header injection attacks in both endpoint-level `config.headers` and per-message `metadata` headers.

### MQTT Topic Validation

MQTT wildcard characters `+` (single-level) and `#` (multi-level) are rejected in publish topics. These characters are only valid for subscribe operations; allowing them in publish topics would be a protocol violation that could cause unintended message routing.

```
Rejected: "devices/+/telemetry"
Rejected: "devices/#"
Accepted:  "devices/sensor-01/telemetry"
```

### NATS Subject Validation

NATS wildcard characters `*` (token wildcard) and `>` (full wildcard) are rejected in publish subjects for the same reason.

```
Rejected: "events.*.audit"
Rejected: "events.>"
Accepted:  "events.order.audit"
```

### NATS Credentials File Path

The `credentials_file` configuration for NATS JWT authentication is validated with three checks:

| Check | Purpose |
|-------|---------|
| Must be an absolute path (starts with `/`) | Prevents relative path ambiguity |
| Must not contain `..` | Prevents path traversal attacks |
| Must end with `.creds` extension | Ensures only credential files are referenced |

### AMQP Exchange Type

The `exchange_type` field accepts only the four valid AMQP exchange types:

- `direct`
- `fanout`
- `topic`
- `headers`

Any other value is rejected with an error.

### Strict Config Key Validation

All dispatchers validate configuration keys strictly. Unknown or misspelled JSON keys in endpoint config are rejected with an error listing the valid keys. This prevents silent misconfiguration where a typo (e.g., `"pasword"` instead of `"password"`) would result in an unauthenticated connection.

### Payload Immutability

A database trigger (`enforce_payload_immutability`) prevents modification of `payload` and `headers` columns on the `queue` table after row creation:

```sql
IF OLD.payload IS DISTINCT FROM NEW.payload THEN
    RAISE EXCEPTION 'payload modification not allowed after creation'
        USING HINT = 'Message payloads are immutable for audit integrity';
END IF;
IF OLD.headers IS DISTINCT FROM NEW.headers THEN
    RAISE EXCEPTION 'headers modification not allowed after creation'
        USING HINT = 'Message headers are immutable for audit integrity';
END IF;
```

This guarantees that the payload delivered to the endpoint is identical to what the application originally enqueued, preserving audit integrity.

### Payload Size Limit

Message payloads are capped at `ulak.max_payload_size` (default 1 MB). Oversized payloads are rejected before being written to the queue.

---

## Security Configuration Reference

| GUC Parameter | Type | Default | Scope | Description |
|---------------|------|---------|-------|-------------|
| `ulak.http_ssl_verify_peer` | bool | `true` | SIGHUP | Verify the SSL/TLS certificate of HTTP endpoints against the CA bundle |
| `ulak.http_ssl_verify_host` | bool | `true` | SIGHUP | Verify that the server certificate CN/SAN matches the target hostname |
| `ulak.http_allow_internal_urls` | bool | `false` | SUSET | Allow requests to private/internal IP addresses. **Superuser only.** Disables SSRF protection when enabled |
| `ulak.http_max_redirects` | int | `0` | SIGHUP | Maximum number of HTTP redirects to follow. Zero means redirects are not followed |
| `ulak.amqp_ssl_verify_peer` | bool | `true` | SIGHUP | Verify the SSL/TLS certificate of AMQP endpoints |
| `ulak.max_payload_size` | int | `1048576` (1 MB) | SIGHUP | Maximum allowed message payload size in bytes |
| `ulak.response_body_max_size` | int | `65536` (64 KB) | SIGHUP | Maximum bytes of HTTP response body captured per dispatch |

---

## Best Practices

### Principle of Least Privilege

- Grant `ulak_application` to application roles that only need to send messages. Never grant `ulak_admin` to application code.
- Grant `ulak_monitor` to observability systems. They need read access to dashboards, not write access to endpoints.
- Never grant roles directly to `PUBLIC`.

### Credential Management

- Store endpoint configurations with credentials via `ulak_admin` roles only.
- Rotate secrets by calling `ulak.alter_endpoint()` -- the old credentials are zeroed in memory immediately.
- For AWS workloads, prefer STS temporary credentials (`session_token`) over long-lived access keys.
- For OAuth2, prefer scoped tokens (`scope` parameter) with the minimum permissions required.

### Network Security

- Keep `ulak.http_allow_internal_urls` set to `false` in production. Always.
- Keep `ulak.http_max_redirects` at `0` unless your endpoints legitimately redirect. Open redirects are a common SSRF amplification vector.
- Use TLS for all protocols. Prefer mTLS where the endpoint supports it.
- Use certificate pinning (`tls_pinned_public_key`) for high-value endpoints to defend against CA compromise.

### TLS Hardening

- Never set `ulak.http_ssl_verify_peer` or `ulak.http_ssl_verify_host` to `false` in production.
- Never set `ulak.amqp_ssl_verify_peer` to `false` in production.
- Use dedicated CA bundles (`tls_ca_cert`) rather than relying on system CA stores when connecting to internal PKI.
- Ensure TLS private key files have restrictive permissions (`chmod 600`) and are readable only by the `postgres` OS user.

### Webhook Security

- Always configure a `signing_secret` for HTTP webhook endpoints to enable HMAC signing.
- Instruct consumers to verify signatures and enforce a timestamp tolerance window (5 minutes recommended).
- Use constant-time comparison functions on the receiver side to prevent timing attacks.

### Monitoring for Security Events

- Monitor `event_log` for authentication failures (HTTP 401/403) which may indicate credential expiry or compromise.
- Monitor for circuit breaker state changes -- repeated failures to a single endpoint could indicate a network security issue.
- Review `dlq` for messages that failed due to SSRF blocks or TLS errors, which may indicate misconfiguration or attack attempts.

### PostgreSQL-Level Hardening

- Use `pg_hba.conf` to restrict which hosts can connect to the database.
- Enable SSL for PostgreSQL client connections (`ssl = on` in `postgresql.conf`).
- Run ulak workers under a dedicated database user, not the superuser.
- Consider using `pgaudit` to log access to ulak schema objects.

---

## See Also

- [Configuration Reference](Configuration-Reference) -- Full list of 57 GUC parameters
- [Protocol: HTTP](Protocol-HTTP) -- HTTP endpoint configuration details
- [Protocol: AMQP](Protocol-AMQP) -- AMQP/RabbitMQ endpoint configuration
- [Protocol: NATS](Protocol-NATS) -- NATS/JetStream endpoint configuration
- [Protocol: MQTT](Protocol-MQTT) -- MQTT endpoint configuration
- [Protocol: Redis](Protocol-Redis) -- Redis Streams endpoint configuration
- [Reliability](Reliability) -- Circuit breaker, retry policies, DLQ
- [Monitoring](Monitoring) -- Health checks, metrics, observability
- [SQL API Reference](SQL-API-Reference) -- Complete function reference
