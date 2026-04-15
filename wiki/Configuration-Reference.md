# Configuration Reference

All ulak GUC parameters use the `ulak.` prefix. Set them in `postgresql.conf` or via `ALTER SYSTEM`. Parameters with **POSTMASTER** context require a full server restart to take effect. Parameters with **SIGHUP** context only need a configuration reload (`SELECT pg_reload_conf();`). Parameters with **SUSET** context require superuser privileges.

There are **57 parameters** in total across 15 categories.

---

## Worker Settings

These parameters control background worker processes. Both require a **server restart** after changes.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.workers` | int | 4 | 1-32 | POSTMASTER | Number of background worker processes |
| `ulak.worker_restart_delay` | int | 5s | 1-300 | POSTMASTER | Delay in seconds before restarting a crashed worker |

---

## General Settings

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.database` | string | `"postgres"` | -- | SIGHUP | Database name that background workers connect to |
| `ulak.log_level` | enum | `warning` | `error`, `warning`, `info`, `debug` | SIGHUP | Extension log verbosity level |

---

## Queue & Polling

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.poll_interval` | int | 500ms | 100-60000 | SIGHUP | Queue polling frequency in milliseconds |
| `ulak.batch_size` | int | 200 | 1-1000 | SIGHUP | Number of messages processed per batch cycle |
| `ulak.max_queue_size` | int | 1000000 | 0-INT_MAX | SIGHUP | Backpressure threshold; 0 disables the limit |

---

## Retry & Backoff

ulak uses a linear backoff strategy. The delay for retry N is calculated as: `min(retry_base_delay + (N * retry_increment), retry_max_delay)`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.default_max_retries` | int | 10 | 0-1000 | SIGHUP | Default maximum number of delivery attempts |
| `ulak.retry_base_delay` | int | 10s | 1-3600 | SIGHUP | Base delay in seconds for backoff calculation |
| `ulak.retry_max_delay` | int | 300s | 1-86400 | SIGHUP | Maximum backoff delay in seconds |
| `ulak.retry_increment` | int | 30s | 1-3600 | SIGHUP | Linear backoff increment in seconds per retry |

---

## Stale Recovery

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.stale_recovery_timeout` | int | 300s | 60-3600 | SIGHUP | Timeout in seconds after which in-flight messages are considered stuck and eligible for recovery |

---

## Circuit Breaker

The circuit breaker protects endpoints from being overwhelmed during failures. After the threshold is reached, the circuit opens and no messages are dispatched until the cooldown period elapses and a half-open probe succeeds.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.circuit_breaker_threshold` | int | 10 | 1-1000 | SIGHUP | Consecutive failures required to open the circuit |
| `ulak.circuit_breaker_cooldown` | int | 30s | 5-3600 | SIGHUP | Cooldown period in seconds before a half-open probe is attempted |

---

## HTTP Settings

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.http_timeout` | int | 10s | 1-300 | SIGHUP | Total HTTP request timeout in seconds |
| `ulak.http_connect_timeout` | int | 5s | 1-60 | SIGHUP | TCP connection timeout in seconds |
| `ulak.http_max_redirects` | int | 0 | 0-20 | SIGHUP | Maximum number of HTTP redirects to follow; 0 disables redirects |
| `ulak.http_ssl_verify_peer` | bool | `true` | -- | SIGHUP | Enable SSL peer certificate verification |
| `ulak.http_ssl_verify_host` | bool | `true` | -- | SIGHUP | Enable SSL hostname verification |
| `ulak.http_allow_internal_urls` | bool | `false` | -- | SUSET | Allow dispatching to internal/private IP addresses (SSRF bypass); requires superuser |
| `ulak.http_batch_capacity` | int | 200 | 1-1000 | SIGHUP | HTTP batch buffer size |
| `ulak.http_max_connections_per_host` | int | 10 | 1-100 | SIGHUP | Maximum concurrent connections per host |
| `ulak.http_max_total_connections` | int | 25 | 1-200 | SIGHUP | Maximum total concurrent HTTP connections |
| `ulak.http_flush_timeout` | int | 30000ms | 1000-300000 | SIGHUP | Batch flush timeout in milliseconds |
| `ulak.http_enable_pipelining` | bool | `true` | -- | SIGHUP | Enable HTTP/2 multiplexing for improved throughput |
| `ulak.capture_response` | bool | `false` | -- | SIGHUP | Store HTTP response body with dispatch result |
| `ulak.response_body_max_size` | int | 65536 | 0-10485760 | SIGHUP | Maximum response body size to capture in bytes |

---

## Payload & CloudEvents

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.max_payload_size` | int | 1048576 | 1024-104857600 | SIGHUP | Maximum message payload size in bytes (default 1 MB) |
| `ulak.cloudevents_source` | string | `"/ulak"` | -- | SIGHUP | Source URI used in CloudEvents envelope |

---

## Retention

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.event_log_retention_days` | int | 30 | 1-365 | SIGHUP | Number of days to retain event log entries |
| `ulak.dlq_retention_days` | int | 30 | 1-3650 | SIGHUP | Number of days to retain dead letter queue messages |
| `ulak.archive_retention_months` | int | 6 | 1-120 | SIGHUP | Number of months to retain archive partitions |

---

## Kafka Settings

> Requires the extension to be compiled with `ENABLE_KAFKA=1`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.kafka_delivery_timeout` | int | 30000ms | 5000-300000 | SIGHUP | Delivery confirmation timeout in milliseconds |
| `ulak.kafka_poll_interval` | int | 100ms | 10-1000 | SIGHUP | Producer poll interval in milliseconds |
| `ulak.kafka_flush_timeout` | int | 5000ms | 1000-60000 | SIGHUP | Batch flush timeout in milliseconds |
| `ulak.kafka_batch_capacity` | int | 64 | 16-1024 | SIGHUP | Maximum pending messages in producer buffer |
| `ulak.kafka_acks` | string | `"all"` | `0`, `1`, `-1`, `all` | SIGHUP | Broker acknowledgment level |
| `ulak.kafka_compression` | string | `"snappy"` | `none`, `gzip`, `snappy`, `lz4`, `zstd` | SIGHUP | Message compression algorithm |

---

## MQTT Settings

> Requires the extension to be compiled with `ENABLE_MQTT=1`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.mqtt_keepalive` | int | 60s | 10-600 | SIGHUP | Keep-alive interval in seconds |
| `ulak.mqtt_timeout` | int | 5000ms | 1000-60000 | SIGHUP | Operation timeout in milliseconds |
| `ulak.mqtt_default_qos` | int | 0 | 0-2 | SIGHUP | Default MQTT Quality of Service level |
| `ulak.mqtt_default_port` | int | 1883 | 1-65535 | SIGHUP | Default broker port |

---

## Redis Settings

> Requires the extension to be compiled with `ENABLE_REDIS=1`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.redis_connect_timeout` | int | 5s | 1-60 | SIGHUP | Connection timeout in seconds |
| `ulak.redis_command_timeout` | int | 30s | 1-300 | SIGHUP | Command execution timeout in seconds |
| `ulak.redis_default_db` | int | 0 | 0-15 | SIGHUP | Default Redis database index |
| `ulak.redis_default_port` | int | 6379 | 1-65535 | SIGHUP | Default Redis server port |

---

## AMQP Settings

> Requires the extension to be compiled with `ENABLE_AMQP=1`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.amqp_connection_timeout` | int | 10s | 1-300 | SIGHUP | Connection timeout in seconds |
| `ulak.amqp_heartbeat` | int | 60s | 0-600 | SIGHUP | Heartbeat interval in seconds; 0 disables heartbeats |
| `ulak.amqp_frame_max` | int | 131072 | 4096-2097152 | SIGHUP | Maximum AMQP frame size in bytes |
| `ulak.amqp_delivery_timeout` | int | 30000ms | 5000-300000 | SIGHUP | Publisher confirm timeout in milliseconds |
| `ulak.amqp_batch_capacity` | int | 64 | 16-1024 | SIGHUP | Batch buffer size for pending messages |
| `ulak.amqp_ssl_verify_peer` | bool | `true` | -- | SIGHUP | Enable SSL peer certificate verification |

---

## NATS Settings

> Requires the extension to be compiled with `ENABLE_NATS=1`.

| Parameter | Type | Default | Range | Context | Description |
|-----------|------|---------|-------|---------|-------------|
| `ulak.nats_delivery_timeout` | int | 5000ms | 1000-60000 | SIGHUP | JetStream acknowledgment timeout in milliseconds |
| `ulak.nats_flush_timeout` | int | 10000ms | 1000-300000 | SIGHUP | Batch flush timeout in milliseconds |
| `ulak.nats_batch_capacity` | int | 64 | 16-1024 | SIGHUP | Batch buffer size for pending messages |
| `ulak.nats_reconnect_wait` | int | 2000ms | 100-60000 | SIGHUP | Reconnect backoff delay in milliseconds |
| `ulak.nats_max_reconnect` | int | 60 | -1 to 10000 | SIGHUP | Maximum reconnect attempts; -1 for infinite |

---

## Production Configuration Example

```ini
# postgresql.conf — ulak production settings
shared_preload_libraries = 'ulak'

ulak.database = 'myapp'
ulak.workers = 8
ulak.batch_size = 200
ulak.poll_interval = 250
ulak.max_queue_size = 500000

ulak.default_max_retries = 15
ulak.circuit_breaker_threshold = 5
ulak.circuit_breaker_cooldown = 60

ulak.http_timeout = 30
ulak.http_ssl_verify_peer = true
ulak.http_allow_internal_urls = false
```

---

## See Also

- [Getting Started](Getting-Started) -- Installation and first steps
- [SQL API Reference](SQL-API-Reference) -- Complete function reference
- [Architecture](Architecture) -- Internal design and worker model
- [Security](Security) -- RBAC roles and access control
- [Protocol: HTTP](Protocol-HTTP) | [Kafka](Protocol-Kafka) | [MQTT](Protocol-MQTT) | [Redis](Protocol-Redis) | [AMQP](Protocol-AMQP) | [NATS](Protocol-NATS)
