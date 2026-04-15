# Protocol: MQTT

The MQTT dispatcher publishes messages to MQTT brokers using [libmosquitto](https://mosquitto.org/). It is an optional protocol -- build with `ENABLE_MQTT=1` to include it. The dispatcher supports QoS levels 0, 1, and 2, TLS/mTLS, Last Will and Testament (LWT), retain flag, clean sessions, and batch delivery with PUBACK tracking for QoS > 0. Wildcard topics (`+` and `#`) are rejected at validation time to prevent MQTT protocol violations.

---

## Quick Example

```sql
-- Create an MQTT endpoint
SELECT ulak.create_endpoint('sensor', 'mqtt', '{
  "broker": "mosquitto",
  "topic": "sensors/temperature",
  "qos": 1
}'::jsonb);

-- Send a message
SELECT ulak.send('sensor', '{"temp": 22.5, "unit": "celsius"}'::jsonb);
```

The background worker will publish the message to the `sensors/temperature` topic on the specified broker. For QoS 1, the worker waits for a PUBACK from the broker before marking the message as delivered.

---

## Endpoint Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `broker` | string | MQTT broker hostname or IP address. |
| `topic` | string | Target publish topic. Must not contain wildcard characters (`+` or `#`). |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | number | 1883 | Broker port (1--65535). |
| `qos` | number | 0 | Quality of Service level: `0` (at-most-once), `1` (at-least-once), `2` (exactly-once). |
| `client_id` | string | auto-generated | MQTT client identifier. If omitted, auto-generated as `ulak_{pid}_{random}` to avoid session takeover when multiple workers connect to the same broker. |
| `retain` | boolean | `false` | Set the MQTT retain flag on published messages. Retained messages are stored by the broker and delivered to future subscribers. |
| `username` | string | -- | MQTT authentication username. |
| `password` | string | -- | MQTT authentication password. |
| `clean_session` | boolean | `true` | Start a clean MQTT session. When `false`, the broker stores subscriptions and unacknowledged messages between connections. |
| `tls` | boolean | `false` | Enable TLS encryption for the broker connection. |
| `tls_ca_cert` | string | -- | Path to CA certificate file for verifying the broker. |
| `tls_cert` | string | -- | Path to client certificate file for mutual TLS (mTLS). |
| `tls_key` | string | -- | Path to client private key file for mTLS. |
| `tls_insecure` | boolean | `false` | Skip TLS certificate verification. Use only for development. |
| `will_topic` | string | -- | Last Will and Testament topic. |
| `will_payload` | string | -- | Last Will and Testament message payload. |
| `will_qos` | number | 0 | LWT QoS level (0--2). |
| `will_retain` | boolean | `false` | LWT retain flag. |

### Full Configuration Example

```json
{
  "broker": "mqtt.example.com",
  "topic": "devices/sensor-01/telemetry",
  "port": 8883,
  "qos": 2,
  "client_id": "ulak-sensor-publisher",
  "retain": false,
  "username": "producer",
  "password": "secret",
  "clean_session": true,
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/mqtt-ca.pem",
  "tls_cert": "/etc/ssl/client/cert.pem",
  "tls_key": "/etc/ssl/client/key.pem",
  "tls_insecure": false,
  "will_topic": "devices/sensor-01/status",
  "will_payload": "{\"status\": \"offline\"}",
  "will_qos": 1,
  "will_retain": true
}
```

---

## QoS Levels

MQTT defines three Quality of Service levels that trade throughput for delivery guarantees:

| QoS | Name | Protocol Flow | Guarantee | Batch Support |
|-----|------|---------------|-----------|---------------|
| 0 | At most once | PUBLISH only | Fire-and-forget. Message may be lost. | No |
| 1 | At least once | PUBLISH + PUBACK | Broker acknowledges receipt. Message may be delivered more than once. | Yes |
| 2 | Exactly once | PUBLISH + PUBREC + PUBREL + PUBCOMP | Four-step handshake guarantees exactly-once delivery. Highest latency. | Yes |

**QoS 0** is the fastest option -- the worker publishes the message and flushes the write buffer in a single non-blocking loop iteration. No broker acknowledgment is expected.

**QoS 1** provides a good balance between reliability and performance. The worker waits for a PUBACK from the broker within the configured timeout (`ulak.mqtt_timeout`).

**QoS 2** provides the strongest guarantee at the cost of additional round trips. Use it when duplicate messages are unacceptable.

The default QoS level is controlled by the `ulak.mqtt_default_qos` GUC parameter (default: 0) and can be overridden per-endpoint via the `qos` config field.

---

## Last Will and Testament

The MQTT Last Will and Testament (LWT) feature allows the broker to publish a predefined message on behalf of a client when the connection is lost unexpectedly. This is useful for signaling device offline status or triggering failover logic.

```json
{
  "broker": "mosquitto",
  "topic": "devices/sensor-01/telemetry",
  "qos": 1,
  "will_topic": "devices/sensor-01/status",
  "will_payload": "{\"status\": \"offline\", \"reason\": \"unexpected_disconnect\"}",
  "will_qos": 1,
  "will_retain": true
}
```

When the worker disconnects uncleanly (e.g., crash, network failure), the broker publishes the LWT message to `devices/sensor-01/status`. With `will_retain: true`, the offline status persists for any future subscriber until a new retained message replaces it.

LWT is configured at connection time via `mosquitto_will_set()` and applies for the lifetime of the MQTT connection.

---

## TLS

### TLS (Server Verification Only)

```json
{
  "broker": "mqtt.example.com",
  "topic": "secure/events",
  "port": 8883,
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/mqtt-ca.pem"
}
```

### Mutual TLS (mTLS)

```json
{
  "broker": "mqtt.example.com",
  "topic": "secure/events",
  "port": 8883,
  "tls": true,
  "tls_ca_cert": "/etc/ssl/certs/mqtt-ca.pem",
  "tls_cert": "/etc/ssl/client/cert.pem",
  "tls_key": "/etc/ssl/client/key.pem"
}
```

### Insecure TLS (Development Only)

```json
{
  "broker": "localhost",
  "topic": "test/events",
  "port": 8883,
  "tls": true,
  "tls_insecure": true
}
```

> **Warning:** Setting `tls_insecure` to `true` disables certificate verification. The connection is still encrypted but vulnerable to man-in-the-middle attacks. Use only in development environments.

TLS is configured via `mosquitto_tls_set()` and `mosquitto_tls_insecure_set()` at dispatcher creation time.

---

## Wildcard Rejection

MQTT wildcard characters `+` (single-level) and `#` (multi-level) are valid for subscribe operations but are protocol violations when used in publish topics. The MQTT dispatcher rejects any topic containing these characters during configuration validation:

```sql
-- This will fail validation
SELECT ulak.create_endpoint('bad', 'mqtt', '{
  "broker": "mosquitto",
  "topic": "sensors/+/temperature"
}'::jsonb);
-- ERROR: MQTT config: 'topic' must not contain wildcard characters ('+' or '#')
```

This check prevents silent message loss or broker rejection at publish time.

---

## Batch Support

Batch delivery is available when QoS > 0. For QoS 0, `supports_batch()` returns `false` because there are no PUBACK messages to track.

### Batch Flow (QoS 1/2)

1. **produce()** -- Calls `mosquitto_publish()` to enqueue the message without waiting for PUBACK. The message ID (`mid`) and queue row ID are stored in the pending array.

2. **flush()** -- Calls `mosquitto_loop()` repeatedly (100ms poll intervals) until all pending messages receive their PUBACK/PUBCOMP callback or the timeout expires.

The publish callback (`mqtt_publish_callback`) marks each pending message as delivered when the broker acknowledges its message ID.

### Connection Staleness

Connections are considered stale after 30 seconds of inactivity (`MQTT_CONNECTION_STALE_SECONDS`). On the next operation, the dispatcher reconnects automatically via `mqtt_ensure_connected()`.

### Delivery Flow

```
Worker                          Broker
  |                                |
  |-- produce(msg1, mid=1) ------>|  (non-blocking publish)
  |-- produce(msg2, mid=2) ------>|  (non-blocking publish)
  |-- produce(msg3, mid=3) ------>|  (non-blocking publish)
  |                                |
  |-- flush(timeout) ------------>|  (mosquitto_loop poll)
  |                                |-- PUBACK mid=1 -->
  |                                |-- PUBACK mid=2 -->
  |                                |-- PUBACK mid=3 -->
  |<-- publish_callback ----------|
  |                                |
  |  (match failed mids to batch)  |
```

Messages that do not receive PUBACK within the timeout are marked as retryable failures.

---

## Error Classification

The MQTT dispatcher classifies libmosquitto error codes into permanent and retryable categories. Permanent errors skip retry and move the message directly to the failed state (and eventually the DLQ).

### Permanent Errors

| Error Code | Meaning |
|------------|---------|
| `MOSQ_ERR_INVAL` | Invalid arguments to mosquitto function |
| `MOSQ_ERR_PROTOCOL` | MQTT protocol violation |
| `MOSQ_ERR_CONN_REFUSED` | Broker refused connection |
| `MOSQ_ERR_AUTH` | Authentication failure |
| `MOSQ_ERR_ACL_DENIED` | Access control denied |
| `MOSQ_ERR_PAYLOAD_SIZE` | Payload exceeds broker maximum |
| `MOSQ_ERR_TLS` | TLS handshake or certificate error |
| `MOSQ_ERR_NOT_SUPPORTED` | Unsupported feature requested |
| `MOSQ_ERR_QOS_NOT_SUPPORTED` | Requested QoS level not supported |
| `MOSQ_ERR_OVERSIZE_PACKET` | MQTT packet exceeds maximum size |
| `MOSQ_ERR_MALFORMED_UTF8` | Malformed UTF-8 in topic or payload |
| `MOSQ_ERR_MALFORMED_PACKET` | Malformed MQTT packet |

### Retryable Errors

All other errors are treated as retryable, including:

- Connection lost or timed out
- Network I/O errors
- DNS resolution failures
- Broker temporarily unavailable

Retryable errors cause the message to return to `pending` status with an incremented `retry_count` and a backoff delay calculated from the endpoint retry policy.

---

## GUC Parameters

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `ulak.mqtt_keepalive` | 60 s | 10 -- 600 | MQTT keep-alive interval. The client sends PINGREQ to the broker at this interval to maintain the connection. |
| `ulak.mqtt_timeout` | 5000 ms | 1000 -- 60000 | Timeout for MQTT operations (PUBACK/PUBCOMP wait, loop iterations). |
| `ulak.mqtt_default_qos` | 0 | 0 -- 2 | Default QoS level applied when not specified in the endpoint config. |
| `ulak.mqtt_default_port` | 1883 | 1 -- 65535 | Default broker port applied when not specified in the endpoint config. |

All parameters have `PGC_SIGHUP` context -- they can be changed with `ALTER SYSTEM SET` and applied with `SELECT pg_reload_conf()` without restarting PostgreSQL. Workers pick up the new values on their next SIGHUP processing cycle.

---

## See Also

- [Getting Started](Getting-Started) -- Installation with `ENABLE_MQTT=1` and first MQTT message
- [Architecture](Architecture) -- Dispatcher factory, batch delivery model, dispatcher cache
- [Security](Security) -- Credential zeroing, TLS configuration
- [Reliability](Reliability) -- Circuit breaker, retry backoff, DLQ
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [SQL API Reference](SQL-API-Reference) -- `send()`, `send_with_options()`, `create_endpoint()`
- [Protocol-HTTP](Protocol-HTTP) | [Protocol-Kafka](Protocol-Kafka) | [Protocol-Redis](Protocol-Redis) | [Protocol-AMQP](Protocol-AMQP) | [Protocol-NATS](Protocol-NATS)
