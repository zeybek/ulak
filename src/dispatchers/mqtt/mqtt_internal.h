/**
 * @file mqtt_internal.h
 * @brief Internal declarations for the MQTT dispatcher module.
 *
 * @warning This header is for internal use within src/dispatchers/mqtt/ only.
 *          External code should use mqtt_dispatcher.h instead.
 */

#ifndef ULAK_DISPATCHERS_MQTT_INTERNAL_H
#define ULAK_DISPATCHERS_MQTT_INTERNAL_H

#include <mosquitto.h>
#include <string.h>
#include <time.h>
#include "dispatchers/mqtt/mqtt_dispatcher.h"

/* ── JSONB config key constants ── */

#define MQTT_CONFIG_KEY_BROKER "broker"
#define MQTT_CONFIG_KEY_TOPIC "topic"
#define MQTT_CONFIG_KEY_QOS "qos"
#define MQTT_CONFIG_KEY_PORT "port"
#define MQTT_CONFIG_KEY_CLIENT_ID "client_id"
#define MQTT_CONFIG_KEY_RETAIN "retain"
#define MQTT_CONFIG_KEY_USERNAME "username"
#define MQTT_CONFIG_KEY_PASSWORD "password"
#define MQTT_CONFIG_KEY_OPTIONS "options"
#define MQTT_CONFIG_KEY_TLS "tls"
#define MQTT_CONFIG_KEY_TLS_CA_CERT "tls_ca_cert"
#define MQTT_CONFIG_KEY_TLS_CERT "tls_cert"
#define MQTT_CONFIG_KEY_TLS_KEY "tls_key"
#define MQTT_CONFIG_KEY_TLS_INSECURE "tls_insecure"
#define MQTT_CONFIG_KEY_WILL_TOPIC "will_topic"
#define MQTT_CONFIG_KEY_WILL_PAYLOAD "will_payload"
#define MQTT_CONFIG_KEY_WILL_QOS "will_qos"
#define MQTT_CONFIG_KEY_WILL_RETAIN "will_retain"
#define MQTT_CONFIG_KEY_CLEAN_SESSION "clean_session"
#define MQTT_CONFIG_KEY_PROTOCOL_VERSION "protocol_version"

/** @brief Allowed configuration keys for strict validation. */
extern const char *MQTT_ALLOWED_CONFIG_KEYS[];

/* ── Defaults ── */

#define MQTT_DEFAULT_RETAIN false        /**< Default retain flag value. */
#define MQTT_CONNECTION_STALE_SECONDS 30 /**< Seconds before connection is considered stale. */

/* ── Connection Functions (mqtt_connection.c) ── */

/**
 * @brief Publish callback -- records the broker's acknowledgment of a mid.
 *
 * Registered with mosquitto_publish_v5_callback_set(), which libmosquitto also
 * calls on MQTT 3.1.1 connections, with reason code 0.
 *
 * @param mosq        mosquitto client instance.
 * @param obj         User data pointer (MqttDispatcher).
 * @param mid         Message ID that was acknowledged.
 * @param reason_code PUBACK (QoS 1) or PUBREC/PUBCOMP (QoS 2) reason code;
 *                    0x80 and above is a refusal (MQTT 5 only).
 * @param props       MQTT 5 properties of the acknowledgment (unused).
 */
void mqtt_publish_callback(struct mosquitto *mosq, void *obj, int mid, int reason_code,
                           const mosquitto_property *props);

/**
 * @brief Message callback -- called when a message is received.
 * @param mosq    mosquitto client instance.
 * @param obj     User data pointer (MqttDispatcher).
 * @param message Received message.
 */
void mqtt_message_callback(struct mosquitto *mosq, void *obj,
                           const struct mosquitto_message *message);

/**
 * @brief Connection callback -- called when connection state changes.
 * @param mosq   mosquitto client instance.
 * @param obj    User data pointer (MqttDispatcher).
 * @param result Connection result code (0 = success).
 */
void mqtt_connect_callback(struct mosquitto *mosq, void *obj, int result);

/**
 * @brief Ensure MQTT connection is established (connection pooling).
 *
 * Reuses existing connection if still valid, otherwise reconnects.
 * Connection is considered stale after MQTT_CONNECTION_STALE_SECONDS seconds.
 *
 * @param mqtt      The MQTT dispatcher.
 * @param error_msg OUT - error message on failure (caller must pfree).
 * @return true on success, false on failure with error message set.
 */
bool mqtt_ensure_connected(MqttDispatcher *mqtt, char **error_msg);

/**
 * @brief Clean up MQTT string fields.
 *
 * Frees broker, topic, client_id, username, password, TLS certs/keys,
 * and LWT strings. Securely zeros sensitive data before freeing.
 *
 * @param mqtt_dispatcher The MQTT dispatcher whose strings to free.
 */
void mqtt_cleanup_strings(MqttDispatcher *mqtt_dispatcher);

/**
 * @brief Classify an MQTT error as retryable or permanent.
 * @param rc mosquitto error code.
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT.
 */
const char *mqtt_classify_error(int rc);

/**
 * @brief Classify an error returned by mosquitto_loop().
 *
 * A loop error means the connection broke. A refused CONNACK is permanent
 * only when it rejected the client (credentials, authorization, protocol
 * version, client id, ban); "server unavailable" and similar refusals, and TLS
 * failures, are retryable. libmosquitto reports an MQTT 5 refusal as
 * MOSQ_ERR_PROTOCOL rather than MOSQ_ERR_CONN_REFUSED, so the CONNACK code
 * recorded by the connect callback decides.
 *
 * @param mqtt MQTT dispatcher (for the last CONNACK code).
 * @param rc   mosquitto error code from mosquitto_loop().
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT.
 */
const char *mqtt_classify_loop_error(MqttDispatcher *mqtt, int rc);

/**
 * @brief Whether the last connect attempt got a CONNACK that refused it.
 * @param mqtt MQTT dispatcher.
 * @return true if a CONNACK arrived with a non-zero code.
 */
bool mqtt_connack_refused(const MqttDispatcher *mqtt);

/**
 * @brief Text for the last CONNACK code, in the connection's protocol version.
 * @param mqtt MQTT dispatcher.
 * @return Static string from libmosquitto.
 */
const char *mqtt_connack_text(const MqttDispatcher *mqtt);

/**
 * @brief Classify an MQTT 5 refusal of a publish (PUBACK/PUBREC reason code).
 *
 * Not authorized, topic name invalid and payload format invalid do not change
 * by themselves; unspecified and implementation-specific errors, packet
 * identifier in use and quota exceeded may.
 *
 * @param reason_code Reason code of 0x80 or above.
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT.
 */
const char *mqtt_classify_publish_reason(int reason_code);

#endif /* ULAK_DISPATCHERS_MQTT_INTERNAL_H */
