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

/** @brief Allowed configuration keys for strict validation. */
extern const char *MQTT_ALLOWED_CONFIG_KEYS[];

/* ── Defaults ── */

#define MQTT_DEFAULT_RETAIN false        /**< Default retain flag value. */
#define MQTT_CONNECTION_STALE_SECONDS 30 /**< Seconds before connection is considered stale. */

/* ── Connection Functions (mqtt_connection.c) ── */

/**
 * @brief Publish callback for batch mode -- tracks per-message PUBACK delivery.
 * @param mosq mosquitto client instance.
 * @param obj  User data pointer (MqttDispatcher).
 * @param mid  Message ID that was acknowledged.
 */
void mqtt_publish_callback(struct mosquitto *mosq, void *obj, int mid);

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

#endif /* ULAK_DISPATCHERS_MQTT_INTERNAL_H */
