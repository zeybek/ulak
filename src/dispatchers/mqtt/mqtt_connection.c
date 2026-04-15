/**
 * @file mqtt_connection.c
 * @brief MQTT connection management and callbacks.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles MQTT connection pooling, callbacks, and string cleanup.
 */

#include "config/guc.h"
#include "mqtt_internal.h"
#include "postgres.h"
#include "utils/logging.h"

#include <strings.h>

/**
 * @brief Clean up MQTT dispatcher strings.
 *
 * Securely zeros sensitive data (password, TLS key) before freeing.
 *
 * @param mqtt_dispatcher MQTT dispatcher whose strings to free
 */
void mqtt_cleanup_strings(MqttDispatcher *mqtt_dispatcher) {
    if (mqtt_dispatcher->broker) {
        pfree(mqtt_dispatcher->broker);
        mqtt_dispatcher->broker = NULL;
    }
    if (mqtt_dispatcher->topic) {
        pfree(mqtt_dispatcher->topic);
        mqtt_dispatcher->topic = NULL;
    }
    if (mqtt_dispatcher->client_id) {
        pfree(mqtt_dispatcher->client_id);
        mqtt_dispatcher->client_id = NULL;
    }
    if (mqtt_dispatcher->username) {
        explicit_bzero(mqtt_dispatcher->username, strlen(mqtt_dispatcher->username));
        pfree(mqtt_dispatcher->username);
        mqtt_dispatcher->username = NULL;
    }
    if (mqtt_dispatcher->password) {
        explicit_bzero(mqtt_dispatcher->password, strlen(mqtt_dispatcher->password));
        pfree(mqtt_dispatcher->password);
        mqtt_dispatcher->password = NULL;
    }

    /* TLS strings */
    if (mqtt_dispatcher->tls_ca_cert) {
        pfree(mqtt_dispatcher->tls_ca_cert);
        mqtt_dispatcher->tls_ca_cert = NULL;
    }
    if (mqtt_dispatcher->tls_cert) {
        pfree(mqtt_dispatcher->tls_cert);
        mqtt_dispatcher->tls_cert = NULL;
    }
    if (mqtt_dispatcher->tls_key) {
        explicit_bzero(mqtt_dispatcher->tls_key, strlen(mqtt_dispatcher->tls_key));
        pfree(mqtt_dispatcher->tls_key);
        mqtt_dispatcher->tls_key = NULL;
    }

    /* LWT strings */
    if (mqtt_dispatcher->will_topic) {
        pfree(mqtt_dispatcher->will_topic);
        mqtt_dispatcher->will_topic = NULL;
    }
    if (mqtt_dispatcher->will_payload) {
        pfree(mqtt_dispatcher->will_payload);
        mqtt_dispatcher->will_payload = NULL;
    }
}

/**
 * @brief Publish callback for MQTT batch mode.
 *
 * Called by mosquitto_loop() when broker sends PUBACK (QoS 1)
 * or PUBCOMP (QoS 2) for a published message.
 *
 * IMPORTANT: This is called from the SAME thread as mosquitto_loop(),
 * NOT from a background thread like Kafka's delivery callback.
 * Therefore, no spinlock needed (unless threaded mode is used later).
 *
 * @param mosq Mosquitto client instance (unused)
 * @param obj User data pointer (MqttDispatcher)
 * @param mid Message ID that was acknowledged
 */
void mqtt_publish_callback(struct mosquitto *mosq, void *obj, int mid) {
    MqttDispatcher *mqtt = (MqttDispatcher *)obj;
    int i;

    if (!mqtt || !mqtt->pending_messages)
        return;

    /* Find the pending message with this mid and mark it as delivered */
    for (i = 0; i < mqtt->pending_count; i++) {
        if (mqtt->pending_messages[i].mid == mid) {
            mqtt->pending_messages[i].delivered = true;
            mqtt->pending_messages[i].success = true;
            ulak_log("debug", "MQTT PUBACK received for mid=%d (index=%d)", mid, i);
            return;
        }
    }

    /* mid not found in pending array - ignore (could be from sync dispatch) */
    ulak_log("debug", "MQTT PUBACK for unknown mid=%d (sync mode?)", mid);
}

/**
 * @brief Message callback for MQTT.
 *
 * Called when a message is received on a subscribed topic.
 *
 * @param mosq Mosquitto client instance (unused)
 * @param obj User data pointer (unused)
 * @param message Received MQTT message
 */
void mqtt_message_callback(struct mosquitto *mosq, void *obj,
                           const struct mosquitto_message *message) {
    if (message->payload) {
        ulak_log("debug", "MQTT message received on topic %s: %s", message->topic,
                 (char *)message->payload);
    } else {
        ulak_log("debug", "MQTT message received on topic %s (empty payload)", message->topic);
    }
}

/**
 * @brief Connection callback for MQTT.
 *
 * Called when connection state changes.
 *
 * @param mosq Mosquitto client instance (unused)
 * @param obj User data pointer (unused)
 * @param result Connection result code (0 = success)
 */
void mqtt_connect_callback(struct mosquitto *mosq, void *obj, int result) {
    if (result == 0) {
        ulak_log("debug", "MQTT connected successfully");
    } else {
        ulak_log("error", "MQTT connection failed: %d", result);
    }
}

/**
 * @brief Ensure MQTT connection is established (connection pooling).
 *
 * Reuses existing connection if still valid, otherwise reconnects.
 * Connection is considered stale after MQTT_CONNECTION_STALE_SECONDS seconds of inactivity.
 *
 * @param mqtt MQTT dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected, false on failure
 */
bool mqtt_ensure_connected(MqttDispatcher *mqtt, char **error_msg) {
    time_t now = time(NULL);
    int rc;
    int keepalive;

    /* Check if we have a valid, recent connection */
    if (mqtt->connected && mqtt->client) {
        /* Connection is considered valid for MQTT_CONNECTION_STALE_SECONDS */
        if (now - mqtt->last_activity < MQTT_CONNECTION_STALE_SECONDS) {
            return true;
        }
        /* Check if connection is still alive with a quick loop */
        rc = mosquitto_loop(mqtt->client, 0, 1);
        if (rc == MOSQ_ERR_SUCCESS) {
            mqtt->last_activity = now;
            return true;
        }
        /* Connection is broken, will reconnect below */
        ulak_log("debug", "MQTT connection stale, reconnecting");
        mqtt->connected = false;
    }

    /* Connect or reconnect */
    keepalive = ulak_mqtt_keepalive > 0 ? ulak_mqtt_keepalive : 60;

    if (mqtt->last_activity > 0) {
        /* Previously connected - use reconnect to reuse existing session state */
        rc = mosquitto_reconnect(mqtt->client);
    } else {
        /* First connection */
        rc = mosquitto_connect(mqtt->client, mqtt->broker, mqtt->port, keepalive);
    }

    if (rc != MOSQ_ERR_SUCCESS) {
        const char *prefix = mqtt_classify_error(rc);
        if (error_msg) {
            *error_msg = psprintf("%s MQTT connect failed: %s", prefix, mosquitto_strerror(rc));
        }
        mqtt->connected = false;
        return false;
    }

    mqtt->connected = true;
    mqtt->last_activity = now;
    ulak_log("debug", "MQTT connection established to %s:%d", mqtt->broker, mqtt->port);
    return true;
}
