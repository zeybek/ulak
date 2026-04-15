/**
 * @file mqtt_dispatcher.c
 * @brief MQTT protocol dispatcher implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles MQTT message dispatching using libmosquitto.
 *
 * This file contains:
 * - Dispatcher operations struct
 * - create/cleanup lifecycle
 * - dispatch implementation
 * - config validation
 *
 * Connection management and callbacks are in mqtt_connection.c
 */

#include "config/guc.h"
#include "core/entities.h"
#include "mqtt_internal.h"
#include "postgres.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

#include <time.h>

const char *MQTT_ALLOWED_CONFIG_KEYS[] = {
    MQTT_CONFIG_KEY_BROKER,        MQTT_CONFIG_KEY_TOPIC,
    MQTT_CONFIG_KEY_QOS,           MQTT_CONFIG_KEY_PORT,
    MQTT_CONFIG_KEY_CLIENT_ID,     MQTT_CONFIG_KEY_RETAIN,
    MQTT_CONFIG_KEY_USERNAME,      MQTT_CONFIG_KEY_PASSWORD,
    MQTT_CONFIG_KEY_OPTIONS,       MQTT_CONFIG_KEY_TLS,
    MQTT_CONFIG_KEY_TLS_CA_CERT,   MQTT_CONFIG_KEY_TLS_CERT,
    MQTT_CONFIG_KEY_TLS_KEY,       MQTT_CONFIG_KEY_TLS_INSECURE,
    MQTT_CONFIG_KEY_WILL_TOPIC,    MQTT_CONFIG_KEY_WILL_PAYLOAD,
    MQTT_CONFIG_KEY_WILL_QOS,      MQTT_CONFIG_KEY_WILL_RETAIN,
    MQTT_CONFIG_KEY_CLEAN_SESSION, NULL};

/**
 * @brief Classify an MQTT error as retryable or permanent.
 * @param rc Mosquitto error code
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT string constant
 */
const char *mqtt_classify_error(int rc) {
    switch (rc) {
    case MOSQ_ERR_INVAL:
    case MOSQ_ERR_PROTOCOL:
    case MOSQ_ERR_CONN_REFUSED:
    case MOSQ_ERR_NOT_SUPPORTED:
    case MOSQ_ERR_AUTH:
    case MOSQ_ERR_ACL_DENIED:
    case MOSQ_ERR_PAYLOAD_SIZE:
    case MOSQ_ERR_MALFORMED_UTF8:
    case MOSQ_ERR_MALFORMED_PACKET:
    case MOSQ_ERR_QOS_NOT_SUPPORTED:
    case MOSQ_ERR_OVERSIZE_PACKET:
    case MOSQ_ERR_TLS:
        return ERROR_PREFIX_PERMANENT;
    default:
        return ERROR_PREFIX_RETRYABLE;
    }
}

/** Forward declaration */
bool mqtt_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result);

/** MQTT Dispatcher Operations */
static DispatcherOperations mqtt_dispatcher_ops = {.dispatch = mqtt_dispatcher_dispatch,
                                                   .validate_config =
                                                       mqtt_dispatcher_validate_config,
                                                   .cleanup = mqtt_dispatcher_cleanup,
                                                   .produce = mqtt_dispatcher_produce,
                                                   .flush = mqtt_dispatcher_flush,
                                                   .supports_batch = mqtt_dispatcher_supports_batch,
                                                   .dispatch_ex = mqtt_dispatcher_dispatch_ex,
                                                   .produce_ex = NULL};

/**
 * @brief Create a new MQTT dispatcher instance.
 * @param config JSONB configuration with broker, topic, and optional settings
 * @return Dispatcher pointer on success, NULL on failure
 */
Dispatcher *mqtt_dispatcher_create(Jsonb *config) {
    MqttDispatcher *mqtt_dispatcher;
    JsonbValue broker_val, topic_val, qos_val, port_val, client_id_val, retain_val, username_val,
        password_val, options_val;
    JsonbValue tls_val, tls_ca_cert_val, tls_cert_val, tls_key_val, tls_insecure_val;
    JsonbValue will_topic_val, will_payload_val, will_qos_val, will_retain_val, clean_session_val;
    int default_qos;
    int default_port;
    const char *client_id;
    bool clean_session;

    if (!mqtt_dispatcher_validate_config(config)) {
        ulak_log("error", "Invalid MQTT dispatcher configuration");
        return NULL;
    }

    /* mosquitto_lib_init() is called once at worker startup */

    mqtt_dispatcher = (MqttDispatcher *)palloc0(sizeof(MqttDispatcher));

    /* Initialize base dispatcher */
    mqtt_dispatcher->base.protocol = PROTOCOL_TYPE_MQTT;
    mqtt_dispatcher->base.config = config;
    mqtt_dispatcher->base.ops = &mqtt_dispatcher_ops;
    mqtt_dispatcher->base.private_data = mqtt_dispatcher;

    /* Parse configuration - use stack-allocated JsonbValues (no memory leak) */

    /* Get broker (required) */
    if (!extract_jsonb_value(config, MQTT_CONFIG_KEY_BROKER, &broker_val) ||
        broker_val.type != jbvString) {
        ulak_log("error", "MQTT config missing or invalid 'broker'");
        pfree(mqtt_dispatcher);
        return NULL;
    }
    mqtt_dispatcher->broker = pnstrdup(broker_val.val.string.val, broker_val.val.string.len);

    /* Get topic (required) */
    if (!extract_jsonb_value(config, MQTT_CONFIG_KEY_TOPIC, &topic_val) ||
        topic_val.type != jbvString) {
        ulak_log("error", "MQTT config missing or invalid 'topic'");
        pfree(mqtt_dispatcher->broker);
        pfree(mqtt_dispatcher);
        return NULL;
    }
    mqtt_dispatcher->topic = pnstrdup(topic_val.val.string.val, topic_val.val.string.len);

    /* Get QoS (optional, default from GUC) */
    default_qos =
        (ulak_mqtt_default_qos >= 0 && ulak_mqtt_default_qos <= 2) ? ulak_mqtt_default_qos : 0;
    mqtt_dispatcher->qos = default_qos;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_QOS, &qos_val) && qos_val.type == jbvNumeric) {
        mqtt_dispatcher->qos = jsonb_get_int32((Jsonb *)config, MQTT_CONFIG_KEY_QOS, default_qos);
        if (mqtt_dispatcher->qos < 0 || mqtt_dispatcher->qos > 2) {
            ulak_log("warning", "Invalid MQTT QoS value %d, using default %d", mqtt_dispatcher->qos,
                     default_qos);
            mqtt_dispatcher->qos = default_qos;
        }
    }

    /* Get port (optional, default from GUC) */
    default_port = (ulak_mqtt_default_port > 0 && ulak_mqtt_default_port <= 65535)
                       ? ulak_mqtt_default_port
                       : 1883;
    mqtt_dispatcher->port = default_port;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_PORT, &port_val) &&
        port_val.type == jbvNumeric) {
        mqtt_dispatcher->port =
            jsonb_get_int32((Jsonb *)config, MQTT_CONFIG_KEY_PORT, default_port);
        if (mqtt_dispatcher->port <= 0 || mqtt_dispatcher->port > 65535) {
            ulak_log("warning", "Invalid MQTT port value %d, using default %d",
                     mqtt_dispatcher->port, default_port);
            mqtt_dispatcher->port = default_port;
        }
    }

    /* Get client_id (optional) */
    mqtt_dispatcher->client_id = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_CLIENT_ID, &client_id_val) &&
        client_id_val.type == jbvString) {
        mqtt_dispatcher->client_id =
            pnstrdup(client_id_val.val.string.val, client_id_val.val.string.len);
    }

    /* Get retain flag (optional, default false) */
    mqtt_dispatcher->retain = MQTT_DEFAULT_RETAIN;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_RETAIN, &retain_val) &&
        retain_val.type == jbvBool) {
        mqtt_dispatcher->retain = retain_val.val.boolean;
    }

    /* Get username (optional) */
    mqtt_dispatcher->username = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_USERNAME, &username_val) &&
        username_val.type == jbvString) {
        mqtt_dispatcher->username =
            pnstrdup(username_val.val.string.val, username_val.val.string.len);
    }

    /* Get password (optional) */
    mqtt_dispatcher->password = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_PASSWORD, &password_val) &&
        password_val.type == jbvString) {
        mqtt_dispatcher->password =
            pnstrdup(password_val.val.string.val, password_val.val.string.len);
    }

    /* Get options (optional) - reference parent config for option lookup */
    mqtt_dispatcher->options = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_OPTIONS, &options_val) &&
        (options_val.type == jbvBinary || options_val.type == jbvObject)) {
        mqtt_dispatcher->options = (Jsonb *)config;
    }

    /* Parse TLS configuration */
    mqtt_dispatcher->tls = false;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS, &tls_val) && tls_val.type == jbvBool) {
        mqtt_dispatcher->tls = tls_val.val.boolean;
    }

    mqtt_dispatcher->tls_ca_cert = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS_CA_CERT, &tls_ca_cert_val) &&
        tls_ca_cert_val.type == jbvString) {
        mqtt_dispatcher->tls_ca_cert =
            pnstrdup(tls_ca_cert_val.val.string.val, tls_ca_cert_val.val.string.len);
    }

    mqtt_dispatcher->tls_cert = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS_CERT, &tls_cert_val) &&
        tls_cert_val.type == jbvString) {
        mqtt_dispatcher->tls_cert =
            pnstrdup(tls_cert_val.val.string.val, tls_cert_val.val.string.len);
    }

    mqtt_dispatcher->tls_key = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS_KEY, &tls_key_val) &&
        tls_key_val.type == jbvString) {
        mqtt_dispatcher->tls_key = pnstrdup(tls_key_val.val.string.val, tls_key_val.val.string.len);
    }

    mqtt_dispatcher->tls_insecure = false;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS_INSECURE, &tls_insecure_val) &&
        tls_insecure_val.type == jbvBool) {
        mqtt_dispatcher->tls_insecure = tls_insecure_val.val.boolean;
    }

    /* Parse LWT configuration */
    mqtt_dispatcher->will_topic = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_TOPIC, &will_topic_val) &&
        will_topic_val.type == jbvString) {
        mqtt_dispatcher->will_topic =
            pnstrdup(will_topic_val.val.string.val, will_topic_val.val.string.len);
    }

    mqtt_dispatcher->will_payload = NULL;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_PAYLOAD, &will_payload_val) &&
        will_payload_val.type == jbvString) {
        mqtt_dispatcher->will_payload =
            pnstrdup(will_payload_val.val.string.val, will_payload_val.val.string.len);
    }

    mqtt_dispatcher->will_qos = 0;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_QOS, &will_qos_val) &&
        will_qos_val.type == jbvNumeric) {
        mqtt_dispatcher->will_qos = jsonb_get_int32((Jsonb *)config, MQTT_CONFIG_KEY_WILL_QOS, 0);
        if (mqtt_dispatcher->will_qos < 0 || mqtt_dispatcher->will_qos > 2) {
            mqtt_dispatcher->will_qos = 0;
        }
    }

    mqtt_dispatcher->will_retain = false;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_RETAIN, &will_retain_val) &&
        will_retain_val.type == jbvBool) {
        mqtt_dispatcher->will_retain = will_retain_val.val.boolean;
    }

    /* Parse clean_session (optional, default true) */
    clean_session = true;
    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_CLEAN_SESSION, &clean_session_val) &&
        clean_session_val.type == jbvBool) {
        clean_session = clean_session_val.val.boolean;
    }
    mqtt_dispatcher->clean_session = clean_session;

    /* Initialize extended fields */
    mqtt_dispatcher->last_mid = 0;

    /* Initialize connection pooling state */
    mqtt_dispatcher->connected = false;
    mqtt_dispatcher->last_activity = 0;

    /* Create Mosquitto client -- unique client_id per dispatcher to avoid
     * MQTT "session taken over" when multiple workers connect to same broker.
     * MQTT protocol requires unique client_id per connection. */
    if (mqtt_dispatcher->client_id) {
        client_id = mqtt_dispatcher->client_id;
    } else {
        char auto_id[64];
        snprintf(auto_id, sizeof(auto_id), "ulak_%d_%ld", MyProcPid, random());
        mqtt_dispatcher->client_id = pstrdup(auto_id);
        client_id = mqtt_dispatcher->client_id;
    }
    mqtt_dispatcher->client =
        mosquitto_new(client_id, mqtt_dispatcher->clean_session, mqtt_dispatcher);
    if (!mqtt_dispatcher->client) {
        ulak_log("error", "Failed to create MQTT client");
        mqtt_cleanup_strings(mqtt_dispatcher);
        pfree(mqtt_dispatcher);
        return NULL;
    }

    /* Set callbacks */
    mosquitto_connect_callback_set(mqtt_dispatcher->client, mqtt_connect_callback);
    mosquitto_message_callback_set(mqtt_dispatcher->client, mqtt_message_callback);
    mosquitto_publish_callback_set(mqtt_dispatcher->client, mqtt_publish_callback);

    /* Set username and password if provided */
    if (mqtt_dispatcher->username) {
        if (mosquitto_username_pw_set(mqtt_dispatcher->client, mqtt_dispatcher->username,
                                      mqtt_dispatcher->password) != MOSQ_ERR_SUCCESS) {
            ulak_log("error", "Failed to set MQTT credentials");
            mosquitto_destroy(mqtt_dispatcher->client);
            mqtt_cleanup_strings(mqtt_dispatcher);
            pfree(mqtt_dispatcher);
            return NULL;
        }
    }

    /* TLS setup */
    if (mqtt_dispatcher->tls) {
        int tls_ret = mosquitto_tls_set(mqtt_dispatcher->client, mqtt_dispatcher->tls_ca_cert, NULL,
                                        mqtt_dispatcher->tls_cert, mqtt_dispatcher->tls_key, NULL);
        if (tls_ret != MOSQ_ERR_SUCCESS) {
            ulak_log("error", "MQTT TLS setup failed: %s", mosquitto_strerror(tls_ret));
            mosquitto_destroy(mqtt_dispatcher->client);
            mqtt_cleanup_strings(mqtt_dispatcher);
            pfree(mqtt_dispatcher);
            return NULL;
        }
        if (mqtt_dispatcher->tls_insecure) {
            mosquitto_tls_insecure_set(mqtt_dispatcher->client, true);
        }
    }

    /* LWT setup */
    if (mqtt_dispatcher->will_topic) {
        mosquitto_will_set(
            mqtt_dispatcher->client, mqtt_dispatcher->will_topic,
            mqtt_dispatcher->will_payload ? (int)strlen(mqtt_dispatcher->will_payload) : 0,
            mqtt_dispatcher->will_payload, mqtt_dispatcher->will_qos, mqtt_dispatcher->will_retain);
    }

    /* Initialize batch tracking */
    mqtt_dispatcher->pending_messages =
        palloc(sizeof(MqttPendingMessage) * MQTT_PENDING_INITIAL_CAPACITY);
    mqtt_dispatcher->pending_count = 0;
    mqtt_dispatcher->pending_capacity = MQTT_PENDING_INITIAL_CAPACITY;

    ulak_log("info", "Created MQTT dispatcher for broker: %s:%d, topic: %s, QoS: %d",
             mqtt_dispatcher->broker, mqtt_dispatcher->port, mqtt_dispatcher->topic,
             mqtt_dispatcher->qos);
    return (Dispatcher *)mqtt_dispatcher;
}

/**
 * @brief Dispatch a single MQTT message synchronously.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param error_msg Output error message on failure
 * @return true on successful delivery, false on failure
 */
bool mqtt_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    MqttDispatcher *mqtt_dispatcher;
    int ret;
    int mqtt_timeout;
    int mid = 0;

    mqtt_dispatcher = (MqttDispatcher *)dispatcher->private_data;
    if (!mqtt_dispatcher || !payload || !mqtt_dispatcher->client) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid MQTT dispatcher or payload");
        return false;
    }

    /* Ensure connection (with connection pooling) */
    if (!mqtt_ensure_connected(mqtt_dispatcher, error_msg)) {
        return false;
    }

    /* Publish message */
    ulak_log("debug", "Publishing MQTT message to topic: %s, QoS: %d, retain: %s",
             mqtt_dispatcher->topic, mqtt_dispatcher->qos,
             mqtt_dispatcher->retain ? "true" : "false");

    ret = mosquitto_publish(mqtt_dispatcher->client, &mid, mqtt_dispatcher->topic, strlen(payload),
                            payload, mqtt_dispatcher->qos, mqtt_dispatcher->retain);
    if (ret != MOSQ_ERR_SUCCESS) {
        const char *prefix = mqtt_classify_error(ret);
        ulak_log("warning", "Failed to publish MQTT message: %s", mosquitto_strerror(ret));
        if (error_msg) {
            *error_msg =
                psprintf("%s Failed to publish MQTT message: %s", prefix, mosquitto_strerror(ret));
        }
        /* Mark connection as broken for next attempt to reconnect */
        mqtt_dispatcher->connected = false;
        return false;
    }

    /* Store message ID for dispatch_ex */
    mqtt_dispatcher->last_mid = mid;

    /* Wait for message to be delivered - use GUC value for timeout */
    mqtt_timeout = ulak_mqtt_timeout > 0 ? ulak_mqtt_timeout : 5000;

    if (mqtt_dispatcher->qos > 0) {
        /* For QoS 1/2, loop until PUBACK/PUBCOMP or timeout */
        struct timespec start, now;
        int elapsed_ms = 0;
        clock_gettime(CLOCK_MONOTONIC, &start);
        while (elapsed_ms < mqtt_timeout) {
            ret = mosquitto_loop(mqtt_dispatcher->client, 100, 1);
            if (ret != MOSQ_ERR_SUCCESS) {
                const char *prefix = mqtt_classify_error(ret);
                ulak_log("warning", "MQTT loop failed: %s", mosquitto_strerror(ret));
                if (error_msg) {
                    *error_msg =
                        psprintf("%s MQTT loop failed: %s", prefix, mosquitto_strerror(ret));
                }
                mqtt_dispatcher->connected = false;
                return false;
            }
            clock_gettime(CLOCK_MONOTONIC, &now);
            elapsed_ms =
                (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
            /* Check if published message is no longer in flight */
            if (mosquitto_want_write(mqtt_dispatcher->client) == false)
                break;
        }
    } else {
        /* QoS 0: single non-blocking loop iteration to flush write buffer.
         * timeout=0 means don't block -- just send pending data and return. */
        ret = mosquitto_loop(mqtt_dispatcher->client, 0, 1);
        if (ret != MOSQ_ERR_SUCCESS) {
            const char *prefix = mqtt_classify_error(ret);
            ulak_log("warning", "MQTT loop failed: %s", mosquitto_strerror(ret));
            if (error_msg) {
                *error_msg = psprintf("%s MQTT loop failed: %s", prefix, mosquitto_strerror(ret));
            }
            mqtt_dispatcher->connected = false;
            return false;
        }
    }

    /* Update last activity time for connection pooling */
    mqtt_dispatcher->last_activity = time(NULL);

    ulak_log("debug", "MQTT message published successfully to topic: %s (mid: %d)",
             mqtt_dispatcher->topic, mid);

    return true;
}

/**
 * @brief Extended dispatch with headers/metadata and response capture.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param headers Per-message JSONB headers (unused for MQTT)
 * @param metadata JSONB metadata (unused for MQTT)
 * @param result Output dispatch result with MQTT mid
 * @return true on success, false on failure
 */
bool mqtt_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result) {
    MqttDispatcher *mqtt = (MqttDispatcher *)dispatcher->private_data;
    char *error_msg = NULL;
    bool success;

    mqtt->last_mid = 0;
    success = mqtt_dispatcher_dispatch(dispatcher, payload, &error_msg);

    if (result) {
        result->success = success;
        if (!success && error_msg) {
            result->error_msg = error_msg;
        } else {
            if (error_msg)
                pfree(error_msg);
        }
        result->mqtt_mid = mqtt->last_mid;
    } else {
        /* No result struct, free error_msg if allocated */
        if (error_msg)
            pfree(error_msg);
    }
    return success;
}

/**
 * @brief Produce a message to MQTT without waiting for PUBACK (non-blocking).
 *
 * The message is added to the pending array and will be confirmed during flush.
 *
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param error_msg Output error message on failure
 * @return true if message was published, false on failure
 */
bool mqtt_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                             char **error_msg) {
    MqttDispatcher *mqtt;
    int mid = 0;
    int ret;
    MqttPendingMessage *pm;

    mqtt = (MqttDispatcher *)dispatcher->private_data;
    if (!mqtt || !payload || !mqtt->client) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid MQTT dispatcher or payload");
        return false;
    }

    /* Ensure connection */
    if (!mqtt_ensure_connected(mqtt, error_msg))
        return false;

    /* Ensure batch capacity */
    if (mqtt->pending_count >= mqtt->pending_capacity) {
        int new_capacity = mqtt->pending_capacity * 2;
        MqttPendingMessage *new_array = palloc(sizeof(MqttPendingMessage) * new_capacity);
        if (mqtt->pending_count > 0) {
            memcpy(new_array, mqtt->pending_messages,
                   sizeof(MqttPendingMessage) * mqtt->pending_count);
        }
        pfree(mqtt->pending_messages);
        mqtt->pending_messages = new_array;
        mqtt->pending_capacity = new_capacity;
    }

    /* Non-blocking publish */
    ret = mosquitto_publish(mqtt->client, &mid, mqtt->topic, strlen(payload), payload, mqtt->qos,
                            mqtt->retain);

    if (ret != MOSQ_ERR_SUCCESS) {
        const char *prefix = mqtt_classify_error(ret);
        if (error_msg)
            *error_msg = psprintf("%s MQTT publish failed: %s", prefix, mosquitto_strerror(ret));
        mqtt->connected = false;
        return false;
    }

    /* Add to pending array */
    pm = &mqtt->pending_messages[mqtt->pending_count];
    pm->msg_id = msg_id;
    pm->mid = mid;
    pm->delivered = false;
    pm->success = false;
    pm->error[0] = '\0';

    mqtt->pending_count++;

    ulak_log("debug", "MQTT produced msg_id=%lld mid=%d (pending: %d)", (long long)msg_id, mid,
             mqtt->pending_count);

    return true;
}

/**
 * @brief Flush all pending MQTT messages and wait for PUBACKs.
 *
 * Calls mosquitto_loop() repeatedly until all pending messages
 * have received their PUBACK/PUBCOMP, or timeout expires.
 *
 * @param dispatcher Dispatcher instance
 * @param timeout_ms Maximum time to wait for delivery confirmations
 * @param failed_ids Output array of failed message IDs
 * @param failed_count Output count of failed messages
 * @param failed_errors Output array of error strings for failed messages
 * @return Number of successfully delivered messages
 */
int mqtt_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                          int *failed_count, char ***failed_errors) {
    MqttDispatcher *mqtt;
    struct timespec start_time;
    int elapsed_ms = 0;
    int ret;
    bool all_delivered;
    int i, j;
    int success_count = 0;
    int fail_count = 0;

    mqtt = (MqttDispatcher *)dispatcher->private_data;

    if (!mqtt || mqtt->pending_count == 0) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    /* Use CLOCK_MONOTONIC for accurate timeout (same as Kafka/HTTP) */
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    /* Main loop: call mosquitto_loop() until all PUBACKs received or timeout */
    do {
        ret = mosquitto_loop(mqtt->client, 100, 1); /* 100ms poll */

        if (ret != MOSQ_ERR_SUCCESS) {
            ulak_log("warning", "MQTT loop error during flush: %s", mosquitto_strerror(ret));
            /* Mark remaining undelivered messages as failed */
            for (i = 0; i < mqtt->pending_count; i++) {
                if (!mqtt->pending_messages[i].delivered) {
                    mqtt->pending_messages[i].delivered = true;
                    mqtt->pending_messages[i].success = false;
                    snprintf(mqtt->pending_messages[i].error, MQTT_ERROR_BUFFER_SIZE,
                             "%s MQTT loop failed: %s", mqtt_classify_error(ret),
                             mosquitto_strerror(ret));
                }
            }
            mqtt->connected = false;
            break;
        }

        /* Check if all messages have been delivered */
        all_delivered = true;
        for (i = 0; i < mqtt->pending_count; i++) {
            if (!mqtt->pending_messages[i].delivered) {
                all_delivered = false;
                break;
            }
        }

        if (all_delivered)
            break;

        /* Calculate elapsed time */
        {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            elapsed_ms = (int)((now.tv_sec - start_time.tv_sec) * 1000 +
                               (now.tv_nsec - start_time.tv_nsec) / 1000000);
        }
    } while (elapsed_ms < timeout_ms);

    /* Mark timed-out messages as failed */
    for (i = 0; i < mqtt->pending_count; i++) {
        if (!mqtt->pending_messages[i].delivered) {
            mqtt->pending_messages[i].delivered = true;
            mqtt->pending_messages[i].success = false;
            snprintf(mqtt->pending_messages[i].error, MQTT_ERROR_BUFFER_SIZE,
                     ERROR_PREFIX_RETRYABLE " MQTT delivery timed out after %d ms", timeout_ms);
        }
    }

    /* Count successes and failures */
    for (i = 0; i < mqtt->pending_count; i++) {
        if (mqtt->pending_messages[i].success)
            success_count++;
        else
            fail_count++;
    }

    /* Build failed_ids and failed_errors arrays (same pattern as Kafka) */
    if (fail_count > 0 && failed_ids && failed_count) {
        *failed_ids = palloc(sizeof(int64) * fail_count);
        *failed_count = fail_count;
        if (failed_errors)
            *failed_errors = palloc(sizeof(char *) * fail_count);

        j = 0;
        for (i = 0; i < mqtt->pending_count; i++) {
            MqttPendingMessage *pm = &mqtt->pending_messages[i];
            if (!pm->success && j < fail_count) {
                (*failed_ids)[j] = pm->msg_id;
                if (failed_errors && *failed_errors) {
                    (*failed_errors)[j] = pm->error[0] != '\0' ? pstrdup(pm->error)
                                                               : pstrdup(ERROR_PREFIX_RETRYABLE
                                                                         " MQTT delivery failed");
                }
                j++;
            }
        }
    } else {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
    }

    /* Reset pending count for next batch */
    mqtt->pending_count = 0;

    /* Update last activity time */
    mqtt->last_activity = time(NULL);

    ulak_log("debug", "MQTT flush complete: %d success, %d failed", success_count, fail_count);

    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher Dispatcher instance
 * @return true if QoS > 0 (batch with PUBACK tracking), false for QoS 0
 */
bool mqtt_dispatcher_supports_batch(Dispatcher *dispatcher) {
    MqttDispatcher *mqtt = (MqttDispatcher *)dispatcher->private_data;

    /* Batch mode only makes sense for QoS > 0.
     * QoS 0 is already fire-and-forget, no PUBACK to wait for. */
    return (mqtt && mqtt->qos > 0);
}

/**
 * @brief Validate MQTT configuration from JSONB.
 * @param config JSONB configuration to validate
 * @return true if configuration is valid, false otherwise
 */
bool mqtt_dispatcher_validate_config(Jsonb *config) {
    JsonbValue broker_val, topic_val, val;
    char *unknown_key = NULL;

    if (!config) {
        return false;
    }

    /* Strict validation: reject unknown configuration keys */
    if (!jsonb_validate_keys(config, MQTT_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        ulak_log("error",
                 "MQTT config contains unknown key '%s'. "
                 "Allowed keys: broker, topic, qos, port, client_id, retain, username, "
                 "password, options, tls, tls_ca_cert, tls_cert, tls_key, tls_insecure, "
                 "will_topic, will_payload, will_qos, will_retain, clean_session",
                 unknown_key ? unknown_key : "unknown");
        if (unknown_key) {
            pfree(unknown_key);
        }
        return false;
    }

    if (!extract_jsonb_value(config, MQTT_CONFIG_KEY_BROKER, &broker_val) ||
        broker_val.type != jbvString || broker_val.val.string.len == 0) {
        ulak_log("error", "MQTT config: 'broker' is required and must be non-empty string");
        return false;
    }

    if (!extract_jsonb_value(config, MQTT_CONFIG_KEY_TOPIC, &topic_val) ||
        topic_val.type != jbvString || topic_val.val.string.len == 0) {
        ulak_log("error", "MQTT config: 'topic' is required and must be non-empty string");
        return false;
    }
    /* MQTT publish topics must not contain wildcards ('+' single-level,
     * '#' multi-level). These are only valid for subscribe operations. */
    if (memchr(topic_val.val.string.val, '+', topic_val.val.string.len) != NULL ||
        memchr(topic_val.val.string.val, '#', topic_val.val.string.len) != NULL) {
        ulak_log("error", "MQTT config: 'topic' must not contain wildcard characters ('+' or '#')");
        return false;
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_QOS, &val)) {
        int qos;
        if (val.type != jbvNumeric) {
            ulak_log("error", "MQTT config: 'qos' must be a number");
            return false;
        }
        qos = jsonb_get_int32(config, MQTT_CONFIG_KEY_QOS, 0);
        if (qos < 0 || qos > 2) {
            ulak_log("error", "MQTT config: 'qos' must be between 0 and 2");
            return false;
        }
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_PORT, &val)) {
        int port;
        if (val.type != jbvNumeric) {
            ulak_log("error", "MQTT config: 'port' must be a number");
            return false;
        }
        port = jsonb_get_int32(config, MQTT_CONFIG_KEY_PORT, 0);
        if (port < 1 || port > 65535) {
            ulak_log("error", "MQTT config: 'port' must be between 1 and 65535");
            return false;
        }
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_RETAIN, &val) && val.type != jbvBool) {
        ulak_log("error", "MQTT config: 'retain' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS, &val) && val.type != jbvBool) {
        ulak_log("error", "MQTT config: 'tls' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_TLS_INSECURE, &val) && val.type != jbvBool) {
        ulak_log("error", "MQTT config: 'tls_insecure' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_OPTIONS, &val)) {
        if (val.type != jbvBinary && val.type != jbvObject) {
            ulak_log("error", "MQTT config: 'options' must be an object");
            return false;
        }
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_QOS, &val)) {
        int will_qos;
        if (val.type != jbvNumeric) {
            ulak_log("error", "MQTT config: 'will_qos' must be a number");
            return false;
        }
        will_qos = jsonb_get_int32(config, MQTT_CONFIG_KEY_WILL_QOS, 0);
        if (will_qos < 0 || will_qos > 2) {
            ulak_log("error", "MQTT config: 'will_qos' must be between 0 and 2");
            return false;
        }
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_WILL_RETAIN, &val) && val.type != jbvBool) {
        ulak_log("error", "MQTT config: 'will_retain' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, MQTT_CONFIG_KEY_CLEAN_SESSION, &val) && val.type != jbvBool) {
        ulak_log("error", "MQTT config: 'clean_session' must be a boolean");
        return false;
    }

    return true;
}

/**
 * @brief Clean up and destroy an MQTT dispatcher.
 * @param dispatcher Dispatcher to clean up
 */
void mqtt_dispatcher_cleanup(Dispatcher *dispatcher) {
    MqttDispatcher *mqtt_dispatcher = (MqttDispatcher *)dispatcher->private_data;
    if (!mqtt_dispatcher)
        return;

    if (mqtt_dispatcher->client) {
        /* Disconnect if still connected (connection pooling cleanup) */
        if (mqtt_dispatcher->connected) {
            mosquitto_disconnect(mqtt_dispatcher->client);
            mqtt_dispatcher->connected = false;
            ulak_log("debug", "MQTT connection closed during cleanup");
        }

        /*
         * Clear user data pointer BEFORE destroying the client.
         * This prevents potential use-after-free if any callback
         * is invoked during destruction (defense in depth).
         */
        mosquitto_user_data_set(mqtt_dispatcher->client, NULL);

        /* Destroy Mosquitto client */
        mosquitto_destroy(mqtt_dispatcher->client);
        mqtt_dispatcher->client = NULL;
        ulak_log("debug", "MQTT client destroyed");
    }

    /* Batch tracking cleanup */
    if (mqtt_dispatcher->pending_messages) {
        pfree(mqtt_dispatcher->pending_messages);
        mqtt_dispatcher->pending_messages = NULL;
    }
    mqtt_dispatcher->pending_count = 0;
    mqtt_dispatcher->pending_capacity = 0;

    /* Cleanup strings (includes zeroing sensitive data) */
    mqtt_cleanup_strings(mqtt_dispatcher);

    /*
     * Note: Do NOT free mqtt_dispatcher->options - it's a reference to the
     * parent config which is owned by the caller (worker/queue).
     */

    pfree(mqtt_dispatcher);

    ulak_log("debug", "MQTT dispatcher cleaned up");
}
