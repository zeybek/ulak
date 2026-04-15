/**
 * @file kafka_dispatcher.c
 * @brief Kafka protocol dispatcher implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles Apache Kafka message dispatching using librdkafka.
 *
 * This file contains:
 * - Dispatcher operations struct
 * - create/cleanup lifecycle functions
 * - validate_config function
 *
 * Other functionality is split into:
 * - kafka_config.c: Configuration parsing
 * - kafka_callback.c: Delivery report callback (thread safety)
 * - kafka_delivery.c: dispatch, produce, flush operations
 */

#include <string.h>
#include "config/guc.h"
#include "core/entities.h"
#include "kafka_internal.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "utils/numeric.h"

/** Allowed configuration keys for strict validation */
const char *KAFKA_ALLOWED_CONFIG_KEYS[] = {
    CONFIG_KEY_BROKER,
    CONFIG_KEY_TOPIC,
    CONFIG_KEY_OPTIONS,
    CONFIG_KEY_KEY,
    CONFIG_KEY_PARTITION,
    CONFIG_KEY_HEADERS,
    NULL /* NULL terminator */
};

/** Kafka Dispatcher Operations */
static DispatcherOperations kafka_dispatcher_ops = {
    .dispatch = kafka_dispatcher_dispatch,
    .validate_config = kafka_dispatcher_validate_config,
    .cleanup = kafka_dispatcher_cleanup,
    /* Batch operations */
    .produce = kafka_dispatcher_produce,
    .flush = kafka_dispatcher_flush,
    .supports_batch = kafka_dispatcher_supports_batch,
    /* Extended dispatch with headers/metadata support */
    .dispatch_ex = kafka_dispatcher_dispatch_ex,
    .produce_ex = kafka_dispatcher_produce_ex};

/**
 * @brief Validate Kafka configuration from JSONB.
 * @param config JSONB configuration to validate
 * @return true if configuration is valid, false otherwise
 */
bool kafka_dispatcher_validate_config(Jsonb *config) {
    JsonbValue broker_val, topic_val;
    char *unknown_key = NULL;

    if (!config) {
        return false;
    }

    /* Strict validation: reject unknown configuration keys */
    if (!jsonb_validate_keys(config, KAFKA_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        ulak_log("error",
                 "Kafka config contains unknown key '%s'. "
                 "Allowed keys: broker, topic, options, key, partition",
                 unknown_key ? unknown_key : "unknown");
        if (unknown_key) {
            pfree(unknown_key);
        }
        return false;
    }

    /* Kafka requires broker and topic - use stack allocation */
    return extract_jsonb_value(config, CONFIG_KEY_BROKER, &broker_val) &&
           broker_val.type == jbvString && broker_val.val.string.len > 0 &&
           extract_jsonb_value(config, CONFIG_KEY_TOPIC, &topic_val) &&
           topic_val.type == jbvString && topic_val.val.string.len > 0;
}

/**
 * @brief Create a new Kafka dispatcher instance.
 * @param config JSONB configuration with broker, topic, and optional settings
 * @return Dispatcher pointer on success, NULL on failure
 */
Dispatcher *kafka_dispatcher_create(Jsonb *config) {
    KafkaDispatcher *kafka_dispatcher;
    int batch_capacity;
    JsonbValue broker_val, topic_val, key_val, partition_val;
    char error_str[512];

    if (!kafka_dispatcher_validate_config(config)) {
        ulak_log("warning", "Invalid Kafka dispatcher configuration");
        return NULL;
    }

    kafka_dispatcher = (KafkaDispatcher *)palloc0(sizeof(KafkaDispatcher));
    /* palloc0 never returns NULL - it ereport(ERROR)s on OOM */

    /* Initialize all fields to safe defaults */
    kafka_dispatcher->broker = NULL;
    kafka_dispatcher->topic = NULL;
    kafka_dispatcher->key = NULL;
    kafka_dispatcher->partition = RD_KAFKA_PARTITION_UA;
    kafka_dispatcher->static_headers = NULL;
    kafka_dispatcher->producer = NULL;
    kafka_dispatcher->topic_handle = NULL;
    kafka_dispatcher->conf = NULL;
    kafka_dispatcher->delivery_completed = false;
    kafka_dispatcher->delivery_success = false;
    kafka_dispatcher->delivery_error[0] = '\0'; /* Initialize fixed buffer */

    /* Initialize batch tracking - use GUC value for initial capacity */
    batch_capacity =
        ulak_kafka_batch_capacity > 0 ? ulak_kafka_batch_capacity : KAFKA_PENDING_INITIAL_CAPACITY;
    kafka_dispatcher->pending_messages = palloc(sizeof(KafkaPendingMessage) * batch_capacity);
    kafka_dispatcher->pending_count = 0;
    kafka_dispatcher->pending_capacity = batch_capacity;

    /* Initialize spinlock for thread safety during callback/realloc */
    SpinLockInit(&kafka_dispatcher->pending_lock);

    /* Initialize base dispatcher */
    kafka_dispatcher->base.protocol = PROTOCOL_TYPE_KAFKA;
    kafka_dispatcher->base.config = config;
    kafka_dispatcher->base.ops = &kafka_dispatcher_ops;
    kafka_dispatcher->base.private_data = kafka_dispatcher;

    /* Parse configuration - use stack-allocated JsonbValues (no memory leak) */

    /* Get broker (required) */
    if (!extract_jsonb_value(config, CONFIG_KEY_BROKER, &broker_val) ||
        broker_val.type != jbvString) {
        ulak_log("warning", "Kafka config missing or invalid 'broker'");
        goto error;
    }
    kafka_dispatcher->broker = pnstrdup(broker_val.val.string.val, broker_val.val.string.len);

    /* Get topic (required) */
    if (!extract_jsonb_value(config, CONFIG_KEY_TOPIC, &topic_val) || topic_val.type != jbvString) {
        ulak_log("warning", "Kafka config missing or invalid 'topic'");
        goto error;
    }
    kafka_dispatcher->topic = pnstrdup(topic_val.val.string.val, topic_val.val.string.len);

    /* Get message key (optional) */
    if (extract_jsonb_value(config, CONFIG_KEY_KEY, &key_val) && key_val.type == jbvString) {
        kafka_dispatcher->key = pnstrdup(key_val.val.string.val, key_val.val.string.len);
    }

    /* Get partition (optional) - parse numeric value */
    if (extract_jsonb_value(config, CONFIG_KEY_PARTITION, &partition_val)) {
        if (partition_val.type == jbvNumeric) {
            kafka_dispatcher->partition = DatumGetInt32(
                DirectFunctionCall1(numeric_int4, NumericGetDatum(partition_val.val.numeric)));
        }
    }

    /* Get static headers (optional) - store reference to config (which is
     * deep-copied into DispatcherCacheContext by worker.c). The headers
     * are extracted at dispatch time by iterating the full config. */
    {
        JsonbValue headers_val;
        if (extract_jsonb_value(config, CONFIG_KEY_HEADERS, &headers_val) &&
            headers_val.type == jbvBinary) {
            kafka_dispatcher->static_headers = config;
        }
    }

    /* Create Kafka configuration */
    kafka_dispatcher->conf = rd_kafka_conf_new();
    if (!kafka_dispatcher->conf) {
        ulak_log("warning", "Failed to create Kafka configuration");
        goto error;
    }

    /* Parse and apply options from config */
    kafka_parse_options(kafka_dispatcher->conf, config);

    /* Set broker list */
    if (rd_kafka_conf_set(kafka_dispatcher->conf, "bootstrap.servers", kafka_dispatcher->broker,
                          error_str, sizeof(error_str)) != RD_KAFKA_CONF_OK) {
        ulak_log("warning", "Failed to set Kafka bootstrap servers: %s", error_str);
        goto error;
    }

    /* Set delivery report callback BEFORE creating producer */
    rd_kafka_conf_set_dr_msg_cb(kafka_dispatcher->conf, kafka_delivery_report);

    /* Set opaque pointer to dispatcher for callback */
    rd_kafka_conf_set_opaque(kafka_dispatcher->conf, kafka_dispatcher);

    /* Create producer - this takes ownership of conf on success */
    kafka_dispatcher->producer =
        rd_kafka_new(RD_KAFKA_PRODUCER, kafka_dispatcher->conf, error_str, sizeof(error_str));
    if (!kafka_dispatcher->producer) {
        ulak_log("warning", "Failed to create Kafka producer: %s", error_str);
        goto error;
    }

    /* Producer now owns the conf object, clear our reference */
    kafka_dispatcher->conf = NULL;

    /* Create and cache topic handle */
    kafka_dispatcher->topic_handle =
        rd_kafka_topic_new(kafka_dispatcher->producer, kafka_dispatcher->topic, NULL);
    if (!kafka_dispatcher->topic_handle) {
        rd_kafka_resp_err_t err = rd_kafka_last_error();
        ulak_log("warning", "Failed to create Kafka topic handle: %s", rd_kafka_err2str(err));
        goto error;
    }

    ulak_log("info", "Created Kafka dispatcher for broker: %s, topic: %s", kafka_dispatcher->broker,
             kafka_dispatcher->topic);
    return (Dispatcher *)kafka_dispatcher;

error:
    /* Cleanup on error */
    if (kafka_dispatcher->producer) {
        rd_kafka_destroy(kafka_dispatcher->producer);
    } else if (kafka_dispatcher->conf) {
        /* Only destroy conf if producer wasn't created (producer takes ownership) */
        rd_kafka_conf_destroy(kafka_dispatcher->conf);
    }
    if (kafka_dispatcher->broker)
        pfree(kafka_dispatcher->broker);
    if (kafka_dispatcher->topic)
        pfree(kafka_dispatcher->topic);
    if (kafka_dispatcher->key)
        pfree(kafka_dispatcher->key);
    /* Note: delivery_error is a fixed buffer, no need to free */
    if (kafka_dispatcher->pending_messages)
        pfree(kafka_dispatcher->pending_messages);
    pfree(kafka_dispatcher);
    return NULL;
}

/**
 * @brief Clean up and destroy a Kafka dispatcher.
 * @param dispatcher Dispatcher to clean up
 */
void kafka_dispatcher_cleanup(Dispatcher *dispatcher) {
    KafkaDispatcher *kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;
    if (!kafka_dispatcher)
        return;

    /* Flush outstanding messages BEFORE destroying topic handle */
    if (kafka_dispatcher->producer) {
        int flush_timeout = ulak_kafka_flush_timeout > 0 ? ulak_kafka_flush_timeout : 5000;
        rd_kafka_flush(kafka_dispatcher->producer, flush_timeout);
    }

    /* Destroy topic handle after flush (no more references in flight) */
    if (kafka_dispatcher->topic_handle) {
        rd_kafka_topic_destroy(kafka_dispatcher->topic_handle);
        kafka_dispatcher->topic_handle = NULL;
    }

    /* Destroy Kafka producer */
    if (kafka_dispatcher->producer) {
        rd_kafka_destroy(kafka_dispatcher->producer);
        kafka_dispatcher->producer = NULL;
        ulak_log("debug", "Kafka producer destroyed");
    }

    /* Cleanup configuration (only if not owned by producer - shouldn't happen normally) */
    if (kafka_dispatcher->conf) {
        rd_kafka_conf_destroy(kafka_dispatcher->conf);
        kafka_dispatcher->conf = NULL;
    }

    /* Cleanup allocated strings */
    if (kafka_dispatcher->broker) {
        pfree(kafka_dispatcher->broker);
    }
    if (kafka_dispatcher->topic) {
        pfree(kafka_dispatcher->topic);
    }
    if (kafka_dispatcher->key) {
        pfree(kafka_dispatcher->key);
    }
    /* Note: delivery_error is a fixed buffer, no need to free */

    /* Cleanup batch tracking */
    if (kafka_dispatcher->pending_messages) {
        /* Note: pm->error is now a fixed buffer, no need to free individual entries */
        pfree(kafka_dispatcher->pending_messages);
    }

    /* Note: Do NOT free base.config - it's owned by the caller (worker/queue) */

    pfree(kafka_dispatcher);
    ulak_log("debug", "Kafka dispatcher cleaned up");
}
