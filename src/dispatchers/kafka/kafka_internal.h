/**
 * @file kafka_internal.h
 * @brief Internal declarations for the Kafka dispatcher module.
 *
 * @warning This header is for internal use within src/dispatchers/kafka/ only.
 *          Do not include from files outside this module.
 *
 * Contains config parsing, delivery callbacks, error classification,
 * and batch delivery functions.
 */

#ifndef ULAK_KAFKA_INTERNAL_H
#define ULAK_KAFKA_INTERNAL_H

#include <librdkafka/rdkafka.h>
#include "kafka_dispatcher.h"

/* ── JSONB config key constants ── */

#define CONFIG_KEY_BROKER "broker"
#define CONFIG_KEY_TOPIC "topic"
#define CONFIG_KEY_OPTIONS "options"
#define CONFIG_KEY_KEY "key"
#define CONFIG_KEY_PARTITION "partition"
#define CONFIG_KEY_HEADERS "headers"

/** @brief Allowed configuration keys -- declared in kafka_dispatcher.c. */
extern const char *KAFKA_ALLOWED_CONFIG_KEYS[];

/* ── Defaults ── */

#define DEFAULT_RETRIES "3"        /**< Default librdkafka message.send.max.retries. */
#define DEFAULT_BATCH_SIZE "16384" /**< Default librdkafka batch.num.messages. */
#define DEFAULT_LINGER_MS "5"      /**< Default librdkafka linger.ms. */

/* ── Configuration Functions (kafka_config.c) ── */

/**
 * @brief Parse Kafka options from JSONB configuration.
 *
 * Sets librdkafka configuration options from the "options" field.
 * Also applies GUC-based defaults for acks and compression.
 *
 * @param conf   librdkafka configuration to populate.
 * @param config JSONB endpoint configuration containing "options".
 */
void kafka_parse_options(rd_kafka_conf_t *conf, Jsonb *config);

/**
 * @brief Validate SASL/SSL configuration fields.
 * @param config JSONB endpoint configuration.
 * @return true if SASL/SSL configuration is valid.
 */
bool kafka_validate_sasl_ssl_config(Jsonb *config);

/* ── Callback Functions (kafka_callback.c) ── */

/**
 * @brief Classify a Kafka error as retryable or permanent.
 *
 * Safe to call from the librdkafka callback thread (uses only string constants).
 *
 * @param err librdkafka response error code.
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT.
 */
const char *kafka_classify_error(rd_kafka_resp_err_t err);

/**
 * @brief Delivery report callback invoked by librdkafka.
 *
 * Called from librdkafka's background thread, NOT the PostgreSQL thread.
 * Therefore, PostgreSQL memory functions (pstrdup, palloc, etc.) must
 * NOT be used. Fixed-size buffers and strncpy are used for error messages.
 *
 * @param rk        librdkafka producer handle.
 * @param rkmessage Delivery report message containing status and metadata.
 * @param opaque    Pointer to KafkaDispatcher (set via rd_kafka_conf_set_opaque).
 */
void kafka_delivery_report(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

/* ── Delivery Functions (kafka_delivery.c) ── */

/**
 * @brief Synchronous single-message dispatch with delivery confirmation.
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true on successful delivery, false on failure.
 */
bool kafka_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

/**
 * @brief Enqueue a message for async batch publish (non-blocking).
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param msg_id     PostgreSQL queue row ID for tracking.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true if message was enqueued successfully.
 */
bool kafka_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                              char **error_msg);

/**
 * @brief Flush all pending messages and collect delivery results.
 *
 * Waits up to timeout_ms for all in-flight messages to receive delivery
 * reports, then partitions results into successes and failures.
 *
 * @param dispatcher         The Kafka dispatcher.
 * @param timeout_ms         Maximum time to wait for delivery (milliseconds).
 * @param[out] failed_ids    palloc'd array of failed message IDs.
 * @param[out] failed_count  Number of failed messages.
 * @param[out] failed_errors palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
int kafka_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                           int *failed_count, char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher The Kafka dispatcher.
 * @return Always true for Kafka dispatcher.
 */
bool kafka_dispatcher_supports_batch(Dispatcher *dispatcher);

/**
 * @brief Extended dispatch with headers, key, and DispatchResult capture.
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param headers    Per-message JSONB headers (may be NULL).
 * @param metadata   Per-message JSONB metadata (may be NULL).
 * @param result     OUT - dispatch result with partition/offset/timestamp.
 * @return true on successful delivery, false on failure.
 */
bool kafka_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                  Jsonb *metadata, DispatchResult *result);

/**
 * @brief Extended produce with per-message headers and key from metadata.
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param msg_id     PostgreSQL queue row ID for tracking.
 * @param headers    Per-message JSONB headers (may be NULL).
 * @param metadata   Per-message JSONB metadata (may be NULL).
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true if message was enqueued successfully.
 */
bool kafka_dispatcher_produce_ex(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                                 Jsonb *headers, Jsonb *metadata, char **error_msg);

#endif /* ULAK_KAFKA_INTERNAL_H */
