/**
 * @file kafka_dispatcher.h
 * @brief Apache Kafka dispatcher using librdkafka.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Provides synchronous single-message dispatch and high-throughput
 * batch produce/flush via librdkafka's asynchronous producer.
 * Delivery tracking uses spinlock-protected arrays safe for
 * librdkafka's background callback thread.
 */

#ifndef ULAK_DISPATCHERS_KAFKA_DISPATCHER_H
#define ULAK_DISPATCHERS_KAFKA_DISPATCHER_H

#include <librdkafka/rdkafka.h>
#include "dispatchers/dispatcher.h"
#include "postgres.h"
#include "storage/spin.h" /* SpinLock for thread safety */
#include "utils/jsonb.h"

#define KAFKA_PENDING_INITIAL_CAPACITY 64 /**< Initial capacity for pending messages array. */
#define KAFKA_ERROR_BUFFER_SIZE 256 /**< Maximum error message length for thread-safe storage. */

/** @brief Tracks a single in-flight message during batch produce/flush. */
typedef struct KafkaPendingMessage {
    int64 msg_id;                        /**< PostgreSQL queue row ID. */
    bool delivered;                      /**< Delivery callback received. */
    bool success;                        /**< Delivery successful. */
    char error[KAFKA_ERROR_BUFFER_SIZE]; /**< Error message (fixed buffer for thread safety). */
    int32 kafka_partition;               /**< Partition the message was delivered to. */
    int64 kafka_offset;                  /**< Offset within partition. */
    int64 kafka_timestamp;               /**< Message timestamp from broker. */
} KafkaPendingMessage;

/**
 * @brief Kafka protocol dispatcher state.
 *
 * Holds librdkafka producer, topic handle, authentication config,
 * static headers, and both single-message and batch delivery tracking.
 * The pending_lock spinlock protects the pending_messages array since
 * librdkafka's delivery callbacks run on a background thread.
 */
typedef struct KafkaDispatcher {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Endpoint configuration (parsed from JSONB) */
    /** @{ */
    char *broker;      /**< Kafka broker address(es). */
    char *topic;       /**< Target topic name. */
    char *key;         /**< Optional message key. */
    int32_t partition; /**< Optional partition (-1 = unassigned). */
    /** @} */

    /** @name librdkafka handles */
    /** @{ */
    rd_kafka_t *producer;           /**< Kafka producer instance. */
    rd_kafka_topic_t *topic_handle; /**< Cached topic handle. */
    rd_kafka_conf_t *conf;          /**< Kafka configuration (before producer creation). */
    /** @} */

    /** @name Single-message delivery tracking for dispatch() fallback */
    /** @{ */
    volatile bool delivery_completed; /**< Set by callback when delivery report received. */
    volatile bool delivery_success;   /**< True if message delivered successfully. */
    char
        delivery_error[KAFKA_ERROR_BUFFER_SIZE]; /**< Fixed buffer for thread-safe error storage. */
    /** @} */

    /** @name Batch delivery tracking */
    /** @{ */
    KafkaPendingMessage *pending_messages; /**< Array of pending message statuses. */
    volatile int
        pending_count;    /**< Current number of pending messages (volatile for thread safety). */
    int pending_capacity; /**< Allocated capacity of pending array. */
    /** @} */

    /** @name Thread safety */
    /** @{ */
    slock_t pending_lock; /**< Spinlock protecting pending_messages during reallocation. */
    /** @} */

    Jsonb *static_headers; /**< Static headers from endpoint config "headers" field. */

    /** @name Last synchronous dispatch result (from delivery callback) */
    /** @{ */
    int32 last_partition; /**< Partition from most recent delivery. */
    int64 last_offset;    /**< Offset from most recent delivery. */
    int64 last_timestamp; /**< Timestamp from most recent delivery. */
    /** @} */
} KafkaDispatcher;

/** @name Core dispatcher functions */
/** @{ */

/**
 * @brief Create a Kafka dispatcher from endpoint JSONB config.
 * @param config JSONB endpoint configuration.
 * @return Allocated dispatcher, or NULL on failure.
 */
extern Dispatcher *kafka_dispatcher_create(Jsonb *config);

/**
 * @brief Synchronous single-message dispatch with delivery confirmation.
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true on successful delivery, false on failure.
 */
extern bool kafka_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload,
                                      char **error_msg);

/**
 * @brief Validate Kafka endpoint JSONB configuration.
 * @param config JSONB endpoint configuration to validate.
 * @return true if configuration is valid.
 */
extern bool kafka_dispatcher_validate_config(Jsonb *config);

/**
 * @brief Clean up Kafka dispatcher resources.
 * @param dispatcher The Kafka dispatcher to clean up.
 */
extern void kafka_dispatcher_cleanup(Dispatcher *dispatcher);

/** @} */

/** @name Batch operation functions */
/** @{ */

/**
 * @brief Enqueue a message for async batch publish (non-blocking).
 * @param dispatcher The Kafka dispatcher.
 * @param payload    Message payload string.
 * @param msg_id     PostgreSQL queue row ID for tracking.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true if message was enqueued successfully.
 */
extern bool kafka_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                                     char **error_msg);

/**
 * @brief Flush all pending messages and collect delivery results.
 *
 * Waits up to timeout_ms for all in-flight messages to receive delivery
 * reports, then partitions results into successes and failures.
 *
 * @param dispatcher   The Kafka dispatcher.
 * @param timeout_ms   Maximum time to wait for delivery (milliseconds).
 * @param[out] failed_ids    palloc'd array of failed message IDs.
 * @param[out] failed_count  Number of failed messages.
 * @param[out] failed_errors palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
extern int kafka_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                                  int *failed_count, char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher The Kafka dispatcher.
 * @return Always true for Kafka dispatcher.
 */
extern bool kafka_dispatcher_supports_batch(Dispatcher *dispatcher);

/** @} */

#endif /* ULAK_DISPATCHERS_KAFKA_DISPATCHER_H */
