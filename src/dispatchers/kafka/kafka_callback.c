/**
 * @file kafka_callback.c
 * @brief Kafka delivery callback handling.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles async delivery callbacks from librdkafka.
 *
 * CRITICAL THREAD SAFETY NOTES:
 * - This callback is called from librdkafka's background thread, NOT PostgreSQL
 * - NEVER use palloc, pfree, pstrdup, or any PostgreSQL memory functions
 * - Use fixed-size buffers and strncpy for error messages
 * - All access to shared state must be protected by spinlock
 */

#include <string.h>
#include "kafka_internal.h"
#include "postgres.h"
#include "storage/spin.h"

/**
 * @brief Classify a Kafka error as retryable or permanent.
 * @param err Kafka response error code
 * @return ERROR_PREFIX_RETRYABLE or ERROR_PREFIX_PERMANENT string constant
 *
 * THREAD SAFETY: Safe to call from librdkafka callback thread.
 * Only returns pointers to string constants (no palloc).
 */
const char *kafka_classify_error(rd_kafka_resp_err_t err) {
    switch (err) {
    case RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE:
    case RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED:
    case RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION:
    case RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM:
    case RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED:
        return "[PERMANENT]";
    default:
        return "[RETRYABLE]";
    }
}

/**
 * @brief Delivery report callback invoked by librdkafka when delivery status is known.
 *
 * IMPORTANT: This callback is called from librdkafka's background thread, NOT
 * the PostgreSQL thread. Therefore, we CANNOT use PostgreSQL memory functions
 * like pstrdup(), palloc(), etc. as they are not thread-safe.
 *
 * Solution: Use fixed-size buffers and strncpy for error messages.
 *
 * @param rk Kafka producer handle (unused)
 * @param rkmessage Delivered message with status and metadata
 * @param opaque Pointer to KafkaDispatcher set via rd_kafka_conf_set_opaque
 */
void kafka_delivery_report(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    KafkaDispatcher *kafka_dispatcher = (KafkaDispatcher *)opaque;
    intptr_t index;
    int pending_count;
    KafkaPendingMessage *pm;
    const char *err_str;
    const char *prefix;

    /* Suppress unused parameter warning */
    (void)rk;

    if (!kafka_dispatcher) {
        return;
    }

    /* Check if this is a batch mode message (has valid index in _private) */
    index = (intptr_t)rkmessage->_private;

    /*
     * Thread safety: Acquire spinlock before accessing ANY shared state.
     * This prevents race conditions when:
     * - Main thread is reallocating the pending_messages array
     * - Main thread is reading delivery_completed/success/error fields
     */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);

    pending_count = kafka_dispatcher->pending_count;

    if (index >= 0 && index < pending_count && kafka_dispatcher->pending_messages != NULL) {
        /* Batch mode - update specific message status */
        pm = &kafka_dispatcher->pending_messages[index];
        pm->delivered = true;
        pm->success = (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR);
        if (pm->success) {
            /* Capture delivery metadata */
            pm->kafka_partition = rkmessage->partition;
            pm->kafka_offset = rkmessage->offset;
            pm->kafka_timestamp = rd_kafka_message_timestamp(rkmessage, NULL);
        } else {
            /* Thread-safe: copy classified error to fixed buffer */
            prefix = kafka_classify_error(rkmessage->err);
            err_str = rd_kafka_err2str(rkmessage->err);
            snprintf(pm->error, KAFKA_ERROR_BUFFER_SIZE, "%s %s", prefix, err_str);
        }
    } else {
        /* Legacy single-message mode - also protected by spinlock */
        if (rkmessage->err) {
            kafka_dispatcher->delivery_success = false;
            /* Thread-safe: copy classified error to fixed buffer */
            prefix = kafka_classify_error(rkmessage->err);
            err_str = rd_kafka_err2str(rkmessage->err);
            snprintf(kafka_dispatcher->delivery_error, KAFKA_ERROR_BUFFER_SIZE, "%s %s", prefix,
                     err_str);
        } else {
            kafka_dispatcher->delivery_success = true;
            kafka_dispatcher->delivery_error[0] = '\0';
            /* Capture delivery metadata for sync dispatch */
            kafka_dispatcher->last_partition = rkmessage->partition;
            kafka_dispatcher->last_offset = rkmessage->offset;
            kafka_dispatcher->last_timestamp = rd_kafka_message_timestamp(rkmessage, NULL);
        }
        kafka_dispatcher->delivery_completed = true;
    }

    SpinLockRelease(&kafka_dispatcher->pending_lock);
}
