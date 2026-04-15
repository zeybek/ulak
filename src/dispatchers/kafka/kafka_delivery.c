/**
 * @file kafka_delivery.c
 * @brief Kafka message delivery operations.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles synchronous dispatch and async batch operations (produce/flush).
 */

#include <string.h>
#include <time.h>
#include "config/guc.h"
#include "kafka_internal.h"
#include "postgres.h"
#include "storage/spin.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/* ============================================================================
 * Headers Helpers
 * ============================================================================ */

/**
 * @private
 * @brief Add headers from a Jsonb config that contains a "headers" sub-object.
 *
 * Iterates the full config, finds the "headers" key, extracts inner key-value pairs.
 * Same pattern as HTTP dispatcher's http_build_headers.
 *
 * @param hdrs rd_kafka_headers to populate
 * @param jsonb JSONB config containing a "headers" sub-object
 */
static void kafka_add_jsonb_headers(rd_kafka_headers_t *hdrs, Jsonb *jsonb) {
    JsonbIterator *it;
    JsonbValue v;
    JsonbIteratorToken tok;
    bool in_headers = false;
    char *current_key = NULL;

    if (!jsonb || !hdrs)
        return;

    it = JsonbIteratorInit(&jsonb->root);

    while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        if (tok == WJB_KEY && v.type == jbvString) {
            size_t hdr_key_len = strlen(CONFIG_KEY_HEADERS);
            if (v.val.string.len == hdr_key_len &&
                strncmp(v.val.string.val, CONFIG_KEY_HEADERS, hdr_key_len) == 0) {
                in_headers = true;
            } else if (in_headers) {
                current_key = pnstrdup(v.val.string.val, v.val.string.len);
            }
        } else if (in_headers && tok == WJB_VALUE && v.type == jbvString && current_key) {
            char *val = pnstrdup(v.val.string.val, v.val.string.len);
            rd_kafka_header_add(hdrs, current_key, -1, val, strlen(val));
            pfree(val);
            pfree(current_key);
            current_key = NULL;
        } else if (tok == WJB_END_OBJECT && in_headers) {
            in_headers = false;
        }
    }
    if (current_key)
        pfree(current_key);
}

/**
 * @private
 * @brief Build rd_kafka_headers from static endpoint headers and per-message headers.
 *
 * Per-message headers override static headers with the same key.
 *
 * @param kd Kafka dispatcher with static_headers config
 * @param per_msg_headers Per-message JSONB headers, or NULL
 * @return Populated rd_kafka_headers (never NULL)
 */
static rd_kafka_headers_t *kafka_build_headers(KafkaDispatcher *kd, Jsonb *per_msg_headers) {
    rd_kafka_headers_t *hdrs;

    /* Always return a valid (possibly empty) headers object.
     * RD_KAFKA_V_HEADERS(NULL) causes segfault in rd_kafka_producev. */
    hdrs = rd_kafka_headers_new(8);

    /* Static headers from endpoint config */
    if (kd->static_headers) {
        kafka_add_jsonb_headers(hdrs, kd->static_headers);
    }

    /* Per-message headers override static ones */
    if (per_msg_headers) {
        kafka_add_jsonb_headers(hdrs, per_msg_headers);
    }

    return hdrs;
}

/* ============================================================================
 * MONOTONIC TIMEOUT HELPER
 * ============================================================================ */

/**
 * @private
 * @brief Calculate elapsed milliseconds since a given start time.
 * @param start Monotonic clock start time
 * @return Elapsed time in milliseconds
 */
static int kafka_elapsed_ms(struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int)((now.tv_sec - start->tv_sec) * 1000 + (now.tv_nsec - start->tv_nsec) / 1000000);
}

/* ============================================================================
 * SYNCHRONOUS DISPATCH
 * ============================================================================ */

/**
 * @brief Dispatch a single Kafka message synchronously.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param error_msg Output error message on failure (caller must pfree)
 * @return true on successful delivery, false on failure
 */
bool kafka_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    KafkaDispatcher *kafka_dispatcher;
    rd_kafka_resp_err_t err;
    int delivery_timeout;
    int poll_interval;
    int elapsed_ms;
    bool completed;
    bool final_completed;
    bool final_success;
    char err_copy[KAFKA_ERROR_BUFFER_SIZE];
    const char *err_str;
    const char *prefix;
    struct timespec start_time;
    rd_kafka_headers_t *hdrs;

    kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;

    if (!kafka_dispatcher || !payload || !kafka_dispatcher->producer ||
        !kafka_dispatcher->topic_handle) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Kafka dispatcher or payload");
        return false;
    }

    /* Reset delivery tracking state - protected by spinlock */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);
    kafka_dispatcher->delivery_completed = false;
    kafka_dispatcher->delivery_success = false;
    kafka_dispatcher->delivery_error[0] = '\0';
    kafka_dispatcher->last_partition = -1;
    kafka_dispatcher->last_offset = -1;
    kafka_dispatcher->last_timestamp = 0;
    SpinLockRelease(&kafka_dispatcher->pending_lock);

    /* Build headers from static config (dispatch() path has no per-message headers) */
    hdrs = kafka_build_headers(kafka_dispatcher, NULL);

    /* Produce message using rd_kafka_producev for headers support */
    err = rd_kafka_producev(
        kafka_dispatcher->producer, RD_KAFKA_V_TOPIC(kafka_dispatcher->topic),
        RD_KAFKA_V_PARTITION(kafka_dispatcher->partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)payload, strlen(payload)),
        RD_KAFKA_V_KEY(kafka_dispatcher->key,
                       kafka_dispatcher->key ? strlen(kafka_dispatcher->key) : 0),
        RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_OPAQUE(NULL), RD_KAFKA_V_END);

    if (err) {
        /* On failure we still own the headers */
        if (hdrs)
            rd_kafka_headers_destroy(hdrs);

        prefix = kafka_classify_error(err);
        ulak_log("warning", "Failed to enqueue Kafka message: %s", rd_kafka_err2str(err));
        if (error_msg) {
            *error_msg =
                psprintf("%s Failed to enqueue Kafka message: %s", prefix, rd_kafka_err2str(err));
        }
        return false;
    }
    /* On success, librdkafka takes ownership of hdrs */

    /* Wait for delivery confirmation with CLOCK_MONOTONIC timeout */
    delivery_timeout = ulak_kafka_delivery_timeout > 0 ? ulak_kafka_delivery_timeout : 30000;
    poll_interval = ulak_kafka_poll_interval > 0 ? ulak_kafka_poll_interval : 100;
    completed = false;

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    do {
        rd_kafka_poll(kafka_dispatcher->producer, poll_interval);

        /* Thread-safe read of completion flag */
        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        completed = kafka_dispatcher->delivery_completed;
        SpinLockRelease(&kafka_dispatcher->pending_lock);

        elapsed_ms = kafka_elapsed_ms(&start_time);
    } while (!completed && elapsed_ms < delivery_timeout);

    /* Check delivery result - read all fields under lock for consistency */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);
    final_completed = kafka_dispatcher->delivery_completed;
    final_success = kafka_dispatcher->delivery_success;
    strlcpy(err_copy, kafka_dispatcher->delivery_error, KAFKA_ERROR_BUFFER_SIZE);
    SpinLockRelease(&kafka_dispatcher->pending_lock);

    if (!final_completed) {
        ulak_log("warning", "Kafka message delivery timed out after %d ms", delivery_timeout);
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Kafka message delivery timed out after %d ms",
                         delivery_timeout);
        }
        return false;
    }

    if (!final_success) {
        err_str = err_copy[0] != '\0' ? err_copy : ERROR_PREFIX_RETRYABLE " Unknown error";
        ulak_log("warning", "Kafka message delivery failed: %s", err_str);
        if (error_msg) {
            /* err_copy already contains the classification prefix from callback */
            *error_msg = psprintf("Kafka message delivery failed: %s", err_str);
        }
        return false;
    }

    ulak_log("debug", "Kafka message delivered successfully to topic: %s", kafka_dispatcher->topic);
    return true;
}

/* ============================================================================
 * BATCH OPERATIONS - Async produce/flush pattern for high throughput
 * ============================================================================ */

/**
 * @private
 * @brief Ensure batch capacity is sufficient, growing the array if needed.
 *
 * CRITICAL: This must NOT be called while holding the spinlock,
 * because palloc/repalloc can ereport(ERROR) which is not safe under spinlock.
 *
 * @param kd Kafka dispatcher
 * @param error_msg Output error message on failure
 * @return true if capacity is sufficient (possibly after growth), false on error
 */
static bool kafka_ensure_batch_capacity(KafkaDispatcher *kd, char **error_msg) {
    int current_count;
    int current_capacity;
    int new_capacity;
    KafkaPendingMessage *new_array;

    /* Read current state under lock */
    SpinLockAcquire(&kd->pending_lock);
    current_count = kd->pending_count;
    current_capacity = kd->pending_capacity;
    SpinLockRelease(&kd->pending_lock);

    if (current_count < current_capacity)
        return true;

    /* Need to grow - allocate new array OUTSIDE the lock */
    new_capacity = current_capacity * 2;
    new_array = palloc(sizeof(KafkaPendingMessage) * new_capacity);

    /* Copy and swap under lock */
    SpinLockAcquire(&kd->pending_lock);

    /* Re-check: another thread may have changed things */
    if (kd->pending_count < kd->pending_capacity) {
        /* Someone else grew it or flushed - no longer needed */
        SpinLockRelease(&kd->pending_lock);
        pfree(new_array);
        return true;
    }

    /* Copy existing entries to new array */
    if (kd->pending_count > 0) {
        memcpy(new_array, kd->pending_messages, sizeof(KafkaPendingMessage) * kd->pending_count);
    }

    /* Swap arrays */
    {
        KafkaPendingMessage *old_array = kd->pending_messages;
        kd->pending_messages = new_array;
        kd->pending_capacity = new_capacity;
        SpinLockRelease(&kd->pending_lock);

        /* Free old array outside lock */
        if (old_array)
            pfree(old_array);
    }

    return true;
}

/**
 * @brief Produce a message to Kafka without waiting for delivery (non-blocking).
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param error_msg Output error message on failure
 * @return true if message was enqueued, false on failure
 */
bool kafka_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                              char **error_msg) {
    KafkaDispatcher *kafka_dispatcher;
    int index;
    KafkaPendingMessage *pm;
    rd_kafka_resp_err_t err;
    const char *prefix;

    kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;

    if (!kafka_dispatcher || !payload || !kafka_dispatcher->producer ||
        !kafka_dispatcher->topic_handle) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Kafka dispatcher or payload");
        return false;
    }

    /* Ensure capacity - allocates OUTSIDE spinlock */
    if (!kafka_ensure_batch_capacity(kafka_dispatcher, error_msg)) {
        return false;
    }

    /* Initialize pending message entry under lock */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);

    /* Double-check capacity after acquiring lock */
    if (kafka_dispatcher->pending_count >= kafka_dispatcher->pending_capacity) {
        SpinLockRelease(&kafka_dispatcher->pending_lock);
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE
                                 " Kafka batch capacity reached, flush before producing more");
        return false;
    }

    index = kafka_dispatcher->pending_count;
    pm = &kafka_dispatcher->pending_messages[index];
    pm->msg_id = msg_id;
    pm->delivered = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->kafka_partition = -1;
    pm->kafka_offset = -1;
    pm->kafka_timestamp = 0;

    /* Increment count BEFORE releasing lock to reserve the slot */
    kafka_dispatcher->pending_count++;

    SpinLockRelease(&kafka_dispatcher->pending_lock);

    /* Build headers from static config */
    {
        rd_kafka_headers_t *hdrs = kafka_build_headers(kafka_dispatcher, NULL);

        /* Produce message using rd_kafka_producev - pass index as opaque for callback */
        err = rd_kafka_producev(
            kafka_dispatcher->producer, RD_KAFKA_V_TOPIC(kafka_dispatcher->topic),
            RD_KAFKA_V_PARTITION(kafka_dispatcher->partition),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE((void *)payload, strlen(payload)),
            RD_KAFKA_V_KEY(kafka_dispatcher->key,
                           kafka_dispatcher->key ? strlen(kafka_dispatcher->key) : 0),
            RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_OPAQUE((void *)(intptr_t)index), RD_KAFKA_V_END);

        if (err) {
            /* On failure we still own headers */
            rd_kafka_headers_destroy(hdrs);
        }
        /* On success librdkafka owns headers */
    }

    if (err) {
        /* Rollback the pending count on failure */
        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        kafka_dispatcher->pending_count--;
        SpinLockRelease(&kafka_dispatcher->pending_lock);

        prefix = kafka_classify_error(err);
        ulak_log("warning", "Failed to enqueue Kafka message (batch): %s", rd_kafka_err2str(err));
        if (error_msg) {
            *error_msg =
                psprintf("%s Failed to enqueue Kafka message: %s", prefix, rd_kafka_err2str(err));
        }
        return false;
    }

    return true;
}

/**
 * @brief Flush all pending Kafka messages and collect delivery results.
 * @param dispatcher Dispatcher instance
 * @param timeout_ms Maximum time to wait for delivery confirmations
 * @param failed_ids Output array of failed message IDs
 * @param failed_count Output count of failed messages
 * @param failed_errors Output array of error strings for failed messages
 * @return Number of successfully delivered messages
 */
int kafka_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                           int *failed_count, char ***failed_errors) {
    KafkaDispatcher *kafka_dispatcher;
    int total_pending;
    int fail_count = 0;
    int success_count;
    int i, j;
    rd_kafka_resp_err_t flush_err;
    struct timespec start_time;
    int elapsed_ms;
    int poll_interval;
    bool all_delivered;

    kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;

    if (!kafka_dispatcher || !kafka_dispatcher->producer) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    /* Check pending count under lock */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);
    total_pending = kafka_dispatcher->pending_count;
    SpinLockRelease(&kafka_dispatcher->pending_lock);

    if (total_pending == 0) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    /* Use CLOCK_MONOTONIC for accurate timeout tracking */
    poll_interval = ulak_kafka_poll_interval > 0 ? ulak_kafka_poll_interval : 100;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    /* Poll until all messages delivered or timeout */
    do {
        rd_kafka_poll(kafka_dispatcher->producer, poll_interval);

        /* Check if all messages have been delivered */
        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        all_delivered = true;
        for (i = 0; i < kafka_dispatcher->pending_count; i++) {
            if (!kafka_dispatcher->pending_messages[i].delivered) {
                all_delivered = false;
                break;
            }
        }
        SpinLockRelease(&kafka_dispatcher->pending_lock);

        elapsed_ms = kafka_elapsed_ms(&start_time);
    } while (!all_delivered && elapsed_ms < timeout_ms);

    /* If not all delivered, try a final flush */
    if (!all_delivered) {
        int remaining_ms = timeout_ms - elapsed_ms;
        if (remaining_ms > 0) {
            flush_err = rd_kafka_flush(kafka_dispatcher->producer, remaining_ms);
            if (flush_err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
                ulak_log("warning", "Kafka flush timed out, some messages may not be delivered");
            }
        }
        /* Final poll to process any remaining callbacks */
        rd_kafka_poll(kafka_dispatcher->producer, 0);
    }

    /*
     * At this point, all callbacks should have fired.
     * Copy results under lock into local arrays, then release lock
     * before doing any palloc (palloc must NEVER be called under SpinLock).
     */
    {
        int pending;

        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        pending = kafka_dispatcher->pending_count;

        /* Count failures and snapshot data under lock */
        for (i = 0; i < pending; i++) {
            KafkaPendingMessage *pm = &kafka_dispatcher->pending_messages[i];
            if (!pm->delivered || !pm->success) {
                fail_count++;
            }
        }

        /*
         * If we need to return failure data, snapshot IDs and errors
         * into stack/local storage under the lock, then palloc after release.
         * For simplicity, use a two-pass approach: count under lock, allocate
         * outside, then re-read under lock.
         */
        success_count = pending - fail_count;

        /* Reset pending count for next batch while still under lock */
        kafka_dispatcher->pending_count = 0;

        if (fail_count > 0 && failed_ids && failed_count) {
            /*
             * We need to copy failure data. Since pending_count is now 0,
             * no new callbacks will write to these slots (callbacks check index < pending_count).
             * The array memory is still valid. We can safely read it after releasing the lock.
             * BUT: we must not hold the lock during palloc. The slots we read are "frozen"
             * because pending_count=0 means callbacks skip them.
             */
            SpinLockRelease(&kafka_dispatcher->pending_lock);

            *failed_ids = palloc(sizeof(int64) * fail_count);
            *failed_count = fail_count;
            if (failed_errors) {
                *failed_errors = palloc(sizeof(char *) * fail_count);
            }

            /*
             * Re-read the frozen slots. No lock needed because:
             * 1. pending_count is 0, so callbacks skip all slots
             * 2. No new produce calls can reuse slots until after we return
             */
            j = 0;
            for (i = 0; i < pending; i++) {
                KafkaPendingMessage *pm = &kafka_dispatcher->pending_messages[i];
                if (!pm->delivered || !pm->success) {
                    if (j < fail_count) {
                        (*failed_ids)[j] = pm->msg_id;
                        if (failed_errors && *failed_errors) {
                            if (pm->error[0] != '\0') {
                                (*failed_errors)[j] = pstrdup(pm->error);
                            } else if (!pm->delivered) {
                                (*failed_errors)[j] =
                                    pstrdup(ERROR_PREFIX_RETRYABLE " Kafka delivery timed out");
                            } else {
                                (*failed_errors)[j] =
                                    pstrdup(ERROR_PREFIX_RETRYABLE " Kafka delivery failed");
                            }
                        }
                        j++;
                    }
                }
            }
        } else {
            SpinLockRelease(&kafka_dispatcher->pending_lock);

            if (failed_count)
                *failed_count = fail_count;
            if (failed_errors)
                *failed_errors = NULL;
        }
    }

    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher Dispatcher instance (unused)
 * @return Always true for Kafka
 */
bool kafka_dispatcher_supports_batch(Dispatcher *dispatcher) {
    (void)dispatcher; /* unused */
    return true;
}

/* ============================================================================
 * EXTENDED OPERATIONS - dispatch_ex / produce_ex with headers + metadata
 * ============================================================================ */

/**
 * @brief Extended dispatch with per-message headers, metadata key, and DispatchResult capture.
 *
 * Metadata Jsonb may contain "key" field to override the endpoint-level message key.
 * Headers Jsonb contains per-message Kafka headers (flat string key-value object).
 *
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param headers Per-message JSONB headers, or NULL
 * @param metadata JSONB metadata with optional "key" override, or NULL
 * @param result Output dispatch result with delivery metadata
 * @return true on successful delivery, false on failure
 */
bool kafka_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                  Jsonb *metadata, DispatchResult *result) {
    KafkaDispatcher *kafka_dispatcher;
    rd_kafka_resp_err_t err;
    int delivery_timeout;
    int poll_interval;
    int elapsed_ms;
    bool completed;
    bool final_completed;
    bool final_success;
    char err_copy[KAFKA_ERROR_BUFFER_SIZE];
    const char *err_str;
    const char *prefix;
    struct timespec start_time;
    rd_kafka_headers_t *hdrs;
    const char *msg_key;
    size_t msg_key_len;
    JsonbValue key_val;

    kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;

    if (!kafka_dispatcher || !payload || !kafka_dispatcher->producer ||
        !kafka_dispatcher->topic_handle) {
        if (result) {
            result->success = false;
            result->error_msg =
                pstrdup(ERROR_PREFIX_PERMANENT " Invalid Kafka dispatcher or payload");
        }
        return false;
    }

    /* Determine message key: metadata "key" overrides endpoint config key */
    msg_key = kafka_dispatcher->key;
    msg_key_len = msg_key ? strlen(msg_key) : 0;

    if (metadata && extract_jsonb_value(metadata, "key", &key_val) && key_val.type == jbvString &&
        key_val.val.string.len > 0) {
        msg_key = pnstrdup(key_val.val.string.val, key_val.val.string.len);
        msg_key_len = key_val.val.string.len;
    }

    /* Reset delivery tracking state */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);
    kafka_dispatcher->delivery_completed = false;
    kafka_dispatcher->delivery_success = false;
    kafka_dispatcher->delivery_error[0] = '\0';
    kafka_dispatcher->last_partition = -1;
    kafka_dispatcher->last_offset = -1;
    kafka_dispatcher->last_timestamp = 0;
    SpinLockRelease(&kafka_dispatcher->pending_lock);

    /* Build headers from static config + per-message headers */
    hdrs = kafka_build_headers(kafka_dispatcher, headers);

    /* Produce using rd_kafka_producev for headers support */
    err = rd_kafka_producev(
        kafka_dispatcher->producer, RD_KAFKA_V_TOPIC(kafka_dispatcher->topic),
        RD_KAFKA_V_PARTITION(kafka_dispatcher->partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)payload, strlen(payload)), RD_KAFKA_V_KEY(msg_key, msg_key_len),
        RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_OPAQUE(NULL), RD_KAFKA_V_END);

    if (err) {
        if (hdrs)
            rd_kafka_headers_destroy(hdrs);

        prefix = kafka_classify_error(err);
        if (result) {
            result->success = false;
            result->error_msg =
                psprintf("%s Failed to enqueue Kafka message: %s", prefix, rd_kafka_err2str(err));
        }

        /* Free dynamically allocated key if we overrode it */
        if (msg_key != kafka_dispatcher->key && msg_key)
            pfree((char *)msg_key);

        return false;
    }

    /* Free dynamically allocated key now that produce succeeded */
    if (msg_key != kafka_dispatcher->key && msg_key)
        pfree((char *)msg_key);

    /* Wait for delivery with CLOCK_MONOTONIC */
    delivery_timeout = ulak_kafka_delivery_timeout > 0 ? ulak_kafka_delivery_timeout : 30000;
    poll_interval = ulak_kafka_poll_interval > 0 ? ulak_kafka_poll_interval : 100;
    completed = false;

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    do {
        rd_kafka_poll(kafka_dispatcher->producer, poll_interval);

        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        completed = kafka_dispatcher->delivery_completed;
        SpinLockRelease(&kafka_dispatcher->pending_lock);

        elapsed_ms = kafka_elapsed_ms(&start_time);
    } while (!completed && elapsed_ms < delivery_timeout);

    /* Read final result under lock */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);
    final_completed = kafka_dispatcher->delivery_completed;
    final_success = kafka_dispatcher->delivery_success;
    strlcpy(err_copy, kafka_dispatcher->delivery_error, KAFKA_ERROR_BUFFER_SIZE);

    if (result) {
        result->response_time_ms = kafka_elapsed_ms(&start_time);
        if (final_success) {
            result->kafka_partition = kafka_dispatcher->last_partition;
            result->kafka_offset = kafka_dispatcher->last_offset;
            result->kafka_timestamp = kafka_dispatcher->last_timestamp;
        }
    }
    SpinLockRelease(&kafka_dispatcher->pending_lock);

    if (!final_completed) {
        if (result) {
            result->success = false;
            result->error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Kafka message delivery timed out after %d ms",
                         delivery_timeout);
        }
        return false;
    }

    if (!final_success) {
        err_str = err_copy[0] != '\0' ? err_copy : ERROR_PREFIX_RETRYABLE " Unknown error";
        if (result) {
            result->success = false;
            result->error_msg = psprintf("Kafka message delivery failed: %s", err_str);
        }
        return false;
    }

    if (result) {
        result->success = true;
        result->error_msg = NULL;
    }

    ulak_log("debug", "Kafka message delivered (ex) to topic: %s partition: %d offset: %ld",
             kafka_dispatcher->topic, result ? result->kafka_partition : -1,
             result ? (long)result->kafka_offset : -1L);
    return true;
}

/**
 * @brief Extended produce with per-message headers and key from metadata.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param headers Per-message JSONB headers, or NULL
 * @param metadata JSONB metadata with optional "key" override, or NULL
 * @param error_msg Output error message on failure
 * @return true if message was enqueued, false on failure
 */
bool kafka_dispatcher_produce_ex(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                                 Jsonb *headers, Jsonb *metadata, char **error_msg) {
    KafkaDispatcher *kafka_dispatcher;
    int index;
    KafkaPendingMessage *pm;
    rd_kafka_resp_err_t err;
    const char *prefix;
    rd_kafka_headers_t *hdrs;
    const char *msg_key;
    size_t msg_key_len;
    JsonbValue key_val;

    kafka_dispatcher = (KafkaDispatcher *)dispatcher->private_data;

    if (!kafka_dispatcher || !payload || !kafka_dispatcher->producer ||
        !kafka_dispatcher->topic_handle) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Kafka dispatcher or payload");
        return false;
    }

    /* Determine message key: metadata "key" overrides endpoint config key */
    msg_key = kafka_dispatcher->key;
    msg_key_len = msg_key ? strlen(msg_key) : 0;

    if (metadata && extract_jsonb_value(metadata, "key", &key_val) && key_val.type == jbvString &&
        key_val.val.string.len > 0) {
        msg_key = pnstrdup(key_val.val.string.val, key_val.val.string.len);
        msg_key_len = key_val.val.string.len;
    }

    /* Ensure capacity - allocates OUTSIDE spinlock */
    if (!kafka_ensure_batch_capacity(kafka_dispatcher, error_msg)) {
        if (msg_key != kafka_dispatcher->key && msg_key)
            pfree((char *)msg_key);
        return false;
    }

    /* Initialize pending message entry under lock */
    SpinLockAcquire(&kafka_dispatcher->pending_lock);

    if (kafka_dispatcher->pending_count >= kafka_dispatcher->pending_capacity) {
        SpinLockRelease(&kafka_dispatcher->pending_lock);
        if (msg_key != kafka_dispatcher->key && msg_key)
            pfree((char *)msg_key);
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE
                                 " Kafka batch capacity reached, flush before producing more");
        return false;
    }

    index = kafka_dispatcher->pending_count;
    pm = &kafka_dispatcher->pending_messages[index];
    pm->msg_id = msg_id;
    pm->delivered = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->kafka_partition = -1;
    pm->kafka_offset = -1;
    pm->kafka_timestamp = 0;

    kafka_dispatcher->pending_count++;

    SpinLockRelease(&kafka_dispatcher->pending_lock);

    /* Build headers */
    hdrs = kafka_build_headers(kafka_dispatcher, headers);

    /* Produce with rd_kafka_producev for headers support */
    err = rd_kafka_producev(
        kafka_dispatcher->producer, RD_KAFKA_V_TOPIC(kafka_dispatcher->topic),
        RD_KAFKA_V_PARTITION(kafka_dispatcher->partition), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void *)payload, strlen(payload)), RD_KAFKA_V_KEY(msg_key, msg_key_len),
        RD_KAFKA_V_HEADERS(hdrs), RD_KAFKA_V_OPAQUE((void *)(intptr_t)index), RD_KAFKA_V_END);

    if (err) {
        /* On failure we still own the headers */
        if (hdrs)
            rd_kafka_headers_destroy(hdrs);

        /* Rollback the pending count */
        SpinLockAcquire(&kafka_dispatcher->pending_lock);
        kafka_dispatcher->pending_count--;
        SpinLockRelease(&kafka_dispatcher->pending_lock);

        prefix = kafka_classify_error(err);
        ulak_log("warning", "Failed to enqueue Kafka message (batch_ex): %s",
                 rd_kafka_err2str(err));
        if (error_msg) {
            *error_msg =
                psprintf("%s Failed to enqueue Kafka message: %s", prefix, rd_kafka_err2str(err));
        }

        if (msg_key != kafka_dispatcher->key && msg_key)
            pfree((char *)msg_key);
        return false;
    }
    /* On success, librdkafka takes ownership of hdrs */

    /* Free dynamically allocated key */
    if (msg_key != kafka_dispatcher->key && msg_key)
        pfree((char *)msg_key);

    return true;
}
