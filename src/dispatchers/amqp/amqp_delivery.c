/**
 * @file amqp_delivery.c
 * @brief AMQP message delivery operations.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles synchronous dispatch and async batch operations (produce/flush)
 * with publisher confirms for reliable delivery.
 */

#include <string.h>
#include <time.h>

#include "amqp_internal.h"
#include "config/guc.h"
#include "miscadmin.h"
#include "postgres.h"
#include "storage/spin.h"
#include "utils/logging.h"

/* Error prefix constants */
#define ERROR_PREFIX_RETRYABLE "[RETRYABLE]"
#define ERROR_PREFIX_PERMANENT "[PERMANENT]"

/** Prefix of the message_id property; the rest is the queue row id. */
#define AMQP_MESSAGE_ID_PREFIX "msg_"

/**
 * @private
 * @brief Process-unique id for callers that do not know the queue row id.
 *
 * Same scheme as the HTTP dispatcher's fallback: pid in the high half, a
 * counter in the low half, never a delivery tag (those restart at 1 on every
 * connection).
 *
 * @return Fallback id.
 */
static int64 amqp_fallback_msg_id(void) {
    static uint32 counter = 0;

    counter++;
    return ((int64)MyProcPid << 32) | (int64)counter;
}

/**
 * @private
 * @brief Read the id back from a returned message's message_id property.
 * @param message_id Property value ("msg_<id>").
 * @return The id, or 0 if the value is not ours.
 */
static int64 amqp_parse_message_id(amqp_bytes_t message_id) {
    char buf[64];
    size_t prefix_len = strlen(AMQP_MESSAGE_ID_PREFIX);
    char *end;
    long long id;

    if (message_id.len <= prefix_len || message_id.len >= sizeof(buf))
        return 0;
    memcpy(buf, message_id.bytes, message_id.len);
    buf[message_id.len] = '\0';
    if (strncmp(buf, AMQP_MESSAGE_ID_PREFIX, prefix_len) != 0)
        return 0;
    id = strtoll(buf + prefix_len, &end, 10);
    return *end == '\0' ? (int64)id : 0;
}

/**
 * @brief Process publisher confirms from the broker.
 *
 * Reads ACK/NACK frames and updates pending message status.
 *
 * @param amqp AMQP dispatcher instance
 * @param timeout_ms Maximum time to wait for confirm frames
 * @return Number of confirms processed (positive) or -1 on error
 */
int amqp_process_confirms(AmqpDispatcher *amqp, int timeout_ms) {
    amqp_frame_t frame;
    struct timeval tv;
    int confirms_processed = 0;
    amqp_rpc_reply_t ret;
    int status;
    char err_buffer[256];
    const char *err;

    if (!amqp || !amqp->conn || !amqp->confirm_mode) {
        return -1;
    }

    /* Set timeout for waiting */
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    while (1) {
        /* Try to read a frame with timeout */
        status = amqp_simple_wait_frame_noblock(amqp->conn, &frame, &tv);

        if (status == AMQP_STATUS_TIMEOUT) {
            /* No more frames available within timeout */
            break;
        }

        if (status != AMQP_STATUS_OK) {
            ulak_log("warning", "AMQP wait frame error: %s", amqp_error_string2(status));
            amqp->connected = false;
            return -1;
        }

        /* Process the frame based on type */
        if (frame.frame_type == AMQP_FRAME_METHOD) {
            if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
                amqp_basic_ack_t *ack = (amqp_basic_ack_t *)frame.payload.method.decoded;
                uint64_t delivery_tag = ack->delivery_tag;
                bool multiple = ack->multiple;

                /* Mark message(s) as confirmed -- no spinlock (single-threaded) */
                for (int i = 0; i < amqp->pending_count; i++) {
                    AmqpPendingMessage *pm = &amqp->pending_messages[i];
                    if (multiple) {
                        /* ACK all messages up to and including delivery_tag */
                        if (pm->delivery_tag <= delivery_tag && !pm->confirmed) {
                            pm->confirmed = true;
                            pm->success = true;
                            confirms_processed++;
                        }
                    } else {
                        /* ACK only this specific delivery_tag */
                        if (pm->delivery_tag == delivery_tag && !pm->confirmed) {
                            pm->confirmed = true;
                            pm->success = true;
                            confirms_processed++;
                            break;
                        }
                    }
                }

                ulak_log("debug", "AMQP ACK received: delivery_tag=%lu, multiple=%d",
                         (unsigned long)delivery_tag, multiple);

            } else if (frame.payload.method.id == AMQP_BASIC_NACK_METHOD) {
                amqp_basic_nack_t *nack = (amqp_basic_nack_t *)frame.payload.method.decoded;
                uint64_t delivery_tag = nack->delivery_tag;
                bool multiple = nack->multiple;

                /* Mark message(s) as NACK'd -- no spinlock (single-threaded) */
                for (int i = 0; i < amqp->pending_count; i++) {
                    AmqpPendingMessage *pm = &amqp->pending_messages[i];
                    if (multiple) {
                        if (pm->delivery_tag <= delivery_tag && !pm->confirmed) {
                            pm->confirmed = true;
                            pm->success = false;
                            snprintf(pm->error, AMQP_ERROR_BUFFER_SIZE, "Message NACK'd by broker");
                            confirms_processed++;
                        }
                    } else {
                        if (pm->delivery_tag == delivery_tag && !pm->confirmed) {
                            pm->confirmed = true;
                            pm->success = false;
                            snprintf(pm->error, AMQP_ERROR_BUFFER_SIZE, "Message NACK'd by broker");
                            confirms_processed++;
                            break;
                        }
                    }
                }

                ulak_log("warning", "AMQP NACK received: delivery_tag=%lu, multiple=%d",
                         (unsigned long)delivery_tag, multiple);

            } else if (frame.payload.method.id == AMQP_BASIC_RETURN_METHOD) {
                /*
                 * A mandatory publish that no queue took. The broker follows
                 * it with a basic.ack (not a nack) for the same publish, sent
                 * after the return, so the ack alone would mark a lost message
                 * delivered: fail it here, matched by its message_id.
                 */
                amqp_basic_return_t *ret = (amqp_basic_return_t *)frame.payload.method.decoded;
                amqp_message_t returned;
                amqp_rpc_reply_t read_reply;
                int reply_code = ret->reply_code;
                char reply_text[128];
                int64 returned_id = 0;

                snprintf(reply_text, sizeof(reply_text), "%.*s", (int)ret->reply_text.len,
                         (char *)ret->reply_text.bytes);
                ulak_log("warning",
                         "AMQP message returned: reply_code=%d, reply_text=%s, "
                         "exchange=%.*s, routing_key=%.*s",
                         reply_code, reply_text, (int)ret->exchange.len,
                         (char *)ret->exchange.bytes, (int)ret->routing_key.len,
                         (char *)ret->routing_key.bytes);

                /* The returned content (header + body frames) follows the method */
                read_reply = amqp_read_message(amqp->conn, frame.channel, &returned, 0);
                if (read_reply.reply_type != AMQP_RESPONSE_NORMAL) {
                    err = amqp_reply_error_string(read_reply, err_buffer, sizeof(err_buffer));
                    ulak_log("error", "AMQP failed to read returned message: %s", err);
                    amqp->connected = false;
                    return -1;
                }
                if (returned.properties._flags & AMQP_BASIC_MESSAGE_ID_FLAG)
                    returned_id = amqp_parse_message_id(returned.properties.message_id);
                amqp_destroy_message(&returned);

                for (int i = 0; returned_id != 0 && i < amqp->pending_count; i++) {
                    AmqpPendingMessage *pm = &amqp->pending_messages[i];
                    if (pm->published_id == returned_id && !pm->confirmed) {
                        pm->confirmed = true;
                        pm->success = false;
                        snprintf(pm->error, AMQP_ERROR_BUFFER_SIZE,
                                 "AMQP message returned by broker: %d %s", reply_code, reply_text);
                        confirms_processed++;
                        break;
                    }
                }

            } else if (frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD ||
                       frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD) {
                /* Connection/channel closed unexpectedly */
                ret = amqp_get_rpc_reply(amqp->conn);
                err = amqp_reply_error_string(ret, err_buffer, sizeof(err_buffer));
                ulak_log("error", "AMQP connection/channel closed during confirm: %s", err);
                amqp->connected = false;
                return -1;
            }
        }

        /* Use a very short timeout for subsequent frames */
        tv.tv_sec = 0;
        tv.tv_usec = 1000; /* 1ms */
    }

    return confirms_processed;
}

/**
 * @private
 * @brief Publish a single message to AMQP broker.
 *
 * Does NOT wait for confirm - used by both dispatch() and produce().
 *
 * @param amqp AMQP dispatcher instance
 * @param payload Message payload string
 * @param published_id Id for the message_id property (queue row id or fallback)
 * @param delivery_tag_out Output delivery tag assigned to this message
 * @param error_msg Output error message on failure
 * @return true if message was published, false on failure
 */
static bool amqp_publish_message(AmqpDispatcher *amqp, const char *payload, int64 published_id,
                                 uint64_t *delivery_tag_out, char **error_msg) {
    amqp_bytes_t exchange;
    amqp_bytes_t routing_key;
    amqp_basic_properties_t props;
    char msg_id_buf[64];
    int status;

    if (!amqp || !payload) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP dispatcher or payload");
        return false;
    }

    /* Ensure connection is valid */
    if (!amqp_ensure_connected(amqp, error_msg)) {
        return false;
    }

    /* Set up exchange and routing key */
    exchange.bytes = (void *)amqp->exchange;
    exchange.len = strlen(amqp->exchange);
    routing_key.bytes = (void *)amqp->routing_key;
    routing_key.len = strlen(amqp->routing_key);

    /* Set up message properties */
    memset(&props, 0, sizeof(props));
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG |
                   AMQP_BASIC_TIMESTAMP_FLAG | AMQP_BASIC_APP_ID_FLAG | AMQP_BASIC_MESSAGE_ID_FLAG;
    props.content_type = amqp_cstring_bytes("application/json");
    props.delivery_mode = amqp->persistent ? 2 : 1; /* 2 = persistent, 1 = transient */
    props.timestamp = (uint64_t)time(NULL);
    props.app_id = amqp_cstring_bytes("ulak");

    /* message_id = queue row id: unique, stable across retries, and how a
     * basic.return is matched to its pending entry. Delivery tags restart at 1
     * on every connection, so they cannot serve as an id. */
    snprintf(msg_id_buf, sizeof(msg_id_buf), AMQP_MESSAGE_ID_PREFIX "%lld",
             (long long)published_id);
    props.message_id = amqp_cstring_bytes(msg_id_buf);

    /* Get current delivery tag before publish */
    if (delivery_tag_out) {
        *delivery_tag_out = amqp->next_delivery_tag;
    }

    /* Publish the message */
    status = amqp_basic_publish(amqp->conn, amqp->channel, exchange, routing_key,
                                amqp->mandatory ? 1 : 0, /* mandatory */
                                0,                       /* immediate (deprecated in AMQP 0-9-1) */
                                &props, amqp_cstring_bytes(payload));

    if (status != AMQP_STATUS_OK) {
        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " AMQP publish failed: %s",
                                  amqp_error_string2(status));
        }
        ulak_log("warning", "AMQP basic_publish failed: %s", amqp_error_string2(status));
        /* The socket can look valid after the broker closed the connection;
         * reconnect on the next attempt instead of failing it the same way */
        amqp->connected = false;
        return false;
    }

    /* Increment delivery tag for next message */
    amqp->next_delivery_tag++;

    /* Update last successful operation time */
    amqp->last_successful_op = time(NULL);

    return true;
}

/**
 * @brief Dispatch a single AMQP message synchronously with confirm.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param error_msg Output error message on failure
 * @return true on successful delivery and confirm, false on failure
 */
bool amqp_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    if (!dispatcher || !dispatcher->private_data) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP dispatcher");
        return false;
    }

    return amqp_dispatch_sync((AmqpDispatcher *)dispatcher->private_data, payload, 0, error_msg);
}

/**
 * @brief Publish one message and wait for its publisher confirm.
 * @param amqp AMQP dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue row id, or 0 if unknown
 * @param error_msg Output error message on failure
 * @return true on successful delivery and confirm, false on failure
 */
bool amqp_dispatch_sync(AmqpDispatcher *amqp, const char *payload, int64 msg_id, char **error_msg) {
    uint64_t delivery_tag;
    int64 published_id = msg_id > 0 ? msg_id : amqp_fallback_msg_id();
    int delivery_timeout;
    int poll_interval;
    int elapsed_ms;
    bool confirmed;
    bool success;
    struct timespec start_time, now_time;

    /* Publish the message */
    if (!amqp_publish_message(amqp, payload, published_id, &delivery_tag, error_msg)) {
        return false;
    }

    /* Wait for publisher confirm with timeout */
    delivery_timeout = ulak_amqp_delivery_timeout > 0 ? ulak_amqp_delivery_timeout : 30000;
    poll_interval = 100; /* 100ms poll interval */
    elapsed_ms = 0;
    confirmed = false;
    success = false;

    /*
     * Add a temporary pending entry so amqp_process_confirms can track
     * the ACK/NACK for this delivery tag. Without this, the confirm loop
     * would never find a match since sync dispatch bypasses produce().
     */
    SpinLockAcquire(&amqp->pending_lock);
    if (amqp->pending_count < amqp->pending_capacity) {
        AmqpPendingMessage *pm = &amqp->pending_messages[amqp->pending_count];
        pm->msg_id = msg_id;
        pm->published_id = published_id;
        pm->delivery_tag = delivery_tag;
        pm->confirmed = false;
        pm->success = false;
        pm->error[0] = '\0';
        amqp->pending_count++;
    }
    SpinLockRelease(&amqp->pending_lock);

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    while (!confirmed && elapsed_ms < delivery_timeout) {
        int confirms = amqp_process_confirms(amqp, poll_interval);
        if (confirms < 0) {
            /* Clean up the temporary pending entry */
            SpinLockAcquire(&amqp->pending_lock);
            amqp->pending_count = 0;
            SpinLockRelease(&amqp->pending_lock);
            if (error_msg) {
                *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " AMQP confirm processing failed");
            }
            return false;
        }

        /* Use monotonic clock for accurate elapsed time */
        clock_gettime(CLOCK_MONOTONIC, &now_time);
        elapsed_ms = (int)((now_time.tv_sec - start_time.tv_sec) * 1000 +
                           (now_time.tv_nsec - start_time.tv_nsec) / 1000000);

        /* Check if our delivery tag was confirmed */
        SpinLockAcquire(&amqp->pending_lock);

        for (int i = 0; i < amqp->pending_count; i++) {
            AmqpPendingMessage *pm = &amqp->pending_messages[i];
            if (pm->delivery_tag == delivery_tag && pm->confirmed) {
                confirmed = true;
                success = pm->success;
                if (!success && error_msg && pm->error[0] != '\0') {
                    *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " %s", pm->error);
                }
                break;
            }
        }

        SpinLockRelease(&amqp->pending_lock);
    }

    /* Clean up the temporary pending entry */
    SpinLockAcquire(&amqp->pending_lock);
    amqp->pending_count = 0;
    SpinLockRelease(&amqp->pending_lock);

    if (!confirmed) {
        ulak_log("warning", "AMQP message confirm timed out after %d ms", delivery_timeout);
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " AMQP message confirm timed out after %d ms",
                         delivery_timeout);
        }
        return false;
    }

    if (!success) {
        ulak_log("warning", "AMQP message delivery failed (NACK'd or returned by broker)");
        if (error_msg && *error_msg == NULL) {
            *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " AMQP message NACK'd by broker");
        }
        return false;
    }

    ulak_log("debug", "AMQP message delivered successfully to exchange: %s, routing_key: %s",
             amqp->exchange, amqp->routing_key);
    return true;
}

/* ============================================================================
 * BATCH OPERATIONS - Async produce/flush pattern for high throughput
 * ============================================================================ */

/**
 * @brief Produce a message to AMQP without waiting for confirm (non-blocking).
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param error_msg Output error message on failure
 * @return true if message was published, false on failure
 */
bool amqp_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                             char **error_msg) {
    AmqpDispatcher *amqp;
    uint64_t delivery_tag;
    int new_capacity;
    int index;
    AmqpPendingMessage *pm;

    if (!dispatcher || !dispatcher->private_data) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP dispatcher");
        return false;
    }

    amqp = (AmqpDispatcher *)dispatcher->private_data;

    /* Grow capacity if needed -- no spinlock needed since librabbitmq is
     * single-threaded and worker calls produce/flush sequentially. */
    if (amqp->pending_count >= amqp->pending_capacity) {
        new_capacity = amqp->pending_capacity * 2;
        amqp->pending_messages =
            repalloc(amqp->pending_messages, sizeof(AmqpPendingMessage) * new_capacity);
        amqp->pending_capacity = new_capacity;
    }

    /* Publish the message (connection ensured inside) */
    if (!amqp_publish_message(amqp, payload, msg_id, &delivery_tag, error_msg)) {
        return false;
    }

    /* Track the pending message -- no spinlock needed (single-threaded worker) */
    index = amqp->pending_count;
    pm = &amqp->pending_messages[index];
    pm->msg_id = msg_id;
    pm->published_id = msg_id;
    pm->delivery_tag = delivery_tag;
    pm->confirmed = false;
    pm->success = false;
    pm->error[0] = '\0';

    amqp->pending_count++;

    ulak_log("debug", "AMQP message produced: msg_id=%lld, delivery_tag=%lu", (long long)msg_id,
             (unsigned long)delivery_tag);

    return true;
}

/**
 * @brief Flush all pending AMQP messages and collect confirm results.
 * @param dispatcher Dispatcher instance
 * @param timeout_ms Maximum time to wait for confirms
 * @param failed_ids Output array of failed message IDs
 * @param failed_count Output count of failed messages
 * @param failed_errors Output array of error strings for failed messages
 * @return Number of successfully confirmed messages
 */
int amqp_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                          int *failed_count, char ***failed_errors) {
    AmqpDispatcher *amqp;
    int total_pending;
    int fail_count = 0;
    int success_count;
    int i, j;
    int elapsed_ms = 0;
    int poll_interval = 10; /* 10ms poll interval -- shorter for faster confirm collection */
    int unconfirmed;
    struct timespec start_time, now_time;

    if (!dispatcher || !dispatcher->private_data) {
        if (failed_count)
            *failed_count = 0;
        return 0;
    }

    amqp = (AmqpDispatcher *)dispatcher->private_data;

    /* Get pending count -- no spinlock needed (single-threaded worker) */
    total_pending = amqp->pending_count;

    if (total_pending == 0) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    clock_gettime(CLOCK_MONOTONIC, &start_time);

    /* Wait for all confirms with timeout */
    while (elapsed_ms < timeout_ms) {
        int confirms = amqp_process_confirms(amqp, poll_interval);
        if (confirms < 0) {
            ulak_log("warning", "AMQP confirm processing error during flush");
            break;
        }

        /* Use monotonic clock for accurate elapsed time */
        clock_gettime(CLOCK_MONOTONIC, &now_time);
        elapsed_ms = (int)((now_time.tv_sec - start_time.tv_sec) * 1000 +
                           (now_time.tv_nsec - start_time.tv_nsec) / 1000000);

        /* Check if all messages are confirmed */
        unconfirmed = 0;
        for (i = 0; i < amqp->pending_count; i++) {
            if (!amqp->pending_messages[i].confirmed) {
                unconfirmed++;
            }
        }

        if (unconfirmed == 0) {
            break; /* All messages confirmed */
        }
    }

    /*
     * After timeout or all confirmed, count failures and collect results.
     */

    /* Count failures */
    for (i = 0; i < amqp->pending_count; i++) {
        AmqpPendingMessage *pm = &amqp->pending_messages[i];
        if (!pm->confirmed || !pm->success) {
            fail_count++;
        }
    }

    /* Return failed IDs and error strings if requested */
    if (fail_count > 0 && failed_ids && failed_count) {
        *failed_ids = palloc(sizeof(int64) * fail_count);
        *failed_count = fail_count;
        if (failed_errors) {
            *failed_errors = palloc(sizeof(char *) * fail_count);
        }
        j = 0;
        for (i = 0; i < amqp->pending_count; i++) {
            AmqpPendingMessage *pm = &amqp->pending_messages[i];
            if (!pm->confirmed || !pm->success) {
                (*failed_ids)[j] = pm->msg_id;
                if (failed_errors) {
                    if (!pm->confirmed) {
                        (*failed_errors)[j] =
                            pstrdup(ERROR_PREFIX_RETRYABLE " AMQP confirm timed out");
                    } else if (pm->error[0] != '\0') {
                        (*failed_errors)[j] = psprintf(ERROR_PREFIX_RETRYABLE " %s", pm->error);
                    } else {
                        (*failed_errors)[j] =
                            pstrdup(ERROR_PREFIX_RETRYABLE " AMQP message NACK'd by broker");
                    }
                }
                j++;
            }
        }
    } else {
        if (failed_count)
            *failed_count = fail_count;
        if (failed_errors)
            *failed_errors = NULL;
    }

    success_count = amqp->pending_count - fail_count;

    /* Clear pending count for next batch */
    amqp->pending_count = 0;

    if (fail_count > 0) {
        ulak_log("warning", "AMQP flush completed: %d success, %d failed", success_count,
                 fail_count);
    } else {
        ulak_log("debug", "AMQP flush completed: %d messages delivered successfully",
                 success_count);
    }

    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher Dispatcher instance (unused)
 * @return Always true (AMQP supports batch via publisher confirms)
 */
bool amqp_dispatcher_supports_batch(Dispatcher *dispatcher) {
    (void)dispatcher; /* unused */
    return true;      /* AMQP supports batch via publisher confirms */
}
