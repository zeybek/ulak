/**
 * @file nats_delivery.c
 * @brief NATS dispatch, produce, and flush operations.
 */

#include "postgres.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

#include "config/guc.h"
#include "nats_dispatcher.h"
#include "nats_internal.h"
#include "utils/json_utils.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "utils/memutils.h"

/**
 * @private
 * @brief Build a natsMsg with subject, payload, and headers.
 * @param nats NATS dispatcher instance
 * @param payload Message payload string
 * @param msg_id Message ID for JetStream deduplication (0 to skip)
 * @param per_msg_headers Per-message JSONB headers, or NULL
 * @return natsMsg on success, NULL on failure
 */
static natsMsg *nats_build_msg(NatsDispatcher *nats, const char *payload, int64 msg_id,
                               Jsonb *per_msg_headers) {
    natsMsg *msg = NULL;
    natsStatus s;
    char msg_id_str[32];

    s = natsMsg_Create(&msg, nats->subject, NULL, payload, (int)strlen(payload));
    if (s != NATS_OK)
        return NULL;

    /* Set Nats-Msg-Id for JetStream deduplication */
    if (nats->jetstream && msg_id > 0) {
        snprintf(msg_id_str, sizeof(msg_id_str), "%lld", (long long)msg_id);
        natsMsgHeader_Set(msg, "Nats-Msg-Id", msg_id_str);
    }

    /* Apply static headers from endpoint config */
    if (nats->static_headers != NULL) {
        JsonbIterator *it = JsonbIteratorInit(&nats->static_headers->root);
        JsonbValue key_val, val;
        JsonbIteratorToken tok;
        bool in_headers = false;

        while ((tok = JsonbIteratorNext(&it, &key_val, false)) != WJB_DONE) {
            if (tok == WJB_KEY && !in_headers) {
                char *k = pnstrdup(key_val.val.string.val, key_val.val.string.len);
                if (strcmp(k, "headers") == 0)
                    in_headers = true;
                pfree(k);
                continue;
            }
            if (in_headers && tok == WJB_KEY) {
                char *hdr_key = pnstrdup(key_val.val.string.val, key_val.val.string.len);
                tok = JsonbIteratorNext(&it, &val, true);
                if (tok == WJB_VALUE && val.type == jbvString) {
                    char *hdr_val = pnstrdup(val.val.string.val, val.val.string.len);
                    natsMsgHeader_Set(msg, hdr_key, hdr_val);
                    pfree(hdr_val);
                }
                pfree(hdr_key);
            }
            if (in_headers && tok == WJB_END_OBJECT)
                break;
        }
    }

    /* Apply per-message headers (override static) */
    if (per_msg_headers != NULL) {
        JsonbIterator *it = JsonbIteratorInit(&per_msg_headers->root);
        JsonbValue key_val, val;
        JsonbIteratorToken tok;

        while ((tok = JsonbIteratorNext(&it, &key_val, false)) != WJB_DONE) {
            if (tok == WJB_KEY) {
                char *hdr_key = pnstrdup(key_val.val.string.val, key_val.val.string.len);
                tok = JsonbIteratorNext(&it, &val, true);
                if (tok == WJB_VALUE && val.type == jbvString) {
                    char *hdr_val = pnstrdup(val.val.string.val, val.val.string.len);
                    natsMsgHeader_Set(msg, hdr_key, hdr_val);
                    pfree(hdr_val);
                }
                pfree(hdr_key);
            }
        }
    }

    return msg;
}

/**
 * @private
 * @brief Ensure batch capacity, growing the array if needed.
 *
 * Must be called WITHOUT pending_lock held: the allocation may ereport(ERROR)
 * (longjmp), which would leave the mutex locked forever. Only the worker
 * thread changes pending_count/pending_capacity, so the size check itself is
 * race-free; the pointer swap is done under the lock because the ack callback
 * thread indexes into the array while holding it. The new array is allocated
 * in the context that owns the old one (dispatcher cache context), never in
 * the per-batch SPI context.
 *
 * @param nats NATS dispatcher instance
 */
static void nats_ensure_batch_capacity(NatsDispatcher *nats) {
    NatsPendingMessage *new_array;
    NatsPendingMessage *old_array;
    int new_capacity;

    if (nats->pending_count < nats->pending_capacity)
        return;

    new_capacity = nats->pending_capacity * 2;
    new_array = MemoryContextAlloc(GetMemoryChunkContext(nats->pending_messages),
                                   sizeof(NatsPendingMessage) * new_capacity);

    pthread_mutex_lock(&nats->pending_lock);
    memcpy(new_array, nats->pending_messages, sizeof(NatsPendingMessage) * nats->pending_count);
    old_array = nats->pending_messages;
    nats->pending_messages = new_array;
    nats->pending_capacity = new_capacity;
    pthread_mutex_unlock(&nats->pending_lock);

    pfree(old_array);
}

static bool nats_message_id_from_msg(natsMsg *msg, int64 *msg_id) {
    const char *msg_id_str = NULL;
    char *end = NULL;
    long long parsed;

    if (msg == NULL || msg_id == NULL)
        return false;

    if (natsMsgHeader_Get(msg, "Nats-Msg-Id", &msg_id_str) != NATS_OK || msg_id_str == NULL ||
        msg_id_str[0] == '\0')
        return false;

    parsed = strtoll(msg_id_str, &end, 10);
    if (end == msg_id_str || (end != NULL && *end != '\0') || parsed <= 0)
        return false;

    *msg_id = (int64)parsed;
    return true;
}

static NatsPendingMessage *nats_find_pending_message_locked(NatsDispatcher *nats, int64 msg_id) {
    int i;

    for (i = 0; i < nats->pending_count; i++) {
        if (nats->pending_messages[i].msg_id == msg_id)
            return &nats->pending_messages[i];
    }

    return NULL;
}

static void nats_mark_pending_success(NatsDispatcher *nats, natsMsg *msg, jsPubAck *pa) {
    int64 msg_id;
    NatsPendingMessage *pm;

    if (!nats_message_id_from_msg(msg, &msg_id))
        return;

    pthread_mutex_lock(&nats->pending_lock);
    pm = nats_find_pending_message_locked(nats, msg_id);
    if (pm != NULL) {
        pm->completed = true;
        pm->success = true;
        pm->error[0] = '\0';
        if (pa != NULL) {
            pm->js_sequence = pa->Sequence;
            strlcpy(pm->js_stream, pa->Stream ? pa->Stream : "", sizeof(pm->js_stream));
            pm->js_duplicate = pa->Duplicate;
        }
    }
    pthread_mutex_unlock(&nats->pending_lock);
}

static void nats_mark_pending_failure(NatsDispatcher *nats, natsMsg *msg, natsStatus status,
                                      jsErrCode js_err_code, const char *err_text) {
    int64 msg_id;
    NatsPendingMessage *pm;
    const char *prefix;
    const char *status_text;

    if (!nats_message_id_from_msg(msg, &msg_id))
        return;

    prefix = (js_err_code != 0) ? nats_classify_js_error(js_err_code) : nats_classify_error(status);
    status_text = err_text ? err_text : natsStatus_GetText(status);

    pthread_mutex_lock(&nats->pending_lock);
    pm = nats_find_pending_message_locked(nats, msg_id);
    if (pm != NULL) {
        pm->completed = true;
        pm->success = false;
        snprintf(pm->error, sizeof(pm->error),
                 "%s NATS JetStream async publish failed: %s (status=%d, js_err=%d)", prefix,
                 status_text ? status_text : "unknown", status, js_err_code);
    }
    pthread_mutex_unlock(&nats->pending_lock);
}

static void nats_js_puback_handler(jsCtx *js, natsMsg *msg, jsPubAck *pa, jsPubAckErr *pae,
                                   void *closure) {
    NatsDispatcher *nats = (NatsDispatcher *)closure;

    (void)js;

    if (nats != NULL) {
        if (pae != NULL)
            nats_mark_pending_failure(nats, msg, pae->Err, pae->ErrCode, pae->ErrText);
        else if (pa != NULL)
            nats_mark_pending_success(nats, msg, pa);
        else
            nats_mark_pending_failure(nats, msg, NATS_ERR, 0,
                                      "JetStream publish completed without ack details");
    }

    /* With AckHandler configured, cnats transfers ownership of the original
     * message to the callback. */
    natsMsg_Destroy(msg);
}

void nats_configure_js_options(NatsDispatcher *nats, jsOptions *js_opts) {
    jsOptions_Init(js_opts);
    js_opts->PublishAsync.MaxPending = nats->pending_capacity;
    js_opts->PublishAsync.AckHandler = nats_js_puback_handler;
    js_opts->PublishAsync.AckHandlerClosure = nats;
}

/**
 * @private
 * @brief Reconnect if NATS connection is lost.
 * @param nats NATS dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected, false on failure
 */
static bool nats_ensure_connected(NatsDispatcher *nats, char **error_msg) {
    natsConnStatus status;
    natsStatus s;
    jsOptions jsOpts;

    if (nats->conn == NULL) {
        *error_msg = psprintf("%s NATS not connected", nats_classify_error(NATS_CONNECTION_CLOSED));
        return false;
    }

    status = natsConnection_Status(nats->conn);
    if (status == NATS_CONN_STATUS_CLOSED) {
        /* Connection permanently closed -- need full reconnect */
        natsConnection_Destroy(nats->conn);
        nats->conn = NULL;
        if (nats->js) {
            jsCtx_Destroy(nats->js);
            nats->js = NULL;
        }

        s = natsConnection_Connect(&nats->conn, nats->opts);
        if (s != NATS_OK) {
            *error_msg = psprintf("%s NATS reconnect failed: %s", nats_classify_error(s),
                                  natsStatus_GetText(s));
            return false;
        }

        if (nats->jetstream) {
            nats_configure_js_options(nats, &jsOpts);
            s = natsConnection_JetStream(&nats->js, nats->conn, &jsOpts);
            if (s != NATS_OK) {
                *error_msg = psprintf("%s JetStream context creation failed: %s",
                                      nats_classify_error(s), natsStatus_GetText(s));
                return false;
            }
        }
    }

    return true;
}

/* -- Synchronous Dispatch -- */

/**
 * @brief Dispatch a single NATS message synchronously.
 * @param self Dispatcher instance
 * @param payload Message payload string
 * @param error_msg Output error message on failure
 * @return true on successful publish, false on failure
 */
bool nats_dispatcher_dispatch(Dispatcher *self, const char *payload, char **error_msg) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;
    natsMsg *msg;
    natsStatus s;

    if (!nats_ensure_connected(nats, error_msg))
        return false;

    msg = nats_build_msg(nats, payload, 0, NULL);
    if (msg == NULL) {
        *error_msg = psprintf("[RETRYABLE] Failed to create NATS message");
        return false;
    }

    if (nats->jetstream) {
        jsPubAck *pa = NULL;
        jsErrCode jerr = 0;

        s = js_PublishMsg(&pa, nats->js, msg, NULL, &jerr);
        natsMsg_Destroy(msg);

        if (s != NATS_OK) {
            const char *prefix =
                (jerr != 0) ? nats_classify_js_error(jerr) : nats_classify_error(s);
            *error_msg = psprintf("%s NATS JetStream publish failed: %s (js_err=%d)", prefix,
                                  natsStatus_GetText(s), jerr);
            return false;
        }

        nats->last_js_sequence = pa->Sequence;
        strlcpy(nats->last_js_stream, pa->Stream ? pa->Stream : "", sizeof(nats->last_js_stream));
        nats->last_js_duplicate = pa->Duplicate;
        jsPubAck_Destroy(pa);
    } else {
        /* Core NATS -- fire and forget */
        s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);

        if (s != NATS_OK) {
            *error_msg = psprintf("%s NATS publish failed: %s", nats_classify_error(s),
                                  natsStatus_GetText(s));
            return false;
        }

        nats->last_js_sequence = 0;
        nats->last_js_stream[0] = '\0';
        nats->last_js_duplicate = false;
    }

    return true;
}

/**
 * @brief Extended dispatch with per-message headers and DispatchResult capture.
 * @param self Dispatcher instance
 * @param payload Message payload string
 * @param headers Per-message JSONB headers, or NULL
 * @param metadata JSONB metadata (reserved for future use)
 * @param result Output dispatch result with timing and JetStream metadata
 * @return true on success, false on failure
 */
bool nats_dispatcher_dispatch_ex(Dispatcher *self, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;
    char *error_msg = NULL;
    struct timespec start, end;
    bool success;
    natsMsg *msg;

    clock_gettime(CLOCK_MONOTONIC, &start);

    /* result is a required out-parameter (the batch processor always passes
     * one); the queue row id it carries becomes Nats-Msg-Id so JetStream
     * de-duplicates retries of the same message on the synchronous path too. */
    if (result == NULL) {
        elog(WARNING, "[ulak] NATS dispatch_ex called without a result buffer");
        return false;
    }

    msg = nats_build_msg(nats, payload, result->message_id, headers);
    if (msg == NULL) {
        result->success = false;
        result->error_msg = pstrdup("[RETRYABLE] Failed to create NATS message");
        return false;
    }

    if (!nats_ensure_connected(nats, &error_msg)) {
        natsMsg_Destroy(msg);
        result->success = false;
        result->error_msg = error_msg;
        return false;
    }

    if (nats->jetstream) {
        jsPubAck *pa = NULL;
        jsErrCode jerr = 0;
        natsStatus s = js_PublishMsg(&pa, nats->js, msg, NULL, &jerr);
        natsMsg_Destroy(msg);

        if (s != NATS_OK) {
            const char *prefix =
                (jerr != 0) ? nats_classify_js_error(jerr) : nats_classify_error(s);
            result->success = false;
            result->error_msg =
                psprintf("%s NATS JetStream publish failed: %s", prefix, natsStatus_GetText(s));
            return false;
        }

        result->nats_js_sequence = pa->Sequence;
        result->nats_js_stream = pa->Stream ? pstrdup(pa->Stream) : NULL;
        result->nats_js_duplicate = pa->Duplicate;
        jsPubAck_Destroy(pa);
        success = true;
    } else {
        natsStatus s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);

        if (s != NATS_OK) {
            result->success = false;
            result->error_msg = psprintf("%s NATS publish failed: %s", nats_classify_error(s),
                                         natsStatus_GetText(s));
            return false;
        }
        success = true;
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    result->success = success;
    result->response_time_ms =
        (end.tv_sec - start.tv_sec) * 1000 + (end.tv_nsec - start.tv_nsec) / 1000000;
    return success;
}

/* -- Batch Operations -- */

/**
 * @brief Produce a message to NATS without waiting for acknowledgment.
 * @param self Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param error_msg Output error message on failure
 * @return true if message was published/enqueued, false on failure
 */
bool nats_dispatcher_produce(Dispatcher *self, const char *payload, int64 msg_id,
                             char **error_msg) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;
    natsMsg *msg;
    natsStatus s;
    NatsPendingMessage *pm;

    if (!nats_ensure_connected(nats, error_msg))
        return false;

    msg = nats_build_msg(nats, payload, msg_id, NULL);
    if (msg == NULL) {
        *error_msg = pstrdup("[RETRYABLE] Failed to create NATS message");
        return false;
    }

    nats_ensure_batch_capacity(nats);

    pthread_mutex_lock(&nats->pending_lock);
    pm = &nats->pending_messages[nats->pending_count];
    pm->msg_id = msg_id;
    pm->completed = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->js_sequence = 0;
    pm->js_stream[0] = '\0';
    pm->js_duplicate = false;
    nats->pending_count++;
    pthread_mutex_unlock(&nats->pending_lock);

    if (nats->jetstream) {
        s = js_PublishMsgAsync(nats->js, &msg, NULL);
        if (s != NATS_OK) {
            natsMsg_Destroy(msg);
            pthread_mutex_lock(&nats->pending_lock);
            snprintf(pm->error, sizeof(pm->error), "%s NATS async publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->completed = true;
            pm->success = false;
            pthread_mutex_unlock(&nats->pending_lock);
            *error_msg = pstrdup(pm->error);
            return false;
        }
        /* msg ownership transferred to NATS library */
    } else {
        /* Core NATS -- fire and forget, mark success immediately */
        s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);

        pthread_mutex_lock(&nats->pending_lock);
        pm->completed = true;
        if (s != NATS_OK) {
            snprintf(pm->error, sizeof(pm->error), "%s NATS publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->success = false;
            pthread_mutex_unlock(&nats->pending_lock);
            *error_msg = pstrdup(pm->error);
            return false;
        }
        pm->success = true;
        pthread_mutex_unlock(&nats->pending_lock);
    }

    return true;
}

/**
 * @brief Extended produce with per-message headers.
 * @param self Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param headers Per-message JSONB headers, or NULL
 * @param metadata JSONB metadata (reserved for future use)
 * @param error_msg Output error message on failure
 * @return true if message was published/enqueued, false on failure
 */
bool nats_dispatcher_produce_ex(Dispatcher *self, const char *payload, int64 msg_id, Jsonb *headers,
                                Jsonb *metadata, char **error_msg) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;
    natsMsg *msg;
    natsStatus s;
    NatsPendingMessage *pm;

    if (!nats_ensure_connected(nats, error_msg))
        return false;

    msg = nats_build_msg(nats, payload, msg_id, headers);
    if (msg == NULL) {
        *error_msg = pstrdup("[RETRYABLE] Failed to create NATS message");
        return false;
    }

    nats_ensure_batch_capacity(nats);

    pthread_mutex_lock(&nats->pending_lock);
    pm = &nats->pending_messages[nats->pending_count];
    pm->msg_id = msg_id;
    pm->completed = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->js_sequence = 0;
    pm->js_stream[0] = '\0';
    pm->js_duplicate = false;
    nats->pending_count++;
    pthread_mutex_unlock(&nats->pending_lock);

    if (nats->jetstream) {
        s = js_PublishMsgAsync(nats->js, &msg, NULL);
        if (s != NATS_OK) {
            natsMsg_Destroy(msg);
            pthread_mutex_lock(&nats->pending_lock);
            snprintf(pm->error, sizeof(pm->error), "%s NATS async publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->completed = true;
            pm->success = false;
            pthread_mutex_unlock(&nats->pending_lock);
            *error_msg = pstrdup(pm->error);
            return false;
        }
    } else {
        s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);
        pthread_mutex_lock(&nats->pending_lock);
        pm->completed = true;
        pm->success = (s == NATS_OK);
        if (s != NATS_OK) {
            snprintf(pm->error, sizeof(pm->error), "%s NATS publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pthread_mutex_unlock(&nats->pending_lock);
            *error_msg = pstrdup(pm->error);
            return false;
        }
        pthread_mutex_unlock(&nats->pending_lock);
    }

    return true;
}

/**
 * @brief Flush all pending NATS messages and collect delivery results.
 * @param self Dispatcher instance
 * @param timeout_ms Maximum time to wait for acknowledgments
 * @param failed_ids Output array of failed message IDs
 * @param failed_count Output count of failed messages
 * @param failed_errors Output array of error strings for failed messages
 * @return Number of successfully delivered messages
 */
int nats_dispatcher_flush(Dispatcher *self, int timeout_ms, int64 **failed_ids, int *failed_count,
                          char ***failed_errors) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;
    int success_count = 0;
    int fail_count = 0;
    int i;
    natsStatus s;
    natsMsgList pending;
    int idx;

    *failed_ids = NULL;
    *failed_count = 0;
    *failed_errors = NULL;

    if (nats->pending_count == 0)
        return 0;

    if (nats->jetstream) {
        /* Wait for all async publishes to complete */
        jsPubOptions jsPubOpts;
        jsPubOptions_Init(&jsPubOpts);
        jsPubOpts.MaxWait = timeout_ms;

        s = js_PublishAsyncComplete(nats->js, &jsPubOpts);
        if (s == NATS_TIMEOUT) {
            elog(WARNING, "[ulak] NATS JetStream flush timeout after %dms", timeout_ms);
        }

        /* Get async publish errors */
        memset(&pending, 0, sizeof(pending));
        s = js_PublishAsyncGetPendingList(&pending, nats->js);

        if (s == NATS_OK && pending.Count > 0) {
            /* These messages never received an async publish response. */
            pthread_mutex_lock(&nats->pending_lock);
            for (i = 0; i < pending.Count; i++) {
                int64 msg_id;
                NatsPendingMessage *pm;

                if (nats_message_id_from_msg(pending.Msgs[i], &msg_id)) {
                    pm = nats_find_pending_message_locked(nats, msg_id);
                    if (pm != NULL) {
                        pm->completed = true;
                        pm->success = false;
                        snprintf(pm->error, sizeof(pm->error),
                                 "[RETRYABLE] NATS JetStream async publish unacknowledged");
                    }
                }
            }
            pthread_mutex_unlock(&nats->pending_lock);
            natsMsgList_Destroy(&pending);
        }

        /* Positive AckHandler callbacks are the only success source. Anything
         * still incomplete after Complete/GetPendingList is retryable failure. */
        pthread_mutex_lock(&nats->pending_lock);
        for (i = 0; i < nats->pending_count; i++) {
            if (!nats->pending_messages[i].completed) {
                nats->pending_messages[i].completed = true;
                nats->pending_messages[i].success = false;
                snprintf(nats->pending_messages[i].error, sizeof(nats->pending_messages[i].error),
                         "[RETRYABLE] NATS JetStream async publish unacknowledged");
            }
        }
        pthread_mutex_unlock(&nats->pending_lock);
    } else {
        /* Core NATS -- just flush the connection buffer */
        s = natsConnection_FlushTimeout(nats->conn, timeout_ms);
        if (s != NATS_OK) {
            elog(WARNING, "[ulak] NATS flush failed: %s", natsStatus_GetText(s));
        }

        /* Core NATS messages were already marked success in produce() */
        pthread_mutex_lock(&nats->pending_lock);
        for (i = 0; i < nats->pending_count; i++) {
            if (!nats->pending_messages[i].completed) {
                nats->pending_messages[i].completed = true;
                nats->pending_messages[i].success = (s == NATS_OK);
                if (s != NATS_OK) {
                    snprintf(nats->pending_messages[i].error,
                             sizeof(nats->pending_messages[i].error), "%s NATS flush failed: %s",
                             nats_classify_error(s), natsStatus_GetText(s));
                }
            }
        }
        pthread_mutex_unlock(&nats->pending_lock);
    }

    /* Collect failed messages */
    pthread_mutex_lock(&nats->pending_lock);
    for (i = 0; i < nats->pending_count; i++) {
        if (!nats->pending_messages[i].success)
            fail_count++;
        else
            success_count++;
    }

    if (fail_count > 0) {
        *failed_ids = palloc(sizeof(int64) * fail_count);
        *failed_errors = palloc(sizeof(char *) * fail_count);
        *failed_count = fail_count;

        idx = 0;
        for (i = 0; i < nats->pending_count; i++) {
            if (!nats->pending_messages[i].success) {
                (*failed_ids)[idx] = nats->pending_messages[i].msg_id;
                (*failed_errors)[idx] = pstrdup(nats->pending_messages[i].error);
                idx++;
            }
        }
    }

    nats->pending_count = 0;
    pthread_mutex_unlock(&nats->pending_lock);
    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param self Dispatcher instance (unused)
 * @return Always true for NATS
 */
bool nats_dispatcher_supports_batch(Dispatcher *self) { return true; }
