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

#include <string.h>
#include <time.h>

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
 * @param nats NATS dispatcher instance
 */
static void nats_ensure_batch_capacity(NatsDispatcher *nats) {
    if (nats->pending_count >= nats->pending_capacity) {
        int new_capacity = nats->pending_capacity * 2;
        nats->pending_messages =
            repalloc(nats->pending_messages, sizeof(NatsPendingMessage) * new_capacity);
        nats->pending_capacity = new_capacity;
    }
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
            jsOptions_Init(&jsOpts);
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

    /* Use dispatch with headers support */
    msg = nats_build_msg(nats, payload, 0, headers);
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

    nats_ensure_batch_capacity(nats);

    pm = &nats->pending_messages[nats->pending_count];
    pm->msg_id = msg_id;
    pm->completed = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->js_sequence = 0;
    pm->js_stream[0] = '\0';
    pm->js_duplicate = false;

    msg = nats_build_msg(nats, payload, msg_id, NULL);
    if (msg == NULL) {
        snprintf(pm->error, sizeof(pm->error), "[RETRYABLE] Failed to create NATS message");
        pm->completed = true;
        pm->success = false;
        nats->pending_count++;
        *error_msg = pstrdup(pm->error);
        return false;
    }

    if (nats->jetstream) {
        s = js_PublishMsgAsync(nats->js, &msg, NULL);
        if (s != NATS_OK) {
            natsMsg_Destroy(msg);
            snprintf(pm->error, sizeof(pm->error), "%s NATS async publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->completed = true;
            pm->success = false;
            nats->pending_count++;
            *error_msg = pstrdup(pm->error);
            return false;
        }
        /* msg ownership transferred to NATS library */
    } else {
        /* Core NATS -- fire and forget, mark success immediately */
        s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);

        pm->completed = true;
        if (s != NATS_OK) {
            snprintf(pm->error, sizeof(pm->error), "%s NATS publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->success = false;
            nats->pending_count++;
            *error_msg = pstrdup(pm->error);
            return false;
        }
        pm->success = true;
    }

    nats->pending_count++;
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

    nats_ensure_batch_capacity(nats);

    pm = &nats->pending_messages[nats->pending_count];
    pm->msg_id = msg_id;
    pm->completed = false;
    pm->success = false;
    pm->error[0] = '\0';
    pm->js_sequence = 0;
    pm->js_stream[0] = '\0';
    pm->js_duplicate = false;

    msg = nats_build_msg(nats, payload, msg_id, headers);
    if (msg == NULL) {
        snprintf(pm->error, sizeof(pm->error), "[RETRYABLE] Failed to create NATS message");
        pm->completed = true;
        nats->pending_count++;
        *error_msg = pstrdup(pm->error);
        return false;
    }

    if (nats->jetstream) {
        s = js_PublishMsgAsync(nats->js, &msg, NULL);
        if (s != NATS_OK) {
            natsMsg_Destroy(msg);
            snprintf(pm->error, sizeof(pm->error), "%s NATS async publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            pm->completed = true;
            nats->pending_count++;
            *error_msg = pstrdup(pm->error);
            return false;
        }
    } else {
        s = natsConnection_PublishMsg(nats->conn, msg);
        natsMsg_Destroy(msg);
        pm->completed = true;
        pm->success = (s == NATS_OK);
        if (s != NATS_OK) {
            snprintf(pm->error, sizeof(pm->error), "%s NATS publish failed: %s",
                     nats_classify_error(s), natsStatus_GetText(s));
            nats->pending_count++;
            *error_msg = pstrdup(pm->error);
            return false;
        }
    }

    nats->pending_count++;
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
            /* These are messages that failed -- mark corresponding pending messages */
            for (i = 0; i < pending.Count; i++) {
                /* Try to match by Nats-Msg-Id header */
                const char *msg_id_str = NULL;
                natsMsgHeader_Get(pending.Msgs[i], "Nats-Msg-Id", &msg_id_str);

                if (msg_id_str != NULL) {
                    int64 msg_id = atoll(msg_id_str);
                    int j;
                    for (j = 0; j < nats->pending_count; j++) {
                        if (nats->pending_messages[j].msg_id == msg_id) {
                            nats->pending_messages[j].completed = true;
                            nats->pending_messages[j].success = false;
                            snprintf(nats->pending_messages[j].error,
                                     sizeof(nats->pending_messages[j].error),
                                     "[RETRYABLE] NATS JetStream async publish failed");
                            break;
                        }
                    }
                }
            }
            natsMsgList_Destroy(&pending);
        }

        /* Mark remaining as success */
        for (i = 0; i < nats->pending_count; i++) {
            if (!nats->pending_messages[i].completed) {
                nats->pending_messages[i].completed = true;
                nats->pending_messages[i].success = true;
            }
        }
    } else {
        /* Core NATS -- just flush the connection buffer */
        s = natsConnection_FlushTimeout(nats->conn, timeout_ms);
        if (s != NATS_OK) {
            elog(WARNING, "[ulak] NATS flush failed: %s", natsStatus_GetText(s));
        }

        /* Core NATS messages were already marked success in produce() */
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
    }

    /* Collect failed messages */
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
    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param self Dispatcher instance (unused)
 * @return Always true for NATS
 */
bool nats_dispatcher_supports_batch(Dispatcher *self) { return true; }
