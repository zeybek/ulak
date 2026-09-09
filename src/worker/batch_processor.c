/**
 * @file batch_processor.c
 * @brief Pending-message batch processor.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Extracted from src/worker.c. Behavior unchanged.
 */

#include "worker/batch_processor.h"

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/json_utils.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "config/guc.h"
#include "core/entities.h"
#include "dispatchers/dispatcher.h"
#include "shmem.h"
#include "utils/rate_limit.h"
#include "utils/retry_policy.h"
#include "worker/batch_types.h"
#include "worker/circuit_breaker.h"
#include "worker/dispatcher_cache.h"

/* ------------------------------------------------------------------------
 * Local stats
 * ------------------------------------------------------------------------ */

static struct {
    int64 messages_processed;
    int32 error_count;
    bool has_error;
    char last_error_msg[256];
} worker_local_stats = {0};

/**
 * @brief Accumulate a single message result into process-local stats.
 */
static void worker_update_stats_local(bool success, const char *error_msg) {
    if (success) {
        worker_local_stats.messages_processed++;
    } else {
        worker_local_stats.error_count++;
        worker_local_stats.has_error = true;
        if (error_msg) {
            strlcpy(worker_local_stats.last_error_msg, error_msg,
                    sizeof(worker_local_stats.last_error_msg));
        }
    }
}

/**
 * @brief Flush accumulated stats to shared memory.
 */
static void worker_flush_stats_to_shmem(Oid worker_dboid, int worker_id) {
    /* Skip if nothing to flush */
    if (worker_local_stats.messages_processed == 0 && worker_local_stats.error_count == 0)
        return;

    ulak_update_worker_metrics(worker_dboid, worker_id, worker_local_stats.messages_processed,
                               worker_local_stats.error_count,
                               worker_local_stats.has_error ? worker_local_stats.last_error_msg
                                                            : NULL);

    /* Reset local accumulators */
    worker_local_stats.messages_processed = 0;
    worker_local_stats.error_count = 0;
    worker_local_stats.has_error = false;
    worker_local_stats.last_error_msg[0] = '\0';
}

static void batch_heartbeat_if_due(void);

/* ------------------------------------------------------------------------
 * Batch memory context
 * ------------------------------------------------------------------------ */

/* Saved reference for PG_CATCH cleanup of orphaned batch context */
static MemoryContext worker_batch_context = NULL;

void batch_processor_cleanup_on_error(void) {
    if (worker_batch_context) {
        MemoryContextDelete(worker_batch_context);
        worker_batch_context = NULL;
    }

    worker_local_stats.messages_processed = 0;
    worker_local_stats.error_count = 0;
    worker_local_stats.has_error = false;
    worker_local_stats.last_error_msg[0] = '\0';
}

/* ------------------------------------------------------------------------
 * Per-endpoint dispatch
 * ------------------------------------------------------------------------ */

/**
 * @brief Process a batch of messages for a single endpoint.
 *
 * Uses batch mode for protocols that support it (like Kafka), falls back to
 * synchronous dispatch for others (like HTTP). Applies rate limiting and
 * response capture when configured.
 *
 * @param messages     Array of MessageBatchInfo for this endpoint.
 * @param count        Number of messages in the array.
 * @param protocol     Protocol string (e.g. "http", "kafka").
 * @param config       Endpoint configuration JSONB.
 * @param retry_policy Retry policy JSONB, or NULL.
 */
static void process_endpoint_batch(MessageBatchInfo *messages, int count, const char *protocol,
                                   Jsonb *config, Jsonb *retry_policy) {
    ProtocolType proto_type;
    Dispatcher *dispatcher;
    int i, j;
    int64 *failed_ids = NULL;
    char **failed_errors = NULL;
    int failed_count = 0;
    int flush_timeout;
    int success_count;
    bool failed;

    (void)retry_policy; /* retry policy consumed by caller when scheduling retries */

    if (count == 0)
        return;

    /* Convert protocol string to enum */
    if (!protocol_string_to_type(protocol, &proto_type)) {
        /* Unknown protocol - mark all messages as failed */
        for (i = 0; i < count; i++) {
            messages[i].processed = true;
            messages[i].success = false;
            messages[i].error_message = psprintf("Unknown or disabled protocol: %s", protocol);
        }
        return;
    }

    /* Get or create cached dispatcher for this endpoint */
    dispatcher = get_or_create_dispatcher(messages[0].endpoint_id, proto_type, config);
    if (!dispatcher) {
        /* Failed to create dispatcher - mark all messages as failed */
        for (i = 0; i < count; i++) {
            messages[i].processed = true;
            messages[i].success = false;
            messages[i].error_message =
                psprintf("Failed to create dispatcher for protocol: %s", protocol);
        }
        return;
    }

    /* Apply rate limiting — defer messages that exceed endpoint rate limit.
     * Deferred messages get a next_retry_at of roughly one token interval so
     * they are not re-fetched (and re-reverted) on every poll cycle. */
    {
        double rl_tokens_per_second = 0.0;
        int rl_burst = 0;
        rate_limit_parse_config(config, &rl_tokens_per_second, &rl_burst);
        if (rl_tokens_per_second > 0.0) {
            int defer_ms = (int)(1000.0 / rl_tokens_per_second);
            if (defer_ms < 100)
                defer_ms = 100;
            if (defer_ms > 60000)
                defer_ms = 60000;
            for (i = 0; i < count; i++) {
                if (!rate_limit_acquire(messages[i].endpoint_id, rl_tokens_per_second, rl_burst)) {
                    messages[i].rate_limited = true;
                    messages[i].rate_limit_defer_ms = defer_ms;
                    elog(DEBUG1, "[ulak] Rate limited message %lld for endpoint %lld",
                         (long long)messages[i].message_id, (long long)messages[i].endpoint_id);
                }
            }
        }
    }

    /* Check if dispatcher supports batch mode.
     * When capture_response is enabled, force sync mode so dispatch_ex
     * can capture per-message HTTP response bodies.
     * Batch mode uses produce_ex when available to forward per-message
     * headers/metadata; falls back to produce otherwise. */
    if (!ulak_capture_response && dispatcher->ops->supports_batch &&
        dispatcher->ops->supports_batch(dispatcher)) {
        /* BATCH MODE - produce all messages without waiting, then flush once */

        /* Verify batch operations are implemented */
        if (dispatcher->ops->produce == NULL || dispatcher->ops->flush == NULL) {
            elog(WARNING, "[ulak] Dispatcher supports batch but produce/flush not implemented");
            /* Fall through to synchronous mode by marking batch as unavailable */
            for (i = 0; i < count; i++) {
                messages[i].processed = true;
                messages[i].success = false;
                messages[i].error_message =
                    pstrdup("Dispatcher batch mode incomplete: produce/flush not implemented");
            }
            return;
        }

        /* Phase 1: Produce all messages (non-blocking), skip rate-limited.
         * Use produce_ex when available to forward per-message headers/metadata. */
        for (i = 0; i < count; i++) {
            char *error_msg = NULL;
            bool produced;
            if (messages[i].rate_limited)
                continue;
            if (dispatcher->ops->produce_ex != NULL &&
                (messages[i].headers != NULL || messages[i].metadata != NULL)) {
                produced = dispatcher->ops->produce_ex(dispatcher, messages[i].payload_str,
                                                       messages[i].message_id, messages[i].headers,
                                                       messages[i].metadata, &error_msg);
            } else {
                if (messages[i].headers != NULL || messages[i].metadata != NULL) {
                    elog(DEBUG1,
                         "[ulak] Per-message headers/metadata for message %lld dropped: "
                         "dispatcher does not support produce_ex in batch mode",
                         (long long)messages[i].message_id);
                }
                produced = dispatcher->ops->produce(dispatcher, messages[i].payload_str,
                                                    messages[i].message_id, &error_msg);
            }
            if (!produced) {
                /* Produce failed immediately - mark this message as failed */
                messages[i].processed = true;
                messages[i].success = false;
                messages[i].error_message =
                    error_msg ? error_msg : pstrdup("Failed to enqueue message");
            }
        }

        /* Phase 2: Flush and wait for all deliveries */
        /* Use protocol-specific flush timeout from GUC */
#ifdef ENABLE_KAFKA
        if (strcmp(protocol, "kafka") == 0) {
            flush_timeout = ulak_kafka_flush_timeout;
        } else
#endif
#ifdef ENABLE_MQTT
            if (strcmp(protocol, "mqtt") == 0) {
            flush_timeout = ulak_mqtt_timeout;
        } else
#endif
#ifdef ENABLE_AMQP
            if (strcmp(protocol, "amqp") == 0) {
            flush_timeout = ulak_amqp_delivery_timeout;
        } else
#endif
#ifdef ENABLE_NATS
            if (strcmp(protocol, "nats") == 0) {
            flush_timeout = ulak_nats_flush_timeout;
        } else
#endif
        {
            flush_timeout = ulak_http_flush_timeout;
        }
        success_count = dispatcher->ops->flush(dispatcher, flush_timeout, &failed_ids,
                                               &failed_count, &failed_errors);
        (void)success_count; /* Used for logging in debug builds */

        /* Phase 3: Update message statuses based on flush results */
        for (i = 0; i < count; i++) {
            if (messages[i].processed) {
                /* Already marked as failed during produce phase */
                continue;
            }

            /* Check if this message is in the failed list */
            failed = false;
            for (j = 0; j < failed_count; j++) {
                if (failed_ids && failed_ids[j] == messages[i].message_id) {
                    failed = true;
                    break;
                }
            }

            messages[i].processed = true;
            messages[i].success = !failed;
            if (failed) {
                if (failed_errors && failed_errors[j]) {
                    messages[i].error_message = pstrdup(failed_errors[j]);
                } else {
                    messages[i].error_message = psprintf(
                        ERROR_PREFIX_RETRYABLE " %s batch delivery failed or timed out", protocol);
                }
            }
        }

        if (failed_errors) {
            for (j = 0; j < failed_count; j++) {
                if (failed_errors[j])
                    pfree(failed_errors[j]);
            }
            pfree(failed_errors);
            failed_errors = NULL;
        }
        if (failed_ids) {
            pfree(failed_ids);
        }

    } else {
        /* SYNCHRONOUS MODE - dispatch one message at a time (HTTP, MQTT, etc.)
         * Use dispatch_ex when:
         *  - response capture is enabled (to store HTTP status/body), OR
         *  - the message carries per-message headers/metadata.
         * Fall back to legacy dispatch() only when neither applies. */
        for (i = 0; i < count; i++) {
            bool use_dispatch_ex;

            if (messages[i].rate_limited)
                continue;
            /* Long synchronous groups: keep stale recovery away from our rows */
            batch_heartbeat_if_due();

            use_dispatch_ex = (dispatcher->ops->dispatch_ex != NULL) &&
                              (ulak_capture_response || messages[i].headers != NULL ||
                               messages[i].metadata != NULL);

            if (use_dispatch_ex) {
                DispatchResult *result = dispatch_result_create();
                if (result != NULL)
                    result->message_id = messages[i].message_id;
                if (result == NULL) {
                    messages[i].processed = true;
                    messages[i].success = false;
                    messages[i].error_message = pstrdup("Failed to allocate dispatch result");
                    messages[i].result = NULL;
                } else {
                    bool success =
                        dispatcher_dispatch_ex(dispatcher, messages[i].payload_str,
                                               messages[i].headers, messages[i].metadata, result);
                    messages[i].processed = true;
                    messages[i].success = success;
                    messages[i].error_message =
                        result->error_msg ? pstrdup(result->error_msg) : NULL;
                    /* Keep result for response storage only when capture is on */
                    messages[i].result = ulak_capture_response ? result : NULL;
                    if (!ulak_capture_response)
                        dispatch_result_free(result);
                }
            } else {
                /* Legacy dispatch path — no headers/metadata, no capture */
                char *error_msg = NULL;
                bool success =
                    dispatcher->ops->dispatch(dispatcher, messages[i].payload_str, &error_msg);
                messages[i].processed = true;
                messages[i].success = success;
                messages[i].error_message = error_msg;
                messages[i].result = NULL;
            }
        }
    }

    /* Dispatcher stays in cache for connection reuse across batches.
     * Cleanup happens via: idle eviction, config change, SIGHUP,
     * PG_CATCH error recovery, or before_shmem_exit hook. */
}

/* ------------------------------------------------------------------------
 * Batch entry point
 *
 * A batch runs in three phases so that no database transaction stays open
 * while the worker talks to brokers:
 *
 *   1. claim   — one transaction: SELECT … FOR UPDATE SKIP LOCKED, copy the
 *                rows into the batch memory context, mark them 'processing',
 *                apply circuit-breaker deferrals, COMMIT.
 *   2. deliver — no transaction: dispatch per endpoint. A slow or dead
 *                broker now costs only this worker's time, not row locks,
 *                snapshot age or a transaction held open for the whole
 *                delivery timeout.
 *   3. write   — one transaction: status updates, retries, DLQ archive,
 *                circuit-breaker bookkeeping, COMMIT.
 *
 * A worker that dies between 1 and 3 leaves rows in 'processing'; stale
 * recovery (ulak.stale_recovery_timeout) returns them to 'pending'. An
 * error raised between 1 and 3 goes through batch_processor_release_claimed()
 * from the worker's PG_CATCH, which puts the rows back immediately.
 * ------------------------------------------------------------------------ */

/* Rows claimed by the current batch, for release on error (see above). */
static struct {
    MessageBatchInfo *messages;
    uint64 count;
    bool claimed; /* the claim transaction has committed */
} in_flight = {NULL, 0, false};

/*
 * Heartbeat. Claimed rows are 'processing' in a committed transaction, so
 * stale recovery (ulak.stale_recovery_timeout) would hand them to another
 * worker if a batch delivered for longer than that. Between endpoint groups
 * and between synchronous deliveries the worker therefore refreshes
 * processing_started_at of its in-flight rows once a quarter of the timeout
 * has passed since the claim or the last refresh. A single uninterruptible
 * wait (one batch flush, one HTTP request) must stay below the remaining
 * three quarters, i.e. endpoint timeouts must be well below
 * stale_recovery_timeout.
 */
static TimestampTz batch_last_heartbeat = 0;

static void batch_heartbeat_if_due(void) {
    static const char *touch_query = "UPDATE ulak.queue SET processing_started_at = NOW() "
                                     "WHERE id = ANY($1::bigint[]) AND status = 'processing'";
    MemoryContext caller_context = CurrentMemoryContext;
    int64 interval_us = (int64)config_get_stale_recovery_timeout() * 250000; /* timeout / 4 */
    TimestampTz now;
    Datum *ids;
    ArrayType *id_array;
    uint64 i;
    int ret;

    if (!in_flight.claimed || in_flight.messages == NULL || in_flight.count == 0)
        return;
    now = GetCurrentTimestamp();
    if (batch_last_heartbeat != 0 && now < batch_last_heartbeat + interval_us)
        return;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());
    ids = palloc(sizeof(Datum) * in_flight.count);
    for (i = 0; i < in_flight.count; i++)
        ids[i] = Int64GetDatum(in_flight.messages[i].message_id);
    id_array = construct_array(ids, in_flight.count, INT8OID, sizeof(int64), true, TYPALIGN_DOUBLE);
    ret = SPI_execute_with_args(touch_query, 1, (Oid[]){INT8ARRAYOID},
                                (Datum[]){PointerGetDatum(id_array)}, NULL, false, 0);
    if (ret != SPI_OK_UPDATE)
        elog(WARNING, "[ulak] Heartbeat for %llu in-flight messages failed: SPI error %d",
             (unsigned long long)in_flight.count, ret);
    else
        elog(DEBUG1, "[ulak] Heartbeat refreshed %llu in-flight messages",
             (unsigned long long)SPI_processed);
    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
    MemoryContextSwitchTo(caller_context);
    batch_last_heartbeat = now;
}

/**
 * @brief Put the claimed rows of a batch back to 'pending' in a fresh transaction.
 *
 * Best effort: on failure the rows stay 'processing' and stale recovery
 * picks them up. Safe to call with no batch in flight.
 */
static void batch_release_claimed_rows(MessageBatchInfo *messages, uint64 count) {
    static const char *release_query =
        "UPDATE ulak.queue SET status = 'pending', processing_started_at = NULL, "
        "next_retry_at = NOW() WHERE id = ANY($1::bigint[]) AND status = 'processing'";
    MemoryContext caller_context = CurrentMemoryContext;
    Datum *ids;
    ArrayType *id_array;
    uint64 i;
    int ret;

    if (messages == NULL || count == 0)
        return;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        elog(WARNING,
             "[ulak] Could not release %llu claimed messages (SPI_connect failed); "
             "stale recovery will return them to pending",
             (unsigned long long)count);
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    ids = palloc(sizeof(Datum) * count);
    for (i = 0; i < count; i++)
        ids[i] = Int64GetDatum(messages[i].message_id);
    id_array = construct_array(ids, count, INT8OID, sizeof(int64), true, TYPALIGN_DOUBLE);
    ret = SPI_execute_with_args(release_query, 1, (Oid[]){INT8ARRAYOID},
                                (Datum[]){PointerGetDatum(id_array)}, NULL, false, 0);
    if (ret == SPI_OK_UPDATE)
        elog(LOG, "[ulak] Released %llu of %llu claimed messages back to pending",
             (unsigned long long)SPI_processed, (unsigned long long)count);
    else
        elog(WARNING,
             "[ulak] Could not release claimed messages (SPI error %d); "
             "stale recovery will return them to pending",
             ret);

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
    MemoryContextSwitchTo(caller_context);
}

void batch_processor_release_claimed(void) {
    MessageBatchInfo *messages = in_flight.messages;
    uint64 count = in_flight.count;
    bool claimed = in_flight.claimed;

    in_flight.messages = NULL;
    in_flight.count = 0;
    in_flight.claimed = false;

    if (!claimed || messages == NULL || count == 0)
        return;

    /* Nested PG_TRY is fine here: the caller has already flushed the error
     * state and aborted the failed transaction. */
    PG_TRY();
    {
        batch_release_claimed_rows(messages, count);
    }
    PG_CATCH();
    {
        ErrorData *edata = CopyErrorData();
        FreeErrorData(edata);
        FlushErrorState();
        if (IsTransactionState())
            AbortCurrentTransaction();
        elog(WARNING, "[ulak] Releasing claimed messages failed; stale recovery will "
                      "return them to pending");
    }
    PG_END_TRY();
}

/**
 * @brief Phase 1: claim a batch and commit.
 *
 * On return with *count > 0 the rows are 'processing' in a committed
 * transaction and *messages (in batch_context) describes them. Circuit
 * breaker decisions are applied here too: rows behind an open circuit are
 * returned to 'pending' with a retry delay and flagged `deferred` so the
 * delivery phase skips them.
 *
 * @return false when the batch could not be claimed (error already logged).
 */
static bool batch_claim(int worker_id, int total_workers, MemoryContext batch_context,
                        MessageBatchInfo **messages, uint64 *count) {
    MemoryContext caller_context = CurrentMemoryContext;
    MemoryContext spi_context;
    StringInfoData query;
    int ret;
    MessageBatchInfo *all_messages;
    TupleDesc tupdesc;
    uint64 total_messages;
    uint64 i;
    uint64 batch_start;
    uint64 batch_end;

    *messages = NULL;
    *count = 0;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in batch claim");
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        return false;
    }
    spi_context = CurrentMemoryContext;

    /*
     * READ COMMITTED + SKIP LOCKED is the pattern for concurrent queue
     * consumers; REPEATABLE READ would raise serialization failures between
     * workers. synchronous_commit is off for worker transactions: the
     * status transitions are idempotent and redone after a crash, so we
     * trade durability of the bookkeeping for faster commits.
     */
    SPI_execute_with_args("SET LOCAL synchronous_commit = off", 0, NULL, NULL, NULL, false, 0);
    PushActiveSnapshot(GetTransactionSnapshot());

    /* Extension schema present? (worker may start before CREATE EXTENSION) */
    ret = SPI_execute_with_args("SELECT 1 FROM pg_namespace WHERE nspname = 'ulak'", 0, NULL, NULL,
                                NULL, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
        MemoryContextSwitchTo(caller_context);
        return false;
    }

    /*
     * Select pending messages with their endpoint in one query. Workers
     * partition the queue: unkeyed rows by id, keyed rows by
     * abs(hashtext(ordering_key)), so per-key FIFO holds with N workers.
     * For a keyed row only the head of its key is eligible (no 'processing'
     * row and no lower pending id with the same key).
     */
    initStringInfo(&query);
    appendStringInfo(&query,
                     "SELECT q.id, q.endpoint_id, q.payload, q.retry_count, "
                     "       e.protocol, e.config, e.retry_policy, "
                     "       q.priority, q.scheduled_at, q.expires_at, q.correlation_id, "
                     "       e.enabled, e.circuit_failure_count, e.circuit_state, "
                     "       e.circuit_opened_at, e.circuit_half_open_at, "
                     "       q.headers, q.metadata "
                     "FROM ulak.queue q "
                     "JOIN ulak.endpoints e ON q.endpoint_id = e.id "
                     "WHERE q.status = '%s' "
                     "  AND e.enabled = true "
                     "  AND (q.next_retry_at IS NULL OR q.next_retry_at <= NOW()) "
                     "  AND (q.scheduled_at IS NULL OR q.scheduled_at <= NOW()) "
                     "  AND (q.expires_at IS NULL OR q.expires_at > NOW()) "
                     "  AND (q.ordering_key IS NULL "
                     "       OR (NOT EXISTS ("
                     "               SELECT 1 FROM ulak.queue q2 "
                     "               WHERE q2.ordering_key = q.ordering_key "
                     "                 AND q2.status = 'processing') "
                     "           AND NOT EXISTS ("
                     "               SELECT 1 FROM ulak.queue q2 "
                     "               WHERE q2.ordering_key = q.ordering_key "
                     "                 AND q2.status = 'pending' "
                     "                 AND q2.id < q.id))) ",
                     STATUS_PENDING);
    if (total_workers > 1)
        appendStringInfo(&query,
                         "  AND ((CASE WHEN q.ordering_key IS NULL THEN q.id "
                         "             ELSE abs(hashtext(q.ordering_key))::bigint "
                         "        END) %% %d) = %d ",
                         total_workers, worker_id);
    appendStringInfo(&query,
                     "ORDER BY q.priority DESC, q.endpoint_id, q.created_at ASC "
                     "LIMIT %d FOR UPDATE OF q SKIP LOCKED",
                     ulak_batch_size);

    ret = SPI_execute(query.data, false, 0);
    pfree(query.data);

    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to query pending messages: SPI error %d", ret);
        PopActiveSnapshot();
        SPI_finish();
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        return false;
    }
    if (SPI_processed == 0 || SPI_tuptable == NULL) {
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
        MemoryContextSwitchTo(caller_context);
        return true; /* nothing pending */
    }

    total_messages = SPI_processed;
    tupdesc = SPI_tuptable->tupdesc;
    if (tupdesc->natts < 18) {
        elog(WARNING, "[ulak] Query returned unexpected column count: %d (expected 18)",
             tupdesc->natts);
        PopActiveSnapshot();
        SPI_finish();
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        return false;
    }

    /* Copy every field into batch_context: the SPI result and the
     * transaction are gone before delivery starts. */
    MemoryContextSwitchTo(batch_context);
    all_messages = palloc0(sizeof(MessageBatchInfo) * total_messages);

    for (i = 0; i < total_messages; i++) {
        HeapTuple tuple = SPI_tuptable->vals[i];
        bool isnull;
        Datum datum;

        all_messages[i].message_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
        all_messages[i].endpoint_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &isnull));

        datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
        if (isnull) {
            all_messages[i].payload_str = pstrdup("{}");
        } else {
            Jsonb *payload = DatumGetJsonbP(datum);
            all_messages[i].payload_str = JsonbToCString(NULL, &payload->root, VARSIZE(payload));
        }

        all_messages[i].retry_count = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 4, &isnull));

        datum = SPI_getbinval(tuple, tupdesc, 5, &isnull);
        if (isnull) {
            elog(WARNING, "[ulak] NULL protocol for message %lld, skipping",
                 (long long)all_messages[i].message_id);
            all_messages[i].processed = true;
            all_messages[i].success = false;
            all_messages[i].error_message = pstrdup("Endpoint protocol is NULL");
            continue;
        }
        all_messages[i].protocol = text_to_cstring(DatumGetTextPP(datum));

        datum = SPI_getbinval(tuple, tupdesc, 6, &isnull);
        if (isnull) {
            elog(WARNING, "[ulak] NULL config for message %lld, skipping",
                 (long long)all_messages[i].message_id);
            all_messages[i].processed = true;
            all_messages[i].success = false;
            all_messages[i].error_message = pstrdup("Endpoint config is NULL");
            continue;
        }
        {
            Jsonb *config_jsonb = DatumGetJsonbP(datum);
            all_messages[i].config = (Jsonb *)palloc(VARSIZE(config_jsonb));
            memcpy(all_messages[i].config, config_jsonb, VARSIZE(config_jsonb));
        }

        datum = SPI_getbinval(tuple, tupdesc, 7, &isnull);
        if (!isnull) {
            Jsonb *rp = DatumGetJsonbP(datum);
            all_messages[i].retry_policy = (Jsonb *)palloc(VARSIZE(rp));
            memcpy(all_messages[i].retry_policy, rp, VARSIZE(rp));
        }

        all_messages[i].priority = DatumGetInt16(SPI_getbinval(tuple, tupdesc, 8, &isnull));
        if (isnull)
            all_messages[i].priority = 0;
        all_messages[i].scheduled_at =
            DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 9, &isnull));
        if (isnull)
            all_messages[i].scheduled_at = 0;
        all_messages[i].expires_at =
            DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 10, &isnull));
        if (isnull)
            all_messages[i].expires_at = 0;

        datum = SPI_getbinval(tuple, tupdesc, 11, &isnull);
        if (!isnull)
            all_messages[i].correlation_id = DatumGetCString(DirectFunctionCall1(uuid_out, datum));

        all_messages[i].endpoint_enabled = DatumGetBool(SPI_getbinval(tuple, tupdesc, 12, &isnull));
        if (isnull)
            all_messages[i].endpoint_enabled = true;
        all_messages[i].endpoint_failure_count =
            DatumGetInt32(SPI_getbinval(tuple, tupdesc, 13, &isnull));
        if (isnull)
            all_messages[i].endpoint_failure_count = 0;

        datum = SPI_getbinval(tuple, tupdesc, 14, &isnull);
        if (!isnull) {
            char *cs_str = text_to_cstring(DatumGetTextPP(datum));
            strlcpy(all_messages[i].circuit_state, cs_str, sizeof(all_messages[i].circuit_state));
            pfree(cs_str);
        } else {
            strlcpy(all_messages[i].circuit_state, "closed", sizeof(all_messages[i].circuit_state));
        }
        all_messages[i].circuit_opened_at =
            DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 15, &isnull));
        if (isnull)
            all_messages[i].circuit_opened_at = 0;
        all_messages[i].circuit_half_open_at =
            DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 16, &isnull));
        if (isnull)
            all_messages[i].circuit_half_open_at = 0;

        datum = SPI_getbinval(tuple, tupdesc, 17, &isnull);
        if (!isnull) {
            Jsonb *hdr = DatumGetJsonbP(datum);
            all_messages[i].headers = (Jsonb *)palloc(VARSIZE(hdr));
            memcpy(all_messages[i].headers, hdr, VARSIZE(hdr));
        }
        datum = SPI_getbinval(tuple, tupdesc, 18, &isnull);
        if (!isnull) {
            Jsonb *meta = DatumGetJsonbP(datum);
            all_messages[i].metadata = (Jsonb *)palloc(VARSIZE(meta));
            memcpy(all_messages[i].metadata, meta, VARSIZE(meta));
        }
    }
    MemoryContextSwitchTo(spi_context);

    /* Mark all claimed rows 'processing' in one UPDATE */
    {
        static const char *mark_processing_query =
            "UPDATE ulak.queue SET status = 'processing', "
            "processing_started_at = NOW() WHERE id = ANY($1::bigint[])";
        Datum *mark_ids = palloc(sizeof(Datum) * total_messages);
        ArrayType *mark_array;

        for (i = 0; i < total_messages; i++)
            mark_ids[i] = Int64GetDatum(all_messages[i].message_id);
        mark_array = construct_array(mark_ids, total_messages, INT8OID, sizeof(int64), true,
                                     TYPALIGN_DOUBLE);
        ret = SPI_execute_with_args(mark_processing_query, 1, (Oid[]){INT8ARRAYOID},
                                    (Datum[]){PointerGetDatum(mark_array)}, NULL, false, 0);
        pfree(mark_ids);
        if (ret != SPI_OK_UPDATE || SPI_processed != total_messages) {
            /* Delivering rows we did not manage to claim is not an option. */
            elog(WARNING, "[ulak] Failed to mark %llu messages as processing (SPI %d, %llu rows)",
                 (unsigned long long)total_messages, ret, (unsigned long long)SPI_processed);
            PopActiveSnapshot();
            SPI_finish();
            AbortCurrentTransaction();
            MemoryContextSwitchTo(caller_context);
            return false;
        }
    }

    /*
     * Circuit breaker decisions, per endpoint group (rows are ordered by
     * endpoint_id). Rows behind an open circuit go straight back to
     * 'pending' with a delay in this same transaction and are flagged
     * `deferred` so the delivery phase skips them. The breaker counters are
     * left alone for deferred rows: nothing was attempted.
     */
    batch_start = 0;
    while (batch_start < total_messages) {
        int64 endpoint_id = all_messages[batch_start].endpoint_id;
        uint64 k;

        batch_end = batch_start;
        while (batch_end < total_messages && all_messages[batch_end].endpoint_id == endpoint_id)
            batch_end++;

        if (strcmp(all_messages[batch_start].circuit_state, "open") == 0) {
            const char *defer_query;
            uint64 defer_from = batch_start;

            if (all_messages[batch_start].circuit_half_open_at > 0 &&
                GetCurrentTimestamp() >= all_messages[batch_start].circuit_half_open_at) {
                /* Cooldown elapsed: one worker wins the half_open transition and
                 * sends the first row as the probe; everybody else defers. */
                if (cb_try_half_open_transition(endpoint_id)) {
                    elog(LOG, "[ulak] Circuit breaker half_open for endpoint %lld — sending probe",
                         (long long)endpoint_id);
                    defer_from = batch_start + 1;
                    defer_query = "UPDATE ulak.queue SET status = 'pending', "
                                  "processing_started_at = NULL, "
                                  "next_retry_at = NOW() + '5 seconds'::interval, "
                                  "last_error = 'Circuit breaker half_open - waiting for probe "
                                  "result' WHERE id = $1";
                } else {
                    defer_query = "UPDATE ulak.queue SET status = 'pending', "
                                  "processing_started_at = NULL, "
                                  "next_retry_at = NOW() + '5 seconds'::interval, "
                                  "last_error = 'Circuit breaker transition lost — another worker "
                                  "is probing' WHERE id = $1";
                }
            } else {
                defer_query = "UPDATE ulak.queue SET status = 'pending', "
                              "processing_started_at = NULL, "
                              "next_retry_at = NOW() + '10 seconds'::interval, "
                              "last_error = 'Circuit breaker open - dispatch deferred' "
                              "WHERE id = $1";
                elog(DEBUG1,
                     "[ulak] Deferred %llu messages for endpoint %lld: circuit breaker open",
                     (unsigned long long)(batch_end - batch_start), (long long)endpoint_id);
            }

            for (k = defer_from; k < batch_end; k++) {
                SPI_execute_with_args(defer_query, 1, (Oid[]){INT8OID},
                                      (Datum[]){Int64GetDatum(all_messages[k].message_id)}, NULL,
                                      false, 0);
                all_messages[k].deferred = true;
                all_messages[k].processed = false;
            }
        }
        batch_start = batch_end;
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
    MemoryContextSwitchTo(caller_context);

    *messages = all_messages;
    *count = total_messages;
    return true;
}

/**
 * @brief Phase 2: deliver every non-deferred row, endpoint by endpoint.
 *
 * Runs with no transaction open. Allocations (error strings, dispatch
 * results) land in the current memory context, which the caller sets to
 * the batch context.
 */
static void batch_deliver(MessageBatchInfo *all_messages, uint64 total_messages) {
    uint64 batch_start = 0;

    while (batch_start < total_messages) {
        int64 endpoint_id = all_messages[batch_start].endpoint_id;
        uint64 batch_end = batch_start;
        uint64 live_end;

        /* Allow timely SIGTERM / SIGHUP handling between endpoint groups */
        CHECK_FOR_INTERRUPTS();

        while (batch_end < total_messages && all_messages[batch_end].endpoint_id == endpoint_id)
            batch_end++;

        batch_heartbeat_if_due();

        /* Deferred rows are a suffix of the group (all, or all but the probe) */
        live_end = batch_start;
        while (live_end < batch_end && !all_messages[live_end].deferred)
            live_end++;

        if (live_end > batch_start)
            process_endpoint_batch(&all_messages[batch_start], (int)(live_end - batch_start),
                                   all_messages[batch_start].protocol,
                                   all_messages[batch_start].config,
                                   all_messages[batch_start].retry_policy);

        batch_start = batch_end;
    }
}

/**
 * @brief Phase 3: write the outcome of every row in one transaction.
 *
 * @return false when the transaction had to be aborted; the rows are then
 *         still 'processing' and the caller releases them.
 */
static bool batch_write_results(MessageBatchInfo *all_messages, uint64 total_messages,
                                int64 *messages_processed) {
    MemoryContext caller_context = CurrentMemoryContext;
    int64 processed = 0;
    int failed_updates = 0;
    int ret;
    uint64 i;

    *messages_processed = 0;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed while writing batch results");
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        return false;
    }
    SPI_execute_with_args("SET LOCAL synchronous_commit = off", 0, NULL, NULL, NULL, false, 0);
    PushActiveSnapshot(GetTransactionSnapshot());

    {
        /* Batch queries */
        /*
         * Every update is guarded with status = 'processing': the rows were
         * ours when claimed, but stale recovery may have handed them to
         * another worker while we were delivering. A row we no longer own is
         * left alone (at-least-once; the new owner reports its own outcome).
         */
        static const char *batch_success_query =
            "UPDATE ulak.queue SET status = 'completed', last_error = NULL, "
            "completed_at = NOW(), updated_at = NOW() "
            "WHERE id = ANY($1::bigint[]) AND status = 'processing'";
        static const char *batch_revert_query =
            "UPDATE ulak.queue q SET status = 'pending', "
            "processing_started_at = NULL, "
            "next_retry_at = NOW() + (v.defer_ms::text || ' milliseconds')::interval "
            "FROM (SELECT unnest($1::bigint[]) AS id, "
            "             unnest($2::int[]) AS defer_ms) v "
            "WHERE q.id = v.id AND q.status = 'processing'";
        static const char *batch_failed_query =
            "UPDATE ulak.queue q SET status = 'failed', "
            "retry_count = v.retry_count, last_error = v.last_error, "
            "failed_at = NOW() "
            "FROM (SELECT unnest($1::bigint[]) AS id, "
            "             unnest($2::int[]) AS retry_count, "
            "             unnest($3::text[]) AS last_error) v "
            "WHERE q.id = v.id AND q.status = 'processing' RETURNING q.id";
        static const char *batch_retry_query =
            "UPDATE ulak.queue q SET status = 'pending', "
            "retry_count = v.retry_count, last_error = v.last_error, "
            "next_retry_at = NOW() + (v.delay_seconds || ' seconds')::interval "
            "FROM (SELECT unnest($1::bigint[]) AS id, "
            "             unnest($2::int[]) AS retry_count, "
            "             unnest($3::text[]) AS last_error, "
            "             unnest($4::text[]) AS delay_seconds) v "
            "WHERE q.id = v.id AND q.status = 'processing'";
        static const char *batch_dlq_query = "SELECT ulak.archive_single_to_dlq(id) "
                                             "FROM unnest($1::bigint[]) AS id";
        /* Individual query for response capture (unique per message) */
        static const char *success_response_query =
            "UPDATE ulak.queue SET status = $1, last_error = NULL, "
            "completed_at = NOW(), response = $2::jsonb "
            "WHERE id = $3 AND status = 'processing'";
        /* Batch collection arrays */
        Datum *success_ids = palloc(sizeof(Datum) * total_messages);
        int success_count = 0;
        Datum *rate_limited_ids = palloc(sizeof(Datum) * total_messages);
        Datum *rate_limited_delays = palloc(sizeof(Datum) * total_messages);
        int rate_limited_count = 0;
        Datum *perm_fail_ids = palloc(sizeof(Datum) * total_messages);
        Datum *perm_fail_retries = palloc(sizeof(Datum) * total_messages);
        Datum *perm_fail_errors = palloc(sizeof(Datum) * total_messages);
        int perm_fail_count = 0;
        Datum *retry_fail_ids = palloc(sizeof(Datum) * total_messages);
        Datum *retry_fail_retries = palloc(sizeof(Datum) * total_messages);
        Datum *retry_fail_errors = palloc(sizeof(Datum) * total_messages);
        Datum *retry_fail_delays = palloc(sizeof(Datum) * total_messages);
        int retry_fail_count = 0;

        /* Phase 1: Categorize messages into batch groups */
        for (i = 0; i < total_messages; i++) {
            if (all_messages[i].rate_limited) {
                rate_limited_ids[rate_limited_count] = Int64GetDatum(all_messages[i].message_id);
                rate_limited_delays[rate_limited_count] = Int32GetDatum(
                    all_messages[i].rate_limit_defer_ms > 0 ? all_messages[i].rate_limit_defer_ms
                                                            : 1000);
                rate_limited_count++;
                continue;
            }

            if (!all_messages[i].processed) {
                /* Deferred rows were handled in the claim transaction; anything
                 * else left unattempted goes back to pending right away. */
                if (!all_messages[i].deferred) {
                    rate_limited_ids[rate_limited_count] =
                        Int64GetDatum(all_messages[i].message_id);
                    rate_limited_delays[rate_limited_count] = Int32GetDatum(0);
                    rate_limited_count++;
                }
                continue;
            }

            worker_update_stats_local(all_messages[i].success, all_messages[i].error_message);

            if (all_messages[i].success) {
                processed++;
                if (all_messages[i].result && ulak_capture_response) {
                    /* Response capture: individual UPDATE (unique response per msg) */
                    ProtocolType proto_type;
                    Jsonb *response_jsonb;
                    if (!protocol_string_to_type(all_messages[i].protocol, &proto_type)) {
                        elog(WARNING,
                             "[ulak] Unknown protocol '%s' in response capture for "
                             "message %lld",
                             all_messages[i].protocol ? all_messages[i].protocol : "(null)",
                             (long long)all_messages[i].message_id);
                        proto_type = PROTOCOL_TYPE_HTTP;
                    }
                    response_jsonb = dispatch_result_to_jsonb(all_messages[i].result, proto_type);
                    if (response_jsonb) {
                        char *response_str =
                            JsonbToCString(NULL, &response_jsonb->root, VARSIZE(response_jsonb));
                        Oid argtypes[3] = {TEXTOID, TEXTOID, INT8OID};
                        Datum values[3];
                        char nulls[3] = {' ', ' ', ' '};
                        values[0] = CStringGetTextDatum(STATUS_COMPLETED);
                        values[1] = CStringGetTextDatum(response_str);
                        values[2] = Int64GetDatum(all_messages[i].message_id);
                        ret = SPI_execute_with_args(success_response_query, 3, argtypes, values,
                                                    nulls, false, 0);
                        if (ret != SPI_OK_UPDATE) {
                            elog(WARNING,
                                 "[ulak] Failed to update message %lld status: SPI "
                                 "error %d",
                                 (long long)all_messages[i].message_id, ret);
                            failed_updates++;
                        }
                    } else {
                        success_ids[success_count++] = Int64GetDatum(all_messages[i].message_id);
                    }
                } else {
                    success_ids[success_count++] = Int64GetDatum(all_messages[i].message_id);
                }
            } else {
                /* Categorize failed messages for batch UPDATE */
                int max_retries = get_max_retries_from_policy(all_messages[i].retry_policy);
                int delay_seconds = calculate_delay_from_policy(all_messages[i].retry_policy,
                                                                all_messages[i].retry_count);
                char *error_str =
                    all_messages[i].error_message ? all_messages[i].error_message : "Unknown error";
                bool is_permanent_error = (error_str && strncmp(error_str, ERROR_PREFIX_PERMANENT,
                                                                ERROR_PREFIX_PERMANENT_LEN) == 0);

                /* Retry-After override: use server-specified delay if available */
                if (all_messages[i].result != NULL &&
                    all_messages[i].result->retry_after_seconds > 0) {
                    delay_seconds = all_messages[i].result->retry_after_seconds;
                    elog(DEBUG1, "[ulak] Using Retry-After=%d for message %lld", delay_seconds,
                         (long long)all_messages[i].message_id);
                }

                /* 410 Gone auto-disable: check error string for [DISABLE] marker */
                if (error_str && strstr(error_str, ERROR_PREFIX_DISABLE) != NULL) {
                    bool should_disable = false;

                    /* Check endpoint config for auto_disable_on_gone (default: false) */
                    if (all_messages[i].config != NULL) {
                        JsonbValue val;
                        if (extract_jsonb_value(all_messages[i].config, "auto_disable_on_gone",
                                                &val) &&
                            val.type == jbvBool && val.val.boolean) {
                            should_disable = true;
                        }
                    }
                    /* Also check DispatchResult flag (dispatch_ex path) */
                    if (!should_disable && all_messages[i].result != NULL &&
                        all_messages[i].result->should_disable_endpoint) {
                        if (all_messages[i].config != NULL) {
                            JsonbValue val;
                            if (extract_jsonb_value(all_messages[i].config, "auto_disable_on_gone",
                                                    &val) &&
                                val.type == jbvBool && val.val.boolean) {
                                should_disable = true;
                            }
                        }
                    }

                    if (should_disable) {
                        static const char *disable_query =
                            "UPDATE ulak.endpoints SET enabled = false, "
                            "updated_at = NOW() WHERE id = $1 AND enabled = true";
                        Oid dis_argtypes[1] = {INT8OID};
                        Datum dis_values[1] = {Int64GetDatum(all_messages[i].endpoint_id)};
                        char dis_nulls[1] = {' '};
                        int dis_ret = SPI_execute_with_args(disable_query, 1, dis_argtypes,
                                                            dis_values, dis_nulls, false, 0);
                        if (dis_ret == SPI_OK_UPDATE && SPI_processed > 0) {
                            elog(WARNING,
                                 "[ulak] Auto-disabled endpoint %lld: "
                                 "HTTP 410 Gone (auto_disable_on_gone=true)",
                                 (long long)all_messages[i].endpoint_id);
                        }
                    }
                }

                if (is_permanent_error || all_messages[i].retry_count + 1 >= max_retries) {
                    perm_fail_ids[perm_fail_count] = Int64GetDatum(all_messages[i].message_id);
                    perm_fail_retries[perm_fail_count] =
                        Int32GetDatum(all_messages[i].retry_count + 1);
                    perm_fail_errors[perm_fail_count] = CStringGetTextDatum(error_str);
                    perm_fail_count++;
                } else {
                    char delay_str[32];
                    snprintf(delay_str, sizeof(delay_str), "%d", delay_seconds);
                    retry_fail_ids[retry_fail_count] = Int64GetDatum(all_messages[i].message_id);
                    retry_fail_retries[retry_fail_count] =
                        Int32GetDatum(all_messages[i].retry_count + 1);
                    retry_fail_errors[retry_fail_count] = CStringGetTextDatum(error_str);
                    retry_fail_delays[retry_fail_count] = CStringGetTextDatum(delay_str);
                    retry_fail_count++;
                }
            }

            /* Circuit breaker: track last result per endpoint.
             * We call update_circuit_breaker once per endpoint after the
             * loop, avoiding N SPI calls for N messages to the same endpoint. */

            /* Free DispatchResult if allocated */
            if (all_messages[i].result != NULL) {
                dispatch_result_free(all_messages[i].result);
                all_messages[i].result = NULL;
            }
        }

        /* Circuit breaker: one update per endpoint (last result wins) */
        {
            int64 last_ep_id = -1;
            bool last_ep_success = false;

            for (i = 0; i < total_messages; i++) {
                if (!all_messages[i].processed || all_messages[i].rate_limited)
                    continue;
                /* Messages are ordered by endpoint_id, so track transitions */
                if (all_messages[i].endpoint_id != last_ep_id) {
                    /* Flush previous endpoint's CB if any */
                    if (last_ep_id >= 0)
                        cb_update_after_dispatch(last_ep_id, last_ep_success);
                    last_ep_id = all_messages[i].endpoint_id;
                    last_ep_success = all_messages[i].success;
                } else {
                    /* Same endpoint: if any message failed, mark as failed */
                    if (!all_messages[i].success)
                        last_ep_success = false;
                }
            }
            /* Flush last endpoint */
            if (last_ep_id >= 0)
                cb_update_after_dispatch(last_ep_id, last_ep_success);
        }

        /* Phase 2: Execute batch UPDATEs */

        /* Batch revert rate-limited messages (deferred by ~1 token interval) */
        if (rate_limited_count > 0) {
            ArrayType *id_array = construct_array(rate_limited_ids, rate_limited_count, INT8OID,
                                                  sizeof(int64), true, TYPALIGN_DOUBLE);
            ArrayType *delay_array = construct_array(rate_limited_delays, rate_limited_count,
                                                     INT4OID, sizeof(int32), true, TYPALIGN_INT);
            Oid argtypes[2] = {INT8ARRAYOID, INT4ARRAYOID};
            Datum values[2] = {PointerGetDatum(id_array), PointerGetDatum(delay_array)};
            char nulls[2] = {' ', ' '};
            ret = SPI_execute_with_args(batch_revert_query, 2, argtypes, values, nulls, false, 0);
            if (ret != SPI_OK_UPDATE) {
                elog(WARNING, "[ulak] Batch revert rate-limited failed: SPI error %d", ret);
            }
        }

        /* Batch success UPDATE */
        if (success_count > 0) {
            ArrayType *id_array = construct_array(success_ids, success_count, INT8OID,
                                                  sizeof(int64), true, TYPALIGN_DOUBLE);
            Oid argtypes[1] = {INT8ARRAYOID};
            Datum values[1] = {PointerGetDatum(id_array)};
            char nulls[1] = {' '};
            ret = SPI_execute_with_args(batch_success_query, 1, argtypes, values, nulls, false, 0);
            if (ret != SPI_OK_UPDATE) {
                elog(WARNING, "[ulak] Batch success UPDATE failed: SPI error %d", ret);
                failed_updates += success_count;
            }
        }

        /* Batch permanent failure UPDATE + DLQ archive */
        if (perm_fail_count > 0) {
            ArrayType *id_array = construct_array(perm_fail_ids, perm_fail_count, INT8OID,
                                                  sizeof(int64), true, TYPALIGN_DOUBLE);
            ArrayType *retry_array = construct_array(perm_fail_retries, perm_fail_count, INT4OID,
                                                     sizeof(int32), true, TYPALIGN_INT);
            ArrayType *error_array = construct_array(perm_fail_errors, perm_fail_count, TEXTOID, -1,
                                                     false, TYPALIGN_INT);
            Oid argtypes[3] = {INT8ARRAYOID, INT4ARRAYOID, TEXTARRAYOID};
            Datum values[3] = {PointerGetDatum(id_array), PointerGetDatum(retry_array),
                               PointerGetDatum(error_array)};
            char nulls[3] = {' ', ' ', ' '};
            ret = SPI_execute_with_args(batch_failed_query, 3, argtypes, values, nulls, false, 0);
            if (ret != SPI_OK_UPDATE_RETURNING) {
                elog(WARNING, "[ulak] Batch permanent failure UPDATE failed: SPI error %d", ret);
                failed_updates += perm_fail_count;
            } else if (SPI_processed > 0 && SPI_tuptable != NULL) {
                /* Archive exactly the rows the UPDATE marked failed (still ours) */
                uint64 n = SPI_processed;
                Datum *dlq_ids = palloc(sizeof(Datum) * n);
                ArrayType *dlq_array;
                uint64 r;
                int dlq_ret;

                for (r = 0; r < n; r++) {
                    bool id_null;
                    dlq_ids[r] =
                        SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &id_null);
                }
                dlq_array =
                    construct_array(dlq_ids, n, INT8OID, sizeof(int64), true, TYPALIGN_DOUBLE);
                dlq_ret =
                    SPI_execute_with_args(batch_dlq_query, 1, (Oid[]){INT8ARRAYOID},
                                          (Datum[]){PointerGetDatum(dlq_array)}, NULL, false, 0);
                if (dlq_ret != SPI_OK_SELECT)
                    elog(WARNING, "[ulak] Batch DLQ archive failed: SPI error %d", dlq_ret);
                pfree(dlq_ids);
            }
        }

        /* Batch retryable failure UPDATE */
        if (retry_fail_count > 0) {
            ArrayType *id_array = construct_array(retry_fail_ids, retry_fail_count, INT8OID,
                                                  sizeof(int64), true, TYPALIGN_DOUBLE);
            ArrayType *retry_array = construct_array(retry_fail_retries, retry_fail_count, INT4OID,
                                                     sizeof(int32), true, TYPALIGN_INT);
            ArrayType *error_array = construct_array(retry_fail_errors, retry_fail_count, TEXTOID,
                                                     -1, false, TYPALIGN_INT);
            ArrayType *delay_array = construct_array(retry_fail_delays, retry_fail_count, TEXTOID,
                                                     -1, false, TYPALIGN_INT);
            Oid argtypes[4] = {INT8ARRAYOID, INT4ARRAYOID, TEXTARRAYOID, TEXTARRAYOID};
            Datum values[4] = {PointerGetDatum(id_array), PointerGetDatum(retry_array),
                               PointerGetDatum(error_array), PointerGetDatum(delay_array)};
            char nulls[4] = {' ', ' ', ' ', ' '};
            ret = SPI_execute_with_args(batch_retry_query, 4, argtypes, values, nulls, false, 0);
            if (ret != SPI_OK_UPDATE) {
                elog(WARNING, "[ulak] Batch retry failure UPDATE failed: SPI error %d", ret);
                failed_updates += retry_fail_count;
            }
        }

        pfree(success_ids);
        pfree(rate_limited_ids);
        pfree(rate_limited_delays);
        pfree(perm_fail_ids);
        pfree(perm_fail_retries);
        pfree(perm_fail_errors);
        pfree(retry_fail_ids);
        pfree(retry_fail_retries);
        pfree(retry_fail_errors);
        pfree(retry_fail_delays);

        /*
         * If any status update failed, abort so that no partial result is
         * committed. The rows are still 'processing' from the claim
         * transaction; the caller releases them back to 'pending' so they are
         * re-fetched on the next cycle (consumers are idempotent).
         */
        if (failed_updates > 0) {
            elog(WARNING,
                 "[ulak] %d message status update(s) failed, aborting result "
                 "transaction and releasing the batch",
                 failed_updates);
            PopActiveSnapshot();
            SPI_finish();
            AbortCurrentTransaction();
            return false;
        }

        if (processed > 0)
            elog(DEBUG1, "[ulak] Processed %lld/%lu messages in this batch", (long long)processed,
                 (unsigned long)total_messages);
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
    MemoryContextSwitchTo(caller_context);

    *messages_processed = processed;
    return true;
}

int64 batch_processor_run(Oid worker_dboid, int worker_id, int total_workers) {
    MemoryContext caller_context = CurrentMemoryContext;
    MemoryContext batch_context;
    MessageBatchInfo *all_messages = NULL;
    uint64 total_messages = 0;
    int64 messages_processed = 0;

    /*
     * batch_context is a child of TopMemoryContext: it must outlive the
     * claim transaction because the copied rows are delivered after it
     * commits. It is deleted at the end of the batch (or from
     * batch_processor_cleanup_on_error).
     */
    batch_context = AllocSetContextCreate(TopMemoryContext, "ulak batch", ALLOCSET_DEFAULT_SIZES);
    worker_batch_context = batch_context;

    /* Phase 1: claim + commit */
    if (!batch_claim(worker_id, total_workers, batch_context, &all_messages, &total_messages) ||
        total_messages == 0) {
        MemoryContextSwitchTo(caller_context);
        MemoryContextDelete(batch_context);
        worker_batch_context = NULL;
        return 0;
    }
    in_flight.messages = all_messages;
    in_flight.count = total_messages;
    in_flight.claimed = true;
    batch_last_heartbeat = GetCurrentTimestamp();

    /* Phase 2: deliver, no transaction open */
    MemoryContextSwitchTo(batch_context);
    batch_deliver(all_messages, total_messages);
    MemoryContextSwitchTo(caller_context);

    /* Phase 3: write results; on failure release the rows right away */
    if (batch_write_results(all_messages, total_messages, &messages_processed)) {
        /* Flush stats only after the commit to avoid phantom metrics */
        worker_flush_stats_to_shmem(worker_dboid, worker_id);
    } else {
        batch_release_claimed_rows(all_messages, total_messages);
        /* Nothing was recorded; drop the counters this batch accumulated */
        worker_local_stats.messages_processed = 0;
        worker_local_stats.error_count = 0;
        worker_local_stats.has_error = false;
        worker_local_stats.last_error_msg[0] = '\0';
    }

    in_flight.messages = NULL;
    in_flight.count = 0;
    in_flight.claimed = false;
    batch_last_heartbeat = 0;

    MemoryContextSwitchTo(caller_context);
    MemoryContextDelete(batch_context);
    worker_batch_context = NULL;

    return (int64)total_messages;
}
