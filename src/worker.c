/**
 * @file worker.c
 * @brief Background worker implementation for ulak.
 *
 * Single database worker that handles message monitoring and dispatching.
 * Each worker connects to one database, processes pending messages in
 * batches, dispatches via protocol dispatchers, and performs maintenance.
 */

#include <unistd.h>
#include "config/guc.h"
#include "core/entities.h"
#include "dispatchers/dispatcher.h"
#include "lib/stringinfo.h"
#include "shmem.h"
#include "ulak.h"

#include <curl/curl.h>
#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/json_utils.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rate_limit.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#ifdef ENABLE_MQTT
#include <mosquitto.h>
#endif

#ifdef ENABLE_KAFKA
#include <librdkafka/rdkafka.h>
#endif

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

typedef struct {
    int64 message_id;
    int64 endpoint_id;
    char *payload_str;
    int32 retry_count;
    Jsonb *retry_policy;
    char *protocol; /* Copied from endpoint */
    Jsonb *config;  /* Copied from endpoint */
    bool processed;
    bool success;
    char *error_message;
    /* Additional fields */
    int16 priority;
    TimestampTz scheduled_at;
    TimestampTz expires_at;
    char *correlation_id;
    bool endpoint_enabled;
    int32 endpoint_failure_count;
    /* discrete circuit breaker fields (no jsonb) */
    char circuit_state[16];
    int32 circuit_failure_count;
    TimestampTz circuit_opened_at;
    TimestampTz circuit_half_open_at;
    DispatchResult *result; /* Dispatch result for response capture */
    Jsonb *headers;         /* Per-message headers */
    Jsonb *metadata;        /* Per-message metadata */
    bool rate_limited;      /* Deferred by rate limiter — skip dispatch */
} MessageBatchInfo;

/* Database name from GUC */
static char worker_dbname[NAMEDATALEN] = "";

/* Cached database index in shared memory for fast counter updates */
static int cached_db_index = -1;

/* Multi-worker partitioning info (set at startup from DSM params) */
static Oid worker_dboid = InvalidOid;
static int worker_id = 0;     /* This worker's ID (0 to total_workers-1) */
static int total_workers = 1; /* Total number of workers for this database */

/**
 * @brief Dispatcher connection pool -- caches dispatchers across batch cycles.
 *
 * Pattern: pg_net (Supabase) dedicated MemoryContext for bulk cleanup.
 * Ref: libcurl docs (curl.se), everything.curl.dev/libcurl/caches.html
 *
 * Each worker process has its own cache (no locking needed -- fork-based).
 * curl_multi handles retain connection pool, DNS cache, TLS session cache
 * across batches. Stale dispatchers evicted by idle timeout.
 */
static MemoryContext DispatcherCacheContext = NULL;

typedef struct DispatcherCacheEntry {
    int64 endpoint_id;      /* hash key (must be first field for HASH_BLOBS) */
    Dispatcher *dispatcher; /* cached dispatcher instance */
    uint32 config_hash;     /* hash_any() of Jsonb binary for invalidation */
    TimestampTz last_used;  /* timestamp for idle eviction */
} DispatcherCacheEntry;

static HTAB *dispatcher_cache = NULL;

/* Eviction: check every 60 seconds, evict dispatchers idle for 60 seconds */
#define DISPATCHER_EVICT_INTERVAL_MS 60000L
#define DISPATCHER_IDLE_TIMEOUT_MS 60000L
static TimestampTz last_eviction_check = 0;

/* Saved reference for PG_CATCH cleanup of orphaned batch context */
static MemoryContext worker_batch_context = NULL;

static void dispatcher_cache_init(void);
static void dispatcher_cache_destroy(void);
static void dispatcher_cache_evict_stale(void);
static Dispatcher *get_or_create_dispatcher(int64 endpoint_id, ProtocolType proto_type,
                                            Jsonb *config);
static void dispatcher_cache_exit_callback(int code, Datum arg);

static int64 process_pending_messages_batch(void);
static int get_max_retries_from_policy(Jsonb *retry_policy);
static int calculate_exponential_backoff(int retry_count);
static int calculate_delay_from_policy(Jsonb *retry_policy, int retry_count);
static void process_endpoint_batch(MessageBatchInfo *messages, int count, const char *protocol,
                                   Jsonb *config, Jsonb *retry_policy);
static void ulak_worker_loop(void);
static void ulak_worker_init_libs(void);
static void ulak_worker_cleanup_libs(void);
static void mark_expired_messages(void);
static void recover_stale_processing_messages(void);
static void archive_completed_messages(void);
static void run_periodic_maintenance(void);

/**
 * @brief SIGTERM signal handler.
 * @private
 *
 * Sets the termination flag and wakes the latch.
 *
 * @param postgres_signal_arg Signal arguments (unused).
 */
static void ulak_sigterm_handler(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/**
 * @brief SIGHUP signal handler.
 * @private
 *
 * Sets the config-reload flag and wakes the latch.
 *
 * @param postgres_signal_arg Signal arguments (unused).
 */
static void ulak_sighup_handler(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/** @brief Local stats accumulator -- flushed to shmem once per batch. */
static struct {
    int64 messages_processed;
    int32 error_count;
    bool has_error;
    char last_error_msg[256];
} worker_local_stats = {0};

/**
 * @brief Accumulate a single message result into process-local stats.
 * @private
 *
 * @param success   Whether the message was dispatched successfully.
 * @param error_msg Error message on failure, or NULL.
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
 * @private
 *
 * Flushes batch-local metrics into the shared-memory worker slot,
 * then resets the local accumulators.
 */
static void worker_flush_stats_to_shmem(void) {
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

/**
 * @brief Main entry point for background worker.
 *
 * This function is the single entry point for the ulak worker.
 * The worker connects to the database specified by ulak.database GUC.
 *
 * IMPORTANT: Must be exported with PGDLLEXPORT for background worker to find it.
 *
 * @param main_arg Packed argument: (total_workers << 16) | worker_id.
 */
PGDLLEXPORT void ulak_worker_main(Datum main_arg) {
    int32 packed_arg = DatumGetInt32(main_arg);

    /* Extract worker_id and total_workers from packed argument */
    total_workers = (packed_arg >> 16) & 0xFFFF;
    worker_id = packed_arg & 0xFFFF;
    if (total_workers < 1)
        total_workers = 1;

    /* Initialize external libraries */
    ulak_worker_init_libs();

    /*
     * Connect to the database specified by GUC.
     * This must happen BEFORE setting up signal handlers.
     */
    strlcpy(worker_dbname, ulak_database, NAMEDATALEN);
    BackgroundWorkerInitializeConnection(worker_dbname, NULL, 0);
    worker_dboid = MyDatabaseId;
    ulak_register_database(worker_dbname, worker_dboid);
    ulak_set_target_workers(worker_dboid, total_workers);
    (void)ulak_add_worker_pid(worker_dboid, MyProcPid, worker_id);

    /* Set up signal handlers - MyLatch is now initialized */
    ulak_pqsignal(SIGTERM, ulak_sigterm_handler);
    ulak_pqsignal(SIGHUP, ulak_sighup_handler);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Mark worker as started in shared memory */
    ulak_set_worker_started(MyProcPid);
    ulak_update_worker_activity(worker_dboid, worker_id);

    if (total_workers > 1) {
        elog(LOG, "[ulak] Worker %d/%d started for database '%s' (PID %d)", worker_id + 1,
             total_workers, worker_dbname, MyProcPid);
    } else {
        elog(LOG, "[ulak] Worker started for database '%s' (PID %d)", worker_dbname, MyProcPid);
    }

    /* Run the main worker loop */
    ulak_worker_loop();

    /* Cleanup */
    ulak_update_worker_activity(worker_dboid, worker_id);
    ulak_remove_worker_pid(worker_dboid, MyProcPid);
    ulak_clear_worker();
    ulak_worker_cleanup_libs();

    proc_exit(0);
}

/**
 * @brief Process a batch of pending messages.
 * @private
 *
 * Selects pending messages with FOR UPDATE SKIP LOCKED, groups them by
 * endpoint, dispatches via batch or synchronous mode, and updates statuses.
 * Uses modulo partitioning for multi-worker setups.
 *
 * @return Number of messages successfully processed in this batch.
 */
static int64 process_pending_messages_batch(void) {
    int ret;
    int spi_ret;
    int64 messages_processed = 0;
    MemoryContext batch_context;
    MemoryContext old_context;
    MemoryContext spi_context;

    /* Start transaction for SPI operations */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in batch processing: %s",
             SPI_result_code_string(spi_ret));
        AbortCurrentTransaction();
        return 0;
    }

    /*
     * Save the SPI memory context so we can switch back to it before any
     * SPI calls.  batch_context is a child of TopMemoryContext used for
     * palloc/pstrdup of message field copies; it is deleted at the end of
     * the batch to free all message data at once.
     */
    spi_context = CurrentMemoryContext;
    old_context = CurrentMemoryContext;
    batch_context = AllocSetContextCreate(TopMemoryContext, "ulak batch", ALLOCSET_DEFAULT_SIZES);
    worker_batch_context = batch_context;

    /*
     * Use READ COMMITTED isolation level for multi-worker compatibility.
     * REPEATABLE READ causes serialization failures (SQLSTATE 40001) when
     * multiple workers try to FOR UPDATE SKIP LOCKED on the same rows.
     * READ COMMITTED + SKIP LOCKED is the correct pattern for concurrent
     * queue consumers — each worker sees its own consistent view of the
     * rows it locks, without conflicting with other workers.
     */

    /*
     * Disable synchronous_commit for worker transactions.
     * Worker status updates (pending→processing→completed) are idempotent
     * and will be retried on crash, so we trade durability for ~2-3x
     * faster writes. This is safe because the ulak pattern guarantees
     * at-least-once delivery.
     */
    SPI_execute_with_args("SET LOCAL synchronous_commit = off", 0, NULL, NULL, NULL, false, 0);

    PushActiveSnapshot(GetTransactionSnapshot());

    /* First check if extension schema and tables exist */
    ret = SPI_execute_with_args("SELECT 1 FROM pg_namespace WHERE nspname = 'ulak'", 0, NULL, NULL,
                                NULL, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        /* Extension not installed in this database, skip */
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
        if (batch_context)
            MemoryContextSwitchTo(old_context);
        if (batch_context) {
            MemoryContextDelete(batch_context);
            worker_batch_context = NULL;
        }
        return 0;
    }

    /* Select pending messages WITH endpoint info in a single query.
     * Uses parameterized query to prevent SQL injection. */
    {
        StringInfoData query;

        initStringInfo(&query);

        /*
         * Multi-worker partitioning: Each worker processes a disjoint subset of messages
         * using modulo partitioning on the message ID. This ensures:
         * - No contention between workers (each owns a partition)
         * - Deterministic assignment (same message always goes to same worker)
         * - Even distribution (sequential IDs spread evenly)
         *
         * Added priority ordering, scheduled_at, expires_at, and enabled endpoint checks
         */
        if (total_workers > 1) {
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
                             "                 AND q2.id < q.id))) "
                             "  AND (q.id %% %d) = %d " /* Modulo partitioning */
                             "ORDER BY q.priority DESC, q.endpoint_id, q.created_at ASC "
                             "LIMIT %d FOR UPDATE OF q SKIP LOCKED",
                             STATUS_PENDING, total_workers, worker_id, ulak_batch_size);
        } else {
            /* Single worker - no partitioning needed */
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
                             "                 AND q2.id < q.id))) "
                             "ORDER BY q.priority DESC, q.endpoint_id, q.created_at ASC "
                             "LIMIT %d FOR UPDATE OF q SKIP LOCKED",
                             STATUS_PENDING, ulak_batch_size);
        }

        ret = SPI_execute(query.data, false, 0);
        pfree(query.data);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL) {
            uint64 total_messages = SPI_processed;
            MessageBatchInfo *all_messages;
            TupleDesc tupdesc;
            uint64 i;
            uint64 batch_start;
            uint64 batch_end;
            int64 current_endpoint_id;
            int batch_count;
            char *protocol_str;
            Jsonb *config;
            Jsonb *retry_policy;
            int failed_updates = 0;

            /* Verify tuple descriptor has expected columns (18 columns) */
            tupdesc = SPI_tuptable->tupdesc;
            if (tupdesc->natts < 18) {
                elog(WARNING, "[ulak] Query returned unexpected column count: %d (expected 18)",
                     tupdesc->natts);
                PopActiveSnapshot();
                SPI_finish();
                CommitTransactionCommand();
                MemoryContextSwitchTo(spi_context);
                MemoryContextDelete(batch_context);
                worker_batch_context = NULL;
                return 0;
            }

            /* Switch to batch_context for all message field allocations */
            MemoryContextSwitchTo(batch_context);
            all_messages = palloc0(sizeof(MessageBatchInfo) * total_messages);

            /* Collect all message info - MUST copy all data before any subsequent SPI calls */
            for (i = 0; i < total_messages; i++) {
                HeapTuple tuple = SPI_tuptable->vals[i];
                bool isnull;
                Jsonb *payload;
                text *protocol_text;
                Jsonb *config_jsonb;
                Datum retry_policy_datum;
                Datum payload_datum;
                Datum protocol_datum;
                Datum config_datum;

                all_messages[i].message_id =
                    DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
                all_messages[i].endpoint_id =
                    DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &isnull));

                payload_datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
                if (isnull) {
                    all_messages[i].payload_str = pstrdup("{}");
                } else {
                    payload = DatumGetJsonbP(payload_datum);
                    all_messages[i].payload_str =
                        JsonbToCString(NULL, &payload->root, VARSIZE(payload));
                }

                all_messages[i].retry_count =
                    DatumGetInt32(SPI_getbinval(tuple, tupdesc, 4, &isnull));

                /* Copy protocol string - must not be NULL */
                protocol_datum = SPI_getbinval(tuple, tupdesc, 5, &isnull);
                if (isnull) {
                    elog(WARNING, "[ulak] NULL protocol for message %lld, skipping",
                         (long long)all_messages[i].message_id);
                    all_messages[i].protocol = NULL;
                    all_messages[i].config = NULL;
                    all_messages[i].processed = true;
                    all_messages[i].success = false;
                    all_messages[i].error_message = pstrdup("Endpoint protocol is NULL");
                    continue;
                }
                protocol_text = DatumGetTextPP(protocol_datum);
                all_messages[i].protocol = text_to_cstring(protocol_text);

                /* Copy config JSONB - need to make a deep copy, must not be NULL */
                config_datum = SPI_getbinval(tuple, tupdesc, 6, &isnull);
                if (isnull) {
                    elog(WARNING, "[ulak] NULL config for message %lld, skipping",
                         (long long)all_messages[i].message_id);
                    all_messages[i].config = NULL;
                    all_messages[i].processed = true;
                    all_messages[i].success = false;
                    all_messages[i].error_message = pstrdup("Endpoint config is NULL");
                    continue;
                }
                config_jsonb = DatumGetJsonbP(config_datum);
                all_messages[i].config = (Jsonb *)palloc(VARSIZE(config_jsonb));
                memcpy(all_messages[i].config, config_jsonb, VARSIZE(config_jsonb));

                /* Copy retry_policy if present */
                retry_policy_datum = SPI_getbinval(tuple, tupdesc, 7, &isnull);
                if (!isnull) {
                    Jsonb *rp = DatumGetJsonbP(retry_policy_datum);
                    all_messages[i].retry_policy = (Jsonb *)palloc(VARSIZE(rp));
                    memcpy(all_messages[i].retry_policy, rp, VARSIZE(rp));
                } else {
                    all_messages[i].retry_policy = NULL;
                }

                /* Extract additional fields */
                /* Column 8: priority */
                all_messages[i].priority = DatumGetInt16(SPI_getbinval(tuple, tupdesc, 8, &isnull));
                if (isnull)
                    all_messages[i].priority = 0;

                /* Column 9: scheduled_at */
                all_messages[i].scheduled_at =
                    DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 9, &isnull));
                if (isnull)
                    all_messages[i].scheduled_at = 0;

                /* Column 10: expires_at */
                all_messages[i].expires_at =
                    DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 10, &isnull));
                if (isnull)
                    all_messages[i].expires_at = 0;

                /* Column 11: correlation_id (uuid as text) */
                {
                    Datum corr_datum = SPI_getbinval(tuple, tupdesc, 11, &isnull);
                    if (!isnull) {
                        /* UUID is typically returned as a type, convert to string */
                        all_messages[i].correlation_id =
                            DatumGetCString(DirectFunctionCall1(uuid_out, corr_datum));
                    } else {
                        all_messages[i].correlation_id = NULL;
                    }
                }

                /* Column 12: endpoint enabled */
                all_messages[i].endpoint_enabled =
                    DatumGetBool(SPI_getbinval(tuple, tupdesc, 12, &isnull));
                if (isnull)
                    all_messages[i].endpoint_enabled = true;

                /* Column 13: circuit_failure_count */
                all_messages[i].endpoint_failure_count =
                    DatumGetInt32(SPI_getbinval(tuple, tupdesc, 13, &isnull));
                if (isnull)
                    all_messages[i].endpoint_failure_count = 0;

                /* Column 14: circuit_state (text) */
                {
                    Datum cs_datum = SPI_getbinval(tuple, tupdesc, 14, &isnull);
                    if (!isnull) {
                        text *cs_text = DatumGetTextPP(cs_datum);
                        char *cs_str = text_to_cstring(cs_text);
                        strlcpy(all_messages[i].circuit_state, cs_str,
                                sizeof(all_messages[i].circuit_state));
                        pfree(cs_str);
                    } else {
                        strlcpy(all_messages[i].circuit_state, "closed",
                                sizeof(all_messages[i].circuit_state));
                    }
                }

                /* Column 15: circuit_opened_at */
                all_messages[i].circuit_opened_at =
                    DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 15, &isnull));
                if (isnull)
                    all_messages[i].circuit_opened_at = 0;

                /* Column 16: circuit_half_open_at */
                all_messages[i].circuit_half_open_at =
                    DatumGetTimestampTz(SPI_getbinval(tuple, tupdesc, 16, &isnull));
                if (isnull)
                    all_messages[i].circuit_half_open_at = 0;

                /* Column 17: headers (jsonb, nullable) */
                {
                    Datum hdr_datum = SPI_getbinval(tuple, tupdesc, 17, &isnull);
                    if (!isnull) {
                        Jsonb *hdr = DatumGetJsonbP(hdr_datum);
                        all_messages[i].headers = (Jsonb *)palloc(VARSIZE(hdr));
                        memcpy(all_messages[i].headers, hdr, VARSIZE(hdr));
                    } else {
                        all_messages[i].headers = NULL;
                    }
                }

                /* Column 18: metadata (jsonb, nullable) */
                {
                    Datum meta_datum = SPI_getbinval(tuple, tupdesc, 18, &isnull);
                    if (!isnull) {
                        Jsonb *meta = DatumGetJsonbP(meta_datum);
                        all_messages[i].metadata = (Jsonb *)palloc(VARSIZE(meta));
                        memcpy(all_messages[i].metadata, meta, VARSIZE(meta));
                    } else {
                        all_messages[i].metadata = NULL;
                    }
                }

                all_messages[i].processed = false;
                all_messages[i].success = false;
                all_messages[i].error_message = NULL;
                all_messages[i].result = NULL; /* Will be set during dispatch if capture_response */
            }

            /* Switch back to SPI context before executing queries */
            MemoryContextSwitchTo(spi_context);

            /* Mark all messages as processing — single batch UPDATE */
            {
                static const char *mark_processing_query =
                    "UPDATE ulak.queue SET status = 'processing', "
                    "processing_started_at = NOW() WHERE id = ANY($1::bigint[])";
                Datum *mark_ids = palloc(sizeof(Datum) * total_messages);
                ArrayType *mark_array;
                Oid mark_argtypes[1] = {INT8ARRAYOID};
                Datum mark_values[1];
                char mark_nulls[1] = {' '};

                for (i = 0; i < total_messages; i++)
                    mark_ids[i] = Int64GetDatum(all_messages[i].message_id);

                mark_array = construct_array(mark_ids, total_messages, INT8OID, sizeof(int64), true,
                                             TYPALIGN_DOUBLE);
                mark_values[0] = PointerGetDatum(mark_array);
                ret = SPI_execute_with_args(mark_processing_query, 1, mark_argtypes, mark_values,
                                            mark_nulls, false, 0);
                if (ret != SPI_OK_UPDATE) {
                    elog(WARNING,
                         "[ulak] Failed to batch-mark %llu messages as processing: SPI error "
                         "%d",
                         (unsigned long long)total_messages, ret);
                }
                pfree(mark_ids);
            }

            /* Process messages grouped by endpoint (they're already ordered by endpoint_id) */
            batch_start = 0;
            while (batch_start < total_messages) {
                /* Allow timely SIGTERM / SIGHUP handling between endpoint groups */
                CHECK_FOR_INTERRUPTS();

                current_endpoint_id = all_messages[batch_start].endpoint_id;
                batch_end = batch_start;

                /* Find all messages for this endpoint */
                while (batch_end < total_messages &&
                       all_messages[batch_end].endpoint_id == current_endpoint_id) {
                    batch_end++;
                }

                batch_count = batch_end - batch_start;

                /* Use protocol and config from the first message of this endpoint group */
                protocol_str = all_messages[batch_start].protocol;
                config = all_messages[batch_start].config;
                retry_policy = all_messages[batch_start].retry_policy;

                /*
                 * Circuit breaker enforcement: skip dispatch if circuit is open.
                 * When skipped, messages go back to pending with a retry delay
                 * so they can be retried when the circuit transitions to half_open.
                 * IMPORTANT: We do NOT call update_circuit_breaker for skipped messages
                 * because no actual dispatch was attempted - otherwise the failure count
                 * would keep increasing and the circuit would never recover.
                 */
                if (strcmp(all_messages[batch_start].circuit_state, "open") == 0) {
                    uint64 k;

                    /*
                     * Check if cooldown has elapsed → transition to half_open.
                     * half_open_at is set when CB opens: opened_at + cooldown.
                     * If current time > half_open_at, allow one probe message through.
                     */
                    if (all_messages[batch_start].circuit_half_open_at > 0 &&
                        GetCurrentTimestamp() >= all_messages[batch_start].circuit_half_open_at) {
                        int cb_ret;

                        /* Transition to half_open: use CAS-style UPDATE so exactly one
                         * worker wins the transition. The WHERE clause ensures only one
                         * UPDATE succeeds; others see SPI_processed == 0 and defer. */
                        cb_ret = SPI_execute_with_args(
                            "UPDATE ulak.endpoints SET circuit_state = 'half_open' "
                            "WHERE id = $1 AND circuit_state = 'open'",
                            1, (Oid[]){INT8OID}, (Datum[]){Int64GetDatum(current_endpoint_id)},
                            NULL, false, 0);

                        if (cb_ret != SPI_OK_UPDATE || SPI_processed == 0) {
                            /* Another worker already transitioned — defer like open */
                            uint64 m;
                            for (m = batch_start; m < (uint64)batch_end; m++) {
                                static const char *cb_lost_query =
                                    "UPDATE ulak.queue SET status = 'pending', "
                                    "processing_started_at = NULL, "
                                    "next_retry_at = NOW() + '5 seconds'::interval, "
                                    "last_error = 'Circuit breaker transition lost — "
                                    "another worker is probing' "
                                    "WHERE id = $1";
                                SPI_execute_with_args(
                                    cb_lost_query, 1, (Oid[]){INT8OID},
                                    (Datum[]){Int64GetDatum(all_messages[m].message_id)}, NULL,
                                    false, 0);
                                all_messages[m].processed = false;
                            }
                            batch_start = batch_end;
                            continue;
                        }

                        elog(LOG,
                             "[ulak] Circuit breaker half_open for endpoint %lld — sending "
                             "probe",
                             (long long)current_endpoint_id);

                        /* Let only the first message through as a probe, defer the rest */
                        for (k = batch_start + 1; k < (uint64)batch_end; k++) {
                            static const char *cb_defer_query =
                                "UPDATE ulak.queue SET status = 'pending', "
                                "processing_started_at = NULL, "
                                "next_retry_at = NOW() + '5 seconds'::interval, "
                                "last_error = 'Circuit breaker half_open - waiting for probe "
                                "result' "
                                "WHERE id = $1";
                            Oid defer_argtypes[1] = {INT8OID};
                            Datum defer_values[1];
                            char defer_nulls[1] = {' '};
                            defer_values[0] = Int64GetDatum(all_messages[k].message_id);
                            SPI_execute_with_args(cb_defer_query, 1, defer_argtypes, defer_values,
                                                  defer_nulls, false, 0);
                            all_messages[k].processed = false;
                        }
                        /* Process only batch_start (probe message) */
                        batch_end = batch_start + 1;
                        batch_count = 1;
                        /* Fall through to process_endpoint_batch with 1 message */
                    } else {
                        /*
                         * Circuit is open and cooldown not elapsed: revert messages back
                         * to pending with a retry delay.
                         */
                        for (k = batch_start; k < (uint64)batch_end; k++) {
                            static const char *cb_defer_query =
                                "UPDATE ulak.queue SET status = 'pending', "
                                "processing_started_at = NULL, "
                                "next_retry_at = NOW() + '10 seconds'::interval, "
                                "last_error = 'Circuit breaker open - dispatch deferred' "
                                "WHERE id = $1";
                            Oid defer_argtypes[1] = {INT8OID};
                            Datum defer_values[1];
                            char defer_nulls[1] = {' '};
                            defer_values[0] = Int64GetDatum(all_messages[k].message_id);
                            SPI_execute_with_args(cb_defer_query, 1, defer_argtypes, defer_values,
                                                  defer_nulls, false, 0);
                            all_messages[k].processed = false;
                        }
                        elog(DEBUG1,
                             "[ulak] Deferred %d messages for endpoint %lld: circuit breaker "
                             "open",
                             batch_count, (long long)current_endpoint_id);
                        batch_start = batch_end;
                        continue;
                    }
                }

                /* Process this endpoint's batch */
                process_endpoint_batch(&all_messages[batch_start], batch_count, protocol_str,
                                       config, retry_policy);

                batch_start = batch_end;
            }

            /* Update all message statuses — batch where possible, individual where needed.
             * No string interpolation of user-derived values (especially error messages
             * which may contain attacker-controlled content from HTTP responses). */
            {
                /* Batch queries */
                static const char *batch_success_query =
                    "UPDATE ulak.queue SET status = 'completed', last_error = NULL, "
                    "completed_at = NOW(), updated_at = NOW() "
                    "WHERE id = ANY($1::bigint[])";
                static const char *batch_revert_query =
                    "UPDATE ulak.queue SET status = 'pending', "
                    "processing_started_at = NULL WHERE id = ANY($1::bigint[])";
                static const char *batch_failed_query =
                    "UPDATE ulak.queue q SET status = 'failed', "
                    "retry_count = v.retry_count, last_error = v.last_error, "
                    "failed_at = NOW() "
                    "FROM (SELECT unnest($1::bigint[]) AS id, "
                    "             unnest($2::int[]) AS retry_count, "
                    "             unnest($3::text[]) AS last_error) v "
                    "WHERE q.id = v.id";
                static const char *batch_retry_query =
                    "UPDATE ulak.queue q SET status = 'pending', "
                    "retry_count = v.retry_count, last_error = v.last_error, "
                    "next_retry_at = NOW() + (v.delay_seconds || ' seconds')::interval "
                    "FROM (SELECT unnest($1::bigint[]) AS id, "
                    "             unnest($2::int[]) AS retry_count, "
                    "             unnest($3::text[]) AS last_error, "
                    "             unnest($4::text[]) AS delay_seconds) v "
                    "WHERE q.id = v.id";
                static const char *batch_dlq_query = "SELECT ulak.archive_single_to_dlq(id) "
                                                     "FROM unnest($1::bigint[]) AS id";
                /* Individual query for response capture (unique per message) */
                static const char *success_response_query =
                    "UPDATE ulak.queue SET status = $1, last_error = NULL, "
                    "completed_at = NOW(), response = $2::jsonb WHERE id = $3";
                static const char *cb_query_str = "SELECT ulak.update_circuit_breaker($1, $2)";

                /* Batch collection arrays */
                Datum *success_ids = palloc(sizeof(Datum) * total_messages);
                int success_count = 0;
                Datum *rate_limited_ids = palloc(sizeof(Datum) * total_messages);
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
                        rate_limited_ids[rate_limited_count++] =
                            Int64GetDatum(all_messages[i].message_id);
                        continue;
                    }

                    if (!all_messages[i].processed)
                        continue;

                    worker_update_stats_local(all_messages[i].success,
                                              all_messages[i].error_message);

                    if (all_messages[i].success) {
                        messages_processed++;
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
                            response_jsonb =
                                dispatch_result_to_jsonb(all_messages[i].result, proto_type);
                            if (response_jsonb) {
                                char *response_str = JsonbToCString(NULL, &response_jsonb->root,
                                                                    VARSIZE(response_jsonb));
                                Oid argtypes[3] = {TEXTOID, TEXTOID, INT8OID};
                                Datum values[3];
                                char nulls[3] = {' ', ' ', ' '};
                                values[0] = CStringGetTextDatum(STATUS_COMPLETED);
                                values[1] = CStringGetTextDatum(response_str);
                                values[2] = Int64GetDatum(all_messages[i].message_id);
                                ret = SPI_execute_with_args(success_response_query, 3, argtypes,
                                                            values, nulls, false, 0);
                                if (ret != SPI_OK_UPDATE) {
                                    elog(WARNING,
                                         "[ulak] Failed to update message %lld status: SPI "
                                         "error %d",
                                         (long long)all_messages[i].message_id, ret);
                                    failed_updates++;
                                }
                            } else {
                                success_ids[success_count++] =
                                    Int64GetDatum(all_messages[i].message_id);
                            }
                        } else {
                            success_ids[success_count++] =
                                Int64GetDatum(all_messages[i].message_id);
                        }
                    } else {
                        /* Categorize failed messages for batch UPDATE */
                        int max_retries = get_max_retries_from_policy(all_messages[i].retry_policy);
                        int delay_seconds = calculate_delay_from_policy(
                            all_messages[i].retry_policy, all_messages[i].retry_count);
                        char *error_str = all_messages[i].error_message
                                              ? all_messages[i].error_message
                                              : "Unknown error";
                        bool is_permanent_error =
                            (error_str && strncmp(error_str, ERROR_PREFIX_PERMANENT,
                                                  ERROR_PREFIX_PERMANENT_LEN) == 0);

                        /* Retry-After override: use server-specified delay if available */
                        if (all_messages[i].result != NULL &&
                            all_messages[i].result->retry_after_seconds > 0) {
                            delay_seconds = all_messages[i].result->retry_after_seconds;
                            elog(DEBUG1, "[ulak] Using Retry-After=%d for message %lld",
                                 delay_seconds, (long long)all_messages[i].message_id);
                        }

                        /* 410 Gone auto-disable: check error string for [DISABLE] marker */
                        if (error_str && strstr(error_str, ERROR_PREFIX_DISABLE) != NULL) {
                            bool should_disable = false;

                            /* Check endpoint config for auto_disable_on_gone (default: false) */
                            if (all_messages[i].config != NULL) {
                                JsonbValue val;
                                if (extract_jsonb_value(all_messages[i].config,
                                                        "auto_disable_on_gone", &val) &&
                                    val.type == jbvBool && val.val.boolean) {
                                    should_disable = true;
                                }
                            }
                            /* Also check DispatchResult flag (dispatch_ex path) */
                            if (!should_disable && all_messages[i].result != NULL &&
                                all_messages[i].result->should_disable_endpoint) {
                                if (all_messages[i].config != NULL) {
                                    JsonbValue val;
                                    if (extract_jsonb_value(all_messages[i].config,
                                                            "auto_disable_on_gone", &val) &&
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
                                int dis_ret =
                                    SPI_execute_with_args(disable_query, 1, dis_argtypes,
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
                            perm_fail_ids[perm_fail_count] =
                                Int64GetDatum(all_messages[i].message_id);
                            perm_fail_retries[perm_fail_count] =
                                Int32GetDatum(all_messages[i].retry_count + 1);
                            perm_fail_errors[perm_fail_count] = CStringGetTextDatum(error_str);
                            perm_fail_count++;
                        } else {
                            char delay_str[32];
                            snprintf(delay_str, sizeof(delay_str), "%d", delay_seconds);
                            retry_fail_ids[retry_fail_count] =
                                Int64GetDatum(all_messages[i].message_id);
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
                            if (last_ep_id >= 0) {
                                Oid cb_argtypes[2] = {INT8OID, BOOLOID};
                                Datum cb_values[2];
                                char cb_nulls[2] = {' ', ' '};
                                cb_values[0] = Int64GetDatum(last_ep_id);
                                cb_values[1] = BoolGetDatum(last_ep_success);
                                int cb_ret = SPI_execute_with_args(cb_query_str, 2, cb_argtypes,
                                                                   cb_values, cb_nulls, false, 0);
                                if (cb_ret != SPI_OK_SELECT) {
                                    elog(WARNING,
                                         "[ulak] Failed to update circuit breaker for endpoint "
                                         "%lld: SPI error %d",
                                         (long long)last_ep_id, cb_ret);
                                }
                            }
                            last_ep_id = all_messages[i].endpoint_id;
                            last_ep_success = all_messages[i].success;
                        } else {
                            /* Same endpoint: if any message failed, mark as failed */
                            if (!all_messages[i].success)
                                last_ep_success = false;
                        }
                    }
                    /* Flush last endpoint */
                    if (last_ep_id >= 0) {
                        Oid cb_argtypes[2] = {INT8OID, BOOLOID};
                        Datum cb_values[2];
                        char cb_nulls[2] = {' ', ' '};
                        cb_values[0] = Int64GetDatum(last_ep_id);
                        cb_values[1] = BoolGetDatum(last_ep_success);
                        int cb_ret = SPI_execute_with_args(cb_query_str, 2, cb_argtypes, cb_values,
                                                           cb_nulls, false, 0);
                        if (cb_ret != SPI_OK_SELECT) {
                            elog(WARNING,
                                 "[ulak] Failed to update circuit breaker for endpoint %lld: "
                                 "SPI error %d",
                                 (long long)last_ep_id, cb_ret);
                        }
                    }
                }

                /* Phase 2: Execute batch UPDATEs */

                /* Batch revert rate-limited messages */
                if (rate_limited_count > 0) {
                    ArrayType *id_array =
                        construct_array(rate_limited_ids, rate_limited_count, INT8OID,
                                        sizeof(int64), true, TYPALIGN_DOUBLE);
                    Oid argtypes[1] = {INT8ARRAYOID};
                    Datum values[1] = {PointerGetDatum(id_array)};
                    char nulls[1] = {' '};
                    ret = SPI_execute_with_args(batch_revert_query, 1, argtypes, values, nulls,
                                                false, 0);
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
                    ret = SPI_execute_with_args(batch_success_query, 1, argtypes, values, nulls,
                                                false, 0);
                    if (ret != SPI_OK_UPDATE) {
                        elog(WARNING, "[ulak] Batch success UPDATE failed: SPI error %d", ret);
                        failed_updates += success_count;
                    }
                }

                /* Batch permanent failure UPDATE + DLQ archive */
                if (perm_fail_count > 0) {
                    ArrayType *id_array = construct_array(perm_fail_ids, perm_fail_count, INT8OID,
                                                          sizeof(int64), true, TYPALIGN_DOUBLE);
                    ArrayType *retry_array =
                        construct_array(perm_fail_retries, perm_fail_count, INT4OID, sizeof(int32),
                                        true, TYPALIGN_INT);
                    ArrayType *error_array = construct_array(perm_fail_errors, perm_fail_count,
                                                             TEXTOID, -1, false, TYPALIGN_INT);
                    Oid argtypes[3] = {INT8ARRAYOID, INT4ARRAYOID, TEXTARRAYOID};
                    Datum values[3] = {PointerGetDatum(id_array), PointerGetDatum(retry_array),
                                       PointerGetDatum(error_array)};
                    char nulls[3] = {' ', ' ', ' '};
                    ret = SPI_execute_with_args(batch_failed_query, 3, argtypes, values, nulls,
                                                false, 0);
                    if (ret != SPI_OK_UPDATE) {
                        elog(WARNING, "[ulak] Batch permanent failure UPDATE failed: SPI error %d",
                             ret);
                        failed_updates += perm_fail_count;
                    }

                    /* Batch DLQ archive */
                    {
                        Oid dlq_argtypes[1] = {INT8ARRAYOID};
                        Datum dlq_values[1] = {PointerGetDatum(id_array)};
                        char dlq_nulls[1] = {' '};
                        int dlq_ret = SPI_execute_with_args(batch_dlq_query, 1, dlq_argtypes,
                                                            dlq_values, dlq_nulls, false, 0);
                        if (dlq_ret != SPI_OK_SELECT) {
                            elog(WARNING, "[ulak] Batch DLQ archive failed: SPI error %d", dlq_ret);
                        }
                    }
                }

                /* Batch retryable failure UPDATE */
                if (retry_fail_count > 0) {
                    ArrayType *id_array = construct_array(retry_fail_ids, retry_fail_count, INT8OID,
                                                          sizeof(int64), true, TYPALIGN_DOUBLE);
                    ArrayType *retry_array =
                        construct_array(retry_fail_retries, retry_fail_count, INT4OID,
                                        sizeof(int32), true, TYPALIGN_INT);
                    ArrayType *error_array = construct_array(retry_fail_errors, retry_fail_count,
                                                             TEXTOID, -1, false, TYPALIGN_INT);
                    ArrayType *delay_array = construct_array(retry_fail_delays, retry_fail_count,
                                                             TEXTOID, -1, false, TYPALIGN_INT);
                    Oid argtypes[4] = {INT8ARRAYOID, INT4ARRAYOID, TEXTARRAYOID, TEXTARRAYOID};
                    Datum values[4] = {PointerGetDatum(id_array), PointerGetDatum(retry_array),
                                       PointerGetDatum(error_array), PointerGetDatum(delay_array)};
                    char nulls[4] = {' ', ' ', ' ', ' '};
                    ret = SPI_execute_with_args(batch_retry_query, 4, argtypes, values, nulls,
                                                false, 0);
                    if (ret != SPI_OK_UPDATE) {
                        elog(WARNING, "[ulak] Batch retry failure UPDATE failed: SPI error %d",
                             ret);
                        failed_updates += retry_fail_count;
                    }
                }

                pfree(success_ids);
                pfree(rate_limited_ids);
                pfree(perm_fail_ids);
                pfree(perm_fail_retries);
                pfree(perm_fail_errors);
                pfree(retry_fail_ids);
                pfree(retry_fail_retries);
                pfree(retry_fail_errors);
                pfree(retry_fail_delays);

                /*
                 * If any status updates failed, abort the transaction to prevent
                 * messages from being stuck in 'processing' state forever.
                 * Messages will be re-fetched and re-processed on the next cycle.
                 * This is safe because ulak consumers should be idempotent.
                 */
                if (failed_updates > 0) {
                    elog(WARNING,
                         "[ulak] %d message status update(s) failed, aborting batch to "
                         "prevent stuck "
                         "messages",
                         failed_updates);
                    PopActiveSnapshot();
                    SPI_finish();
                    AbortCurrentTransaction();
                    if (batch_context)
                        MemoryContextSwitchTo(old_context);
                    if (batch_context) {
                        MemoryContextDelete(batch_context);
                        worker_batch_context = NULL;
                    }
                    return 0;
                }

                if (messages_processed > 0)
                    elog(LOG, "[ulak] Processed %lld/%lu messages in this batch",
                         (long long)messages_processed, (unsigned long)total_messages);
            }
        } else if (ret != SPI_OK_SELECT) {
            elog(WARNING, "[ulak] Failed to query pending messages: SPI error %d", ret);
        }

        MemoryContextSwitchTo(spi_context);
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();

        /* Flush stats AFTER successful commit to avoid phantom metrics */
        worker_flush_stats_to_shmem();

        /* Destroy batch context — frees all batch allocations automatically */
        MemoryContextDelete(batch_context);
        worker_batch_context = NULL;

        return messages_processed;
    }
}

/**
 * @brief Extract max retries from retry policy.
 * @private
 *
 * Falls back to ulak_default_max_retries GUC if policy is NULL or
 * does not contain a valid max_retries field.
 *
 * @param retry_policy Retry policy JSONB, or NULL.
 * @return Maximum number of retries.
 */
static int get_max_retries_from_policy(Jsonb *retry_policy) {
    JsonbValue max_retries_val;
    int max_retries;

    /* Default max retries if no policy specified */
    if (!retry_policy)
        return ulak_default_max_retries;

    /* Parse retry_policy JSON to extract max_retries */
    if (extract_jsonb_value(retry_policy, "max_retries", &max_retries_val)) {
        if (max_retries_val.type == jbvNumeric) {
            max_retries = DatumGetInt32(
                DirectFunctionCall1(numeric_int4, NumericGetDatum(max_retries_val.val.numeric)));
            if (max_retries > 0)
                return max_retries;
        }
    }

    /* Fallback to default */
    return ulak_default_max_retries;
}

/**
 * @brief Extract backoff type from retry policy.
 * @private
 *
 * Returns a static string constant -- no memory allocation, no leak possible.
 *
 * @param retry_policy Retry policy JSONB, or NULL.
 * @return One of "exponential", "fixed", or "linear".
 */
static const char *get_backoff_type_from_policy(Jsonb *retry_policy) {
    JsonbValue backoff_val;

    /* Default backoff type if no policy specified */
    if (!retry_policy)
        return "exponential";

    /* Parse retry_policy JSON to extract backoff type */
    if (extract_jsonb_value(retry_policy, "backoff", &backoff_val)) {
        if (backoff_val.type == jbvString) {
            /* Compare against known backoff types and return static string */
            if (backoff_val.val.string.len == 5 &&
                strncmp(backoff_val.val.string.val, "fixed", 5) == 0) {
                return "fixed";
            } else if (backoff_val.val.string.len == 6 &&
                       strncmp(backoff_val.val.string.val, "linear", 6) == 0) {
                return "linear";
            }
            /* Unknown backoff type - fall through to default */
        }
    }

    return "exponential"; /* Default */
}

/**
 * @brief Calculate delay for exponential backoff.
 * @private
 *
 * Uses configurable GUC values: ulak.retry_base_delay, retry_max_delay.
 * Includes overflow protection for large retry counts.
 *
 * @param retry_count Current retry attempt number.
 * @return Delay in seconds, capped at ulak_retry_max_delay.
 */
static int calculate_exponential_backoff(int retry_count) {
    int multiplier;
    int delay;

    /* Simple exponential backoff: base_delay * 2^retry_count */
    /* Max delay capped at ulak_retry_max_delay */

    /* Overflow protection: cap retry_count to prevent overflow */
    if (retry_count > MAX_RETRY_COUNT_FOR_EXPONENTIAL)
        return ulak_retry_max_delay;

    multiplier = (1 << retry_count); /* 2^retry_count */
    delay = ulak_retry_base_delay * multiplier;

    /* Check for overflow (delay should be >= base_delay) */
    if (delay < ulak_retry_base_delay)
        return ulak_retry_max_delay;

    return (delay > ulak_retry_max_delay) ? ulak_retry_max_delay : delay;
}

/**
 * @brief Calculate retry delay based on retry policy.
 * @private
 *
 * Supports fixed, linear, and exponential backoff strategies.
 * Uses configurable GUC values for defaults.
 *
 * @param retry_policy Retry policy JSONB, or NULL for default exponential.
 * @param retry_count  Current retry attempt number.
 * @return Delay in seconds.
 */
static int calculate_delay_from_policy(Jsonb *retry_policy, int retry_count) {
    const char *backoff_type = get_backoff_type_from_policy(retry_policy);

    if (strcmp(backoff_type, "fixed") == 0) {
        /* Fixed delay - use base_delay as fixed value */
        JsonbValue delay_val;
        if (extract_jsonb_value(retry_policy, "delay", &delay_val) &&
            delay_val.type == jbvNumeric) {
            return ulak_retry_base_delay;
        }
        return ulak_retry_base_delay;
    } else if (strcmp(backoff_type, "linear") == 0) {
        /* Linear backoff: base_delay + (retry_count * increment) */
        int delay = ulak_retry_base_delay + (retry_count * ulak_retry_increment);
        return (delay > ulak_retry_max_delay) ? ulak_retry_max_delay : delay;
    } else {
        /* Exponential backoff (default) */
        return calculate_exponential_backoff(retry_count);
    }
}

/** @name Dispatcher Connection Pool
 *
 * Caches dispatchers across batch cycles to enable TCP/TLS connection reuse.
 * curl_multi handles retain connection pool, DNS cache, and TLS session cache.
 * Config changes detected via hash_any() on Jsonb binary representation.
 * Stale entries evicted by idle timeout (DISPATCHER_IDLE_TIMEOUT_MS).
 * Guaranteed cleanup via before_shmem_exit hook.
 * @{ */

/**
 * @brief Initialize the dispatcher cache hash table and memory context.
 * @private
 */
static void dispatcher_cache_init(void) {
    HASHCTL ctl;

    DispatcherCacheContext =
        AllocSetContextCreate(TopMemoryContext, "ulak dispatcher cache", ALLOCSET_DEFAULT_SIZES);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(DispatcherCacheEntry);
    ctl.hcxt = DispatcherCacheContext;
    dispatcher_cache =
        hash_create("ulak dispatcher cache", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    last_eviction_check = GetCurrentTimestamp();
}

/**
 * @brief Destroy the dispatcher cache, freeing all cached dispatchers.
 * @private
 *
 * Iterates all entries, calls dispatcher_free() on each, then destroys
 * the hash table and its memory context.
 */
static void dispatcher_cache_destroy(void) {
    HASH_SEQ_STATUS status;
    DispatcherCacheEntry *entry;

    if (!dispatcher_cache)
        return;

    /* dispatcher_free() cleans up curl handles (libcurl-managed, non-palloc) */
    hash_seq_init(&status, dispatcher_cache);
    while ((entry = hash_seq_search(&status)) != NULL) {
        if (entry->dispatcher) {
            dispatcher_free(entry->dispatcher);
            entry->dispatcher = NULL;
        }
    }

    hash_destroy(dispatcher_cache);
    dispatcher_cache = NULL;

    if (DispatcherCacheContext) {
        MemoryContextDelete(DispatcherCacheContext);
        DispatcherCacheContext = NULL;
    }
}

/**
 * @brief Evict idle dispatchers from the cache.
 * @private
 *
 * Removes entries that have been idle longer than DISPATCHER_IDLE_TIMEOUT_MS.
 */
static void dispatcher_cache_evict_stale(void) {
    HASH_SEQ_STATUS status;
    DispatcherCacheEntry *entry;
    TimestampTz now = GetCurrentTimestamp();
    int evicted = 0;

    if (!dispatcher_cache)
        return;

    hash_seq_init(&status, dispatcher_cache);
    while ((entry = hash_seq_search(&status)) != NULL) {
        if (TimestampDifferenceExceeds(entry->last_used, now, DISPATCHER_IDLE_TIMEOUT_MS)) {
            if (entry->dispatcher) {
                dispatcher_free(entry->dispatcher);
                entry->dispatcher = NULL;
            }
            /* Safe: dynahash allows removing the current entry during hash_seq_search */
            hash_search(dispatcher_cache, &entry->endpoint_id, HASH_REMOVE, NULL);
            evicted++;
        }
    }

    if (evicted > 0)
        elog(DEBUG1, "[ulak] Evicted %d stale dispatchers from cache", evicted);

    last_eviction_check = now;
}

/**
 * @brief Return a cached or newly created dispatcher for an endpoint.
 * @private
 *
 * Config invalidation: hash_any() on Jsonb binary. Different binary
 * representation (e.g. key reordering) causes harmless false invalidation.
 * Connection lifecycle managed by libcurl: MAXAGE_CONN (118s idle),
 * dead connection auto-detection, transparent retry on reused connection.
 *
 * @param endpoint_id Endpoint ID (hash key).
 * @param proto_type  Protocol type enum.
 * @param config      Endpoint configuration JSONB.
 * @return Cached or new Dispatcher, or NULL on creation failure.
 */
static Dispatcher *get_or_create_dispatcher(int64 endpoint_id, ProtocolType proto_type,
                                            Jsonb *config) {
    DispatcherCacheEntry *entry;
    bool found;
    uint32 config_hash;
    MemoryContext old_ctx;

    /* Periodic stale eviction — time-based, not iteration-based */
    if (TimestampDifferenceExceeds(last_eviction_check, GetCurrentTimestamp(),
                                   DISPATCHER_EVICT_INTERVAL_MS))
        dispatcher_cache_evict_stale();

    config_hash = hash_any((unsigned char *)config, VARSIZE(config));

    entry = hash_search(dispatcher_cache, &endpoint_id, HASH_ENTER, &found);

    if (found && entry->dispatcher) {
        if (entry->config_hash == config_hash) {
            /* Cache hit — reuse dispatcher (connection pool preserved) */
            entry->last_used = GetCurrentTimestamp();
            return entry->dispatcher;
        }
        /* Config changed — destroy old dispatcher, create new one */
        elog(DEBUG1, "[ulak] Config changed for endpoint %lld, recreating dispatcher",
             (long long)endpoint_id);
        dispatcher_free(entry->dispatcher);
        entry->dispatcher = NULL;
    }

    /* Create new dispatcher in DispatcherCacheContext (survives batch_context deletion).
     * CRITICAL: Deep-copy config Jsonb because dispatcher stores pointers into it
     * (Dispatcher.config and HttpDispatcher.headers point INTO the Jsonb).
     * The original config lives in batch_context which is freed after each batch. */
    old_ctx = MemoryContextSwitchTo(DispatcherCacheContext);
    {
        Jsonb *config_copy = (Jsonb *)palloc(VARSIZE(config));
        memcpy(config_copy, config, VARSIZE(config));

        entry->dispatcher = dispatcher_create(proto_type, config_copy);
        entry->config_hash = config_hash;
        entry->last_used = GetCurrentTimestamp();
        entry->endpoint_id = endpoint_id;
    }
    MemoryContextSwitchTo(old_ctx);

    if (!entry->dispatcher) {
        hash_search(dispatcher_cache, &endpoint_id, HASH_REMOVE, NULL);
        return NULL;
    }

    elog(DEBUG1, "[ulak] Created and cached dispatcher for endpoint %lld", (long long)endpoint_id);
    return entry->dispatcher;
}

/**
 * @brief before_shmem_exit callback -- guaranteed cleanup on worker exit.
 * @private
 *
 * @param code Exit code (unused).
 * @param arg  Callback argument (unused).
 */
static void dispatcher_cache_exit_callback(int code, Datum arg) { dispatcher_cache_destroy(); }

/** @} */

/**
 * @brief Process a batch of messages for a single endpoint.
 * @private
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

    /* Apply rate limiting — defer messages that exceed endpoint rate limit */
    {
        double rl_tokens_per_second = 0.0;
        int rl_burst = 0;
        rate_limit_parse_config(config, &rl_tokens_per_second, &rl_burst);
        if (rl_tokens_per_second > 0.0) {
            for (i = 0; i < count; i++) {
                if (!rate_limit_acquire(messages[i].endpoint_id, rl_tokens_per_second, rl_burst)) {
                    messages[i].rate_limited = true;
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

            use_dispatch_ex = (dispatcher->ops->dispatch_ex != NULL) &&
                              (ulak_capture_response || messages[i].headers != NULL ||
                               messages[i].metadata != NULL);

            if (use_dispatch_ex) {
                DispatchResult *result = dispatch_result_create();
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

/**
 * @brief Initialize external libraries.
 * @private
 *
 * Called at the start of worker process. Libraries are initialized in the
 * worker process, not the postmaster, to avoid threading issues.
 */
static void ulak_worker_init_libs(void) {
    curl_global_init(CURL_GLOBAL_DEFAULT);

#ifdef ENABLE_MQTT
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        elog(WARNING, "[ulak] Failed to initialize Mosquitto library");
    }
#endif
}

/**
 * @brief Cleanup external libraries.
 * @private
 *
 * Called on worker shutdown. Releases curl, mosquitto, and kafka resources.
 */
static void ulak_worker_cleanup_libs(void) {
    curl_global_cleanup();

#ifdef ENABLE_MQTT
    mosquitto_lib_cleanup();
#endif

#ifdef ENABLE_KAFKA
    rd_kafka_wait_destroyed(5000);
#endif
}

/**
 * @brief Mark messages with expired TTL.
 * @private
 *
 * Calls the SQL function ulak.mark_expired_messages() within its own
 * transaction.
 */
static void mark_expired_messages(void) {
    int ret;
    int spi_ret;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in mark_expired_messages: %s",
             SPI_result_code_string(spi_ret));
        AbortCurrentTransaction();
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    ret =
        SPI_execute_with_args("SELECT ulak.mark_expired_messages()", 0, NULL, NULL, NULL, false, 0);
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to call mark_expired_messages: SPI error %d", ret);
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

/**
 * @brief Recover messages stuck in 'processing' state.
 * @private
 *
 * Runs periodically to recover messages left in 'processing' state due to worker
 * crashes or connection drops. Messages older than stale_recovery_timeout are
 * reset to 'pending' status.
 */
static void recover_stale_processing_messages(void) {
    int ret;
    int spi_ret;
    int stale_timeout_seconds = config_get_stale_recovery_timeout();

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in recover_stale_processing_messages: %s",
             SPI_result_code_string(spi_ret));
        AbortCurrentTransaction();
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    /* Parameterized recovery query with configurable timeout */
    {
        static const char *recovery_sql =
            "UPDATE ulak.queue SET status = 'pending', "
            "retry_count = retry_count + 1, "
            "last_error = 'Recovered from stale processing state (continuous recovery)', "
            "next_retry_at = NOW(), updated_at = NOW() "
            "WHERE status = 'processing' "
            "AND processing_started_at < NOW() - ($1 || ' seconds')::interval";
        Oid argtypes[1] = {TEXTOID};
        Datum values[1];
        char nulls[1] = {' '};
        char timeout_str[32];

        snprintf(timeout_str, sizeof(timeout_str), "%d", stale_timeout_seconds);
        values[0] = CStringGetTextDatum(timeout_str);

        ret = SPI_execute_with_args(recovery_sql, 1, argtypes, values, nulls, false, 0);
        if (ret == SPI_OK_UPDATE && SPI_processed > 0) {
            elog(LOG, "[ulak] Continuous recovery: reset %lu stale processing messages",
                 (unsigned long)SPI_processed);
        }
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

/**
 * @brief Move completed messages from queue to archive.
 * @private
 *
 * Calls the SQL function to batch-move completed messages older than the
 * configured retention period. Prevents queue table bloat in production.
 */
static void archive_completed_messages(void) {
    int ret;
    int spi_ret;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in archive_completed_messages: %s",
             SPI_result_code_string(spi_ret));
        AbortCurrentTransaction();
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    ret = SPI_execute_with_args("SELECT ulak.archive_completed_messages()", 0, NULL, NULL, NULL,
                                false, 0);
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to call archive_completed_messages: SPI error %d", ret);
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

/**
 * @brief Run maintenance tasks at low frequency.
 * @private
 *
 * Called every ~60 iterations (~5 minutes at default poll_interval).
 * Performs: event log cleanup, DLQ cleanup, archive partition maintenance,
 * and old archive partition removal.
 */
static void run_periodic_maintenance(void) {
    int ret;
    int spi_ret;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        elog(WARNING, "[ulak] SPI_connect failed in run_periodic_maintenance: %s",
             SPI_result_code_string(spi_ret));
        AbortCurrentTransaction();
        return;
    }
    PushActiveSnapshot(GetTransactionSnapshot());

    /* Cleanup old event log entries */
    ret = SPI_execute_with_args("SELECT ulak.cleanup_event_log()", 0, NULL, NULL, NULL, false, 0);
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to call cleanup_event_log: SPI error %d", ret);
    }

    /* Cleanup old DLQ messages (retention via ulak.dlq_retention_days GUC).
     * Check function existence first so startup remains safe if the SQL
     * surface is not fully installed yet. */
    ret = SPI_execute("SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
                      "WHERE n.nspname = 'ulak' AND p.proname = 'cleanup_dlq'",
                      true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        ret = SPI_execute_with_args("SELECT ulak.cleanup_dlq()", 0, NULL, NULL, NULL, false, 0);
        if (ret != SPI_OK_SELECT) {
            elog(WARNING, "[ulak] Failed to call cleanup_dlq: SPI error %d", ret);
        }
    }

    /* Ensure future archive partitions exist */
    ret = SPI_execute_with_args("SELECT ulak.maintain_archive_partitions(3)", 0, NULL, NULL, NULL,
                                false, 0);
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to call maintain_archive_partitions: SPI error %d", ret);
    }

    /* Cleanup old archive partitions (retention via ulak.archive_retention_months GUC) */
    {
        Oid argtypes[1] = {INT4OID};
        Datum values[1];
        values[0] = Int32GetDatum(ulak_archive_retention_months);
        ret = SPI_execute_with_args("SELECT ulak.cleanup_old_archive_partitions($1)", 1, argtypes,
                                    values, NULL, false, 0);
        if (ret != SPI_OK_SELECT) {
            elog(WARNING, "[ulak] Failed to call cleanup_old_archive_partitions: SPI error %d",
                 ret);
        }
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

/**
 * @brief Main worker loop.
 * @private
 *
 * Handles message processing for the active worker entrypoint.
 * Runs until SIGTERM: checks for config reload, waits on latch,
 * verifies extension installation, performs maintenance (worker_id=0),
 * and processes pending message batches. Includes PG_TRY/PG_CATCH
 * error recovery with exponential backoff.
 */
static void ulak_worker_loop(void) {
    int ret;
    int consecutive_errors = 0;
    const int max_consecutive_errors = 10; /* Exit after 10 consecutive errors */
    bool extension_ready = false;
    bool listener_registered = false;
    bool stale_recovery_done = false;

    /*
     * pg_cron-style extension check pattern:
     * Don't crash or wait-loop if CREATE EXTENSION hasn't been run yet.
     * Instead, check every iteration — skip work if not ready, start
     * automatically once the extension is installed.
     * Ref: citusdata/pg_cron PgCronHasBeenLoaded() pattern.
     */

    /* Initialize dispatcher connection pool */
    dispatcher_cache_init();

    /* Main worker loop */
    while (!got_sigterm) {
        int rc;

        /* Check for config reload request.
         * sig_atomic_t reads/writes are individually atomic, but multiple SIGHUPs
         * may still coalesce into a single reload request, which is acceptable
         * because ProcessConfigFile(PGC_SIGHUP) is idempotent.
         */
        {
            volatile sig_atomic_t sighup_received = got_sighup;
            got_sighup = false;
            if (sighup_received) {
                ProcessConfigFile(PGC_SIGHUP);
                /* GUC changes (http_timeout, batch_capacity etc.) affect dispatchers.
                 * Flush entire cache — dispatchers will be recreated on next use. */
                dispatcher_cache_destroy();
                dispatcher_cache_init();
                elog(LOG, "[ulak] Configuration reloaded, dispatcher cache flushed");
            }
        }

        /* Wait for notifications or timeout */
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       (long)ulak_poll_interval, PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

#if PG_VERSION_NUM < 150000
        ProcessCompletedNotifies();
#endif

        if (rc & WL_POSTMASTER_DEATH) {
            ulak_log("info", "Postmaster died, exiting");
            proc_exit(1);
        }

        /*
         * Check if the ulak extension is installed (pg_cron pattern).
         * Uses get_extension_oid() which is a lightweight catalog lookup —
         * no SPI needed. Skip all work if extension is not yet created.
         */
        if (!extension_ready) {
            Oid ext_oid;

            StartTransactionCommand();
            ext_oid = get_extension_oid("ulak", true); /* missing_ok = true */
            CommitTransactionCommand();

            if (ext_oid == InvalidOid) {
                /* Extension not installed yet — sleep and retry next iteration */
                continue;
            }

            /* Extension just became available */
            extension_ready = true;
            elog(LOG, "[ulak] Extension detected, worker starting");
        }

        /* One-time setup: subscribe to notifications */
        if (!listener_registered) {
            StartTransactionCommand();
            Async_Listen("ulak_new_msg");
            CommitTransactionCommand();
            listener_registered = true;

            elog(LOG, "[ulak] Worker listening for notifications (database: %s)",
                 worker_dbname[0] ? worker_dbname : "(default)");
        }

        /* One-time setup: recover stale processing messages (worker_id=0 only) */
        if (!stale_recovery_done) {
            stale_recovery_done = true;

            if (worker_id == 0) {
                PG_TRY();
                {
                    int stale_timeout_seconds = config_get_stale_recovery_timeout();
                    int spi_ret;
                    SetCurrentStatementStartTimestamp();
                    StartTransactionCommand();
                    spi_ret = SPI_connect();
                    if (spi_ret == SPI_OK_CONNECT) {
                        static const char *recovery_sql =
                            "UPDATE ulak.queue SET status = 'pending', "
                            "retry_count = retry_count + 1, "
                            "last_error = 'Recovered from stale processing state (worker "
                            "restart)', "
                            "next_retry_at = NOW(), updated_at = NOW() "
                            "WHERE status = 'processing' "
                            "AND processing_started_at < NOW() - ($1 || ' seconds')::interval";
                        Oid argtypes[1] = {TEXTOID};
                        Datum values[1];
                        char nulls[1] = {' '};
                        char timeout_str[32];

                        PushActiveSnapshot(GetTransactionSnapshot());
                        snprintf(timeout_str, sizeof(timeout_str), "%d", stale_timeout_seconds);
                        values[0] = CStringGetTextDatum(timeout_str);

                        ret = SPI_execute_with_args(recovery_sql, 1, argtypes, values, nulls, false,
                                                    0);
                        if (ret == SPI_OK_UPDATE && SPI_processed > 0) {
                            elog(LOG, "[ulak] Recovered %lu stale processing messages",
                                 (unsigned long)SPI_processed);
                        }
                        PopActiveSnapshot();
                        SPI_finish();
                    }
                    CommitTransactionCommand();
                }
                PG_CATCH();
                {
                    ErrorData *edata = CopyErrorData();
                    FreeErrorData(edata);
                    FlushErrorState();
                    if (IsTransactionState())
                        AbortCurrentTransaction();
                    elog(WARNING, "[ulak] Stale recovery failed (non-fatal), continuing");
                }
                PG_END_TRY();
            }
        }

        /*
         * Wrap SPI operations in PG_TRY/PG_CATCH to handle database errors gracefully.
         * Without this, a connection drop would crash the worker immediately.
         */
        PG_TRY();
        {
            /* Process messages on timeout or latch set */
            if ((rc & WL_TIMEOUT) || (rc & WL_LATCH_SET)) {
                /* Periodic stale recovery check - only worker 0 to avoid contention */
                if (worker_id == 0) {
                    static int stale_check_counter = 0;
                    int stale_check_interval =
                        (config_get_stale_recovery_timeout() * 1000) / ulak_poll_interval;
                    if (stale_check_interval < 1)
                        stale_check_interval = 1;

                    stale_check_counter++;
                    if (stale_check_counter >= stale_check_interval) {
                        recover_stale_processing_messages();
                        stale_check_counter = 0;
                    }
                }

                /*
                 * Maintenance tasks: only worker_id=0 runs these to avoid
                 * multiple workers fighting over the same rows (which causes
                 * lock contention and potential crashes under high concurrency).
                 */
                if (worker_id == 0) {
                    mark_expired_messages();

                    /* Periodically archive completed messages to prevent queue bloat */
                    {
                        static int archive_counter = 0;
                        archive_counter++;
                        if (archive_counter >= 10) {
                            archive_completed_messages();
                            archive_counter = 0;
                        }
                    }

                    /* Run low-frequency maintenance tasks */
                    {
                        static int maintenance_counter = 0;
                        maintenance_counter++;
                        if (maintenance_counter >= 60) {
                            run_periodic_maintenance();
                            maintenance_counter = 0;
                        }
                    }
                }

                /* All workers process pending messages (modulo partitioned) */
                (void)process_pending_messages_batch();
            }

            /* Reset error counter on successful iteration */
            consecutive_errors = 0;
            ulak_update_worker_activity(worker_dboid, worker_id);
        }
        PG_CATCH();
        {
            ErrorData *edata;
            char error_buf[256];
            char sqlstate_buf[6];

            /*
             * Copy error details to stack buffers BEFORE any cleanup.
             * CopyErrorData pallocs in CurrentMemoryContext which
             * AbortCurrentTransaction will destroy — so extract what
             * we need first, then free, then abort.
             */
            edata = CopyErrorData();
            strlcpy(error_buf, edata->message ? edata->message : "unknown error",
                    sizeof(error_buf));
            strlcpy(sqlstate_buf, unpack_sql_state(edata->sqlerrcode), sizeof(sqlstate_buf));
            FreeErrorData(edata);
            FlushErrorState();

            if (IsTransactionState())
                AbortCurrentTransaction();

            /* Clean up orphaned batch context from process_pending_messages_batch() */
            if (worker_batch_context) {
                MemoryContextDelete(worker_batch_context);
                worker_batch_context = NULL;
            }

            /* Reset local stats — batch was not committed */
            worker_local_stats.messages_processed = 0;
            worker_local_stats.error_count = 0;
            worker_local_stats.has_error = false;
            worker_local_stats.last_error_msg[0] = '\0';

            /* Error recovery: flush dispatcher cache to avoid reusing
             * potentially corrupted state. Reinit for next iteration. */
            dispatcher_cache_destroy();
            dispatcher_cache_init();

            elog(WARNING, "[ulak] Error in worker loop: %s (SQLSTATE %s)", error_buf, sqlstate_buf);

            ulak_update_worker_metrics(worker_dboid, worker_id, 0, 1, error_buf);

            consecutive_errors++;
            if (consecutive_errors >= max_consecutive_errors) {
                elog(WARNING, "[ulak] %d consecutive errors, worker sleeping for recovery",
                     consecutive_errors);
                /*
                 * DO NOT call proc_exit(1) here — PostgreSQL treats non-zero
                 * exit as a crash and may restart the entire cluster.
                 * Instead, sleep and reset the counter. The worker will
                 * resume processing after the backoff period.
                 * Ref: PostgreSQL postmaster HandleChildCrash().
                 */
                pg_usleep(30000000L); /* 30 second extended backoff */
                consecutive_errors = 0;
            } else {
                pg_usleep(1000000L); /* 1 second backoff */
            }
        }
        PG_END_TRY();
    }

    /* Cleanup dispatcher cache on graceful exit */
    dispatcher_cache_destroy();

    if (total_workers > 1) {
        ulak_log("info", "Worker %d/%d loop exiting (database: %s)", worker_id + 1, total_workers,
                 worker_dbname[0] ? worker_dbname : "(default)");
    } else {
        ulak_log("info", "Worker loop exiting (database: %s)",
                 worker_dbname[0] ? worker_dbname : "(default)");
    }
}

/**
 * @brief Entry point for dynamically spawned workers.
 *
 * This function is used by the dynamic database-worker path. The current
 * production runtime primarily uses statically registered workers that
 * connect to ulak.database directly.
 *
 * IMPORTANT: Must be exported with PGDLLEXPORT for background worker to find it.
 *
 * @param main_arg DSM segment handle containing UlakWorkerParams.
 */
PGDLLEXPORT void ulak_database_worker_main(Datum main_arg) {
    dsm_segment *seg;
    UlakWorkerParams *params;
    dsm_handle handle;

    elog(DEBUG1, "[ulak] Database worker starting, main_arg=%u", DatumGetUInt32(main_arg));

    /* Initialize external libraries */
    ulak_worker_init_libs();

    elog(DEBUG1, "[ulak] Database worker: libs initialized");

    /* Attach to DSM segment to get database info */
    handle = DatumGetUInt32(main_arg);
    elog(DEBUG1, "[ulak] Database worker: attaching to DSM handle %u", handle);

    seg = dsm_attach(handle);
    if (seg == NULL) {
        elog(ERROR, "[ulak] Failed to attach to DSM segment handle %u", handle);
    }

    elog(DEBUG1, "[ulak] Database worker: DSM attached");
    params = dsm_segment_address(seg);

    /* Copy database info before detaching DSM */
    worker_dboid = params->dboid;
    strlcpy(worker_dbname, params->dbname, NAMEDATALEN);
    worker_id = params->worker_id;
    total_workers = params->total_workers;

    elog(DEBUG1,
         "[ulak] Database worker: got dbname='%s', dboid=%u, worker_id=%d, "
         "total_workers=%d",
         worker_dbname, worker_dboid, worker_id, total_workers);

    /*
     * Unpin and detach DSM segment - we've copied the data.
     * The segment was pinned by the parent process to survive transaction boundaries.
     */
    dsm_unpin_segment(dsm_segment_handle(seg));
    dsm_detach(seg);

    /*
     * Connect to the assigned database.
     * This must happen BEFORE setting up signal handlers.
     */
    BackgroundWorkerInitializeConnection(worker_dbname, NULL, 0);

    /* Set up signal handlers - MyLatch is now initialized */
    ulak_pqsignal(SIGTERM, ulak_sigterm_handler);
    ulak_pqsignal(SIGHUP, ulak_sighup_handler);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /*
     * Note: In the dynamic worker path the parent process may have already
     * populated the worker slot before this worker begins its main loop.
     */

    /* Cache our database index in shared memory for fast counter updates */
    if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
        int idx;
        LWLockAcquire(ulak_shmem->lock, LW_SHARED);
        for (idx = 0; idx < ULAK_MAX_DATABASES; idx++) {
            if (ulak_shmem->databases[idx].active &&
                ulak_shmem->databases[idx].dboid == worker_dboid) {
                cached_db_index = idx;
                break;
            }
        }
        LWLockRelease(ulak_shmem->lock);
    }

    if (total_workers > 1) {
        elog(LOG, "[ulak] Database worker %d/%d started for '%s' (OID %u, PID %d)", worker_id + 1,
             total_workers, worker_dbname, worker_dboid, MyProcPid);
    } else {
        elog(LOG, "[ulak] Database worker started for '%s' (OID %u, PID %d)", worker_dbname,
             worker_dboid, MyProcPid);
    }

    /* Register exit callback for guaranteed dispatcher cache cleanup.
     * before_shmem_exit runs even on proc_exit — system still operational. */
    before_shmem_exit(dispatcher_cache_exit_callback, (Datum)0);

    /* Run the main worker loop */
    ulak_worker_loop();

    /* Cleanup */
    ulak_remove_worker_pid(worker_dboid, MyProcPid);
    ulak_worker_cleanup_libs();

    proc_exit(0);
}
