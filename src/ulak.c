/**
 * @file ulak.c
 * @brief Main entry point for ulak PostgreSQL extension.
 *
 * Frameworks & Drivers Layer -- coordinates between PostgreSQL extension
 * API and business modules.
 *
 * This file acts as a facade that delegates to the appropriate modules:
 * - config/ for configuration management
 * - core/ for business entities and domain logic
 * - dispatchers/ for protocol-specific message dispatching
 * - queue/ for queue management operations
 * - utils/ for utility functions
 */

#include "ulak.h"

/* Required for PostgreSQL extensions */
#if PG_VERSION_NUM >= 180000 && defined(PG_MODULE_MAGIC_EXT)
PG_MODULE_MAGIC_EXT(.name = "ulak", .version = ULAK_VERSION);
#else
PG_MODULE_MAGIC;
#endif

/* Module includes */
#include "config/guc.h"
#include "core/entities.h"
#include "dispatchers/dispatcher.h"
#include "shmem.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "wake.h"

/* SRF and system includes */
#include "catalog/pg_type.h"
#include "funcapi.h"   /* For SRF (Set Returning Functions) */
#include "miscadmin.h" /* For max_worker_processes, MyDatabaseId */

typedef struct WorkerStatusContext {
    int row_count;
    int *row_db_index;
    int *row_worker_index;
    UlakDatabaseEntry *entries;
} WorkerStatusContext;
/**
 * @brief Extension initialization.
 *
 * Main entry point that coordinates module initialization.
 *
 * NOTE: External libraries (curl, mosquitto, kafka) are initialized in the
 * background worker, NOT here. Initializing them in postmaster causes
 * "postmaster became multithreaded during startup" error.
 *
 * Static background worker architecture:
 * - Shared memory is initialized during shared_preload_libraries startup
 * - One or more workers are registered at extension load time
 */
void _PG_init(void) {
    /* Initialize shared memory and worker registry */
    ulak_shmem_init();

    /* Initialize configuration module */
    config_init_guc_variables();

    /* Validate configuration settings */
    config_validate_settings();

    /* Warn if workers may exceed available background worker slots */
    if (ulak_workers > max_worker_processes - 2) {
        elog(WARNING,
             "[ulak] ulak.workers (%d) may exceed available "
             "background worker slots (max_worker_processes=%d). "
             "Increase max_worker_processes to at least %d.",
             ulak_workers, max_worker_processes, ulak_workers + 4);
    }

    /* Register static background workers */
    ulak_register_worker();

    elog(LOG, "[ulak] extension initialized successfully");
    elog(DEBUG1,
         "[ulak] Configuration: poll_interval=%dms, "
         "batch_size=%d, max_retries=%d, log_level=%s",
         config_get_poll_interval(), config_get_batch_size(), config_get_default_max_retries(),
         ulak_log_level_to_string((LogLevel)ulak_log_level));

    /* Startup diagnostics */
    ereport(LOG, (errmsg("ulak starting with poll_interval=%d, database=%s", ulak_poll_interval,
                         ulak_database ? ulak_database : "(default)")));
}

/**
 * @brief Extension cleanup.
 *
 * NOTE: External library cleanup is done in the worker process.
 */
void _PG_fini(void) { elog(LOG, "[ulak] extension unloaded successfully"); }

/**
 * @brief Return the number of pending/processing messages for backpressure.
 *
 * The queue is only counted once per ULAK_BACKPRESSURE_CACHE_USEC per
 * database; in between, callers get the cached value from shared memory.
 * Counting on every send() was O(queue size) per insert.
 *
 * @param reserve Rows the caller is about to insert. They are added to the
 *                cached count after it is read, so back-to-back sends within
 *                the cache window still see each other and a burst cannot
 *                blow through the limit. The next real count replaces the
 *                reservation, so a rejected send over-counts for at most one
 *                cache window.
 *
 * Must be called with SPI connected.
 */
static int64 ulak_backpressure_active_count(int64 reserve) {
    TimestampTz now = GetCurrentTimestamp();
    int64 count = 0;
    bool isnull = true;
    int ret;

    if (reserve < 0)
        reserve = 0;

    if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
        LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
        if (ulak_shmem->active_count_dboid == MyDatabaseId &&
            ulak_shmem->active_count_checked_at != 0 &&
            now - ulak_shmem->active_count_checked_at < ULAK_BACKPRESSURE_CACHE_USEC) {
            count = ulak_shmem->active_count_cache;
            ulak_shmem->active_count_cache = count + reserve;
            LWLockRelease(ulak_shmem->lock);
            return count;
        }
        LWLockRelease(ulak_shmem->lock);
    }

    ret = SPI_execute("SELECT count(*) FROM ulak.queue WHERE status IN ('pending', 'processing')",
                      true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0 || SPI_tuptable == NULL)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("[ulak] failed to count active queue messages: %s",
                               SPI_result_code_string(ret))));

    count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull)
        count = 0;

    if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
        LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
        ulak_shmem->active_count_dboid = MyDatabaseId;
        ulak_shmem->active_count_cache = count + reserve;
        ulak_shmem->active_count_checked_at = now;
        LWLockRelease(ulak_shmem->lock);
    }

    return count;
}

/**
 * @brief SQL-callable wrapper: ulak._active_queue_count(p_reserve bigint).
 *
 * Used by ulak._check_backpressure() (PL/pgSQL) so the SQL API shares the
 * same cached count (and reservation) as the C send() path.
 */
PG_FUNCTION_INFO_V1(ulak_active_queue_count);
Datum ulak_active_queue_count(PG_FUNCTION_ARGS) {
    int64 reserve = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT64(0);
    int64 count;
    int ret;

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));

    count = ulak_backpressure_active_count(reserve);

    SPI_finish();
    PG_RETURN_INT64(count);
}

/**
 * @brief Main function to send messages.
 *
 * Interface Adapter -- enqueues a message for async delivery.
 * Usage: SELECT ulak.send('endpoint_name', '{"key": "value"}'::jsonb);
 *
 * Runs backpressure check, endpoint lookup and INSERT in one SPI session.
 * Errors raised via ereport(ERROR) abort the transaction, which also releases
 * SPI state, so no manual cleanup is needed on the error paths.
 *
 * @param fcinfo Function call info (endpoint_name text, payload jsonb).
 * @return Boolean true on success.
 */
PG_FUNCTION_INFO_V1(ulak_send);
Datum ulak_send(PG_FUNCTION_ARGS) {
    text *endpoint_name_text;
    Jsonb *payload_jsonb;
    int ret;
    bool isnull = true;
    int64 endpoint_id;
    size_t payload_size;
    size_t max_payload;

    /* Validate arguments */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("endpoint_name and payload cannot be null")));

    endpoint_name_text = PG_GETARG_TEXT_PP(0);
    payload_jsonb = PG_GETARG_JSONB_P(1);

    /* Payload size guard (ulak.max_payload_size) */
    payload_size = VARSIZE(payload_jsonb);
    max_payload = ulak_max_payload_size > 0 ? (size_t)ulak_max_payload_size : (1024 * 1024);
    if (payload_size > max_payload)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("payload size %zu bytes exceeds ulak.max_payload_size (%zu bytes)",
                               payload_size, max_payload)));

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));

    /* Backpressure check (cached count, see ulak_backpressure_active_count) */
    if (ulak_max_queue_size > 0) {
        int64 active = ulak_backpressure_active_count(1);

        if (active + 1 > (int64)ulak_max_queue_size)
            ereport(ERROR,
                    (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                     errmsg("[ulak] Queue backpressure: pending/processing queue is at or above "
                            "limit %d",
                            ulak_max_queue_size)));
    }

    /* Validate endpoint exists and is enabled */
    ret = SPI_execute_with_args("SELECT id FROM ulak.endpoints WHERE name = $1 AND enabled = true",
                                1, (Oid[]){TEXTOID}, (Datum[]){PointerGetDatum(endpoint_name_text)},
                                NULL, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0 || SPI_tuptable == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("endpoint '%s' does not exist or is disabled",
                               text_to_cstring(endpoint_name_text))));

    endpoint_id =
        DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    /*
     * Insert message into queue (fully parameterized). The statement trigger
     * would wake every worker; we know the row id, so skip it and mark the
     * one partition the row hashes to (wake.c sets its latch at commit).
     */
    ulak_wake_set_skip_trigger(true);
    ret = SPI_execute_with_args(
        "INSERT INTO ulak.queue "
        "(endpoint_id, payload, status, retry_count, next_retry_at) "
        "VALUES ($1, $2, 'pending', 0, NOW()) RETURNING id",
        2, (Oid[]){INT8OID, JSONBOID},
        (Datum[]){Int64GetDatum(endpoint_id), JsonbPGetDatum(payload_jsonb)}, NULL, false, 0);
    ulak_wake_set_skip_trigger(false);
    if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1 || SPI_tuptable == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to insert message into queue: %s", SPI_result_code_string(ret))));

    {
        bool id_null = true;
        int64 message_id =
            DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &id_null));

        if (!id_null)
            ulak_wake_mark(message_id, NULL, false);
    }

    SPI_finish();

    elog(DEBUG1, "[ulak] Message queued successfully for endpoint_id: %lld",
         (long long)endpoint_id);

    PG_RETURN_BOOL(true);
}

/**
 * @brief Create a new message endpoint.
 *
 * Interface Adapter -- validates name, protocol and config, then inserts
 * into ulak.endpoints. Protocols that were not compiled in are rejected.
 *
 * @param fcinfo Function call info (name text, protocol text, config jsonb).
 * @return The new endpoint's ID (int64).
 */
PG_FUNCTION_INFO_V1(ulak_create_endpoint);
Datum ulak_create_endpoint(PG_FUNCTION_ARGS) {
    text *endpoint_name_text;
    text *protocol_text;
    Jsonb *config_jsonb;
    char *endpoint_name;
    char *protocol_str;
    ProtocolType protocol = PROTOCOL_TYPE_HTTP;
    int64 endpoint_id;
    bool isnull = true;
    int ret;

    /* Validate arguments */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("endpoint_name, protocol, and config cannot be null")));

    endpoint_name_text = PG_GETARG_TEXT_PP(0);
    protocol_text = PG_GETARG_TEXT_PP(1);
    config_jsonb = PG_GETARG_JSONB_P(2);
    endpoint_name = text_to_cstring(endpoint_name_text);
    protocol_str = text_to_cstring(protocol_text);

    /* Validate endpoint name */
    if (!endpoint_validate_name(endpoint_name)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid endpoint name: %s", endpoint_name)));
    }

    /* Convert protocol string to enum */
    if (strcmp(protocol_str, "http") == 0) {
        protocol = PROTOCOL_TYPE_HTTP;
    } else if (strcmp(protocol_str, "kafka") == 0) {
#ifdef ENABLE_KAFKA
        protocol = PROTOCOL_TYPE_KAFKA;
#else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Kafka protocol is not enabled. Rebuild with ENABLE_KAFKA=1")));
#endif
    } else if (strcmp(protocol_str, "mqtt") == 0) {
#ifdef ENABLE_MQTT
        protocol = PROTOCOL_TYPE_MQTT;
#else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("MQTT protocol is not enabled. Rebuild with ENABLE_MQTT=1")));
#endif
    } else if (strcmp(protocol_str, "redis") == 0) {
#ifdef ENABLE_REDIS
        protocol = PROTOCOL_TYPE_REDIS;
#else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Redis protocol is not enabled. Rebuild with ENABLE_REDIS=1")));
#endif
    } else if (strcmp(protocol_str, "amqp") == 0) {
#ifdef ENABLE_AMQP
        protocol = PROTOCOL_TYPE_AMQP;
#else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("AMQP protocol is not enabled. Rebuild with ENABLE_AMQP=1")));
#endif
    } else if (strcmp(protocol_str, "nats") == 0) {
#ifdef ENABLE_NATS
        protocol = PROTOCOL_TYPE_NATS;
#else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("NATS protocol is not enabled. Rebuild with ENABLE_NATS=1")));
#endif
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid protocol: %s. Supported protocols: http"
#ifdef ENABLE_KAFKA
                               ", kafka"
#endif
#ifdef ENABLE_MQTT
                               ", mqtt"
#endif
#ifdef ENABLE_REDIS
                               ", redis"
#endif
#ifdef ENABLE_AMQP
                               ", amqp"
#endif
#ifdef ENABLE_NATS
                               ", nats"
#endif
                               ,
                               protocol_str)));
    }

    /* Validate config using dispatcher module */
    if (!dispatcher_validate_config(protocol, config_jsonb)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid configuration for protocol %s", protocol_str)));
    }

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));

    ret = SPI_execute_with_args("INSERT INTO ulak.endpoints (name, protocol, config) "
                                "VALUES ($1, $2, $3) RETURNING id",
                                3, (Oid[]){TEXTOID, TEXTOID, JSONBOID},
                                (Datum[]){PointerGetDatum(endpoint_name_text),
                                          PointerGetDatum(protocol_text),
                                          JsonbPGetDatum(config_jsonb)},
                                NULL, false, 0);

    if (ret != SPI_OK_INSERT_RETURNING || SPI_processed == 0 || SPI_tuptable == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to create endpoint in database")));

    endpoint_id =
        DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

    SPI_finish();
    pfree(endpoint_name);
    pfree(protocol_str);

    elog(LOG, "[ulak] Created endpoint with ID %lld", (long long)endpoint_id);
    PG_RETURN_INT64(endpoint_id);
}

/**
 * @brief Drop a message endpoint.
 *
 * Deletes the endpoint from ulak.endpoints (RESTRICT semantics).
 *
 * @param fcinfo Function call info (endpoint_name text).
 * @return Boolean true on success.
 */
PG_FUNCTION_INFO_V1(ulak_drop_endpoint);
Datum ulak_drop_endpoint(PG_FUNCTION_ARGS) {
    text *endpoint_name;
    int ret;
    StringInfoData query;
    uint64 rows_affected = 0;

    /* Validate arguments */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("endpoint_name cannot be null")));

    endpoint_name = PG_GETARG_TEXT_PP(0);

    /* Delete endpoint (RESTRICT: will fail if messages exist in queue/dlq/archive) */
    initStringInfo(&query);
    appendStringInfo(&query, "DELETE FROM ulak.endpoints WHERE name = $1");

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        pfree(query.data);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    ret = SPI_execute_with_args(query.data, 1, (Oid[]){TEXTOID},
                                (Datum[]){PointerGetDatum(endpoint_name)}, NULL, false, 0);

    if (ret != SPI_OK_DELETE) {
        SPI_finish();
        pfree(query.data);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to drop endpoint")));
    }

    /* Save SPI_processed before SPI_finish() */
    rows_affected = SPI_processed;

    SPI_finish();
    pfree(query.data);

    if (rows_affected > 0) {
        elog(LOG, "[ulak] Dropped endpoint '%s'", text_to_cstring(endpoint_name));
        PG_RETURN_BOOL(true);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("endpoint '%s' does not exist", text_to_cstring(endpoint_name))));
    }
}

/**
 * @brief Alter a message endpoint.
 *
 * Updates config and/or retry_policy for an existing endpoint.
 *
 * @param fcinfo Function call info (endpoint_name text, new_config jsonb,
 *               optional new_retry_policy jsonb).
 * @return Boolean true on success.
 */
PG_FUNCTION_INFO_V1(ulak_alter_endpoint);
Datum ulak_alter_endpoint(PG_FUNCTION_ARGS) {
    text *endpoint_name;
    Jsonb *new_config = NULL;
    Jsonb *new_retry_policy = NULL;
    int ret;
    StringInfoData query;
    char *protocol_str = NULL;
    uint64 rows_affected = 0;
    MemoryContext oldcontext;

    /* Validate arguments */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("endpoint_name cannot be null")));

    endpoint_name = PG_GETARG_TEXT_PP(0);

    if (!PG_ARGISNULL(1))
        new_config = PG_GETARG_JSONB_P(1);

    /* Note: SQL function has 2 args (endpoint_name, new_config).
     * new_retry_policy support is reserved for a future 3-arg overload. */
    if (PG_NARGS() > 2 && !PG_ARGISNULL(2))
        new_retry_policy = PG_GETARG_JSONB_P(2);

    if (new_config == NULL && new_retry_policy == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("at least one of new_config or new_retry_policy "
                               "must be provided")));

    /* Save memory context for allocations that must survive SPI_finish */
    oldcontext = CurrentMemoryContext;

    /* Get current protocol for validation */
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT protocol FROM ulak.endpoints WHERE name = $1");

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        pfree(query.data);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    ret = SPI_execute_with_args(query.data, 1, (Oid[]){TEXTOID},
                                (Datum[]){PointerGetDatum(endpoint_name)}, NULL, true, 0);

    if (ret == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        HeapTuple tuple = SPI_tuptable->vals[0];
        bool isnull;
        text *protocol = DatumGetTextPP(SPI_getbinval(tuple, tupdesc, 1, &isnull));
        /* Copy protocol_str to parent context so it survives SPI_finish */
        MemoryContext spi_context = MemoryContextSwitchTo(oldcontext);
        protocol_str = text_to_cstring(protocol);
        MemoryContextSwitchTo(spi_context);
    } else {
        SPI_finish();
        pfree(query.data);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("endpoint '%s' does not exist", text_to_cstring(endpoint_name))));
    }

    SPI_finish();
    pfree(query.data);

    /* Validate new config if provided */
    if (new_config && !validate_endpoint_config(protocol_str, new_config)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid configuration for protocol %s", protocol_str)));
    }

    /* Update endpoint */
    initStringInfo(&query);
    appendStringInfo(&query, "UPDATE ulak.endpoints SET ");

    if (new_config && new_retry_policy) {
        appendStringInfo(&query, "config = $2, retry_policy = $3");
    } else if (new_config) {
        appendStringInfo(&query, "config = $2");
    } else if (new_retry_policy) {
        appendStringInfo(&query, "retry_policy = $2");
    }

    appendStringInfo(&query, " WHERE name = $1");

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        pfree(query.data); /* Free allocated query buffer before error */
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    if (new_config && new_retry_policy) {
        ret = SPI_execute_with_args(query.data, 3, (Oid[]){TEXTOID, JSONBOID, JSONBOID},
                                    (Datum[]){PointerGetDatum(endpoint_name),
                                              JsonbPGetDatum(new_config),
                                              JsonbPGetDatum(new_retry_policy)},
                                    NULL, false, 0);
    } else if (new_config) {
        ret = SPI_execute_with_args(
            query.data, 2, (Oid[]){TEXTOID, JSONBOID},
            (Datum[]){PointerGetDatum(endpoint_name), JsonbPGetDatum(new_config)}, NULL, false, 0);
    } else {
        ret = SPI_execute_with_args(
            query.data, 2, (Oid[]){TEXTOID, JSONBOID},
            (Datum[]){PointerGetDatum(endpoint_name), JsonbPGetDatum(new_retry_policy)}, NULL,
            false, 0);
    }

    if (ret != SPI_OK_UPDATE) {
        SPI_finish();
        pfree(query.data);
        pfree(protocol_str);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to alter endpoint")));
    }

    /* Save SPI_processed before SPI_finish() */
    rows_affected = SPI_processed;

    SPI_finish();
    pfree(query.data);

    if (rows_affected > 0) {
        elog(LOG, "[ulak] Altered endpoint '%s'", text_to_cstring(endpoint_name));
        pfree(protocol_str);
        PG_RETURN_BOOL(true);
    } else {
        pfree(protocol_str);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("endpoint '%s' does not exist", text_to_cstring(endpoint_name))));
    }
}

/**
 * @brief SQL wrapper for validate_endpoint_config.
 *
 * Allows SQL to call the C validation function.
 *
 * @param fcinfo Function call info (protocol text, config jsonb).
 * @return Boolean true if configuration is valid.
 */
PG_FUNCTION_INFO_V1(validate_endpoint_config_sql);
Datum validate_endpoint_config_sql(PG_FUNCTION_ARGS) {
    text *protocol_text;
    Jsonb *config;
    char *protocol;
    bool result;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_BOOL(false);

    protocol_text = PG_GETARG_TEXT_PP(0);
    config = PG_GETARG_JSONB_P(1);
    protocol = text_to_cstring(protocol_text);

    result = validate_endpoint_config(protocol, config);

    pfree(protocol);
    PG_RETURN_BOOL(result);
}

/** @name SRF (Set Returning Functions) for monitoring views
 * @{ */

/**
 * @brief Returns per-worker shared-memory status rows.
 *
 * Returns: pid, state, started_at, messages_processed, last_activity,
 * error_count, last_error.
 *
 * @param fcinfo Function call info (no arguments).
 * @return Set of worker status rows.
 */
PG_FUNCTION_INFO_V1(ulak_worker_status);

Datum ulak_worker_status(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    TupleDesc tupdesc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        WorkerStatusContext *status_ctx;
        int db_count = 0;
        int i;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(7);
        TupleDescInitEntry(tupdesc, 1, "pid", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "state", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "started_at", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "messages_processed", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, 5, "last_activity", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, 6, "error_count", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 7, "last_error", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        status_ctx = palloc0(sizeof(WorkerStatusContext));
        if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
            status_ctx->entries = palloc0(sizeof(UlakDatabaseEntry) * ULAK_MAX_DATABASES);
            db_count = ulak_get_registered_databases(status_ctx->entries, ULAK_MAX_DATABASES);

            if (db_count > 0) {
                status_ctx->row_db_index = palloc0(sizeof(int) * db_count * ULAK_MAX_WORKERS);
                status_ctx->row_worker_index = palloc0(sizeof(int) * db_count * ULAK_MAX_WORKERS);

                for (i = 0; i < db_count; i++) {
                    int slot;
                    int max_slots = status_ctx->entries[i].target_workers;

                    if (max_slots < 1)
                        max_slots = 1;
                    if (max_slots > ULAK_MAX_WORKERS)
                        max_slots = ULAK_MAX_WORKERS;

                    for (slot = 0; slot < max_slots; slot++) {
                        status_ctx->row_db_index[status_ctx->row_count] = i;
                        status_ctx->row_worker_index[status_ctx->row_count] = slot;
                        status_ctx->row_count++;
                    }
                }
            }
        }

        funcctx->user_fctx = status_ctx;
        funcctx->max_calls = (status_ctx->row_count > 0) ? status_ctx->row_count : 1;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[7];
        bool nulls[7] = {false, false, false, false, false, false, false};
        HeapTuple tuple;
        WorkerStatusContext *status_ctx = (WorkerStatusContext *)funcctx->user_fctx;

        if (status_ctx != NULL && status_ctx->row_count > 0 &&
            funcctx->call_cntr < status_ctx->row_count) {
            UlakDatabaseEntry *entry;
            int slot;

            entry = &status_ctx->entries[status_ctx->row_db_index[funcctx->call_cntr]];
            slot = status_ctx->row_worker_index[funcctx->call_cntr];

            if (entry->worker_pids[slot] > 0) {
                values[0] = Int32GetDatum(entry->worker_pids[slot]);
                values[1] = CStringGetTextDatum("running");
            } else {
                nulls[0] = true;
                values[0] = Int32GetDatum(0);
                values[1] = CStringGetTextDatum("stopped");
            }

            if (entry->worker_started_at[slot] != 0) {
                values[2] = TimestampTzGetDatum(entry->worker_started_at[slot]);
            } else {
                nulls[2] = true;
                values[2] = TimestampTzGetDatum(0);
            }

            values[3] = Int64GetDatum(entry->messages_processed[slot]);

            if (entry->last_activity[slot] != 0) {
                values[4] = TimestampTzGetDatum(entry->last_activity[slot]);
            } else {
                nulls[4] = true;
                values[4] = TimestampTzGetDatum(0);
            }

            values[5] = Int32GetDatum(entry->error_count[slot]);

            if (entry->last_error_msg[slot][0] != '\0') {
                values[6] = CStringGetTextDatum(entry->last_error_msg[slot]);
            } else {
                nulls[6] = true;
                values[6] = CStringGetTextDatum("");
            }
        } else if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
            LWLockAcquire(ulak_shmem->lock, LW_SHARED);

            if (ulak_shmem->worker_pid > 0) {
                values[0] = Int32GetDatum(ulak_shmem->worker_pid);
                values[1] =
                    CStringGetTextDatum(ulak_shmem->worker_started ? "running" : "starting");
            } else {
                nulls[0] = true;
                values[0] = Int32GetDatum(0);
                values[1] = CStringGetTextDatum("stopped");
            }

            if (ulak_shmem->worker_started_at != 0) {
                values[2] = TimestampTzGetDatum(ulak_shmem->worker_started_at);
            } else {
                nulls[2] = true;
                values[2] = TimestampTzGetDatum(0);
            }

            values[3] =
                Int64GetDatum((int64)pg_atomic_read_u64(&ulak_shmem->atomic_messages_processed));

            if (ulak_shmem->last_activity != 0) {
                values[4] = TimestampTzGetDatum(ulak_shmem->last_activity);
            } else {
                nulls[4] = true;
                values[4] = TimestampTzGetDatum(0);
            }

            values[5] = Int32GetDatum((int32)pg_atomic_read_u32(&ulak_shmem->atomic_error_count));

            if (ulak_shmem->last_error_msg[0] != '\0') {
                values[6] = CStringGetTextDatum(ulak_shmem->last_error_msg);
            } else {
                nulls[6] = true;
                values[6] = CStringGetTextDatum("");
            }

            LWLockRelease(ulak_shmem->lock);
        } else {
            nulls[0] = true;
            values[0] = Int32GetDatum(0);
            values[1] = CStringGetTextDatum("not_initialized");
            nulls[2] = true;
            values[2] = TimestampTzGetDatum(0);
            values[3] = Int64GetDatum(0);
            nulls[4] = true;
            values[4] = TimestampTzGetDatum(0);
            values[5] = Int32GetDatum(0);
            nulls[6] = true;
            values[6] = CStringGetTextDatum("");
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/**
 * @brief Returns overall health status.
 *
 * Returns: component, status, details, checked_at.
 * Checks shared_memory and workers components.
 *
 * @param fcinfo Function call info (no arguments).
 * @return Set of health check rows.
 */
PG_FUNCTION_INFO_V1(ulak_health_check);

Datum ulak_health_check(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    TupleDesc tupdesc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build tuple descriptor */
        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, 1, "component", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "status", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "details", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "checked_at", TIMESTAMPTZOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 2; /* shmem, worker */

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[4];
        bool nulls[4] = {false, false, false, false};
        HeapTuple tuple;
        char details[256];

        values[3] = TimestampTzGetDatum(GetCurrentTimestamp());

        switch (funcctx->call_cntr) {
        case 0: /* Shared memory check */
            values[0] = CStringGetTextDatum("shared_memory");
            if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
                values[1] = CStringGetTextDatum("healthy");
                snprintf(details, sizeof(details), "Shared memory initialized");
            } else {
                values[1] = CStringGetTextDatum("unhealthy");
                snprintf(details, sizeof(details), "Shared memory not initialized");
            }
            values[2] = CStringGetTextDatum(details);
            break;

        case 1: /* Worker check — count active workers via pg_stat_activity (always accurate) */
            values[0] = CStringGetTextDatum("workers");
            {
                int active_count = 0;
                int configured = ulak_workers;
                int spi_ret;

                spi_ret = SPI_connect();
                if (spi_ret == SPI_OK_CONNECT) {
                    spi_ret = SPI_execute("SELECT count(*)::int FROM pg_stat_activity "
                                          "WHERE backend_type = 'ulak worker'",
                                          true, 0);
                    if (spi_ret == SPI_OK_SELECT && SPI_processed > 0) {
                        bool isnull_wc;
                        active_count = DatumGetInt32(SPI_getbinval(
                            SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull_wc));
                    }
                    SPI_finish();
                }

                if (active_count >= configured) {
                    values[1] = CStringGetTextDatum("healthy");
                    snprintf(
                        details, sizeof(details), "%d/%d workers active, %lld messages processed",
                        active_count, configured,
                        ulak_shmem
                            ? (long long)pg_atomic_read_u64(&ulak_shmem->atomic_messages_processed)
                            : 0LL);
                } else if (active_count > 0) {
                    values[1] = CStringGetTextDatum("degraded");
                    snprintf(details, sizeof(details),
                             "%d/%d workers active (check max_worker_processes)", active_count,
                             configured);
                } else {
                    values[1] = CStringGetTextDatum("unhealthy");
                    snprintf(details, sizeof(details),
                             "0/%d workers active — check max_worker_processes and "
                             "shared_preload_libraries",
                             configured);
                }
            }
            values[2] = CStringGetTextDatum(details);
            break;

        default:
            SRF_RETURN_DONE(funcctx);
        }

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/**
 * @brief Returns shared memory metrics as key-value rows.
 *
 * Reads per-worker and global counters from shared memory for the
 * unified metrics() function.
 * Returns (metric_name, metric_value, labels, metric_type).
 *
 * @param fcinfo Function call info (no arguments).
 * @return Set of metric rows.
 */
PG_FUNCTION_INFO_V1(ulak_shmem_metrics);

/** @} */

typedef struct ShmemMetricsContext {
    int row_count;
    struct {
        const char *name;
        double value;
        char labels[256];
        const char *type;
    } rows[256];
} ShmemMetricsContext;

Datum ulak_shmem_metrics(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    ShmemMetricsContext *ctx;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, 1, "metric_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, "metric_value", FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, "labels", JSONBOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, "metric_type", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        ctx = palloc0(sizeof(ShmemMetricsContext));

        if (ulak_shmem != NULL && ulak_shmem->lock != NULL) {
            UlakDatabaseEntry entries[ULAK_MAX_DATABASES];
            int db_count;
            int i, slot;
            int64 total_processed = 0;
            int32 total_errors = 0;

            /* Read database registry (takes LW_SHARED internally) */
            db_count = ulak_get_registered_databases(entries, ULAK_MAX_DATABASES);

            /* Per-worker metrics */
            for (i = 0; i < db_count; i++) {
                int max_slots = entries[i].target_workers;
                if (max_slots < 1)
                    max_slots = 1;
                if (max_slots > ULAK_MAX_WORKERS)
                    max_slots = ULAK_MAX_WORKERS;

                for (slot = 0; slot < max_slots; slot++) {
                    if (ctx->row_count >= 250)
                        break;

                    snprintf(ctx->rows[ctx->row_count].labels, sizeof(ctx->rows[0].labels),
                             "{\"worker_id\": \"%d\"}", slot);
                    ctx->rows[ctx->row_count].name = "messages_processed_total";
                    ctx->rows[ctx->row_count].value = (double)entries[i].messages_processed[slot];
                    ctx->rows[ctx->row_count].type = "counter";
                    total_processed += entries[i].messages_processed[slot];
                    ctx->row_count++;

                    if (ctx->row_count >= 250)
                        break;

                    snprintf(ctx->rows[ctx->row_count].labels, sizeof(ctx->rows[0].labels),
                             "{\"worker_id\": \"%d\"}", slot);
                    ctx->rows[ctx->row_count].name = "errors_total";
                    ctx->rows[ctx->row_count].value = (double)entries[i].error_count[slot];
                    ctx->rows[ctx->row_count].type = "counter";
                    total_errors += entries[i].error_count[slot];
                    ctx->row_count++;
                }
            }

            /* Aggregate totals */
            if (ctx->row_count < 250) {
                ctx->rows[ctx->row_count].name = "messages_processed_total";
                ctx->rows[ctx->row_count].value = (double)total_processed;
                snprintf(ctx->rows[ctx->row_count].labels, sizeof(ctx->rows[0].labels), "{}");
                ctx->rows[ctx->row_count].type = "counter";
                ctx->row_count++;
            }
            if (ctx->row_count < 250) {
                ctx->rows[ctx->row_count].name = "errors_total";
                ctx->rows[ctx->row_count].value = (double)total_errors;
                snprintf(ctx->rows[ctx->row_count].labels, sizeof(ctx->rows[0].labels), "{}");
                ctx->rows[ctx->row_count].type = "counter";
                ctx->row_count++;
            }
        }

        funcctx->user_fctx = ctx;
        funcctx->max_calls = ctx->row_count;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    ctx = (ShmemMetricsContext *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[4];
        bool nulls[4] = {false, false, false, false};
        HeapTuple tuple;
        int idx = funcctx->call_cntr;

        values[0] = CStringGetTextDatum(ctx->rows[idx].name);
        values[1] = Float8GetDatum(ctx->rows[idx].value);
        values[2] = DirectFunctionCall1(jsonb_in, CStringGetDatum(ctx->rows[idx].labels));
        values[3] = CStringGetTextDatum(ctx->rows[idx].type);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
