/**
 * @file maintenance.c
 * @brief Worker maintenance helpers.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Extracted from src/worker.c. Behavior unchanged.
 */

#include "worker/maintenance.h"

#include "postgres.h"

#include <curl/curl.h>

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "config/guc.h"

#ifdef ENABLE_MQTT
#include <mosquitto.h>
#endif

#ifdef ENABLE_KAFKA
#include <librdkafka/rdkafka.h>
#endif

void ulak_worker_init_libs(void) {
    curl_global_init(CURL_GLOBAL_DEFAULT);

#ifdef ENABLE_MQTT
    if (mosquitto_lib_init() != MOSQ_ERR_SUCCESS) {
        elog(WARNING, "[ulak] Failed to initialize Mosquitto library");
    }
#endif
}

void ulak_worker_cleanup_libs(void) {
    curl_global_cleanup();

#ifdef ENABLE_MQTT
    mosquitto_lib_cleanup();
#endif

#ifdef ENABLE_KAFKA
    rd_kafka_wait_destroyed(5000);
#endif
}

void mark_expired_messages(void) {
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

void recover_stale_processing_messages(const char *reason) {
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

    /*
     * A row left in 'processing' was in the hands of a worker that died or
     * hung, possibly because of the payload itself. Every recovery charges a
     * retry, and a row whose retries are used up goes to the DLQ like any
     * other exhausted row instead of being delivered again forever. The
     * max_retries expression mirrors get_max_retries_from_policy(): the
     * endpoint's retry_policy number (rounded, at least 1, capped at int4)
     * or ulak.default_max_retries. The rows are locked first so a worker
     * that is just writing its outcome (and holds the row lock) is skipped.
     */
    {
        static const char *recovery_sql =
            "WITH stale AS ("
            "  SELECT q.id, q.retry_count + 1 AS retry_count, "
            "         q.retry_count + 1 >= COALESCE(CASE "
            "           WHEN jsonb_typeof(e.retry_policy->'max_retries') = 'number' "
            "            AND round((e.retry_policy->'max_retries')::numeric) >= 1 "
            "           THEN LEAST(round((e.retry_policy->'max_retries')::numeric), "
            "                      2147483647)::int "
            "         END, $2) AS exhausted "
            "  FROM ulak.queue q JOIN ulak.endpoints e ON e.id = q.endpoint_id "
            "  WHERE q.status = 'processing' "
            "    AND q.processing_started_at < NOW() - ($1 || ' seconds')::interval "
            "  FOR UPDATE OF q SKIP LOCKED) "
            "UPDATE ulak.queue q SET "
            "  status = CASE WHEN s.exhausted THEN 'failed' ELSE 'pending' END, "
            "  retry_count = s.retry_count, "
            "  last_error = CASE WHEN s.exhausted THEN $3 || ', max retries reached' ELSE $3 END, "
            "  failed_at = CASE WHEN s.exhausted THEN NOW() ELSE q.failed_at END, "
            "  next_retry_at = NOW(), updated_at = NOW() "
            "FROM stale s WHERE q.id = s.id AND q.status = 'processing' "
            "RETURNING q.id, s.exhausted";
        Oid argtypes[3] = {TEXTOID, INT4OID, TEXTOID};
        Datum values[3];
        char nulls[3] = {' ', ' ', ' '};
        char timeout_str[32];

        snprintf(timeout_str, sizeof(timeout_str), "%d", stale_timeout_seconds);
        values[0] = CStringGetTextDatum(timeout_str);
        values[1] = Int32GetDatum(ulak_default_max_retries);
        values[2] =
            CStringGetTextDatum(psprintf("Recovered from stale processing state (%s)", reason));

        ret = SPI_execute_with_args(recovery_sql, 3, argtypes, values, nulls, false, 0);
        if (ret != SPI_OK_UPDATE_RETURNING) {
            elog(WARNING, "[ulak] Stale recovery (%s) failed: SPI error %d", reason, ret);
        } else if (SPI_processed > 0 && SPI_tuptable != NULL) {
            uint64 n = SPI_processed;
            Datum *dlq_ids = palloc(sizeof(Datum) * n);
            uint64 dlq_count = 0;
            uint64 r;

            for (r = 0; r < n; r++) {
                bool isnull;
                HeapTuple tuple = SPI_tuptable->vals[r];

                if (DatumGetBool(SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull)) &&
                    !isnull)
                    dlq_ids[dlq_count++] = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
            }
            if (dlq_count > 0) {
                ArrayType *dlq_array = construct_array(dlq_ids, (int)dlq_count, INT8OID,
                                                       sizeof(int64), true, TYPALIGN_DOUBLE);

                ret = SPI_execute_with_args("SELECT ulak.archive_single_to_dlq(id) "
                                            "FROM unnest($1::bigint[]) AS id",
                                            1, (Oid[]){INT8ARRAYOID},
                                            (Datum[]){PointerGetDatum(dlq_array)}, NULL, false, 0);
                if (ret != SPI_OK_SELECT)
                    elog(WARNING, "[ulak] Stale recovery DLQ archive failed: SPI error %d", ret);
            }
            elog(LOG,
                 "[ulak] Stale recovery (%s): %llu message(s) back to pending, %llu moved to "
                 "the DLQ after max retries",
                 reason, (unsigned long long)(n - dlq_count), (unsigned long long)dlq_count);
        }
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

void archive_completed_messages(void) {
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

void run_periodic_maintenance(void) {
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
