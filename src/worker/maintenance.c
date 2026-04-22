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

void recover_stale_processing_messages(void) {
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
