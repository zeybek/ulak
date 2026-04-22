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
#include "shmem.h"
#include "ulak.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "worker/batch_processor.h"
#include "worker/dispatcher_cache.h"
#include "worker/maintenance.h"

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

/* Database name from GUC */
static char worker_dbname[NAMEDATALEN] = "";

/* Cached database index in shared memory for fast counter updates */
static int cached_db_index = -1;

/* Multi-worker partitioning info (set at startup from DSM params) */
static Oid worker_dboid = InvalidOid;
static int worker_id = 0;     /* This worker's ID (0 to total_workers-1) */
static int total_workers = 1; /* Total number of workers for this database */

static void ulak_worker_loop(void);

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
                (void)batch_processor_run(worker_dboid, worker_id, total_workers);
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

            /* Clean up orphaned batch context + reset local stats (batch not committed) */
            batch_processor_cleanup_on_error();

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
