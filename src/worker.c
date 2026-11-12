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
#include "utils/guc.h"
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

/* Multi-worker partitioning info (set at startup from DSM params) */
static Oid worker_dboid = InvalidOid;
static int worker_id = 0;     /* This worker's ID (0 to total_workers-1) */
static int total_workers = 1; /* Total number of workers for this database */

static void ulak_worker_loop(void);

/* Maintenance cadence on worker 0, in poll intervals (ulak.poll_interval):
 * TTL expiry and archiving every ~5 s, the rest every ~30 s at the default */
#define EXPIRE_EVERY_POLLS 10
#define ARCHIVE_EVERY_POLLS 10
#define MAINTENANCE_EVERY_POLLS 60

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
 * @brief Free this worker's registry slot (PID + latch) on any exit.
 * @private
 *
 * A before_shmem_exit callback, so it also runs when the worker leaves
 * through the bgworker error handler (an ERROR outside our PG_TRY, e.g.
 * pg_cancel_backend() while waiting on the latch) and the loop epilogue is
 * skipped. Otherwise the slot keeps the dead PID, the restarted worker cannot
 * register its latch, and commit-time wake-ups never reach it again.
 *
 * @param code Exit code (unused).
 * @param arg  Callback argument (unused).
 */
static void ulak_worker_shmem_exit(int code, Datum arg) {
    (void)code;
    (void)arg;
    ulak_remove_worker_pid(worker_dboid, MyProcPid);
    ulak_clear_worker();
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

    /*
     * No transaction of this worker waits for a synchronous standby: with the
     * standby gone, SyncRepWaitForLSN() ends only on ProcDiePending, a query
     * cancel or postmaster death, none of which our SIGTERM handler sets.
     * The bookkeeping is idempotent and needs no standby durability; 'local'
     * still flushes WAL. A session value outranks postgresql.conf, so a
     * SIGHUP does not undo it.
     */
    SetConfigOption("synchronous_commit", "local", PGC_USERSET, PGC_S_SESSION);

    /*
     * SIGTERM cannot end a heavyweight-lock wait either. The claim never waits
     * (SKIP LOCKED), but result writes, circuit-breaker updates, heartbeats and
     * maintenance can queue behind another session's lock, e.g. an open admin
     * transaction on an ulak.endpoints row. The timeout raises an ERROR that
     * the catch paths handle, so a shutdown waits at most this long; 10 s is
     * far above the normal waits.
     */
    SetConfigOption("lock_timeout", "10s", PGC_USERSET, PGC_S_SESSION);

    worker_dboid = MyDatabaseId;
    ulak_register_database(worker_dbname, worker_dboid);
    ulak_set_target_workers(worker_dboid, total_workers);
    (void)ulak_add_worker_pid(worker_dboid, MyProcPid, worker_id, MyLatch);
    /* Runs after dispatcher_cache_exit_callback (callbacks run in reverse order) */
    before_shmem_exit(ulak_worker_shmem_exit, (Datum)0);

    /* Set up signal handlers - MyLatch is now initialized */
    ulak_pqsignal(SIGTERM, ulak_sigterm_handler);
    ulak_pqsignal(SIGHUP, ulak_sighup_handler);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Guaranteed dispatcher cache teardown (curl/broker handles) even when the
     * worker exits through FATAL/proc_exit paths that skip the loop epilogue. */
    before_shmem_exit(dispatcher_cache_exit_callback, (Datum)0);

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

    /* Cleanup (the registry slot is freed by ulak_worker_shmem_exit) */
    ulak_update_worker_activity(worker_dboid, worker_id);
    ulak_worker_cleanup_libs();

    /*
     * Exit code 1, not 0: the postmaster unregisters a background worker
     * that exits with 0, so pg_terminate_backend() on a worker would leave
     * its partition undelivered until the next server restart. Exit code 1
     * restarts only this worker after bgw_restart_time
     * (ulak.worker_restart_delay); any other code makes the postmaster reset
     * the whole cluster. During a server shutdown nothing is restarted.
     * Ref: postmaster.c CleanupBackend() and bgworker_should_start_now().
     */
    proc_exit(1);
}

/**
 * @brief Sleep for an error backoff, returning early on SIGTERM.
 * @private
 *
 * Waits on the latch rather than in pg_usleep(), which SIGTERM cannot
 * interrupt. Wake-ups that arrive meanwhile are consumed; the next poll
 * picks their rows up.
 *
 * @param timeout_ms Backoff length in milliseconds.
 */
static void ulak_worker_backoff(long timeout_ms) {
    TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

    while (!got_sigterm) {
        long remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
        int rc;

        if (remaining <= 0)
            break;
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, remaining,
                       PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
    }
}

/**
 * @brief Run one maintenance task when its interval has passed.
 * @private
 *
 * Time-based, not iteration-based: a busy worker re-arms its latch after
 * every full batch, so counting iterations would run maintenance far more
 * often than intended. The timestamp moves before the task runs and the task's
 * errors are contained here, so a task that keeps failing is retried once
 * per interval and never stops worker 0 from delivering its own partition.
 *
 * @param last_run    When the task last started; updated.
 * @param interval_ms Minimum time between two runs.
 * @param task        The maintenance function (manages its own transaction).
 * @param name        Task name for the log.
 */
static void ulak_run_maintenance_task(TimestampTz *last_run, int interval_ms, void (*task)(void),
                                      const char *name) {
    MemoryContext caller_context = CurrentMemoryContext;
    TimestampTz now = GetCurrentTimestamp();

    if (*last_run != 0 && !TimestampDifferenceExceeds(*last_run, now, interval_ms))
        return;
    *last_run = now;

    PG_TRY();
    {
        task();
    }
    PG_CATCH();
    {
        ErrorData *edata;

        /* errfinish() left us in ErrorContext, which CopyErrorData() refuses */
        MemoryContextSwitchTo(caller_context);
        edata = CopyErrorData();
        FlushErrorState();
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller_context);
        elog(WARNING, "[ulak] Maintenance task %s failed (non-fatal): %s", name,
             edata->message ? edata->message : "unknown error");
        FreeErrorData(edata);
    }
    PG_END_TRY();
}

static void recover_stale_at_startup(void) { recover_stale_processing_messages("worker restart"); }

static void recover_stale_continuous(void) {
    recover_stale_processing_messages("continuous recovery");
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
    MemoryContext loop_context = CurrentMemoryContext;
    TimestampTz last_stale_check;
    TimestampTz last_expire;
    TimestampTz last_archive;
    TimestampTz last_maintenance;
    int consecutive_errors = 0;
    const int max_consecutive_errors = 10; /* Long backoff after 10 in a row */
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

    /* Maintenance runs one interval after start (stale recovery also once at start) */
    last_stale_check = last_expire = last_archive = last_maintenance = GetCurrentTimestamp();

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

        /* Wait for a latch wake-up (wake.c, or NOTIFY with ulak.wake_notify) or the poll timeout */
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       (long)ulak_poll_interval, PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        /*
         * Drain our LISTEN queue. Regular backends consume notifications in
         * ProcessClientReadInterrupt(); a background worker never gets there,
         * so without this call the worker's queue position never advances,
         * pg_notification_queue_usage() grows without bound and NOTIFY
         * eventually fails for every sender in the cluster. Must run outside
         * a transaction (ProcessNotifyInterrupt starts its own).
         */
        if (listener_registered && notifyInterruptPending)
            ProcessNotifyInterrupt(false);

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

        /* One-time setup: subscribe to notifications. Since the commit-time
         * latch wake-up (wake.c) this only matters when ulak.wake_notify is on
         * or the catalog still carries a pre-0.2.0 trigger that NOTIFYs. */
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
                TimestampTz never = 0;

                ulak_run_maintenance_task(&never, 0, recover_stale_at_startup,
                                          "startup stale recovery");
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
                /*
                 * Maintenance tasks: only worker_id=0 runs these to avoid
                 * multiple workers fighting over the same rows (which causes
                 * lock contention and potential crashes under high concurrency).
                 * TTL expiry every ~10 polls (5s at the default interval) is
                 * plenty; each run scans every pending row.
                 */
                if (worker_id == 0) {
                    ulak_run_maintenance_task(&last_stale_check,
                                              config_get_stale_recovery_timeout() * 1000,
                                              recover_stale_continuous, "stale recovery");
                    ulak_run_maintenance_task(&last_expire, EXPIRE_EVERY_POLLS * ulak_poll_interval,
                                              mark_expired_messages, "mark_expired_messages");
                    ulak_run_maintenance_task(
                        &last_archive, ARCHIVE_EVERY_POLLS * ulak_poll_interval,
                        archive_completed_messages, "archive_completed_messages");
                    ulak_run_maintenance_task(&last_maintenance,
                                              MAINTENANCE_EVERY_POLLS * ulak_poll_interval,
                                              run_periodic_maintenance, "periodic maintenance");
                }

                /* All workers process pending messages (hash/modulo partitioned).
                 * If the batch came back full there is more backlog waiting:
                 * set our own latch so the next WaitLatch() returns at once
                 * instead of capping drain rate at batch_size / poll_interval. */
                {
                    int64 fetched = batch_processor_run(worker_dboid, worker_id, total_workers);
                    if (fetched >= (int64)ulak_batch_size)
                        SetLatch(MyLatch);
                }
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
             * errfinish() leaves CurrentMemoryContext = ErrorContext, which
             * CopyErrorData() asserts against and FlushErrorState() resets:
             * staying there would make the next iteration allocate in a
             * context that the next error wipes. Copy the error details to
             * stack buffers before any cleanup, then free, then abort.
             */
            MemoryContextSwitchTo(loop_context);
            edata = CopyErrorData();
            strlcpy(error_buf, edata->message ? edata->message : "unknown error",
                    sizeof(error_buf));
            strlcpy(sqlstate_buf, unpack_sql_state(edata->sqlerrcode), sizeof(sqlstate_buf));
            FreeErrorData(edata);
            FlushErrorState();

            /* Unconditional: an error raised while starting or committing a
             * transaction leaves one that IsTransactionState() does not
             * report; with no transaction this is a no-op. */
            AbortCurrentTransaction();
            MemoryContextSwitchTo(loop_context);

            /* Rows claimed by the interrupted batch go back to pending now
             * rather than after stale_recovery_timeout */
            batch_processor_release_claimed();

            /* Clean up orphaned batch context + reset local stats */
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
                 * Back off in place rather than exit: proc_exit(1) would only
                 * restart this worker after ulak.worker_restart_delay, and not
                 * at all with BGW_NEVER_RESTART. Sleep, reset the counter and
                 * resume; SIGTERM still ends the sleep at once.
                 */
                ulak_worker_backoff(30000L); /* 30 second extended backoff */
                consecutive_errors = 0;
            } else {
                ulak_worker_backoff(1000L); /* 1 second backoff */
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
