/**
 * @file shmem.c
 * @brief Shared memory implementation for ulak.
 *
 * Stores runtime state for the configured database workers, aggregate
 * monitoring counters, the backpressure count cache, and shared rate-limit
 * buckets.
 */

#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"

#include "config/guc.h"
#include "shmem.h"

/* Global pointer to shared memory state */
UlakShmemState *ulak_shmem = NULL;

/* Previous hooks to chain */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/**
 * @brief Calculate the size of shared memory needed.
 *
 * @return Size in bytes required for UlakShmemState.
 */
Size ulak_shmem_size(void) { return sizeof(UlakShmemState); }

/**
 * @brief Shared memory startup hook.
 * @private
 *
 * Initialize shared memory structure after it's allocated.
 * Sets up LWLock, worker registry, atomic counters, rate-limit buckets,
 * and per-database spinlocks.
 */
static void ulak_shmem_startup(void) {
    bool found;

    /* Call previous hook if any */
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    /* Initialize shared memory */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    ulak_shmem = ShmemInitStruct("ulak", ulak_shmem_size(), &found);

    if (!found) {
        LWLockPadded *tranche;

        /* First time - initialize the structure */
        memset(ulak_shmem, 0, ulak_shmem_size());

        /* Get the LWLock tranche we requested (1 lock).
         * GetNamedLWLockTranche() internally ereport(ERROR)s if the tranche
         * was not registered, so a NULL return indicates a severe internal
         * inconsistency — fail hard rather than limping along without locks. */
        tranche = GetNamedLWLockTranche("ulak");
        if (tranche == NULL) {
            LWLockRelease(AddinShmemInitLock);
            ereport(FATAL, (errmsg("[ulak] GetNamedLWLockTranche returned NULL"),
                            errhint("Ensure ulak is listed in shared_preload_libraries.")));
        }
        ulak_shmem->lock = &tranche[0].lock;
        elog(LOG, "[ulak] Shared memory initialized, lock at %p", ulak_shmem->lock);

        ulak_shmem->worker_pid = 0;
        ulak_shmem->worker_started = false;
        ulak_shmem->worker_started_at = 0;
        pg_atomic_init_u64(&ulak_shmem->atomic_messages_processed, 0);
        pg_atomic_init_u32(&ulak_shmem->atomic_error_count, 0);
        ulak_shmem->last_activity = 0;
        ulak_shmem->last_error_msg[0] = '\0';

        ulak_shmem->database_count = 0;
        ulak_shmem->active_count_dboid = InvalidOid;
        ulak_shmem->active_count_cache = 0;
        ulak_shmem->active_count_checked_at = 0;

        /* Initialize per-database spinlocks for metrics */
        {
            int k;
            for (k = 0; k < ULAK_MAX_DATABASES; k++)
                SpinLockInit(&ulak_shmem->databases[k].metrics_mutex);
        }

        /* Initialize rate limit buckets */
        {
            int rl;
            for (rl = 0; rl < RL_SHMEM_MAX_ENDPOINTS; rl++) {
                SpinLockInit(&ulak_shmem->rate_limit_buckets[rl].mutex);
                ulak_shmem->rate_limit_buckets[rl].endpoint_id = 0;
                ulak_shmem->rate_limit_buckets[rl].active = false;
            }
        }
    } else {
        elog(LOG, "[ulak] Shared memory found, lock at %p", ulak_shmem->lock);
    }

    LWLockRelease(AddinShmemInitLock);
}

#if PG_VERSION_NUM >= 150000
/**
 * @brief Shared memory request hook for PostgreSQL 15+.
 * @private
 *
 * Request shared memory space before it's allocated.
 */
static void ulak_shmem_request(void) {
    /* Call previous hook if any */
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(ulak_shmem_size());
    /* Request 1 lock */
    RequestNamedLWLockTranche("ulak", 1);
}
#endif

/**
 * @brief Initialize shared memory subsystem.
 *
 * Called from _PG_init() during extension load. Hooks into
 * shmem_request_hook (PG15+) or directly requests space (PG14),
 * then hooks into shmem_startup_hook for initialization.
 */
void ulak_shmem_init(void) {
    if (!process_shared_preload_libraries_in_progress)
        return;

#if PG_VERSION_NUM >= 150000
    /* PostgreSQL 15+: Use shmem_request_hook */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = ulak_shmem_request;
#else
    /* PostgreSQL 14: Direct call */
    RequestAddinShmemSpace(ulak_shmem_size());
    /* Request 1 lock */
    RequestNamedLWLockTranche("ulak", 1);
#endif

    /* Hook into shmem_startup for initialization */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = ulak_shmem_startup;
}

/**
 * @brief Register a database in the shared memory registry.
 *
 * Called when a worker begins processing. If the database is already
 * registered, updates the timestamp. Otherwise allocates a new slot.
 *
 * @param dbname Database name.
 * @param dboid  Database OID.
 */
void ulak_register_database(const char *dbname, Oid dboid) {
    int i;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL) {
        elog(WARNING, "[ulak] Shared memory not initialized, cannot register database");
        return;
    }

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    /* Check if already registered */
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            /* Already registered, just update timestamp */
            ulak_shmem->databases[i].registered_at = GetCurrentTimestamp();
            LWLockRelease(ulak_shmem->lock);
            elog(LOG, "[ulak] Database '%s' (OID %u) re-registered", dbname, dboid);
            return;
        }
    }

    /* Check if we have space */
    if (ulak_shmem->database_count >= ULAK_MAX_DATABASES) {
        LWLockRelease(ulak_shmem->lock);
        elog(WARNING, "[ulak] Maximum databases (%d) reached, cannot register '%s'",
             ULAK_MAX_DATABASES, dbname);
        return;
    }

    /* Find an empty slot and register */
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (!ulak_shmem->databases[i].active) {
            int j;
            ulak_shmem->databases[i].dboid = dboid;
            strlcpy(ulak_shmem->databases[i].dbname, dbname, NAMEDATALEN);
            ulak_shmem->databases[i].active = true;
            ulak_shmem->databases[i].target_workers = 1; /* Default until worker startup sets it */
            ulak_shmem->databases[i].active_workers = 0;
            /* Initialize worker PID array and metrics */
            SpinLockInit(&ulak_shmem->databases[i].metrics_mutex);
            for (j = 0; j < ULAK_MAX_WORKERS; j++) {
                ulak_shmem->databases[i].worker_pids[j] = 0;
                ulak_shmem->databases[i].worker_latches[j] = NULL;
                ulak_shmem->databases[i].worker_started_at[j] = 0;
                ulak_shmem->databases[i].messages_processed[j] = 0;
                ulak_shmem->databases[i].error_count[j] = 0;
                ulak_shmem->databases[i].last_error_at[j] = 0;
                ulak_shmem->databases[i].last_error_msg[j][0] = '\0';
                ulak_shmem->databases[i].last_activity[j] = 0;
            }
            ulak_shmem->databases[i].registered_at = GetCurrentTimestamp();
            ulak_shmem->database_count++;

            LWLockRelease(ulak_shmem->lock);
            elog(LOG, "[ulak] Database '%s' (OID %u) registered successfully", dbname, dboid);
            return;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    elog(WARNING, "[ulak] Failed to find empty slot for database '%s'", dbname);
}

/**
 * @brief Mark worker as started in shared memory.
 *
 * Called when the single-database worker begins processing.
 *
 * @param pid Process ID of the started worker.
 */
void ulak_set_worker_started(pid_t pid) {
    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    ulak_shmem->worker_pid = pid;
    ulak_shmem->worker_started = true;
    ulak_shmem->worker_started_at = GetCurrentTimestamp();
    ulak_shmem->last_activity = GetCurrentTimestamp();
    LWLockRelease(ulak_shmem->lock);

    elog(LOG, "[ulak] Worker started with PID %d", pid);
}

/**
 * @brief Get list of registered databases.
 *
 * Copies active database entries from shared memory into the caller's array.
 *
 * @param entries     Output array to receive database entries.
 * @param max_entries Maximum number of entries to copy.
 * @return Number of databases copied to the entries array.
 */
int ulak_get_registered_databases(UlakDatabaseEntry *entries, int max_entries) {
    int i;
    int count = 0;

    if (ulak_shmem == NULL || entries == NULL || max_entries <= 0)
        return 0;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    for (i = 0; i < ULAK_MAX_DATABASES && count < max_entries; i++) {
        if (ulak_shmem->databases[i].active) {
            SpinLockAcquire(&ulak_shmem->databases[i].metrics_mutex);
            memcpy(&entries[count], &ulak_shmem->databases[i], sizeof(UlakDatabaseEntry));
            SpinLockRelease(&ulak_shmem->databases[i].metrics_mutex);
            count++;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return count;
}

/**
 * @brief Add a worker PID to a specific slot for a database.
 *
 * Called when a worker starts processing a database.
 *
 * @param dboid     Database OID.
 * @param pid       Worker process ID.
 * @param worker_id Slot index (0 to ULAK_MAX_WORKERS-1).
 * @param latch     The worker's process latch (MyLatch), set by senders at commit.
 * @return 0 on success, -1 on error.
 */
int ulak_add_worker_pid(Oid dboid, pid_t pid, int worker_id, Latch *latch) {
    int i;
    int result = -1;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return -1;

    if (worker_id < 0 || worker_id >= ULAK_MAX_WORKERS) {
        elog(WARNING, "[ulak] Invalid worker_id %d for database OID %u", worker_id, dboid);
        return -1;
    }

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            /* Check if slot is available */
            if (ulak_shmem->databases[i].worker_pids[worker_id] != 0) {
                elog(WARNING,
                     "[ulak] Worker slot %d already occupied by PID %d for database OID %u",
                     worker_id, ulak_shmem->databases[i].worker_pids[worker_id], dboid);
            } else {
                ulak_shmem->databases[i].worker_pids[worker_id] = pid;
                ulak_shmem->databases[i].worker_latches[worker_id] = latch;
                ulak_shmem->databases[i].worker_started_at[worker_id] = GetCurrentTimestamp();
                ulak_shmem->databases[i].active_workers++;
                result = 0;
                elog(LOG, "[ulak] Worker %d (PID %d) added for database OID %u, active_workers=%d",
                     worker_id, pid, dboid, ulak_shmem->databases[i].active_workers);
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return result;
}

/**
 * @brief Remove a worker PID from a database.
 *
 * Called when a worker terminates. Decrements active_workers and releases
 * the registry slot once the last worker for that database is gone, so a
 * database whose workers were reconfigured away (or whose extension was
 * dropped) does not pin a slot forever.
 *
 * @param dboid Database OID.
 * @param pid   Worker process ID to remove.
 */
void ulak_remove_worker_pid(Oid dboid, pid_t pid) {
    int i, j;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            for (j = 0; j < ULAK_MAX_WORKERS; j++) {
                if (ulak_shmem->databases[i].worker_pids[j] == pid) {
                    ulak_shmem->databases[i].worker_pids[j] = 0;
                    ulak_shmem->databases[i].worker_latches[j] = NULL;
                    if (ulak_shmem->databases[i].active_workers > 0)
                        ulak_shmem->databases[i].active_workers--;
                    elog(LOG, "[ulak] Worker PID %d removed from slot %d for database OID %u", pid,
                         j, dboid);
                    break;
                }
            }
            if (ulak_shmem->databases[i].active_workers == 0) {
                ulak_shmem->databases[i].active = false;
                if (ulak_shmem->database_count > 0)
                    ulak_shmem->database_count--;
                elog(LOG, "[ulak] Database OID %u unregistered (no active workers)", dboid);
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Set the latches of the workers named in @p mask (bit = worker_id).
 *
 * Runs from a transaction-commit callback in every sending backend, so it
 * deliberately takes no lock: the fields it reads are pointer/int sized and
 * a stale value only costs a spurious SetLatch, which is harmless — PGPROC
 * latches live in shared memory for the life of the cluster.
 *
 * @param dboid Database whose workers to wake.
 * @param mask  Worker slots to wake (bit i = worker_id i).
 */
void ulak_wake_workers(Oid dboid, uint32 mask) {
    int i, j;

    if (ulak_shmem == NULL || mask == 0)
        return;

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        UlakDatabaseEntry *entry = &ulak_shmem->databases[i];

        if (!entry->active || entry->dboid != dboid)
            continue;

        for (j = 0; j < ULAK_MAX_WORKERS && j < 32; j++) {
            Latch *latch;

            if ((mask & (1u << j)) == 0)
                continue;
            latch = entry->worker_latches[j];
            if (latch != NULL && entry->worker_pids[j] != 0)
                SetLatch(latch);
        }
        return;
    }
}

/**
 * @brief Clear aggregate worker state in shared memory.
 *
 * Called when the single-database worker terminates.
 */
void ulak_clear_worker(void) {
    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    ulak_shmem->worker_pid = 0;
    ulak_shmem->worker_started = false;
    LWLockRelease(ulak_shmem->lock);

    elog(LOG, "[ulak] Worker cleared from shared memory");
}

/**
 * @brief Set the target number of workers for a database.
 *
 * Clamps count to [1, ULAK_MAX_WORKERS] if out of range.
 *
 * @param dboid Database OID.
 * @param count Desired number of workers.
 */
void ulak_set_target_workers(Oid dboid, int count) {
    int i;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    if (count < 1 || count > ULAK_MAX_WORKERS) {
        elog(WARNING, "[ulak] Invalid target workers %d, clamping to valid range", count);
        count = (count < 1) ? 1 : ULAK_MAX_WORKERS;
    }

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            int old_target = ulak_shmem->databases[i].target_workers;
            ulak_shmem->databases[i].target_workers = count;
            if (old_target != count) {
                elog(LOG, "[ulak] Target workers for database OID %u changed from %d to %d", dboid,
                     old_target, count);
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Update aggregate worker statistics kept for shared monitoring views.
 *
 * Called to flush accumulated batch stats. Uses lock-free atomic
 * counter updates for processed/errors, LWLock for last_activity timestamp.
 *
 * @param processed Number of messages successfully processed.
 * @param errors    Number of errors encountered.
 */
void ulak_update_stats(int64 processed, int32 errors) {
    if (ulak_shmem == NULL)
        return;

    /* Lock-free counter updates — no LWLock needed */
    if (processed > 0)
        pg_atomic_fetch_add_u64(&ulak_shmem->atomic_messages_processed, (uint64)processed);
    if (errors > 0)
        pg_atomic_fetch_add_u32(&ulak_shmem->atomic_error_count, (uint32)errors);

    /* last_activity still needs LWLock (TimestampTz is not atomic) */
    if (ulak_shmem->lock != NULL) {
        LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
        ulak_shmem->last_activity = GetCurrentTimestamp();
        LWLockRelease(ulak_shmem->lock);
    }
}

/**
 * @brief Update per-worker metrics in shared memory.
 *
 * Writes processed count, error count, and optional error message
 * to the worker's slot using spinlock-protected fields.
 *
 * @param dboid     Database OID.
 * @param worker_id Worker slot index.
 * @param processed Number of messages processed in this batch.
 * @param errors    Number of errors in this batch.
 * @param error_msg Last error message, or NULL if no error.
 */
void ulak_update_worker_metrics(Oid dboid, int worker_id, int64 processed, int32 errors,
                                const char *error_msg) {
    int i;
    TimestampTz now;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL || worker_id < 0 ||
        worker_id >= ULAK_MAX_WORKERS)
        return;

    now = GetCurrentTimestamp();

    /* Preserve aggregate counters for shared status readers. */
    ulak_update_stats(processed, errors);

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        UlakDatabaseEntry *entry = &ulak_shmem->databases[i];

        if (!entry->active || entry->dboid != dboid)
            continue;

        SpinLockAcquire(&entry->metrics_mutex);
        entry->last_activity[worker_id] = now;
        if (processed > 0)
            entry->messages_processed[worker_id] += processed;
        if (errors > 0)
            entry->error_count[worker_id] += errors;
        if (error_msg != NULL && error_msg[0] != '\0') {
            entry->last_error_at[worker_id] = now;
            strlcpy(entry->last_error_msg[worker_id], error_msg,
                    sizeof(entry->last_error_msg[worker_id]));
        }
        SpinLockRelease(&entry->metrics_mutex);
        break;
    }
    LWLockRelease(ulak_shmem->lock);

    if (error_msg != NULL && error_msg[0] != '\0')
        ulak_set_last_error(error_msg);
}

/**
 * @brief Update the last_activity timestamp for a specific worker slot.
 *
 * @param dboid     Database OID.
 * @param worker_id Worker slot index.
 */
void ulak_update_worker_activity(Oid dboid, int worker_id) {
    int i;
    TimestampTz now;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL || worker_id < 0 ||
        worker_id >= ULAK_MAX_WORKERS)
        return;

    now = GetCurrentTimestamp();
    ulak_update_activity();

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        UlakDatabaseEntry *entry = &ulak_shmem->databases[i];

        if (!entry->active || entry->dboid != dboid)
            continue;

        SpinLockAcquire(&entry->metrics_mutex);
        entry->last_activity[worker_id] = now;
        SpinLockRelease(&entry->metrics_mutex);
        break;
    }
    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Set last error message in shared memory.
 *
 * Called when worker encounters an error.
 *
 * @param error_msg Error message string to store.
 */
void ulak_set_last_error(const char *error_msg) {
    if (ulak_shmem == NULL || ulak_shmem->lock == NULL || error_msg == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    strlcpy(ulak_shmem->last_error_msg, error_msg, sizeof(ulak_shmem->last_error_msg));
    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Update last activity timestamp.
 *
 * Called periodically by worker to indicate it's alive.
 */
void ulak_update_activity(void) {
    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    ulak_shmem->last_activity = GetCurrentTimestamp();
    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Register static background workers.
 *
 * Called from _PG_init() to register one or more workers at extension
 * load time. Worker count is determined by the ulak.workers GUC.
 * Each worker receives a packed argument: (total_workers << 16) | worker_id.
 */
void ulak_register_worker(void) {
    int i;
    int num_workers = ulak_workers;

    if (!process_shared_preload_libraries_in_progress)
        return;

    if (num_workers < 1)
        num_workers = 1;
    if (num_workers > 32)
        num_workers = 32;

    for (i = 0; i < num_workers; i++) {
        BackgroundWorker worker;

        memset(&worker, 0, sizeof(BackgroundWorker));
        if (num_workers > 1) {
            snprintf(worker.bgw_name, BGW_MAXLEN, "ulak worker %d/%d", i + 1, num_workers);
        } else {
            snprintf(worker.bgw_name, BGW_MAXLEN, "ulak worker");
        }
        snprintf(worker.bgw_type, BGW_MAXLEN, "ulak worker");
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_restart_time = ulak_worker_restart_delay;
        snprintf(worker.bgw_library_name, BGW_MAXLEN, "ulak");
        snprintf(worker.bgw_function_name, BGW_MAXLEN, "ulak_worker_main");
        /* Pass worker_id and total_workers packed into main_arg:
         * high 16 bits = total_workers, low 16 bits = worker_id */
        worker.bgw_main_arg = Int32GetDatum((num_workers << 16) | i);
        worker.bgw_notify_pid = 0;

        RegisterBackgroundWorker(&worker);
    }

    elog(LOG, "[ulak] Registered %d background worker(s)", num_workers);
}
