/**
 * @file shmem.c
 * @brief Shared memory implementation for ulak.
 *
 * Stores runtime state for the configured database workers, aggregate
 * monitoring counters, and shared rate-limit buckets.
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

/* Previous hooks to chain (non-static for _PG_fini restoration) */
shmem_startup_hook_type prev_shmem_startup_hook = NULL;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = NULL;
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

        /* Set integrity markers for cross-version safety */
        ulak_shmem->magic = ULAK_SHMEM_MAGIC;
        ulak_shmem->version = ULAK_SHMEM_VERSION;

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
        ulak_shmem->messages_processed = 0;
        ulak_shmem->error_count = 0;
        ulak_shmem->last_activity = 0;
        ulak_shmem->last_error_msg[0] = '\0';

        ulak_shmem->database_count = 0;
        ulak_shmem->launcher_started = false;
        ulak_shmem->launcher_pid = 0;

        /* Initialize per-database spinlocks for metrics */
        {
            int k;
            for (k = 0; k < ULAK_MAX_DATABASES; k++)
                SpinLockInit(&ulak_shmem->databases[k].metrics_mutex);
        }

        ulak_shmem->launcher_started_at = 0;
        ulak_shmem->total_spawns = 0;
        ulak_shmem->total_spawn_failures = 0;
        ulak_shmem->total_restarts = 0;
        ulak_shmem->last_check_cycle = 0;

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
            /* Initialize worker PID array, generation counters, and metrics */
            SpinLockInit(&ulak_shmem->databases[i].metrics_mutex);
            for (j = 0; j < ULAK_MAX_WORKERS; j++) {
                ulak_shmem->databases[i].worker_pids[j] = 0;
                ulak_shmem->databases[i].generation[j] = 0;
                ulak_shmem->databases[i].worker_started_at[j] = 0;
                ulak_shmem->databases[i].messages_processed[j] = 0;
                ulak_shmem->databases[i].error_count[j] = 0;
                ulak_shmem->databases[i].last_error_at[j] = 0;
                ulak_shmem->databases[i].last_error_msg[j][0] = '\0';
                ulak_shmem->databases[i].last_activity[j] = 0;
                ulak_shmem->databases[i].worker_latches[j] = NULL;
            }
            /* Initialize circuit breaker state */
            ulak_shmem->databases[i].consecutive_spawn_failures = 0;
            ulak_shmem->databases[i].circuit_open_until = 0;
            ulak_shmem->databases[i].total_spawn_failures = 0;
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
 * @brief Unregister a database from the shared memory registry.
 *
 * Called when DROP EXTENSION ulak is executed.
 *
 * @param dboid Database OID to unregister.
 */
void ulak_unregister_database(Oid dboid) {
    int i;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            ulak_shmem->databases[i].active = false;
            ulak_shmem->database_count--;
            elog(LOG, "[ulak] Database OID %u unregistered", dboid);
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
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
 * @brief Check if a database is registered in the shared memory registry.
 *
 * @param dboid Database OID to check.
 * @return true if the database is registered and active.
 */
bool ulak_is_database_registered(Oid dboid) {
    int i;
    bool found = false;

    if (ulak_shmem == NULL) {
        elog(DEBUG1, "[ulak] is_database_registered: shmem is NULL");
        return false;
    }

    if (ulak_shmem->lock == NULL) {
        elog(DEBUG1, "[ulak] is_database_registered: lock is NULL");
        return false;
    }

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    elog(DEBUG1, "[ulak] is_database_registered: checking for dboid=%u, database_count=%d", dboid,
         ulak_shmem->database_count);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active) {
            elog(DEBUG1, "[ulak] is_database_registered: slot %d has active db oid=%u name=%s", i,
                 ulak_shmem->databases[i].dboid, ulak_shmem->databases[i].dbname);
        }
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            found = true;
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    elog(DEBUG1, "[ulak] is_database_registered: result=%s", found ? "true" : "false");
    return found;
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
 * Called when a worker starts processing a database. Increments
 * the generation counter for ABA protection.
 *
 * @param dboid     Database OID.
 * @param pid       Worker process ID.
 * @param worker_id Slot index (0 to ULAK_MAX_WORKERS-1).
 * @return 0 on success, -1 on error.
 */
int ulak_add_worker_pid(Oid dboid, pid_t pid, int worker_id) {
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
                ulak_shmem->databases[i].generation[worker_id]++;
                ulak_shmem->databases[i].worker_started_at[worker_id] = GetCurrentTimestamp();
                ulak_shmem->databases[i].active_workers++;
                result = 0;
                elog(LOG,
                     "[ulak] Worker %d (PID %d) added for database OID %u, "
                     "active_workers=%d, generation=%u",
                     worker_id, pid, dboid, ulak_shmem->databases[i].active_workers,
                     ulak_shmem->databases[i].generation[worker_id]);
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
 * Called when a worker terminates. Decrements active_workers count.
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
                    ulak_shmem->databases[i].active_workers--;
                    elog(LOG, "[ulak] Worker PID %d removed from slot %d for database OID %u", pid,
                         j, dboid);
                    break;
                }
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
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
 * @brief Get the number of additional workers needed for a database.
 *
 * @param dboid Database OID.
 * @return Difference between target and active workers (0 if none needed).
 */
int ulak_get_workers_needed(Oid dboid) {
    int i;
    int needed = 0;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return 0;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            int target = ulak_shmem->databases[i].target_workers;
            int active = ulak_shmem->databases[i].active_workers;
            needed = (target > active) ? (target - active) : 0;
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return needed;
}

/**
 * @brief Get the next available worker slot for a database.
 *
 * @param dboid Database OID.
 * @return Slot index (0 to ULAK_MAX_WORKERS-1) or -1 if no slot available.
 */
int ulak_get_next_worker_slot(Oid dboid) {
    int i, j;
    int slot = -1;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return -1;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            int target = ulak_shmem->databases[i].target_workers;
            /* Find first empty slot within target range */
            for (j = 0; j < target && j < ULAK_MAX_WORKERS; j++) {
                if (ulak_shmem->databases[i].worker_pids[j] == 0) {
                    slot = j;
                    break;
                }
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return slot;
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
 * @brief Get the worker_id for a given PID in a database.
 *
 * @param dboid Database OID.
 * @param pid   Worker process ID to look up.
 * @return Worker slot index (0 to ULAK_MAX_WORKERS-1) or -1 if not found.
 */
int ulak_get_worker_id_for_pid(Oid dboid, pid_t pid) {
    int i, j;
    int worker_id = -1;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return -1;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            for (j = 0; j < ULAK_MAX_WORKERS; j++) {
                if (ulak_shmem->databases[i].worker_pids[j] == pid) {
                    worker_id = j;
                    break;
                }
            }
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return worker_id;
}

/**
 * @brief Get the generation counter for a specific worker slot.
 *
 * Returns the slot generation so callers can detect slot reuse (ABA protection).
 *
 * @param dboid     Database OID.
 * @param worker_id Worker slot index.
 * @return Generation counter for the slot, or 0 on error.
 */
uint32 ulak_get_worker_generation(Oid dboid, int worker_id) {
    int i;
    uint32 gen = 0;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL || worker_id < 0 ||
        worker_id >= ULAK_MAX_WORKERS)
        return 0;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            gen = ulak_shmem->databases[i].generation[worker_id];
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
    return gen;
}

/** @name Aggregate worker helper functions.
 *  These keep the single-worker shared view in sync with runtime state.
 * @{ */

/**
 * @brief Set the worker PID for a database using slot 0.
 *
 * Prefer ulak_add_worker_pid() for multi-worker paths.
 *
 * @param dboid Database OID.
 * @param pid   Worker process ID.
 */
void ulak_set_worker_pid(Oid dboid, pid_t pid) { ulak_add_worker_pid(dboid, pid, 0); }

/**
 * @brief Clear the worker PID for a database by clearing slot 0.
 *
 * Prefer ulak_remove_worker_pid() for multi-worker paths.
 *
 * @param dboid Database OID.
 */
void ulak_clear_worker_pid(Oid dboid) {
    int i;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);

    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            /* Clear slot 0 in the aggregate worker view */
            ulak_shmem->databases[i].worker_pids[0] = 0;
            if (ulak_shmem->databases[i].active_workers > 0)
                ulak_shmem->databases[i].active_workers--;
            break;
        }
    }

    LWLockRelease(ulak_shmem->lock);
}

/** @} */

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
 * @brief Check if a database needs a worker using the aggregate worker view.
 *
 * @param dboid Database OID.
 * @return true if additional workers are needed.
 */
bool ulak_database_needs_worker(Oid dboid) { return ulak_get_workers_needed(dboid) > 0; }

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

/**
 * @brief Validate shared memory integrity markers.
 *
 * Checks that magic and version fields match what this binary expects.
 * Call from worker startup before touching any other shared memory fields.
 *
 * @return true if shared memory is valid, false on mismatch.
 */
bool ulak_shmem_validate(void) {
    if (ulak_shmem == NULL)
        return false;

    if (ulak_shmem->magic != ULAK_SHMEM_MAGIC) {
        elog(WARNING,
             "[ulak] Shared memory magic mismatch: expected 0x%08X, found 0x%08X",
             ULAK_SHMEM_MAGIC, ulak_shmem->magic);
        return false;
    }

    if (ulak_shmem->version != ULAK_SHMEM_VERSION) {
        elog(WARNING,
             "[ulak] Shared memory version mismatch: expected %u, found %u. "
             "Restart PostgreSQL after upgrading the ulak extension.",
             ULAK_SHMEM_VERSION, ulak_shmem->version);
        return false;
    }

    return true;
}

/**
 * @brief Register a worker's latch pointer in shared memory.
 *
 * Called from worker startup so that send() can wake workers via SetLatch.
 */
void ulak_register_worker_latch(Oid dboid, int worker_id, Latch *latch) {
    int i;

    if (!ulak_shmem || worker_id < 0 || worker_id >= ULAK_MAX_WORKERS)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            ulak_shmem->databases[i].worker_latches[worker_id] = latch;
            break;
        }
    }
    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Clear a worker's latch pointer from shared memory.
 *
 * Must be called before worker exit to prevent stale latch references.
 */
void ulak_clear_worker_latch(Oid dboid, int worker_id) {
    int i;

    if (!ulak_shmem || worker_id < 0 || worker_id >= ULAK_MAX_WORKERS)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            ulak_shmem->databases[i].worker_latches[worker_id] = NULL;
            break;
        }
    }
    LWLockRelease(ulak_shmem->lock);
}

/**
 * @brief Wake one worker for the given database by setting its latch.
 *
 * Called from send()/publish() path to achieve near-zero dispatch latency.
 * Uses LW_SHARED to minimize contention with worker registration.
 * Only wakes the first worker with a non-NULL latch (worker 0 preferred).
 */
void ulak_wake_workers(Oid dboid) {
    int i, w;

    if (!ulak_shmem || !ulak_shmem->lock)
        return;

    LWLockAcquire(ulak_shmem->lock, LW_SHARED);
    for (i = 0; i < ULAK_MAX_DATABASES; i++) {
        if (ulak_shmem->databases[i].active && ulak_shmem->databases[i].dboid == dboid) {
            for (w = 0; w < ULAK_MAX_WORKERS; w++) {
                Latch *latch = ulak_shmem->databases[i].worker_latches[w];
                if (latch != NULL) {
                    SetLatch(latch);
                    break; /* Wake only one worker */
                }
            }
            break;
        }
    }
    LWLockRelease(ulak_shmem->lock);
}
