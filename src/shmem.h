/**
 * @file shmem.h
 * @brief Shared memory structures, worker registry, and rate-limit buckets.
 *
 * Manages the extension's shared memory segment which holds registered
 * databases, per-worker tracking, atomic counters, shared rate-limit token
 * buckets, and aggregate fields used by SQL monitoring functions.
 * Protected by LWLock and per-field spinlocks.
 */

#ifndef ULAK_SHMEM_H
#define ULAK_SHMEM_H

#include "postgres.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* Maximum number of databases that can have ulak installed */
#define ULAK_MAX_DATABASES 64

/* Maximum number of workers per database (matches the GUC upper bound) */
#define ULAK_MAX_WORKERS 32

/* Circuit breaker constants */
#define CIRCUIT_FAILURE_THRESHOLD 5
#define CIRCUIT_OPEN_DURATION_SECS 60

/* Rate limiting constants */
#define RL_SHMEM_MAX_ENDPOINTS 256

/* Shared memory integrity constants */
#define ULAK_SHMEM_MAGIC 0x554C414B /* "ULAK" in ASCII */
#define ULAK_SHMEM_VERSION 2        /* Bumped: added worker_latches[] to UlakDatabaseEntry */

/**
 * @brief Shared memory token bucket for per-endpoint rate limiting.
 *
 * Protected by its own spinlock for minimal contention between workers.
 */
typedef struct RateLimitShmemBucket {
    slock_t mutex;           /* Per-bucket spinlock */
    int64 endpoint_id;       /* 0 = unused slot */
    double tokens;           /* Available tokens (fractional for smooth refill) */
    double max_tokens;       /* Burst capacity */
    double refill_rate;      /* Tokens per microsecond */
    TimestampTz last_refill; /* Last refill timestamp */
    bool active;             /* Slot in use */
} RateLimitShmemBucket;

/**
 * @brief Entry for a single database in the registry.
 *
 * Tracks database OID/name, per-worker PIDs, metrics, error counters,
 * and runtime bookkeeping for worker restarts.
 */
typedef struct UlakDatabaseEntry {
    Oid dboid;                           /* Database OID */
    char dbname[NAMEDATALEN];            /* Database name */
    bool active;                         /* Is this entry in use? */
    int target_workers;                  /* Target worker count from GUC */
    int active_workers;                  /* Number of currently running workers */
    pid_t worker_pids[ULAK_MAX_WORKERS]; /* PIDs of workers (0 if slot empty) */
    uint32 generation[ULAK_MAX_WORKERS]; /* Generation counter per slot (ABA protection) */
    TimestampTz registered_at;           /* When database was registered */
    TimestampTz worker_started_at[ULAK_MAX_WORKERS]; /* When each worker started */
    Latch *worker_latches[ULAK_MAX_WORKERS];         /* Latch pointers for SetLatch wake */

    /* Worker metrics and error tracking (protected by metrics_mutex) */
    slock_t metrics_mutex; /* Spinlock for hot counter updates */
    int64 messages_processed[ULAK_MAX_WORKERS];
    int32 error_count[ULAK_MAX_WORKERS];
    TimestampTz last_error_at[ULAK_MAX_WORKERS];
    char last_error_msg[ULAK_MAX_WORKERS][256];
    TimestampTz last_activity[ULAK_MAX_WORKERS];

    /* Circuit breaker state */
    int32 consecutive_spawn_failures;
    TimestampTz circuit_open_until;
    int32 total_spawn_failures;
} UlakDatabaseEntry;

/**
 * @brief Main shared memory state for the extension.
 *
 * Contains the database registry, worker/runtime counters, and shared
 * rate-limit buckets. Protected by an LWLock and per-field spinlocks.
 */
typedef struct UlakShmemState {
    uint32 magic;   /* Must equal ULAK_SHMEM_MAGIC */
    uint32 version; /* Must equal ULAK_SHMEM_VERSION */
    LWLock *lock;
    pid_t worker_pid;
    bool worker_started;
    TimestampTz worker_started_at;
    /* Atomic counters — lock-free updates from workers */
    pg_atomic_uint64 atomic_messages_processed;
    pg_atomic_uint32 atomic_error_count;
    /* Aggregate fields used by health_check/get_worker_status reads via LWLock */
    int64 messages_processed;
    int32 error_count;
    TimestampTz last_activity;
    char last_error_msg[256];

    /* Multi-database registry */
    int database_count;
    UlakDatabaseEntry databases[ULAK_MAX_DATABASES];

    /* Worker/runtime bookkeeping */
    bool launcher_started;
    pid_t launcher_pid;
    TimestampTz launcher_started_at;
    int32 total_spawns;
    int32 total_spawn_failures;
    int32 total_restarts;
    TimestampTz last_check_cycle;

    /* Shared rate limit buckets — all workers share these */
    RateLimitShmemBucket rate_limit_buckets[RL_SHMEM_MAX_ENDPOINTS];
} UlakShmemState;

/**
 * @brief Parameters passed to dynamically spawned workers via DSM segment.
 */
typedef struct UlakWorkerParams {
    Oid dboid;
    char dbname[NAMEDATALEN];
    int worker_id;
    int total_workers;
} UlakWorkerParams;

/* Global pointer to shared memory state */
extern UlakShmemState *ulak_shmem;

/**
 * @brief Validate shared memory magic and version fields.
 *
 * Ensures the shared memory layout matches what this binary expects.
 * Call from worker startup. Returns false on mismatch.
 */
extern bool ulak_shmem_validate(void);

/* Worker latch registration for SetLatch-based wake from send() */
extern void ulak_register_worker_latch(Oid dboid, int worker_id, Latch *latch);
extern void ulak_clear_worker_latch(Oid dboid, int worker_id);
extern void ulak_wake_workers(Oid dboid);

/*
 * Shared memory initialization functions.
 * Called from _PG_init() to set up hooks.
 */
extern void ulak_shmem_init(void);
extern Size ulak_shmem_size(void);

/*
 * Worker tracking functions.
 */
extern int ulak_add_worker_pid(Oid dboid, pid_t pid, int worker_id);
extern void ulak_remove_worker_pid(Oid dboid, pid_t pid);
extern int ulak_get_workers_needed(Oid dboid);
extern int ulak_get_next_worker_slot(Oid dboid);
extern void ulak_set_target_workers(Oid dboid, int count);
extern int ulak_get_worker_id_for_pid(Oid dboid, pid_t pid);
extern uint32 ulak_get_worker_generation(Oid dboid, int worker_id);
extern void ulak_update_worker_metrics(Oid dboid, int worker_id, int64 processed, int32 errors,
                                       const char *error_msg);
extern void ulak_update_worker_activity(Oid dboid, int worker_id);

/* Aggregate stats and monitoring helper functions */
extern void ulak_update_stats(int64 processed, int32 errors);
extern void ulak_set_last_error(const char *error_msg);
extern void ulak_update_activity(void);
extern void ulak_set_worker_started(pid_t pid);
extern void ulak_clear_worker(void);

/* Database registry functions */
extern void ulak_register_database(const char *dbname, Oid dboid);
extern void ulak_unregister_database(Oid dboid);
extern bool ulak_is_database_registered(Oid dboid);
extern int ulak_get_registered_databases(UlakDatabaseEntry *entries, int max_entries);

/* Single-slot helper functions retained for aggregate worker bookkeeping */
extern void ulak_set_worker_pid(Oid dboid, pid_t pid);
extern void ulak_clear_worker_pid(Oid dboid);
extern bool ulak_database_needs_worker(Oid dboid);

/*
 * Worker registration function.
 * Registers a single static background worker at _PG_init time.
 */
extern void ulak_register_worker(void);

#endif /* ULAK_SHMEM_H */
