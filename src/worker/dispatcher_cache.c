/**
 * @file dispatcher_cache.c
 * @brief Per-worker dispatcher connection pool.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Extracted from src/worker.c. Behavior unchanged.
 */

#include "worker/dispatcher_cache.h"

#include "access/hash.h"
#include "storage/ipc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* Eviction: check every 60 seconds, evict dispatchers idle for 60 seconds */
#define DISPATCHER_EVICT_INTERVAL_MS 60000L
#define DISPATCHER_IDLE_TIMEOUT_MS 60000L

typedef struct DispatcherCacheEntry {
    int64 endpoint_id;      /* hash key (must be first field for HASH_BLOBS) */
    Dispatcher *dispatcher; /* cached dispatcher instance */
    uint32 config_hash;     /* hash_any() of Jsonb binary for invalidation */
    TimestampTz last_used;  /* timestamp for idle eviction */
} DispatcherCacheEntry;

static MemoryContext DispatcherCacheContext = NULL;
static HTAB *dispatcher_cache = NULL;
static TimestampTz last_eviction_check = 0;

static void dispatcher_cache_evict_stale(void);

void dispatcher_cache_init(void) {
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

void dispatcher_cache_destroy(void) {
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

Dispatcher *get_or_create_dispatcher(int64 endpoint_id, ProtocolType proto_type, Jsonb *config) {
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

void dispatcher_cache_exit_callback(int code, Datum arg) { dispatcher_cache_destroy(); }
