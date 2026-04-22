/**
 * @file dispatcher_cache.h
 * @brief Per-worker dispatcher connection pool.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Caches Dispatcher instances across batch cycles so that curl_multi
 * handles (connection pool, DNS cache, TLS session cache) are preserved.
 * Each worker process has its own cache; no locking is required because
 * PostgreSQL background workers are fork-based.
 *
 * Config invalidation: hash_any() over the Jsonb binary. Stale entries
 * evicted by idle timeout. Guaranteed teardown via before_shmem_exit
 * (see dispatcher_cache_exit_callback).
 */

#ifndef ULAK_WORKER_DISPATCHER_CACHE_H
#define ULAK_WORKER_DISPATCHER_CACHE_H

#include "postgres.h"
#include "utils/jsonb.h"

#include "dispatchers/dispatcher.h"

/**
 * @brief Initialize the dispatcher cache hash table and memory context.
 *
 * Must be called once before any get_or_create_dispatcher() invocation,
 * typically at the top of ulak_worker_loop().
 */
extern void dispatcher_cache_init(void);

/**
 * @brief Destroy the dispatcher cache, freeing all cached dispatchers.
 *
 * Iterates all entries, calls dispatcher_free() on each, then destroys
 * the hash table and its memory context. Safe to call multiple times.
 */
extern void dispatcher_cache_destroy(void);

/**
 * @brief Return a cached or newly created dispatcher for an endpoint.
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
extern Dispatcher *get_or_create_dispatcher(int64 endpoint_id, ProtocolType proto_type,
                                            Jsonb *config);

/**
 * @brief before_shmem_exit callback -- guaranteed cleanup on worker exit.
 *
 * Register via before_shmem_exit(dispatcher_cache_exit_callback, 0).
 *
 * @param code Exit code (unused).
 * @param arg  Callback argument (unused).
 */
extern void dispatcher_cache_exit_callback(int code, Datum arg);

#endif /* ULAK_WORKER_DISPATCHER_CACHE_H */
