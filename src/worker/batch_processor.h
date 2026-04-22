/**
 * @file batch_processor.h
 * @brief Pending-message batch processor.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Encapsulates the SELECT ... FOR UPDATE SKIP LOCKED fetch, per-endpoint
 * dispatch, circuit breaker enforcement, status bookkeeping, and DLQ
 * archive. Each call is wrapped in its own SPI transaction.
 */

#ifndef ULAK_WORKER_BATCH_PROCESSOR_H
#define ULAK_WORKER_BATCH_PROCESSOR_H

#include "postgres.h"

/**
 * @brief Process a single batch of pending messages.
 *
 * Selects pending messages with FOR UPDATE SKIP LOCKED, groups them by
 * endpoint, dispatches via batch or synchronous mode, and updates
 * statuses. Uses modulo partitioning for multi-worker setups.
 *
 * @param worker_dboid  Database OID for shmem metric updates.
 * @param worker_id     This worker's ID (0 to total_workers-1).
 * @param total_workers Total number of workers for this database.
 * @return Number of messages successfully processed in this batch.
 */
extern int64 batch_processor_run(Oid worker_dboid, int worker_id, int total_workers);

/**
 * @brief Release any partially-initialized batch state after an error.
 *
 * Call from the PG_CATCH arm of the worker loop. Deletes an orphaned
 * batch MemoryContext and resets the local stats accumulator so they
 * do not leak into the next iteration.
 */
extern void batch_processor_cleanup_on_error(void);

#endif /* ULAK_WORKER_BATCH_PROCESSOR_H */
