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
 * @return Number of messages fetched (claimed) in this batch, so the caller can
 *         tell whether the batch was full and another cycle should run immediately.
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

/**
 * @brief Return the rows claimed by an interrupted batch to 'pending'.
 *
 * The claim transaction commits before delivery starts, so an error raised
 * during delivery or while writing results leaves the rows 'processing'.
 * Call from the worker's PG_CATCH after the failed transaction has been
 * aborted and before batch_processor_cleanup_on_error(); it runs its own
 * short transaction and is a no-op when no batch is in flight.
 */
extern void batch_processor_release_claimed(void);

#endif /* ULAK_WORKER_BATCH_PROCESSOR_H */
