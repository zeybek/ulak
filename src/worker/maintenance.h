/**
 * @file maintenance.h
 * @brief Worker maintenance helpers: library init/cleanup and periodic tasks.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Each helper manages its own SPI transaction, so these can be invoked
 * independently from the worker loop.
 */

#ifndef ULAK_WORKER_MAINTENANCE_H
#define ULAK_WORKER_MAINTENANCE_H

/**
 * @brief Initialize external libraries (curl, mosquitto).
 *
 * Called at the start of worker process. Libraries are initialized in
 * the worker process, not the postmaster, to avoid threading issues.
 */
extern void ulak_worker_init_libs(void);

/**
 * @brief Cleanup external libraries on worker shutdown.
 *
 * Releases curl, mosquitto, and kafka resources.
 */
extern void ulak_worker_cleanup_libs(void);

/**
 * @brief Mark messages with expired TTL via ulak.mark_expired_messages().
 *
 * Runs in its own SPI transaction.
 */
extern void mark_expired_messages(void);

/**
 * @brief Recover messages stuck in 'processing' state.
 *
 * Runs periodically to recover messages left in 'processing' state due
 * to worker crashes or connection drops. Messages older than
 * stale_recovery_timeout are reset to 'pending' status.
 */
extern void recover_stale_processing_messages(void);

/**
 * @brief Move completed messages from queue to archive.
 *
 * Calls ulak.archive_completed_messages() to batch-move completed
 * messages older than the configured retention period. Prevents queue
 * table bloat in production.
 */
extern void archive_completed_messages(void);

/**
 * @brief Run low-frequency maintenance tasks.
 *
 * Event log cleanup, DLQ cleanup, archive partition maintenance, and
 * old archive partition removal. Intended to be called every ~60 loop
 * iterations (~5 minutes at default poll_interval).
 */
extern void run_periodic_maintenance(void);

#endif /* ULAK_WORKER_MAINTENANCE_H */
