/**
 * @file queue_manager.h
 * @brief Queue insertion operations for the ulak pattern.
 *
 * Clean Architecture: Use Cases Layer.
 * Provides the QueueManager abstraction for inserting messages into
 * the ulak.queue table via SPI.
 *
 * @note Status updates, batch processing, and monitoring are handled
 *       by worker.c using inline SQL for better performance.
 */

#ifndef ULAK_QUEUE_QUEUE_MANAGER_H
#define ULAK_QUEUE_QUEUE_MANAGER_H

#include "core/entities.h"
#include "postgres.h"
#include "utils/jsonb.h"

/**
 * @brief Opaque queue manager handle.
 *
 * Encapsulates SPI connection state for queue operations.
 */
typedef struct QueueManager QueueManager;

/**
 * @brief Result of a queue operation (insert, update, etc.).
 *
 * Returned by queue_insert_message() and freed via
 * queue_operation_result_free().
 */
typedef struct {
    int32 affected_rows; /**< Number of rows affected by the operation */
    bool success;        /**< Whether the operation completed successfully */
    char *error_message; /**< Error description on failure -- palloc'd, NULL on success */
} QueueOperationResult;

/** @name Queue manager lifecycle
 * @{ */

/**
 * @brief Create a new queue manager instance.
 *
 * @return Pointer to a newly allocated QueueManager.
 */
extern QueueManager *queue_manager_create(void);

/**
 * @brief Free a queue manager and its resources.
 *
 * @param manager The queue manager to free.
 */
extern void queue_manager_free(QueueManager *manager);

/** @} */

/** @name Message queue operations
 * @{ */

/**
 * @brief Insert a message into the ulak queue.
 *
 * Executes an SPI INSERT into ulak.queue with the message
 * payload, endpoint, and metadata.
 *
 * @param manager The queue manager handle.
 * @param message The message entity to insert.
 * @return Pointer to a QueueOperationResult (caller must free).
 */
extern QueueOperationResult *queue_insert_message(QueueManager *manager, Message *message);

/** @} */

/** @name Memory management
 * @{ */

/**
 * @brief Free a QueueOperationResult and its error_message string.
 *
 * @param result The result to free.
 */
extern void queue_operation_result_free(QueueOperationResult *result);

/** @} */

#endif /* ULAK_QUEUE_QUEUE_MANAGER_H */
