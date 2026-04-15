/**
 * @file queue_manager.c
 * @brief Queue management operations implementation
 *
 * Clean Architecture: Use Cases Layer
 * Manages queue operations for the ulak pattern.
 *
 * NOTE: Status updates, batch processing, and monitoring are handled
 * directly by worker.c using inline SQL for better performance within
 * a single SPI session. This module provides only the core queue
 * insertion functionality used by the public API.
 */

#include "queue/queue_manager.h"
#include <executor/spi.h>
#include <string.h>
#include <utils/timestamp.h>
#include "core/entities.h"
#include "utils/logging.h"

/** Queue Manager Structure */
struct QueueManager {
    /** No internal state needed for now - uses SPI directly */
    bool initialized;
};

/**
 * @brief Create a new queue manager instance.
 * @return Newly allocated QueueManager
 */
QueueManager *queue_manager_create(void) {
    QueueManager *manager = (QueueManager *)palloc(sizeof(QueueManager));
    /* palloc never returns NULL — it ereport(ERROR)s on OOM */
    manager->initialized = true;
    return manager;
}

/**
 * @brief Free a queue manager instance.
 * @param manager Queue manager to free (NULL-safe)
 */
void queue_manager_free(QueueManager *manager) {
    if (manager) {
        pfree(manager);
    }
}

/**
 * @brief Free a queue operation result and its error message.
 * @param result Result to free (NULL-safe)
 */
void queue_operation_result_free(QueueOperationResult *result) {
    if (!result)
        return;

    if (result->error_message) {
        pfree(result->error_message);
    }

    pfree(result);
}

/**
 * @brief Fully parameterized INSERT query using SPI_execute_with_args.
 * @private
 *
 * No string interpolation of user-derived values.
 *
 * Parameter mapping:
 *   $1 = endpoint_id   (INT8OID)
 *   $2 = payload        (JSONBOID)
 *   $3 = retry_count    (INT4OID)
 *   $4 = next_retry_at  (TIMESTAMPTZOID) -- scheduled_at if set, else NOW()
 *   $5 = priority       (INT2OID)
 *   $6 = scheduled_at   (TIMESTAMPTZOID) -- nullable
 *   $7 = idempotency_key (TEXTOID)       -- nullable
 *   $8 = correlation_id  (TEXTOID, cast to uuid in SQL) -- nullable
 *   $9 = expires_at      (TIMESTAMPTZOID) -- nullable
 */
static const char *INSERT_QUERY =
    "INSERT INTO ulak.queue "
    "(endpoint_id, payload, status, retry_count, next_retry_at, "
    "priority, scheduled_at, idempotency_key, correlation_id, expires_at) "
    "VALUES ($1, $2, 'pending', $3, COALESCE($4, NOW()), $5, $6, $7, $8::uuid, $9)";

#define INSERT_NPARAMS 9

/**
 * @brief Insert a message into the queue using parameterized SPI query.
 * @param manager Queue manager instance
 * @param message Message to insert
 * @return QueueOperationResult with success/failure and affected rows
 */
QueueOperationResult *queue_insert_message(QueueManager *manager, Message *message) {
    QueueOperationResult *result;
    int spi_ret;
    int ret;
    Oid argtypes[INSERT_NPARAMS];
    Datum values[INSERT_NPARAMS];
    char nulls[INSERT_NPARAMS];

    /* palloc never returns NULL — it ereport(ERROR)s on OOM */
    result = (QueueOperationResult *)palloc(sizeof(QueueOperationResult));

    if (!manager) {
        result->success = false;
        result->affected_rows = 0;
        result->error_message = pstrdup("Invalid queue manager (NULL)");
        return result;
    }
    if (!message) {
        result->success = false;
        result->affected_rows = 0;
        result->error_message = pstrdup("Invalid message (NULL)");
        return result;
    }

    /* Initialize all nulls to space (non-null) */
    memset(nulls, ' ', sizeof(nulls));

    /* $1: endpoint_id (INT8OID) */
    argtypes[0] = INT8OID;
    values[0] = Int64GetDatum(message->endpoint_id);

    /* $2: payload (JSONBOID) */
    argtypes[1] = JSONBOID;
    values[1] = JsonbPGetDatum(message->payload);

    /* $3: retry_count (INT4OID) */
    argtypes[2] = INT4OID;
    values[2] = Int32GetDatum(message->retry_count);

    /* $4: next_retry_at (TIMESTAMPTZOID) — use scheduled_at if set, else NULL (COALESCE to NOW()) */
    argtypes[3] = TIMESTAMPTZOID;
    if (message->scheduled_at != 0) {
        values[3] = TimestampTzGetDatum(message->scheduled_at);
    } else {
        values[3] = (Datum)0;
        nulls[3] = 'n';
    }

    /* $5: priority (INT2OID) */
    argtypes[4] = INT2OID;
    values[4] = Int16GetDatum(message->priority);

    /* $6: scheduled_at (TIMESTAMPTZOID) — nullable */
    argtypes[5] = TIMESTAMPTZOID;
    if (message->scheduled_at != 0) {
        values[5] = TimestampTzGetDatum(message->scheduled_at);
    } else {
        values[5] = (Datum)0;
        nulls[5] = 'n';
    }

    /* $7: idempotency_key (TEXTOID) — nullable */
    argtypes[6] = TEXTOID;
    if (message->idempotency_key != NULL && message->idempotency_key[0] != '\0') {
        values[6] = CStringGetTextDatum(message->idempotency_key);
    } else {
        values[6] = (Datum)0;
        nulls[6] = 'n';
    }

    /* $8: correlation_id (TEXTOID, cast to uuid in SQL) — nullable */
    argtypes[7] = TEXTOID;
    if (message->correlation_id[0] != '\0') {
        values[7] = CStringGetTextDatum(message->correlation_id);
    } else {
        values[7] = (Datum)0;
        nulls[7] = 'n';
    }

    /* $9: expires_at (TIMESTAMPTZOID) — nullable */
    argtypes[8] = TIMESTAMPTZOID;
    if (message->expires_at != 0) {
        values[8] = TimestampTzGetDatum(message->expires_at);
    } else {
        values[8] = (Datum)0;
        nulls[8] = 'n';
    }

    /* Execute parameterized query */
    spi_ret = SPI_connect();
    if (spi_ret != SPI_OK_CONNECT) {
        result->success = false;
        result->affected_rows = 0;
        result->error_message = pstrdup("SPI_connect failed");
        ulak_log("error", "SPI_connect failed: %s", SPI_result_code_string(spi_ret));
        return result;
    }

    ret = SPI_execute_with_args(INSERT_QUERY, INSERT_NPARAMS, argtypes, values, nulls, false, 0);

    if (ret == SPI_OK_INSERT) {
        result->success = true;
        result->affected_rows = SPI_processed;
        result->error_message = NULL;
        ulak_log("debug", "Inserted message into queue, endpoint_id: %lld",
                 (long long)message->endpoint_id);
    } else {
        result->success = false;
        result->affected_rows = 0;
        result->error_message = pstrdup("Failed to insert message into queue");
        ulak_log("error", "Failed to insert message into queue: SPI error %d", ret);
    }

    SPI_finish();
    return result;
}
