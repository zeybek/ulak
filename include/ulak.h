/**
 * @file ulak.h
 * @brief Main header for the ulak PostgreSQL extension.
 *
 * Provides the public API for ulak: a native transactional ulak
 * with multi-protocol asynchronous dispatch via background workers.
 *
 * @note This header must be included first in all extension source files.
 */

#ifndef ULAK_H
#define ULAK_H

/* postgres.h must be included first */
#include "postgres.h"

/* Minimum PostgreSQL version check - require PostgreSQL 14+ */
#if PG_VERSION_NUM < 140000
#error "ulak requires PostgreSQL 14 or later"
#endif

#include "commands/async.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"

#include "utils/wait_event.h"

/* Feature flags - protocol enablement */
#include "ulak_features.h"

/**
 * @brief Signal handling compatibility macro.
 *
 * PostgreSQL's pqsignal() uses sigaction() internally for portable behavior.
 * Backend code should use pqsignal() instead of raw signal() to ensure
 * consistent signal handling across platforms.
 */
#define ulak_pqsignal(signo, handler) pqsignal(signo, handler)

/** @brief Extension version string -- must match version.txt. */
#define ULAK_VERSION "0.0.1" // x-release-please-version

/*
 * GUC variable declarations are authoritative in config/guc.h.
 * Include config/guc.h directly when GUC variables are needed.
 * Do NOT duplicate extern declarations here to avoid drift.
 */

/** @name Protocol type constants
 * @{ */
#define PROTOCOL_HTTP "http"   /**< HTTP/HTTPS protocol */
#define PROTOCOL_KAFKA "kafka" /**< Apache Kafka protocol */
#define PROTOCOL_MQTT "mqtt"   /**< MQTT protocol */
#define PROTOCOL_REDIS "redis" /**< Redis Streams protocol */
#define PROTOCOL_AMQP "amqp"   /**< AMQP 0-9-1 protocol */
#define PROTOCOL_NATS "nats"   /**< NATS / JetStream protocol */
/** @} */

/** @name Message status constants
 * @{ */
#define STATUS_PENDING "pending"       /**< Awaiting dispatch */
#define STATUS_PROCESSING "processing" /**< Currently being dispatched */
#define STATUS_COMPLETED "completed"   /**< Successfully dispatched */
#define STATUS_FAILED "failed"         /**< Dispatch failed permanently */
/** @} */

/** @name Error message prefixes for retry classification
 * @{ */
#define ERROR_PREFIX_PERMANENT "[PERMANENT]" /**< Non-retryable error prefix */
#define ERROR_PREFIX_RETRYABLE "[RETRYABLE]" /**< Retryable error prefix */
#define ERROR_PREFIX_DISABLE "[DISABLE]"     /**< Auto-disable endpoint prefix (410 Gone) */
#define ERROR_PREFIX_PERMANENT_LEN 11        /**< Length of permanent prefix */
#define ERROR_PREFIX_RETRYABLE_LEN 11        /**< Length of retryable prefix */
#define ERROR_PREFIX_DISABLE_LEN 9           /**< Length of disable prefix */
/** @} */

/** @name Extension lifecycle functions
 * @{ */

/**
 * @brief Extension initialization entry point.
 *
 * Called by PostgreSQL when the extension shared library is loaded.
 * Registers shared memory, GUC parameters, and background workers.
 */
extern void _PG_init(void);

/**
 * @brief Extension finalization entry point.
 *
 * Called by PostgreSQL when the extension shared library is unloaded.
 * Cleans up any global resources allocated during _PG_init().
 */
extern void _PG_fini(void);

/** @} */

/** @name Core SQL-callable functions
 * @{ */

/**
 * @brief Send a message to an endpoint via the ulak queue.
 *
 * @param fcinfo Standard PG function call info (endpoint_name, payload).
 * @return Datum The queued message ID.
 */
extern Datum ulak_send(PG_FUNCTION_ARGS);

/**
 * @brief Create a new message endpoint.
 *
 * @param fcinfo Standard PG function call info (name, protocol, config).
 * @return Datum The created endpoint ID.
 */
extern Datum ulak_create_endpoint(PG_FUNCTION_ARGS);

/**
 * @brief Alter an existing message endpoint.
 *
 * @param fcinfo Standard PG function call info (name, new config).
 * @return Datum Void.
 */
extern Datum ulak_alter_endpoint(PG_FUNCTION_ARGS);

/**
 * @brief Drop an existing message endpoint.
 *
 * @param fcinfo Standard PG function call info (name).
 * @return Datum Void.
 */
extern Datum ulak_drop_endpoint(PG_FUNCTION_ARGS);

/** @} */

/** @name Background worker functions
 * @{ */

/**
 * @brief Main entry point for ulak background workers.
 *
 * Runs the dispatch loop: polls the queue, dispatches messages via the
 * protocol dispatcher, updates statuses, and performs maintenance.
 *
 * @param main_arg Encoded worker identity: (total_workers << 16) | worker_id.
 */
PGDLLEXPORT extern void ulak_worker_main(Datum main_arg);

/**
 * @brief Entry point for dynamically parameterized database workers.
 *
 * Uses the main_arg DSM handle to load database-specific worker parameters.
 *
 * @param main_arg DSM handle for UlakWorkerParams.
 */
PGDLLEXPORT extern void ulak_database_worker_main(Datum main_arg);

/** @} */

/** @name Utility functions
 * @{ */

/* Documented in utils/json_utils.h */
extern bool extract_jsonb_value(Jsonb *jsonb, const char *key, JsonbValue *value);

/* Documented in utils/logging.h */
extern void ulak_log(const char *level, const char *message, ...);

/* Documented in dispatchers/dispatcher.h */
extern bool validate_endpoint_config(const char *protocol, Jsonb *config);

/** @} */

#endif /* ULAK_H */
