/**
 * @file redis_internal.h
 * @brief Internal declarations for the Redis dispatcher module.
 *
 * @warning This header is private to src/dispatchers/redis/ — do not
 *          include it from outside the Redis dispatcher module.
 */

#ifndef ULAK_REDIS_INTERNAL_H
#define ULAK_REDIS_INTERNAL_H

#include "redis_dispatcher.h"

#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>

/* Error Message Prefixes */
#define ERROR_PREFIX_RETRYABLE "[RETRYABLE]"
#define ERROR_PREFIX_PERMANENT "[PERMANENT]"

/* ── Extended Dispatch (redis_dispatcher.c) ── */

/* Documented in redis_dispatcher.h */
bool redis_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                  Jsonb *metadata, DispatchResult *result);

/* ── Batch / Pipelining Functions (redis_dispatcher.c) ── */

/**
 * @brief Enqueue message for pipelined batch publish (non-blocking).
 *
 * @param dispatcher  The Redis dispatcher.
 * @param payload     Message payload string.
 * @param msg_id      PostgreSQL queue row ID for tracking.
 * @param error_msg   OUT - error description on failure.
 * @return true if message was enqueued, false on error.
 */
bool redis_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                              char **error_msg);

/**
 * @brief Flush pending pipelined messages and collect results.
 *
 * Reads replies for all enqueued XADD commands and marks
 * each message as succeeded or failed.
 *
 * @param dispatcher    The Redis dispatcher.
 * @param timeout_ms    Maximum time to wait for replies (milliseconds).
 * @param[out] failed_ids     palloc'd array of failed message IDs.
 * @param[out] failed_count   Number of failed messages.
 * @param[out] failed_errors  palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
int redis_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                           int *failed_count, char ***failed_errors);

/** @brief Always returns true — Redis supports batch via pipelining. */
bool redis_dispatcher_supports_batch(Dispatcher *dispatcher);

/* ── Config Validation Functions (redis_config.c) ── */

extern const char *REDIS_ALLOWED_CONFIG_KEYS[];

/**
 * @brief Validate a numeric field with type and range check.
 *
 * @param config      JSONB endpoint configuration.
 * @param field_name  Key name to validate.
 * @param min_val     Minimum acceptable value (inclusive).
 * @param max_val     Maximum acceptable value (inclusive).
 * @param required    If true, missing field is an error.
 * @return true if valid (or absent and not required), false otherwise.
 */
bool redis_validate_numeric_field(Jsonb *config, const char *field_name, int64 min_val,
                                  int64 max_val, bool required);

/* ── Connection Management Functions (redis_connection.c) ── */

/**
 * @brief Connect to Redis server (without TLS initialization).
 *
 * Handles TCP connection, authentication, and database selection.
 *
 * @param redis      The Redis dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true on successful connection, false on error.
 */
bool redis_connect_internal(RedisDispatcher *redis, char **error_msg);

/**
 * @brief Disconnect from Redis server.
 *
 * Frees redisContext and SSL context if present.
 *
 * @param redis  The Redis dispatcher.
 */
void redis_disconnect_internal(RedisDispatcher *redis);

/**
 * @brief Check if a Redis error is permanent (should not be retried).
 *
 * Permanent errors include authentication failures, permission issues, etc.
 *
 * @param error_str  Error string from hiredis.
 * @return true if the error is permanent, false if retryable.
 */
bool redis_is_permanent_error(const char *error_str);

/* ── TLS Functions (redis_tls.c) ── */

/**
 * @brief Create Redis SSL context from certificate files.
 *
 * @param ca_cert      CA certificate file path.
 * @param client_cert  Client certificate file path (mTLS).
 * @param client_key   Client key file path.
 * @param error_msg    OUT - error description on failure.
 * @return redisSSLContext on success, NULL on failure.
 */
redisSSLContext *redis_tls_create_context(const char *ca_cert, const char *client_cert,
                                          const char *client_key, char **error_msg);

/**
 * @brief Initiate TLS handshake on an existing Redis connection.
 *
 * @param ctx       Active hiredis connection.
 * @param ssl_ctx   SSL context created by redis_tls_create_context().
 * @param error_msg OUT - error description on failure.
 * @return true on success, false on failure.
 */
bool redis_tls_initiate(redisContext *ctx, redisSSLContext *ssl_ctx, char **error_msg);

/**
 * @brief Clean up Redis SSL context.
 *
 * Safe to call with NULL.
 *
 * @param ssl_ctx  SSL context to free (may be NULL).
 */
void redis_tls_cleanup(redisSSLContext *ssl_ctx);

#endif /* ULAK_REDIS_INTERNAL_H */
