/**
 * @file amqp_internal.h
 * @brief Internal declarations for the AMQP dispatcher module.
 *
 * @warning This header is private to src/dispatchers/amqp/ — do not
 *          include it from outside the AMQP dispatcher module.
 */

#ifndef ULAK_AMQP_INTERNAL_H
#define ULAK_AMQP_INTERNAL_H

#if defined(__has_include)
#if __has_include(<rabbitmq-c/amqp.h>)
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/framing.h>
#else
#include <amqp.h>
#include <amqp_framing.h>
#endif
#else
#include <amqp.h>
#include <amqp_framing.h>
#endif
#include "amqp_dispatcher.h"

/* Configuration Keys */
#define AMQP_CONFIG_KEY_HOST "host"
#define AMQP_CONFIG_KEY_PORT "port"
#define AMQP_CONFIG_KEY_VHOST "vhost"
#define AMQP_CONFIG_KEY_USERNAME "username"
#define AMQP_CONFIG_KEY_PASSWORD "password"
#define AMQP_CONFIG_KEY_EXCHANGE "exchange"
#define AMQP_CONFIG_KEY_ROUTING_KEY "routing_key"
#define AMQP_CONFIG_KEY_EXCHANGE_TYPE "exchange_type"
#define AMQP_CONFIG_KEY_PERSISTENT "persistent"
#define AMQP_CONFIG_KEY_MANDATORY "mandatory"
#define AMQP_CONFIG_KEY_HEARTBEAT "heartbeat"
#define AMQP_CONFIG_KEY_FRAME_MAX "frame_max"
#define AMQP_CONFIG_KEY_TLS "tls"
#define AMQP_CONFIG_KEY_TLS_CA_CERT "tls_ca_cert"
#define AMQP_CONFIG_KEY_TLS_CERT "tls_cert"
#define AMQP_CONFIG_KEY_TLS_KEY "tls_key"
#define AMQP_CONFIG_KEY_TLS_VERIFY_PEER "tls_verify_peer"

/* Allowed configuration keys for strict validation - defined in amqp_config.c */
extern const char *AMQP_ALLOWED_CONFIG_KEYS[];

/* ── Configuration Functions (amqp_config.c) ── */

/* Documented in amqp_dispatcher.h */
bool amqp_dispatcher_validate_config(Jsonb *config);

/* ── Connection Functions (amqp_connection.c) ── */

/**
 * @brief Establish connection to AMQP broker.
 *
 * Creates socket, opens connection, authenticates, and opens channel.
 *
 * @param amqp       The AMQP dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true on successful connection, false on error.
 */
bool amqp_connect_internal(AmqpDispatcher *amqp, char **error_msg);

/**
 * @brief Close AMQP connection gracefully.
 *
 * Closes channel, connection, and destroys connection state.
 *
 * @param amqp  The AMQP dispatcher.
 */
void amqp_disconnect_internal(AmqpDispatcher *amqp);

/**
 * @brief Ensure connection is valid, reconnect if necessary.
 *
 * @param amqp       The AMQP dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true if connected (or reconnection succeeded).
 */
bool amqp_ensure_connected(AmqpDispatcher *amqp, char **error_msg);

/**
 * @brief Enable publisher confirms on the channel.
 *
 * Required for reliable delivery tracking.
 *
 * @param amqp       The AMQP dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true if confirms enabled, false on error.
 */
bool amqp_enable_confirms(AmqpDispatcher *amqp, char **error_msg);

/**
 * @brief Check if an AMQP error is permanent (auth failure, permission denied).
 *
 * Permanent errors should not be retried.
 *
 * @param reply  AMQP RPC reply to classify.
 * @return true if the error is permanent, false if retryable.
 */
bool amqp_is_permanent_error(amqp_rpc_reply_t reply);

/**
 * @brief Convert AMQP reply to human-readable error string.
 *
 * @param reply   AMQP RPC reply to describe.
 * @param buffer  Output buffer for the error string.
 * @param buflen  Size of the output buffer.
 * @return Pointer to the error description (may be buffer or static string).
 */
const char *amqp_reply_error_string(amqp_rpc_reply_t reply, char *buffer, size_t buflen);

/* ── Delivery Functions (amqp_delivery.c) ── */

/* Documented in amqp_dispatcher.h */
bool amqp_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

/**
 * @brief Extended dispatch with headers, metadata, and result capture.
 *
 * Captures delivery_tag and confirm status in DispatchResult.
 *
 * @param dispatcher  The AMQP dispatcher.
 * @param payload     Message payload string.
 * @param headers     Optional JSONB headers (mapped to AMQP headers).
 * @param metadata    Optional JSONB metadata.
 * @param result      OUT - dispatch result with delivery tag.
 * @return true on successful delivery, false on error.
 */
bool amqp_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result);

/**
 * @brief Enqueue message for batch publish without waiting for confirm (non-blocking).
 *
 * Used for high-throughput batch operations with publisher confirms.
 *
 * @param dispatcher  The AMQP dispatcher.
 * @param payload     Message payload string.
 * @param msg_id      PostgreSQL queue row ID for tracking.
 * @param error_msg   OUT - error description on failure.
 * @return true if message was enqueued, false on error.
 */
bool amqp_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                             char **error_msg);

/**
 * @brief Flush all pending messages and collect results.
 *
 * Waits for publisher confirms and returns delivery status.
 *
 * @param dispatcher          The AMQP dispatcher.
 * @param timeout_ms          Maximum time to wait for confirms (milliseconds).
 * @param[out] failed_ids     palloc'd array of failed message IDs.
 * @param[out] failed_count   Number of failed messages.
 * @param[out] failed_errors  palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
int amqp_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                          int *failed_count, char ***failed_errors);

/** @brief Always returns true — AMQP supports batch via publisher confirms. */
bool amqp_dispatcher_supports_batch(Dispatcher *dispatcher);

/**
 * @brief Process publisher confirms from broker.
 *
 * Updates pending message status based on ACK/NACK responses.
 *
 * @param amqp        The AMQP dispatcher.
 * @param timeout_ms  Maximum time to wait for confirms (milliseconds).
 * @return Number of confirms processed.
 */
int amqp_process_confirms(AmqpDispatcher *amqp, int timeout_ms);

/* ── Utility Functions ── */

/**
 * @brief Secure memory clearing for sensitive data (passwords, keys).
 *
 * Delegates to explicit_bzero to prevent compiler optimization.
 *
 * @param ptr  Pointer to memory to clear.
 * @param len  Number of bytes to clear.
 */
void amqp_secure_zero_memory(void *ptr, size_t len);

#endif /* ULAK_AMQP_INTERNAL_H */
