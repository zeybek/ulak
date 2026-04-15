/**
 * @file nats_internal.h
 * @brief Internal declarations for the NATS dispatcher.
 *
 * @warning This header is private to src/dispatchers/nats/ — do not
 *          include it from outside the NATS dispatcher module.
 *
 * Defines JSONB config key constants, default values, and declares
 * internal functions for error classification, config parsing,
 * and all dispatch/produce/flush operations.
 */

#ifndef ULAK_NATS_INTERNAL_H
#define ULAK_NATS_INTERNAL_H

#include <nats/nats.h>
#include "dispatchers/dispatcher.h"

/* ── JSONB config key constants ── */

#define NATS_CONFIG_KEY_URL "url"
#define NATS_CONFIG_KEY_SUBJECT "subject"
#define NATS_CONFIG_KEY_JETSTREAM "jetstream"
#define NATS_CONFIG_KEY_STREAM "stream"
#define NATS_CONFIG_KEY_TOKEN "token"
#define NATS_CONFIG_KEY_USERNAME "username"
#define NATS_CONFIG_KEY_PASSWORD "password"
#define NATS_CONFIG_KEY_NKEY_SEED "nkey_seed"
#define NATS_CONFIG_KEY_CREDENTIALS "credentials_file"
#define NATS_CONFIG_KEY_TLS "tls"
#define NATS_CONFIG_KEY_TLS_CA_CERT "tls_ca_cert"
#define NATS_CONFIG_KEY_TLS_CERT "tls_cert"
#define NATS_CONFIG_KEY_TLS_KEY "tls_key"
#define NATS_CONFIG_KEY_HEADERS "headers"
#define NATS_CONFIG_KEY_OPTIONS "options"

/* ── Defaults ── */

#define NATS_DEFAULT_URL "nats://localhost:4222" /**< Fallback when "url" key is absent. */
#define NATS_PENDING_INITIAL_CAPACITY 64         /**< Initial batch array size. */
#define NATS_ERROR_BUFFER_SIZE 256               /**< Fixed error buffer per pending message. */

/* ── Error Classification ── */

/**
 * @brief Map natsStatus to "[RETRYABLE]" or "[PERMANENT]" prefix.
 *
 * Permanent: AUTH_FAILED, NOT_PERMITTED, INVALID_ARG, MAX_PAYLOAD.
 * Everything else (TIMEOUT, CONNECTION_CLOSED, IO_ERROR, …) is retryable.
 */
extern const char *nats_classify_error(natsStatus status);

/**
 * @brief Map JetStream-specific jsErrCode to error prefix.
 *
 * Permanent: JSStreamNotFoundErr (10059), JSNotEnabledErr (10076),
 *            JSBadRequestErr (10003).
 * Retryable: JSInsufficientResourcesErr, JSClusterNoPeersErr, etc.
 */
extern const char *nats_classify_js_error(int js_err_code);

/* ── Config Parsing ── */

/**
 * @brief Apply "options" sub-object entries to natsOptions.
 *
 * Recognised keys: max_reconnect, reconnect_wait, ping_interval,
 * max_pings_out, io_buf_size.
 */
extern void nats_apply_options(natsOptions *opts, Jsonb *config);

/* ── Delivery Functions ── */

/** @brief Synchronous single-message dispatch (JetStream or Core NATS). */
extern bool nats_dispatcher_dispatch(Dispatcher *self, const char *payload, char **error_msg);

/** @brief Extended dispatch with per-message headers and result capture. */
extern bool nats_dispatcher_dispatch_ex(Dispatcher *self, const char *payload, Jsonb *headers,
                                        Jsonb *metadata, DispatchResult *result);

/** @brief Enqueue message for async batch publish (non-blocking). */
extern bool nats_dispatcher_produce(Dispatcher *self, const char *payload, int64 msg_id,
                                    char **error_msg);

/** @brief Enqueue with per-message headers and metadata. */
extern bool nats_dispatcher_produce_ex(Dispatcher *self, const char *payload, int64 msg_id,
                                       Jsonb *headers, Jsonb *metadata, char **error_msg);

/**
 * @brief Flush pending async publishes and collect results.
 *
 * JetStream: calls js_PublishAsyncComplete(), marks failed messages.
 * Core NATS: calls natsConnection_FlushTimeout().
 *
 * @param self              The NATS dispatcher.
 * @param timeout_ms        Maximum time to wait (milliseconds).
 * @param[out] failed_ids   palloc'd array of failed message IDs.
 * @param[out] failed_count number of failed messages.
 * @param[out] failed_errors palloc'd array of error strings.
 * @return number of successfully delivered messages.
 */
extern int nats_dispatcher_flush(Dispatcher *self, int timeout_ms, int64 **failed_ids,
                                 int *failed_count, char ***failed_errors);

/** @brief Always returns true — NATS supports batch via produce/flush. */
extern bool nats_dispatcher_supports_batch(Dispatcher *self);

#endif /* ULAK_NATS_INTERNAL_H */
