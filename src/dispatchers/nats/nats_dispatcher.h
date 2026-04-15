/**
 * @file nats_dispatcher.h
 * @brief NATS/JetStream dispatcher using the cnats C client.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Supports two dispatch modes: Core NATS (fire-and-forget, at-most-once)
 * and JetStream (persistent streams with publisher acknowledgments,
 * at-least-once). JetStream is the default because the ulak pattern
 * requires confirmed delivery. Batch mode uses js_PublishAsync +
 * js_PublishAsyncComplete — no spinlock needed since all calls run
 * on the PG worker thread (unlike Kafka's background callback thread).
 */

#ifndef ULAK_NATS_DISPATCHER_H
#define ULAK_NATS_DISPATCHER_H

#include <nats/nats.h>
#include "dispatchers/dispatcher.h"

/** @brief Tracks a single in-flight message during batch produce/flush. */
typedef struct NatsPendingMessage {
    int64 msg_id;         /**< PostgreSQL queue row ID. */
    bool completed;       /**< Ack or error received. */
    bool success;         /**< Delivery confirmed by server. */
    char error[256];      /**< Error text (fixed buffer, no palloc). */
    uint64_t js_sequence; /**< JetStream sequence from PubAck. */
    char js_stream[64];   /**< JetStream stream name from PubAck. */
    bool js_duplicate;    /**< Server detected Nats-Msg-Id duplicate. */
} NatsPendingMessage;

/**
 * @brief NATS protocol dispatcher state.
 *
 * Holds connection, JetStream context, authentication credentials,
 * TLS config, static headers, and batch tracking arrays.
 */
typedef struct NatsDispatcher {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Endpoint configuration (parsed from JSONB) */
    /** @{ */
    char *url;      /**< NATS URL(s), e.g. "nats://h1:4222,nats://h2:4222". */
    char *subject;  /**< Target subject for all publishes. */
    bool jetstream; /**< true = JetStream (default), false = Core NATS. */
    char *stream;   /**< Optional JetStream stream name (validation only). */
    /** @} */

    /** @name Authentication (only one method active; sensitive fields zeroed on cleanup) */
    /** @{ */
    char *token;            /**< Token auth. */
    char *username;         /**< User/pass auth — username. */
    char *password;         /**< User/pass auth — password (zeroed). */
    char *nkey_seed;        /**< NKey seed (zeroed). */
    char *credentials_file; /**< Path to JWT .creds file. */
    /** @} */

    /** @name TLS */
    /** @{ */
    bool tls_enabled;  /**< Enable TLS handshake. */
    char *tls_ca_cert; /**< CA certificate path. */
    char *tls_cert;    /**< Client certificate path (mTLS). */
    char *tls_key;     /**< Client key path (zeroed). */
    /** @} */

    Jsonb *static_headers; /**< Ref to endpoint config for header iteration. */

    /** @name Runtime connection state */
    /** @{ */
    natsConnection *conn; /**< Active NATS connection (NULL if disconnected). */
    natsOptions *opts;    /**< Connection options (reused on reconnect). */
    jsCtx *js;            /**< JetStream context (NULL for Core NATS). */
    /** @} */

    /** @name Batch tracking (no spinlock — single-threaded PG worker) */
    /** @{ */
    NatsPendingMessage *pending_messages; /**< Array of in-flight messages. */
    int pending_count;                    /**< Current number of pending messages. */
    int pending_capacity;                 /**< Allocated capacity (grows via repalloc). */
    /** @} */

    /** @name Last synchronous dispatch result (for dispatch_ex) */
    /** @{ */
    uint64_t last_js_sequence; /**< Sequence from most recent PubAck. */
    char last_js_stream[64];   /**< Stream name from most recent PubAck. */
    bool last_js_duplicate;    /**< Duplicate flag from most recent PubAck. */
    /** @} */
} NatsDispatcher;

/** @name Factory functions */
/** @{ */
extern Dispatcher *
nats_dispatcher_create(Jsonb *config); /**< Create NATS dispatcher from endpoint config. */
extern bool
nats_dispatcher_validate_config(Jsonb *config); /**< Validate NATS endpoint JSONB config. */
/** @} */

/** @name Synchronous dispatch */
/** @{ */

/**
 * @brief Dispatch a single NATS message synchronously.
 * @param self      Dispatcher instance.
 * @param payload   Message payload string.
 * @param error_msg Output error message on failure.
 * @return true on successful publish, false on failure.
 */
extern bool nats_dispatcher_dispatch(Dispatcher *self, const char *payload, char **error_msg);

/**
 * @brief Extended dispatch with per-message headers and DispatchResult capture.
 * @param self      Dispatcher instance.
 * @param payload   Message payload string.
 * @param headers   Per-message JSONB headers, or NULL.
 * @param metadata  JSONB metadata (reserved for future use).
 * @param result    Output dispatch result with timing and JetStream metadata.
 * @return true on success, false on failure.
 */
extern bool nats_dispatcher_dispatch_ex(Dispatcher *self, const char *payload, Jsonb *headers,
                                        Jsonb *metadata, DispatchResult *result);
/** @} */

/** @name Batch operations */
/** @{ */

/**
 * @brief Produce a message to NATS without waiting for acknowledgment.
 * @param self      Dispatcher instance.
 * @param payload   Message payload string.
 * @param msg_id    Queue message ID for tracking.
 * @param error_msg Output error message on failure.
 * @return true if message was published/enqueued, false on failure.
 */
extern bool nats_dispatcher_produce(Dispatcher *self, const char *payload, int64 msg_id,
                                    char **error_msg);

/**
 * @brief Extended produce with per-message headers.
 * @param self      Dispatcher instance.
 * @param payload   Message payload string.
 * @param msg_id    Queue message ID for tracking.
 * @param headers   Per-message JSONB headers, or NULL.
 * @param metadata  JSONB metadata (reserved for future use).
 * @param error_msg Output error message on failure.
 * @return true if message was published/enqueued, false on failure.
 */
extern bool nats_dispatcher_produce_ex(Dispatcher *self, const char *payload, int64 msg_id,
                                       Jsonb *headers, Jsonb *metadata, char **error_msg);

/**
 * @brief Flush all pending NATS messages and collect delivery results.
 * @param self          Dispatcher instance.
 * @param timeout_ms    Maximum time to wait for acknowledgments.
 * @param failed_ids    Output array of failed message IDs.
 * @param failed_count  Output count of failed messages.
 * @param failed_errors Output array of error strings for failed messages.
 * @return Number of successfully delivered messages.
 */
extern int nats_dispatcher_flush(Dispatcher *self, int timeout_ms, int64 **failed_ids,
                                 int *failed_count, char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param self  Dispatcher instance (unused).
 * @return Always true for NATS.
 */
extern bool nats_dispatcher_supports_batch(Dispatcher *self);
/** @} */

#endif /* ULAK_NATS_DISPATCHER_H */
