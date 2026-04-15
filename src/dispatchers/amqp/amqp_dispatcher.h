/**
 * @file amqp_dispatcher.h
 * @brief AMQP 0-9-1 dispatcher using librabbitmq (RabbitMQ).
 *
 * Clean Architecture: Interface Adapters Layer.
 * Publishes messages to AMQP exchanges with routing keys,
 * publisher confirms, TLS, and batch delivery support.
 */

#ifndef ULAK_DISPATCHERS_AMQP_DISPATCHER_H
#define ULAK_DISPATCHERS_AMQP_DISPATCHER_H

#if defined(__has_include)
#if __has_include(<rabbitmq-c/amqp.h>)
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#else
#include <amqp.h>
#include <amqp_tcp_socket.h>
#endif
#else
#include <amqp.h>
#include <amqp_tcp_socket.h>
#endif
#include "dispatchers/dispatcher.h"
#include "postgres.h"
#include "storage/spin.h"
#include "utils/jsonb.h"

/* Initial capacity for pending messages array */
#define AMQP_PENDING_INITIAL_CAPACITY 64

/* Maximum error message length for thread-safe storage */
#define AMQP_ERROR_BUFFER_SIZE 256

/* AMQP Default Values */
#define AMQP_DEFAULT_PORT 5672
#define AMQP_DEFAULT_TLS_PORT 5671
#define AMQP_DEFAULT_VHOST "/"
#define AMQP_DEFAULT_USERNAME "guest"
#define AMQP_DEFAULT_PASSWORD "guest"
#define PGX_AMQP_DEFAULT_HEARTBEAT 60
#define AMQP_DEFAULT_FRAME_MAX 131072
#define AMQP_DEFAULT_CHANNEL 1

/** @brief Tracks a single in-flight message during batch delivery with publisher confirms. */
typedef struct {
    int64 msg_id;                       /**< PostgreSQL queue row ID. */
    uint64_t delivery_tag;              /**< AMQP delivery tag for confirms. */
    bool confirmed;                     /**< Confirm received from broker. */
    bool success;                       /**< ACK (not NACK) from broker. */
    char error[AMQP_ERROR_BUFFER_SIZE]; /**< Error text (fixed buffer, no palloc). */
} AmqpPendingMessage;

/**
 * @brief AMQP 0-9-1 protocol dispatcher state.
 *
 * Holds connection, channel, publisher confirm tracking,
 * exchange/routing configuration, TLS settings, and batch state.
 */
typedef struct {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Connection settings (parsed from JSONB) */
    /** @{ */
    char *host;          /**< AMQP broker hostname. */
    int port;            /**< AMQP broker port. */
    char *vhost;         /**< Virtual host (default "/"). */
    char *username;      /**< Authentication username (zeroed on cleanup). */
    char *password;      /**< Authentication password (zeroed on cleanup). */
    char *exchange;      /**< Target exchange name. */
    char *routing_key;   /**< Routing key for publish. */
    char *exchange_type; /**< Exchange type: direct, fanout, topic, headers. */
    bool persistent;     /**< delivery_mode = 2 (persistent messages). */
    bool mandatory;      /**< Require routing to at least one queue. */
    int heartbeat;       /**< AMQP heartbeat interval in seconds. */
    int frame_max;       /**< Maximum AMQP frame size in bytes. */
    /** @} */

    /** @name TLS configuration */
    /** @{ */
    bool tls;             /**< Enable TLS handshake. */
    char *tls_ca_cert;    /**< CA certificate path. */
    char *tls_cert;       /**< Client certificate path (mTLS). */
    char *tls_key;        /**< Client key path (zeroed on cleanup). */
    bool tls_verify_peer; /**< Verify broker certificate against CA. */
    /** @} */

    /** @name Runtime connection state */
    /** @{ */
    amqp_connection_state_t conn; /**< librabbitmq connection state (NULL if disconnected). */
    amqp_socket_t *socket;        /**< Underlying TCP/TLS socket. */
    int channel;                  /**< Active AMQP channel number. */
    bool connected;               /**< Connection is established and healthy. */
    bool confirm_mode;            /**< Publisher confirms enabled on channel. */
    /** @} */

    /** @name Batch delivery tracking */
    /** @{ */
    uint64_t next_delivery_tag;           /**< Next expected delivery tag from broker. */
    AmqpPendingMessage *pending_messages; /**< Array of in-flight messages. */
    volatile int pending_count; /**< Current number of pending messages (volatile for confirms). */
    int pending_capacity;       /**< Allocated capacity (grows via repalloc). */
    slock_t pending_lock;       /**< Spinlock protecting pending state. */
    /** @} */

    /** @name Connection health tracking */
    /** @{ */
    time_t last_successful_op; /**< Time of last successful AMQP operation. */
    /** @} */
} AmqpDispatcher;

/** @name Core dispatcher interface functions */
/** @{ */

/**
 * @brief Create an AMQP dispatcher from endpoint JSONB config.
 *
 * @param config  JSONB object containing connection and exchange settings.
 * @return Allocated Dispatcher, or NULL on configuration error.
 */
extern Dispatcher *amqp_dispatcher_create(Jsonb *config);

/**
 * @brief Dispatch a single message to an AMQP exchange.
 *
 * Publishes the message and waits for publisher confirm (if confirm mode enabled).
 *
 * @param dispatcher  The AMQP dispatcher.
 * @param payload     Message payload string.
 * @param error_msg   OUT - error description on failure (caller must pfree).
 * @return true on successful delivery, false on error.
 */
extern bool amqp_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

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
extern bool amqp_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                        Jsonb *metadata, DispatchResult *result);

/**
 * @brief Validate AMQP endpoint JSONB config.
 *
 * @param config  JSONB object to validate.
 * @return true if configuration is valid, false otherwise.
 */
extern bool amqp_dispatcher_validate_config(Jsonb *config);

/**
 * @brief Release all resources held by the AMQP dispatcher.
 *
 * Closes channel and connection, frees socket, and zeroes sensitive data.
 *
 * @param dispatcher  The AMQP dispatcher to clean up.
 */
extern void amqp_dispatcher_cleanup(Dispatcher *dispatcher);

/** @} */

/** @name Batch operation functions */
/** @{ */

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
extern bool amqp_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
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
extern int amqp_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                                 int *failed_count, char ***failed_errors);

/** @brief Always returns true — AMQP supports batch via publisher confirms. */
extern bool amqp_dispatcher_supports_batch(Dispatcher *dispatcher);

/** @} */

#endif /* ULAK_DISPATCHERS_AMQP_DISPATCHER_H */
