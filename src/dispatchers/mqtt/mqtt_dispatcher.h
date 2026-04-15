/**
 * @file mqtt_dispatcher.h
 * @brief MQTT protocol dispatcher using libmosquitto.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Publishes messages to MQTT brokers with configurable QoS,
 * topic mapping, TLS, and authentication.
 */

#ifndef ULAK_DISPATCHERS_MQTT_DISPATCHER_H
#define ULAK_DISPATCHERS_MQTT_DISPATCHER_H

#include <mosquitto.h>
#include "dispatchers/dispatcher.h"
#include "postgres.h"
#include "utils/jsonb.h"

#define MQTT_ERROR_BUFFER_SIZE 256 /**< Maximum error message length for thread-safe storage. */
#define MQTT_PENDING_INITIAL_CAPACITY 64 /**< Initial capacity for pending messages array. */

/** @brief Tracks a single in-flight message during batch produce/flush. */
typedef struct MqttPendingMessage {
    int64 msg_id;                       /**< PostgreSQL queue row ID. */
    int mid;                            /**< mosquitto message ID (from mosquitto_publish). */
    bool delivered;                     /**< PUBACK/PUBCOMP received. */
    bool success;                       /**< Delivery successful. */
    char error[MQTT_ERROR_BUFFER_SIZE]; /**< Error message (fixed buffer). */
} MqttPendingMessage;

/**
 * @brief MQTT protocol dispatcher state.
 *
 * Holds mosquitto client, connection credentials, TLS config,
 * Last Will and Testament settings, and batch delivery tracking.
 */
typedef struct MqttDispatcher {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Endpoint configuration (parsed from JSONB) */
    /** @{ */
    char *broker;    /**< MQTT broker hostname. */
    char *topic;     /**< Target topic for publishes. */
    char *client_id; /**< Optional client ID. */
    char *username;  /**< Optional username. */
    char *password;  /**< Optional password (zeroed on cleanup). */
    int qos;         /**< QoS level (0, 1, or 2). */
    int port;        /**< Broker port number. */
    bool retain;     /**< Publish retain flag. */
    Jsonb *options;  /**< Additional MQTT options. */
    /** @} */

    /** @name mosquitto client state */
    /** @{ */
    struct mosquitto *client; /**< Mosquitto client instance. */
    bool connected;           /**< Connection state for pooling. */
    time_t last_activity;     /**< Timestamp of last successful operation. */
    /** @} */

    /** @name TLS configuration */
    /** @{ */
    char *tls_ca_cert; /**< CA certificate path. */
    char *tls_cert;    /**< Client certificate (mTLS). */
    char *tls_key;     /**< Client key (zeroed on cleanup). */
    bool tls;          /**< Enable TLS. */
    bool tls_insecure; /**< Skip certificate verification. */
    /** @} */

    /** @name Last Will and Testament (LWT) */
    /** @{ */
    char *will_topic;   /**< LWT topic. */
    char *will_payload; /**< LWT payload. */
    int will_qos;       /**< LWT QoS. */
    bool will_retain;   /**< LWT retain flag. */
    /** @} */

    /** @name Extended fields */
    /** @{ */
    int last_mid;       /**< Last message ID for dispatch_ex. */
    bool clean_session; /**< Clean session flag. */
    /** @} */

    /** @name Batch delivery tracking */
    /** @{ */
    MqttPendingMessage *pending_messages; /**< Array of pending message statuses. */
    int pending_count;                    /**< Current number of pending messages. */
    int pending_capacity;                 /**< Allocated capacity of pending array. */
    /** @} */
} MqttDispatcher;

/** @name Core dispatcher functions */
/** @{ */

/**
 * @brief Create an MQTT dispatcher from endpoint JSONB config.
 * @param config JSONB endpoint configuration.
 * @return Allocated dispatcher, or NULL on failure.
 */
extern Dispatcher *mqtt_dispatcher_create(Jsonb *config);

/**
 * @brief Synchronous single-message dispatch.
 * @param dispatcher The MQTT dispatcher.
 * @param payload    Message payload string.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true on successful delivery, false on failure.
 */
extern bool mqtt_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

/**
 * @brief Extended dispatch with per-message headers and result capture.
 * @param dispatcher The MQTT dispatcher.
 * @param payload    Message payload string.
 * @param headers    Per-message JSONB headers (may be NULL).
 * @param metadata   Per-message JSONB metadata (may be NULL).
 * @param result     OUT - dispatch result with message ID.
 * @return true on successful delivery, false on failure.
 */
extern bool mqtt_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                        Jsonb *metadata, DispatchResult *result);

/**
 * @brief Validate MQTT endpoint JSONB configuration.
 * @param config JSONB endpoint configuration to validate.
 * @return true if configuration is valid.
 */
extern bool mqtt_dispatcher_validate_config(Jsonb *config);

/**
 * @brief Clean up MQTT dispatcher resources.
 * @param dispatcher The MQTT dispatcher to clean up.
 */
extern void mqtt_dispatcher_cleanup(Dispatcher *dispatcher);

/** @} */

/** @name Batch operation functions */
/** @{ */

/**
 * @brief Enqueue a message for async batch publish (non-blocking).
 * @param dispatcher The MQTT dispatcher.
 * @param payload    Message payload string.
 * @param msg_id     PostgreSQL queue row ID for tracking.
 * @param error_msg  OUT - error message on failure (caller must pfree).
 * @return true if message was enqueued successfully.
 */
extern bool mqtt_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                                    char **error_msg);

/**
 * @brief Flush all pending messages and collect delivery results.
 *
 * Waits up to timeout_ms for all in-flight messages to receive
 * PUBACK/PUBCOMP acknowledgments.
 *
 * @param dispatcher         The MQTT dispatcher.
 * @param timeout_ms         Maximum time to wait for delivery (milliseconds).
 * @param[out] failed_ids    palloc'd array of failed message IDs.
 * @param[out] failed_count  Number of failed messages.
 * @param[out] failed_errors palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
extern int mqtt_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                                 int *failed_count, char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher The MQTT dispatcher.
 * @return Always true for MQTT dispatcher.
 */
extern bool mqtt_dispatcher_supports_batch(Dispatcher *dispatcher);

/** @} */

#endif /* ULAK_DISPATCHERS_MQTT_DISPATCHER_H */
