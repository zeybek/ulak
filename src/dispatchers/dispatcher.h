/**
 * @file dispatcher.h
 * @brief Protocol dispatcher interface, factory, and result types.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Defines the polymorphic DispatcherOperations vtable, the Dispatcher
 * base structure, the DispatchResult response container, and factory
 * functions for every compiled-in protocol.
 */

#ifndef ULAK_DISPATCHERS_DISPATCHER_H
#define ULAK_DISPATCHERS_DISPATCHER_H

#include "core/entities.h"
#include "postgres.h"
#include "ulak_features.h"

/* Forward declarations */
typedef struct Dispatcher Dispatcher;
typedef struct DispatchResult DispatchResult;

/**
 * @brief Result of a dispatch operation.
 *
 * Contains protocol-specific response data for tracking and debugging.
 * Memory is managed by the caller (worker) -- the dispatcher fills values.
 */
typedef struct DispatchResult {
    /** @name Common fields
     * @{ */
    bool success;           /**< Dispatch succeeded */
    char *error_msg;        /**< Error message (if failed) -- palloc'd */
    int64 response_time_ms; /**< Response time in milliseconds */
    /** @} */

    /** @name HTTP-specific fields
     * @{ */
    long http_status_code;        /**< HTTP response status code */
    char *http_response_body;     /**< HTTP response body (truncated) -- palloc'd */
    size_t http_response_size;    /**< Actual response size in bytes */
    char *http_content_type;      /**< Response Content-Type -- palloc'd */
    int32 retry_after_seconds;    /**< Retry-After override from server, 0 = not set */
    bool should_disable_endpoint; /**< 410 Gone: signal worker to disable endpoint */
    bool is_throttle; /**< 429 Too Many Requests: snooze without retry count increment */
    /** @} */

    /** @name Kafka-specific fields
     * @{ */
    int32 kafka_partition; /**< Partition message was sent to */
    int64 kafka_offset;    /**< Offset in partition */
    int64 kafka_timestamp; /**< Message timestamp (ms since epoch) */
    /** @} */

    /** @name MQTT-specific fields
     * @{ */
    int mqtt_mid; /**< Message ID for QoS 1/2 acknowledgement */
    /** @} */

    /** @name Redis-specific fields
     * @{ */
    char *redis_stream_id; /**< XADD returned stream ID -- palloc'd */
    /** @} */

    /** @name AMQP-specific fields
     * @{ */
    uint64 amqp_delivery_tag; /**< Delivery tag for publisher confirms */
    bool amqp_confirmed;      /**< Publisher confirm received from broker */
    /** @} */

    /** @name NATS-specific fields
     * @{ */
    uint64 nats_js_sequence; /**< JetStream sequence number */
    char *nats_js_stream;    /**< JetStream stream name -- palloc'd */
    bool nats_js_duplicate;  /**< Server detected duplicate via Nats-Msg-Id */
    /** @} */
} DispatchResult;

/**
 * @brief Virtual-table (vtable) for protocol-specific dispatcher operations.
 *
 * Every protocol must implement at least dispatch(), validate_config(),
 * and cleanup(). Batch and extended operations are optional (set to NULL).
 */
typedef struct DispatcherOperations {
    /** @name Core operations (required) */
    /** @{ */
    bool (*dispatch)(Dispatcher *self, const char *payload,
                     char **error_msg);     /**< Synchronous single-message dispatch. */
    bool (*validate_config)(Jsonb *config); /**< Validate endpoint JSONB config. */
    void (*cleanup)(Dispatcher *self);      /**< Free resources. */
    /** @} */

    /** @name Batch operations (optional -- NULL if unsupported) */
    /** @{ */
    bool (*produce)(Dispatcher *self, const char *payload, int64 msg_id,
                    char **error_msg); /**< Enqueue without waiting. */
    int (*flush)(Dispatcher *self, int timeout_ms, int64 **failed_ids, int *failed_count,
                 char ***failed_errors);      /**< Flush pending, return success count. */
    bool (*supports_batch)(Dispatcher *self); /**< Returns true if batch is supported. */
    /** @} */

    /** @name Extended operations (optional -- NULL falls back to core) */
    /** @{ */
    bool (*dispatch_ex)(
        Dispatcher *self, const char *payload, Jsonb *headers, Jsonb *metadata,
        DispatchResult *result); /**< Dispatch with headers/metadata and result capture. */
    bool (*produce_ex)(Dispatcher *self, const char *payload, int64 msg_id, Jsonb *headers,
                       Jsonb *metadata, char **error_msg); /**< Produce with per-message headers. */
    /** @} */
} DispatcherOperations;

/**
 * @brief Base structure for all protocol dispatchers.
 *
 * Protocol-specific dispatchers embed this as their first field
 * (e.g. HttpDispatcher.base) and store private state in @c private_data.
 */
struct Dispatcher {
    ProtocolType protocol;     /**< Protocol enum. */
    Jsonb *config;             /**< Original endpoint JSONB config. */
    DispatcherOperations *ops; /**< Vtable with protocol callbacks. */
    void *private_data;        /**< Protocol-specific opaque data. */
};

/** @name Factory functions
 * @{ */

/**
 * @brief Create an HTTP protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (url, method, headers, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_http(Jsonb *config);

/**
 * @brief Create a dispatcher for the given protocol type.
 *
 * Routes to the appropriate protocol-specific factory function.
 *
 * @param protocol The protocol enum value.
 * @param config   JSONB endpoint configuration.
 * @return Pointer to a new Dispatcher, or NULL on invalid config/protocol.
 */
extern Dispatcher *dispatcher_create(ProtocolType protocol, Jsonb *config);

#ifdef ENABLE_KAFKA
/**
 * @brief Create a Kafka protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (brokers, topic, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_kafka(Jsonb *config);
#endif

#ifdef ENABLE_MQTT
/**
 * @brief Create an MQTT protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (broker, topic, qos, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_mqtt(Jsonb *config);
#endif

#ifdef ENABLE_REDIS
/**
 * @brief Create a Redis Streams protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (host, port, stream, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_redis(Jsonb *config);
#endif

#ifdef ENABLE_AMQP
/**
 * @brief Create an AMQP 0-9-1 protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (host, exchange, routing_key, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_amqp(Jsonb *config);
#endif

#ifdef ENABLE_NATS
/**
 * @brief Create a NATS / JetStream protocol dispatcher.
 *
 * @param config JSONB endpoint configuration (url, subject, stream, etc.).
 * @return Pointer to a new Dispatcher, or NULL on invalid config.
 */
extern Dispatcher *dispatcher_create_nats(Jsonb *config);
#endif

/** @} */

/** @name Dispatcher interface functions
 * @{ */

/**
 * @brief Dispatch a single message payload via the dispatcher's protocol.
 *
 * @param dispatcher The dispatcher instance.
 * @param payload    The message payload string.
 * @param error_msg  Output: error message on failure -- palloc'd.
 * @return true on success, false on failure.
 */
extern bool dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

/**
 * @brief Validate endpoint configuration for a protocol.
 *
 * @param protocol The protocol enum value.
 * @param config   JSONB configuration to validate.
 * @return true if the configuration is valid.
 */
extern bool dispatcher_validate_config(ProtocolType protocol, Jsonb *config);

/**
 * @brief Free a dispatcher and all its resources.
 *
 * Calls the protocol's cleanup() function and frees the base struct.
 *
 * @param dispatcher The dispatcher to free.
 */
extern void dispatcher_free(Dispatcher *dispatcher);

/**
 * @brief Extended dispatch with headers, metadata, and result capture.
 *
 * Falls back to core dispatch() if dispatch_ex is not implemented.
 *
 * @param dispatcher The dispatcher instance.
 * @param payload    The message payload string.
 * @param headers    Optional JSONB headers (may be NULL).
 * @param metadata   Optional JSONB metadata (may be NULL).
 * @param result     Output: protocol-specific dispatch result.
 * @return true on success, false on failure.
 */
extern bool dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                   Jsonb *metadata, DispatchResult *result);

/**
 * @brief Allocate and zero-initialize a new DispatchResult.
 *
 * @return Pointer to a palloc'd DispatchResult.
 */
extern DispatchResult *dispatch_result_create(void);

/**
 * @brief Free a DispatchResult and its palloc'd string fields.
 *
 * @param result The result to free.
 */
extern void dispatch_result_free(DispatchResult *result);

/**
 * @brief Convert a DispatchResult to a JSONB representation.
 *
 * Includes only the fields relevant to the given protocol.
 *
 * @param result   The dispatch result to convert.
 * @param protocol The protocol type (controls which fields are included).
 * @return A palloc'd Jsonb object.
 */
extern Jsonb *dispatch_result_to_jsonb(DispatchResult *result, ProtocolType protocol);

/** @} */

/**
 * @brief Config validation helper for SQL and external callers.
 *
 * @param protocol Protocol name string (e.g. "http", "kafka").
 * @param config   JSONB configuration object.
 * @return true if valid.
 */
extern bool validate_endpoint_config(const char *protocol, Jsonb *config);

/*
 * Protocol-specific headers are NOT included here to avoid circular includes.
 * Protocol headers (http_dispatcher.h, kafka_dispatcher.h, etc.) include
 * dispatcher.h for the Dispatcher base struct. Including them here would
 * create a circular dependency chain.
 *
 * Consumers that need protocol-specific types (e.g., HttpDispatcher) should
 * include the protocol header directly. The factory in dispatcher.c includes
 * all protocol headers it needs.
 */

#endif /* ULAK_DISPATCHERS_DISPATCHER_H */
