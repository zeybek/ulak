/**
 * @file entities.h
 * @brief Core domain enums, constants, and validation helpers.
 *
 * Clean Architecture: Core/Domain Layer.
 * Defines ProtocolType and the message/circuit state enums that the
 * dispatcher and worker layers share, plus endpoint name validation and
 * protocol name <-> enum conversion. Persistence is done with SQL/SPI
 * directly; there is intentionally no in-memory Endpoint/Message object model.
 */

#ifndef ULAK_CORE_ENTITIES_H
#define ULAK_CORE_ENTITIES_H

#include "postgres.h"
#include "ulak.h"

/* Constants - Domain Rules */
#define MAX_ENDPOINT_NAME_LENGTH 255
#define MAX_RETRY_COUNT_FOR_EXPONENTIAL 10 /**< Cap for 2^retry_count in backoff math. */

/** @brief Lifecycle states for ulak messages. */
typedef enum {
    MESSAGE_STATUS_PENDING = 0, /**< Awaiting dispatch. */
    MESSAGE_STATUS_PROCESSING,  /**< Locked by a worker. */
    MESSAGE_STATUS_COMPLETED,   /**< Successfully dispatched. */
    MESSAGE_STATUS_FAILED,      /**< Exhausted retries, moved to DLQ. */
    MESSAGE_STATUS_EXPIRED      /**< TTL expired before dispatch. */
} MessageStatus;

/** @brief Circuit breaker states (per-endpoint). */
typedef enum {
    CIRCUIT_BREAKER_CLOSED = 0, /**< Normal operation -- dispatching allowed. */
    CIRCUIT_BREAKER_OPEN,       /**< Blocking all dispatches. */
    CIRCUIT_BREAKER_HALF_OPEN   /**< Testing recovery with a single probe. */
} CircuitBreakerState;

/** @brief Supported dispatch protocol types. */
typedef enum {
    PROTOCOL_TYPE_HTTP = 0, /**< HTTP/HTTPS webhooks (always compiled in). */
    PROTOCOL_TYPE_KAFKA,    /**< Apache Kafka (requires ENABLE_KAFKA). */
    PROTOCOL_TYPE_MQTT,     /**< MQTT (requires ENABLE_MQTT). */
    PROTOCOL_TYPE_REDIS,    /**< Redis Streams (requires ENABLE_REDIS). */
    PROTOCOL_TYPE_AMQP,     /**< RabbitMQ AMQP 0-9-1 (requires ENABLE_AMQP). */
    PROTOCOL_TYPE_NATS      /**< NATS JetStream (requires ENABLE_NATS). */
} ProtocolType;

/* Domain Validation Rules */
extern bool endpoint_validate_name(const char *name);

/* Protocol Conversion Functions */
extern bool protocol_string_to_type(const char *protocol_str, ProtocolType *out_type);
extern const char *protocol_type_to_string(ProtocolType type);

#endif /* ULAK_CORE_ENTITIES_H */
