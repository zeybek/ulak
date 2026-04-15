/**
 * @file entities.h
 * @brief Core business entities, enums, and domain logic.
 *
 * Clean Architecture: Core/Domain Layer.
 * Defines Endpoint, Message, RetryPolicy, QueueItem structures and
 * the associated enums (MessageStatus, CircuitBreakerState, ProtocolType,
 * BackoffStrategy). Also provides entity lifecycle, validation, and
 * retry-delay calculation functions.
 */

#ifndef ULAK_CORE_ENTITIES_H
#define ULAK_CORE_ENTITIES_H

#include "postgres.h"
#include "ulak.h"
#include "utils/jsonb.h"

/* Forward declarations */
struct Endpoint;
struct Message;
struct QueueItem;
struct RetryPolicy;

/* Constants - Domain Rules */
#define MAX_ENDPOINT_NAME_LENGTH 255
#define MAX_MESSAGE_PAYLOAD_SIZE (1024 * 1024) /* 1MB */
#define DEFAULT_RETRY_COUNT 0
#define DEFAULT_MAX_RETRIES 10

/* Retry Policy Defaults */
#define DEFAULT_BASE_DELAY_SECONDS 30
#define DEFAULT_MAX_DELAY_SECONDS 300
#define DEFAULT_INCREMENT_SECONDS 60
#define MAX_RETRY_COUNT_FOR_EXPONENTIAL 10

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

/** @brief Retry backoff strategies. */
typedef enum {
    BACKOFF_STRATEGY_EXPONENTIAL = 0, /**< Exponential backoff (base * 2^retry). */
    BACKOFF_STRATEGY_LINEAR,          /**< Linear increment (base + retry * inc). */
    BACKOFF_STRATEGY_FIXED            /**< Fixed delay between retries. */
} BackoffStrategy;

/** @brief Endpoint entity -- a named dispatch target with protocol config and circuit breaker. */
typedef struct Endpoint {
    int64 id;
    char name[MAX_ENDPOINT_NAME_LENGTH];
    ProtocolType protocol;
    Jsonb *config;
    struct RetryPolicy *retry_policy;
    TimestampTz created_at;
    TimestampTz updated_at;
    /* Endpoint control and health tracking */
    bool enabled;
    char *description;
    /* Circuit breaker decomposed from jsonb to discrete fields */
    CircuitBreakerState circuit_state;
    int32 circuit_failure_count;
    TimestampTz circuit_opened_at;
    TimestampTz circuit_half_open_at;
    TimestampTz last_success_at;
    TimestampTz last_failure_at;
} Endpoint;

/** @brief Message entity -- a queued ulak message with retry, priority, and tracing metadata. */
typedef struct Message {
    int64 id;
    int64 endpoint_id;
    Jsonb *payload;
    MessageStatus status;
    int32 retry_count;
    TimestampTz next_retry_at;
    char *last_error;
    TimestampTz processing_started_at;
    TimestampTz completed_at;
    TimestampTz failed_at;
    TimestampTz created_at;
    TimestampTz updated_at;
    /* Message prioritization, scheduling, and tracing */
    int16 priority;           /* 0-10, higher = processed first */
    TimestampTz scheduled_at; /* Delayed delivery */
    TimestampTz expires_at;   /* TTL - skip if expired */
    char *idempotency_key;    /* Deduplication key */
    char correlation_id[37];  /* UUID string for distributed tracing */
    /* Per-message protocol options (override endpoint config) */
    Jsonb *headers;  /* Per-message headers (HTTP headers, Kafka headers, etc.) */
    Jsonb *metadata; /* Per-message protocol options (key, qos, routing_key, etc.) */
    Jsonb *response; /* Dispatch result (HTTP status, Kafka offset, etc.) */
} Message;

/** @brief Retry policy value object -- controls backoff strategy and limits. */
typedef struct RetryPolicy {
    int32 max_retries;
    BackoffStrategy backoff_strategy;
    int32 base_delay_seconds;
    int32 max_delay_seconds;
    int32 increment_seconds; /* for linear backoff */
} RetryPolicy;

/** @brief Queue item -- a message paired with its target endpoint for dispatch. */
typedef struct QueueItem {
    struct Message *message;
    struct Endpoint *endpoint;
} QueueItem;

/* Entity Creation Functions */
extern Endpoint *endpoint_create(const char *name, ProtocolType protocol, Jsonb *config,
                                 RetryPolicy *retry_policy);
extern Message *message_create(int64 endpoint_id, Jsonb *payload);
extern RetryPolicy *retry_policy_create(int32 max_retries, BackoffStrategy strategy);
extern void endpoint_free(Endpoint *endpoint);
extern void message_free(Message *message);
extern void retry_policy_free(RetryPolicy *policy);

/* Domain Validation Rules */
extern bool endpoint_validate_name(const char *name);
extern bool endpoint_validate_config(ProtocolType protocol, Jsonb *config);
extern bool message_validate_payload(Jsonb *payload);
extern bool retry_policy_validate(RetryPolicy *policy);

/* Business Logic Functions */
extern int32 retry_policy_calculate_delay(RetryPolicy *policy, int32 retry_count);
extern bool retry_policy_should_retry(RetryPolicy *policy, int32 retry_count);
extern MessageStatus message_calculate_next_status(Message *message, RetryPolicy *policy,
                                                   bool dispatch_success);

/* Protocol Conversion Functions */
extern bool protocol_string_to_type(const char *protocol_str, ProtocolType *out_type);
extern const char *protocol_type_to_string(ProtocolType type);

#endif /* ULAK_CORE_ENTITIES_H */
