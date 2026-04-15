/**
 * @file entities.c
 * @brief Core business entities and domain logic implementation
 *
 * Clean Architecture: Core/Domain Layer
 * Contains business entities, value objects, and core business rules
 */

#include "core/entities.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include "config/guc.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "utils/timestamp.h"

/** External functions from dispatcher module */
extern bool validate_endpoint_config(const char *protocol, Jsonb *config);

/**
 * @brief Create a new endpoint with validated name and config.
 * @param name Endpoint name (alphanumeric, underscore, dash)
 * @param protocol Protocol type enum value
 * @param config JSONB configuration for the protocol
 * @param retry_policy Retry policy to attach (may be NULL)
 * @return Newly allocated Endpoint, or NULL on validation failure
 */
Endpoint *endpoint_create(const char *name, ProtocolType protocol, Jsonb *config,
                          RetryPolicy *retry_policy) {
    Endpoint *endpoint;

    if (!endpoint_validate_name(name) || !endpoint_validate_config(protocol, config)) {
        return NULL;
    }

    endpoint = (Endpoint *)palloc(sizeof(Endpoint));

    /* Initialize endpoint */
    endpoint->id = 0; /* Will be set by database */
    strlcpy(endpoint->name, name, MAX_ENDPOINT_NAME_LENGTH);
    endpoint->protocol = protocol;
    endpoint->config = config;
    endpoint->retry_policy = retry_policy;
    endpoint->created_at = GetCurrentTimestamp();
    endpoint->updated_at = GetCurrentTimestamp();
    /* Initialize additional fields */
    endpoint->enabled = true;
    endpoint->description = NULL;
    endpoint->circuit_state = CIRCUIT_BREAKER_CLOSED;
    endpoint->circuit_failure_count = 0;
    endpoint->circuit_opened_at = 0;
    endpoint->circuit_half_open_at = 0;
    endpoint->last_success_at = 0;
    endpoint->last_failure_at = 0;

    return endpoint;
}

/**
 * @brief Create a new message for a given endpoint.
 * @param endpoint_id Target endpoint ID
 * @param payload JSONB message payload
 * @return Newly allocated Message, or NULL on validation failure
 */
Message *message_create(int64 endpoint_id, Jsonb *payload) {
    Message *message;

    if (!message_validate_payload(payload)) {
        return NULL;
    }

    message = (Message *)palloc(sizeof(Message));

    /* Initialize message */
    message->id = 0; /* Will be set by database */
    message->endpoint_id = endpoint_id;
    message->payload = payload;
    message->status = MESSAGE_STATUS_PENDING;
    message->retry_count = DEFAULT_RETRY_COUNT;
    message->next_retry_at = GetCurrentTimestamp();
    message->last_error = NULL;
    message->processing_started_at = 0;
    message->completed_at = 0;
    message->failed_at = 0;
    message->created_at = GetCurrentTimestamp();
    message->updated_at = GetCurrentTimestamp();
    /* Initialize additional fields */
    message->priority = 0;
    message->scheduled_at = 0;
    message->expires_at = 0;
    message->idempotency_key = NULL;
    message->correlation_id[0] = '\0';

    return message;
}

/**
 * @brief Create a new retry policy with the given strategy.
 * @param max_retries Maximum retry attempts (clamped to DEFAULT_MAX_RETRIES if <= 0)
 * @param strategy Backoff strategy enum
 * @return Newly allocated RetryPolicy
 */
RetryPolicy *retry_policy_create(int32 max_retries, BackoffStrategy strategy) {
    RetryPolicy *policy = (RetryPolicy *)palloc(sizeof(RetryPolicy));

    policy->max_retries = max_retries > 0 ? max_retries : DEFAULT_MAX_RETRIES;
    policy->backoff_strategy = strategy;
    policy->base_delay_seconds = DEFAULT_BASE_DELAY_SECONDS;
    policy->max_delay_seconds = DEFAULT_MAX_DELAY_SECONDS;
    policy->increment_seconds = DEFAULT_INCREMENT_SECONDS;

    return policy;
}

/**
 * @brief Free an endpoint and its owned resources.
 * @param endpoint Endpoint to free (NULL-safe)
 */
void endpoint_free(Endpoint *endpoint) {
    if (!endpoint)
        return;

    /* Note: config is typically owned by the caller/database,
     * so we don't free it here to avoid double-free */

    if (endpoint->retry_policy) {
        retry_policy_free(endpoint->retry_policy);
    }

    /* Free allocated fields */
    if (endpoint->description) {
        pfree(endpoint->description);
    }
    /* circuit_state is a scalar enum, no freeing needed */

    pfree(endpoint);
}

/**
 * @brief Free a message and its owned resources.
 * @param message Message to free (NULL-safe)
 */
void message_free(Message *message) {
    if (!message)
        return;

    /* Note: payload is typically owned by the caller/database,
     * so we don't free it here to avoid double-free */

    if (message->last_error) {
        pfree(message->last_error);
    }

    /* Free allocated fields */
    if (message->idempotency_key) {
        pfree(message->idempotency_key);
    }

    pfree(message);
}

/**
 * @brief Free a retry policy.
 * @param policy Policy to free (NULL-safe)
 */
void retry_policy_free(RetryPolicy *policy) {
    if (policy) {
        pfree(policy);
    }
}

/**
 * @brief Validate an endpoint name for allowed characters and length.
 * @param name Name string to validate
 * @return true if valid (alphanumeric, underscore, dash; non-empty; within max length)
 */
bool endpoint_validate_name(const char *name) {
    size_t i;

    if (!name || strlen(name) == 0 || strlen(name) >= MAX_ENDPOINT_NAME_LENGTH) {
        return false;
    }

    /* Check for valid characters (alphanumeric, underscore, dash) */
    for (i = 0; i < strlen(name); i++) {
        if (!isalnum((unsigned char)name[i]) && name[i] != '_' && name[i] != '-') {
            return false;
        }
    }

    return true;
}

/**
 * @brief Validate endpoint configuration via the dispatcher module.
 * @param protocol Protocol type enum
 * @param config JSONB configuration to validate
 * @return true if config is valid for the given protocol
 */
bool endpoint_validate_config(ProtocolType protocol, Jsonb *config) {
    const char *protocol_str;

    if (!config) {
        return false;
    }

    /* Use dispatcher validation functions */
    switch (protocol) {
    case PROTOCOL_TYPE_HTTP:
        protocol_str = PROTOCOL_HTTP;
        break;
    case PROTOCOL_TYPE_KAFKA:
        protocol_str = PROTOCOL_KAFKA;
        break;
    case PROTOCOL_TYPE_MQTT:
        protocol_str = PROTOCOL_MQTT;
        break;
#ifdef ENABLE_REDIS
    case PROTOCOL_TYPE_REDIS:
        protocol_str = PROTOCOL_REDIS;
        break;
#endif
#ifdef ENABLE_AMQP
    case PROTOCOL_TYPE_AMQP:
        protocol_str = PROTOCOL_AMQP;
        break;
#endif
#ifdef ENABLE_NATS
    case PROTOCOL_TYPE_NATS:
        protocol_str = "nats";
        break;
#endif
    default:
        return false;
    }

    return validate_endpoint_config(protocol_str, config);
}

/**
 * @brief Validate message payload size against GUC or compile-time limit.
 * @param payload JSONB payload to validate
 * @return true if payload is non-NULL and within size limit
 */
bool message_validate_payload(Jsonb *payload) {
    size_t payload_size;
    int max_size;

    if (!payload) {
        return false;
    }

    /* Check payload size - use GUC value if available, otherwise fallback to compile-time constant */
    payload_size = VARSIZE(payload);
    max_size = ulak_max_payload_size > 0 ? ulak_max_payload_size : MAX_MESSAGE_PAYLOAD_SIZE;
    if (payload_size > (size_t)max_size) {
        return false;
    }

    return true;
}

/**
 * @brief Validate a retry policy for consistent parameters.
 * @param policy Policy to validate
 * @return true if policy is non-NULL with valid delay/retry values
 */
bool retry_policy_validate(RetryPolicy *policy) {
    if (!policy) {
        return false;
    }

    return policy->max_retries > 0 && policy->base_delay_seconds > 0 &&
           policy->max_delay_seconds >= policy->base_delay_seconds;
}

/**
 * @brief Calculate the next retry delay based on backoff strategy.
 * @param policy Retry policy with strategy and delay bounds
 * @param retry_count Current retry attempt number
 * @return Delay in seconds, or 0 if no retry should occur
 */
int32 retry_policy_calculate_delay(RetryPolicy *policy, int32 retry_count) {
    if (!policy || !retry_policy_should_retry(policy, retry_count)) {
        return 0;
    }

    switch (policy->backoff_strategy) {
    case BACKOFF_STRATEGY_FIXED:
        return policy->base_delay_seconds;

    case BACKOFF_STRATEGY_LINEAR: {
        /* Linear: base_delay + (retry_count * increment) */
        int32 increment;
        int32 delay;

        /* SEC-006 FIX: Check for overflow BEFORE multiplication to avoid UB */
        if (retry_count < 0 || policy->increment_seconds <= 0) {
            return policy->max_delay_seconds;
        }
        /* Safe multiplication check: ensure retry_count * increment won't overflow */
        if (retry_count > INT32_MAX / policy->increment_seconds) {
            return policy->max_delay_seconds;
        }
        increment = retry_count * policy->increment_seconds;
        delay = policy->base_delay_seconds + increment;
        if (delay < policy->base_delay_seconds) {
            /* Addition overflow detected */
            return policy->max_delay_seconds;
        }
        return delay > policy->max_delay_seconds ? policy->max_delay_seconds : delay;
    }

    case BACKOFF_STRATEGY_EXPONENTIAL:
    default: {
        /* Exponential: base_delay * 2^retry_count */
        int32 multiplier;
        int32 delay;

        /* SEC-005 FIX: Protect against negative retry_count (bit shift UB) */
        if (retry_count < 0 || retry_count > 10) {
            return policy->max_delay_seconds;
        }

        multiplier = (1 << retry_count); /* 2^retry_count */
        delay = policy->base_delay_seconds * multiplier;

        /* Check for overflow (delay should be >= base_delay) */
        if (delay < policy->base_delay_seconds) {
            return policy->max_delay_seconds;
        }

        return delay > policy->max_delay_seconds ? policy->max_delay_seconds : delay;
    }
    }
}

/**
 * @brief Check whether a retry should be attempted.
 * @param policy Retry policy
 * @param retry_count Current retry attempt number
 * @return true if retry_count is below max_retries
 */
bool retry_policy_should_retry(RetryPolicy *policy, int32 retry_count) {
    return policy && retry_count < policy->max_retries;
}

/**
 * @brief Determine the next message status after a dispatch attempt.
 * @param message Current message state
 * @param policy Retry policy (may be NULL)
 * @param dispatch_success Whether the dispatch succeeded
 * @return Next MessageStatus (COMPLETED, PENDING for retry, or FAILED)
 */
MessageStatus message_calculate_next_status(Message *message, RetryPolicy *policy,
                                            bool dispatch_success) {
    if (!message) {
        return MESSAGE_STATUS_FAILED;
    }

    if (dispatch_success) {
        return MESSAGE_STATUS_COMPLETED;
    }

    /* Check if we should retry */
    if (retry_policy_should_retry(policy, message->retry_count)) {
        return MESSAGE_STATUS_PENDING;
    }

    return MESSAGE_STATUS_FAILED;
}

/**
 * @brief Convert a protocol name string to ProtocolType enum.
 * @param protocol_str Protocol name (e.g. "http", "kafka")
 * @param out_type Output enum value
 * @return true if conversion succeeded
 */
bool protocol_string_to_type(const char *protocol_str, ProtocolType *out_type) {
    if (!protocol_str || !out_type) {
        return false;
    }

    if (strcmp(protocol_str, PROTOCOL_HTTP) == 0) {
        *out_type = PROTOCOL_TYPE_HTTP;
        return true;
    }
#ifdef ENABLE_KAFKA
    if (strcmp(protocol_str, PROTOCOL_KAFKA) == 0) {
        *out_type = PROTOCOL_TYPE_KAFKA;
        return true;
    }
#endif
#ifdef ENABLE_MQTT
    if (strcmp(protocol_str, PROTOCOL_MQTT) == 0) {
        *out_type = PROTOCOL_TYPE_MQTT;
        return true;
    }
#endif
#ifdef ENABLE_REDIS
    if (strcmp(protocol_str, PROTOCOL_REDIS) == 0) {
        *out_type = PROTOCOL_TYPE_REDIS;
        return true;
    }
#endif
#ifdef ENABLE_AMQP
    if (strcmp(protocol_str, PROTOCOL_AMQP) == 0) {
        *out_type = PROTOCOL_TYPE_AMQP;
        return true;
    }
#endif
#ifdef ENABLE_NATS
    if (strcmp(protocol_str, "nats") == 0) {
        *out_type = PROTOCOL_TYPE_NATS;
        return true;
    }
#endif
    return false;
}

/**
 * @brief Convert a ProtocolType enum to its string representation.
 * @param type Protocol type enum value
 * @return Static string for the protocol name, or "unknown"
 */
const char *protocol_type_to_string(ProtocolType type) {
    switch (type) {
    case PROTOCOL_TYPE_HTTP:
        return PROTOCOL_HTTP;
    case PROTOCOL_TYPE_KAFKA:
        return PROTOCOL_KAFKA;
    case PROTOCOL_TYPE_MQTT:
        return PROTOCOL_MQTT;
#ifdef ENABLE_REDIS
    case PROTOCOL_TYPE_REDIS:
        return PROTOCOL_REDIS;
#endif
#ifdef ENABLE_AMQP
    case PROTOCOL_TYPE_AMQP:
        return PROTOCOL_AMQP;
#endif
#ifdef ENABLE_NATS
    case PROTOCOL_TYPE_NATS:
        return "nats";
#endif
    default:
        return "unknown";
    }
}
