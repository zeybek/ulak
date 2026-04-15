/**
 * @file dispatcher.c
 * @brief Protocol dispatcher factory and base implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Factory pattern for creating protocol-specific dispatchers.
 */

#include "dispatchers/dispatcher.h"
#include "dispatchers/http/http_dispatcher.h"
#include "utils/logging.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

#ifdef ENABLE_KAFKA
#include "dispatchers/kafka/kafka_dispatcher.h"
#endif

#ifdef ENABLE_MQTT
#include "dispatchers/mqtt/mqtt_dispatcher.h"
#endif

#ifdef ENABLE_REDIS
#include "dispatchers/redis/redis_dispatcher.h"
#endif

#ifdef ENABLE_AMQP
#include "dispatchers/amqp/amqp_dispatcher.h"
#endif

#ifdef ENABLE_NATS
#include "dispatchers/nats/nats_dispatcher.h"
#endif

/**
 * @brief Create a dispatcher for the given protocol type.
 * @param protocol Protocol type to create a dispatcher for.
 * @param config JSONB endpoint configuration.
 * @return Newly allocated Dispatcher, or NULL on unsupported protocol.
 */
Dispatcher *dispatcher_create(ProtocolType protocol, Jsonb *config) {
    switch (protocol) {
    case PROTOCOL_TYPE_HTTP:
        return http_dispatcher_create(config);

#ifdef ENABLE_KAFKA
    case PROTOCOL_TYPE_KAFKA:
        return kafka_dispatcher_create(config);
#endif

#ifdef ENABLE_MQTT
    case PROTOCOL_TYPE_MQTT:
        return mqtt_dispatcher_create(config);
#endif

#ifdef ENABLE_REDIS
    case PROTOCOL_TYPE_REDIS:
        return redis_dispatcher_create(config);
#endif

#ifdef ENABLE_AMQP
    case PROTOCOL_TYPE_AMQP:
        return amqp_dispatcher_create(config);
#endif

#ifdef ENABLE_NATS
    case PROTOCOL_TYPE_NATS:
        return nats_dispatcher_create(config);
#endif

    default:
        ulak_log("error", "Unsupported protocol type: %d", protocol);
        return NULL;
    }
}

/**
 * @brief Dispatch a payload through the given dispatcher.
 * @param dispatcher Dispatcher instance.
 * @param payload Message payload string.
 * @param error_msg Output error message on failure (caller must pfree).
 * @return true on success, false on failure.
 */
bool dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    if (!dispatcher || !dispatcher->ops || !dispatcher->ops->dispatch) {
        if (error_msg)
            *error_msg = pstrdup("Invalid dispatcher");
        return false;
    }

    return dispatcher->ops->dispatch(dispatcher, payload, error_msg);
}

/**
 * @brief Validate endpoint configuration for a given protocol.
 * @param protocol Protocol type to validate against.
 * @param config JSONB configuration to validate.
 * @return true if configuration is valid.
 */
bool dispatcher_validate_config(ProtocolType protocol, Jsonb *config) {
    if (!config) {
        return false;
    }

    switch (protocol) {
    case PROTOCOL_TYPE_HTTP:
        return http_dispatcher_validate_config(config);

#ifdef ENABLE_KAFKA
    case PROTOCOL_TYPE_KAFKA:
        return kafka_dispatcher_validate_config(config);
#endif

#ifdef ENABLE_MQTT
    case PROTOCOL_TYPE_MQTT:
        return mqtt_dispatcher_validate_config(config);
#endif

#ifdef ENABLE_REDIS
    case PROTOCOL_TYPE_REDIS:
        return redis_dispatcher_validate_config(config);
#endif

#ifdef ENABLE_AMQP
    case PROTOCOL_TYPE_AMQP:
        return amqp_dispatcher_validate_config(config);
#endif

#ifdef ENABLE_NATS
    case PROTOCOL_TYPE_NATS:
        return nats_dispatcher_validate_config(config);
#endif

    default:
        return false;
    }
}

/**
 * @brief Free a dispatcher and release its resources.
 * @param dispatcher Dispatcher instance to free (may be NULL).
 */
void dispatcher_free(Dispatcher *dispatcher) {
    if (!dispatcher)
        return;

    if (dispatcher->ops && dispatcher->ops->cleanup) {
        dispatcher->ops->cleanup(dispatcher);
    } else {
        /* Fallback cleanup - note: don't free config as it's owned elsewhere */
        pfree(dispatcher);
    }

    ulak_log("debug", "Dispatcher freed");
}

/**
 * @brief Validate endpoint configuration from protocol string (SQL/external callers).
 * @param protocol Protocol name string (e.g. "http", "kafka").
 * @param config JSONB configuration to validate.
 * @return true if configuration is valid.
 */
bool validate_endpoint_config(const char *protocol, Jsonb *config) {
    ProtocolType proto_type;

    if (!protocol || !config) {
        return false;
    }

    if (!protocol_string_to_type(protocol, &proto_type)) {
        return false;
    }

    return dispatcher_validate_config(proto_type, config);
}

/**
 * @brief Extended dispatch with per-message headers/metadata and response capture.
 *
 * If the dispatcher supports dispatch_ex, use it. Otherwise fall back to
 * dispatch() and populate the basic result fields.
 *
 * @param dispatcher Dispatcher instance.
 * @param payload Message payload string.
 * @param headers Per-message JSONB headers (may be NULL).
 * @param metadata Per-message JSONB metadata (may be NULL).
 * @param result Output dispatch result (may be NULL).
 * @return true on success, false on failure.
 */
bool dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                            Jsonb *metadata, DispatchResult *result) {
    char *error_msg = NULL;
    bool success;
    TimestampTz start_time;
    TimestampTz end_time;

    if (!dispatcher || !dispatcher->ops) {
        if (result) {
            result->success = false;
            result->error_msg = pstrdup("Invalid dispatcher");
        }
        return false;
    }

    /* Record start time for response_time_ms calculation */
    start_time = GetCurrentTimestamp();

    /* Use dispatch_ex if available */
    if (dispatcher->ops->dispatch_ex != NULL) {
        success = dispatcher->ops->dispatch_ex(dispatcher, payload, headers, metadata, result);
    } else {
        /* Fall back to dispatch() */
        success = dispatcher->ops->dispatch(dispatcher, payload, &error_msg);

        /* Populate result from dispatch() */
        if (result) {
            result->success = success;
            result->error_msg = error_msg; /* Transfer ownership */
        } else if (error_msg) {
            pfree(error_msg);
        }
    }

    /* Calculate response time if result is provided */
    if (result) {
        end_time = GetCurrentTimestamp();
        result->response_time_ms =
            (int64)((end_time - start_time) / 1000); /* microseconds to milliseconds */
    }

    return success;
}

/**
 * @brief Create a new DispatchResult with all fields zeroed.
 * @return Newly allocated DispatchResult.
 */
DispatchResult *dispatch_result_create(void) {
    DispatchResult *result = palloc0(sizeof(DispatchResult));
    return result;
}

/**
 * @brief Free a DispatchResult and all its allocated strings.
 * @param result DispatchResult to free (may be NULL).
 */
void dispatch_result_free(DispatchResult *result) {
    if (!result)
        return;

    if (result->error_msg)
        pfree(result->error_msg);
    if (result->http_response_body)
        pfree(result->http_response_body);
    if (result->http_content_type)
        pfree(result->http_content_type);
    if (result->redis_stream_id)
        pfree(result->redis_stream_id);
    if (result->nats_js_stream)
        pfree(result->nats_js_stream);

    pfree(result);
}

/**
 * @brief Convert DispatchResult to JSONB for storage in the response column.
 *
 * Format depends on protocol:
 * HTTP: {"protocol":"http","success":true,"http_status_code":200,...}
 * Kafka: {"protocol":"kafka","success":true,"kafka_partition":0,"kafka_offset":123,...}
 * etc.
 *
 * @param result DispatchResult to convert.
 * @param protocol Protocol type for protocol-specific fields.
 * @return JSONB representation, or NULL if result is NULL.
 */
Jsonb *dispatch_result_to_jsonb(DispatchResult *result, ProtocolType protocol) {
    StringInfoData buf;
    Datum jsonb_datum;

    if (!result)
        return NULL;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '{');

    /* Common fields */
    appendStringInfo(&buf, "\"protocol\":\"%s\"", protocol_type_to_string(protocol));
    appendStringInfo(&buf, ",\"success\":%s", result->success ? "true" : "false");
    appendStringInfo(&buf, ",\"response_time_ms\":%ld", (long)result->response_time_ms);

    if (result->error_msg) {
        /* Escape JSON string properly */
        appendStringInfo(&buf, ",\"error\":");
        escape_json(&buf, result->error_msg);
    }

    /* Protocol-specific fields */
    switch (protocol) {
    case PROTOCOL_TYPE_HTTP:
        if (result->http_status_code > 0) {
            appendStringInfo(&buf, ",\"http_status_code\":%ld", result->http_status_code);
        }
        if (result->http_response_size > 0) {
            appendStringInfo(&buf, ",\"http_response_size\":%lu",
                             (unsigned long)result->http_response_size);
        }
        if (result->http_content_type) {
            appendStringInfo(&buf, ",\"http_content_type\":");
            escape_json(&buf, result->http_content_type);
        }
        if (result->http_response_body) {
            appendStringInfo(&buf, ",\"http_response_body\":");
            escape_json(&buf, result->http_response_body);
        }
        break;

    case PROTOCOL_TYPE_KAFKA:
        appendStringInfo(&buf, ",\"kafka_partition\":%d", result->kafka_partition);
        if (result->kafka_offset >= 0) {
            appendStringInfo(&buf, ",\"kafka_offset\":%ld", (long)result->kafka_offset);
        }
        if (result->kafka_timestamp > 0) {
            appendStringInfo(&buf, ",\"kafka_timestamp\":%ld", (long)result->kafka_timestamp);
        }
        break;

    case PROTOCOL_TYPE_MQTT:
        if (result->mqtt_mid > 0) {
            appendStringInfo(&buf, ",\"mqtt_mid\":%d", result->mqtt_mid);
        }
        break;

    case PROTOCOL_TYPE_REDIS:
        if (result->redis_stream_id) {
            appendStringInfo(&buf, ",\"redis_stream_id\":");
            escape_json(&buf, result->redis_stream_id);
        }
        break;

    case PROTOCOL_TYPE_AMQP:
        appendStringInfo(&buf, ",\"amqp_delivery_tag\":%lu",
                         (unsigned long)result->amqp_delivery_tag);
        appendStringInfo(&buf, ",\"amqp_confirmed\":%s", result->amqp_confirmed ? "true" : "false");
        break;

#ifdef ENABLE_NATS
    case PROTOCOL_TYPE_NATS:
        if (result->nats_js_sequence > 0)
            appendStringInfo(&buf, ",\"nats_js_sequence\":%lu",
                             (unsigned long)result->nats_js_sequence);
        if (result->nats_js_stream) {
            appendStringInfo(&buf, ",\"nats_js_stream\":");
            escape_json(&buf, result->nats_js_stream);
        }
        appendStringInfo(&buf, ",\"nats_js_duplicate\":%s",
                         result->nats_js_duplicate ? "true" : "false");
        break;
#else
    case PROTOCOL_TYPE_NATS:
        break;
#endif
    }

    appendStringInfoChar(&buf, '}');

    /* Convert to JSONB */
    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(buf.data));

    pfree(buf.data);

    return DatumGetJsonbP(jsonb_datum);
}
