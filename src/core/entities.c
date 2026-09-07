/**
 * @file entities.c
 * @brief Domain validation helpers and protocol conversion.
 *
 * Clean Architecture: Core/Domain Layer
 */

#include "core/entities.h"
#include <ctype.h>
#include <string.h>

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
