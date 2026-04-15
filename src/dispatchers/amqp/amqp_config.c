/**
 * @file amqp_config.c
 * @brief AMQP configuration validation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles validation of AMQP configuration options.
 */

#include <string.h>
#include "amqp_internal.h"
#include "config/guc.h"
#include "postgres.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/** Allowed configuration keys for strict validation */
const char *AMQP_ALLOWED_CONFIG_KEYS[] = {
    AMQP_CONFIG_KEY_HOST,
    AMQP_CONFIG_KEY_PORT,
    AMQP_CONFIG_KEY_VHOST,
    AMQP_CONFIG_KEY_USERNAME,
    AMQP_CONFIG_KEY_PASSWORD,
    AMQP_CONFIG_KEY_EXCHANGE,
    AMQP_CONFIG_KEY_ROUTING_KEY,
    AMQP_CONFIG_KEY_EXCHANGE_TYPE,
    AMQP_CONFIG_KEY_PERSISTENT,
    AMQP_CONFIG_KEY_MANDATORY,
    AMQP_CONFIG_KEY_HEARTBEAT,
    AMQP_CONFIG_KEY_FRAME_MAX,
    AMQP_CONFIG_KEY_TLS,
    AMQP_CONFIG_KEY_TLS_CA_CERT,
    AMQP_CONFIG_KEY_TLS_CERT,
    AMQP_CONFIG_KEY_TLS_KEY,
    AMQP_CONFIG_KEY_TLS_VERIFY_PEER,
    NULL /* NULL terminator */
};

/**
 * @brief Validate AMQP configuration from JSONB.
 *
 * Required fields:
 *   - host: AMQP broker hostname
 *   - exchange: Exchange name to publish to
 *   - routing_key: Routing key for messages
 *
 * Optional fields:
 *   - port: Broker port (default: 5672 or 5671 for TLS)
 *   - vhost: Virtual host (default: "/")
 *   - username: Authentication username (default: "guest")
 *   - password: Authentication password (default: "guest")
 *   - exchange_type: Exchange type - direct, fanout, topic, headers
 *   - persistent: Use persistent delivery mode (default: true)
 *   - mandatory: Require message routing to queue (default: false)
 *   - heartbeat: Connection heartbeat interval (default: GUC value)
 *   - frame_max: Maximum frame size (default: GUC value)
 *   - tls: Enable TLS/SSL (default: false)
 *   - tls_ca_cert: CA certificate file path
 *   - tls_cert: Client certificate file path (for mTLS)
 *   - tls_key: Client private key file path (for mTLS)
 *   - tls_verify_peer: Verify server certificate (default: true)
 *
 * @param config JSONB configuration to validate
 * @return true if configuration is valid, false otherwise
 */
bool amqp_dispatcher_validate_config(Jsonb *config) {
    JsonbValue host_val, exchange_val, routing_key_val;
    JsonbValue port_val, heartbeat_val, exchange_type_val, tls_val, ca_val, verify_val;
    char *unknown_key = NULL;
    int port;
    int heartbeat;
    const char *type;
    int type_len;
    bool verify_peer;

    if (!config) {
        ulak_log("error", "AMQP config is NULL");
        return false;
    }

    /* Strict validation: reject unknown configuration keys */
    if (!jsonb_validate_keys(config, AMQP_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        ulak_log("error",
                 "AMQP config contains unknown key '%s'. "
                 "Allowed keys: host, port, vhost, username, password, "
                 "exchange, routing_key, exchange_type, persistent, mandatory, "
                 "heartbeat, frame_max, tls, tls_ca_cert, tls_cert, tls_key, tls_verify_peer",
                 unknown_key ? unknown_key : "unknown");
        if (unknown_key) {
            pfree(unknown_key);
        }
        return false;
    }

    /* Validate required field: host */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_HOST, &host_val) ||
        host_val.type != jbvString || host_val.val.string.len == 0) {
        ulak_log("error", "AMQP config missing or invalid 'host' field");
        return false;
    }

    /* Validate required field: exchange (empty string "" = RabbitMQ default exchange) */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_EXCHANGE, &exchange_val) ||
        exchange_val.type != jbvString) {
        ulak_log("error", "AMQP config missing or invalid 'exchange' field");
        return false;
    }

    /* Validate required field: routing_key */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_ROUTING_KEY, &routing_key_val) ||
        routing_key_val.type != jbvString) {
        ulak_log("error", "AMQP config missing or invalid 'routing_key' field");
        return false;
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_PORT, &port_val)) {
        if (port_val.type != jbvNumeric) {
            ulak_log("error", "AMQP config 'port' must be a number");
            return false;
        }
        port =
            DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(port_val.val.numeric)));
        if (port < 1 || port > 65535) {
            ulak_log("error", "AMQP config 'port' must be between 1 and 65535");
            return false;
        }
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_HEARTBEAT, &heartbeat_val)) {
        if (heartbeat_val.type != jbvNumeric) {
            ulak_log("error", "AMQP config 'heartbeat' must be a number");
            return false;
        }
        heartbeat = DatumGetInt32(
            DirectFunctionCall1(numeric_int4, NumericGetDatum(heartbeat_val.val.numeric)));
        if (heartbeat < 0 || heartbeat > 600) {
            ulak_log("error", "AMQP config 'heartbeat' must be between 0 and 600");
            return false;
        }
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_EXCHANGE_TYPE, &exchange_type_val)) {
        if (exchange_type_val.type != jbvString) {
            ulak_log("error", "AMQP config 'exchange_type' must be a string");
            return false;
        }
        /* Check valid exchange types -- compare both content and length to avoid
         * partial matches (e.g., "dir" matching "direct" via strncmp) */
        type = exchange_type_val.val.string.val;
        type_len = exchange_type_val.val.string.len;
        if (!((type_len == 6 && strncmp(type, "direct", 6) == 0) ||
              (type_len == 6 && strncmp(type, "fanout", 6) == 0) ||
              (type_len == 5 && strncmp(type, "topic", 5) == 0) ||
              (type_len == 7 && strncmp(type, "headers", 7) == 0))) {
            ulak_log("error",
                     "AMQP config 'exchange_type' must be one of: direct, fanout, topic, headers");
            return false;
        }
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS, &tls_val)) {
        if (tls_val.type != jbvBool) {
            ulak_log("error", "AMQP config 'tls' must be a boolean");
            return false;
        }
        if (tls_val.val.boolean) {
            /* Check tls_verify_peer - if true (default), warn if no CA cert */
            verify_peer = true;
            if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_VERIFY_PEER, &verify_val)) {
                if (verify_val.type == jbvBool) {
                    verify_peer = verify_val.val.boolean;
                }
            }

            if (verify_peer && !extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_CA_CERT, &ca_val)) {
                ulak_log("warning",
                         "AMQP TLS enabled with peer verification but no 'tls_ca_cert' provided. "
                         "Consider providing CA certificate for secure connection.");
            }
        }
    }

    ulak_log("debug", "AMQP config validation successful");
    return true;
}
