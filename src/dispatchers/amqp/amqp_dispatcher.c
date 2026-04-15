/**
 * @file amqp_dispatcher.c
 * @brief AMQP protocol dispatcher implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles RabbitMQ/AMQP message dispatching using librabbitmq.
 *
 * This file contains:
 * - Dispatcher operations struct
 * - create/cleanup lifecycle functions
 *
 * Other functionality is split into:
 * - amqp_config.c: Configuration validation
 * - amqp_connection.c: Connection lifecycle management
 * - amqp_delivery.c: dispatch, produce, flush operations
 */

#include <string.h>
#include <strings.h>

#include "amqp_internal.h"
#include "config/guc.h"
#include "core/entities.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "utils/numeric.h"

/** AMQP Dispatcher Operations */
static DispatcherOperations amqp_dispatcher_ops = {.dispatch = amqp_dispatcher_dispatch,
                                                   .validate_config =
                                                       amqp_dispatcher_validate_config,
                                                   .cleanup = amqp_dispatcher_cleanup,
                                                   /* Batch operations */
                                                   .produce = amqp_dispatcher_produce,
                                                   .flush = amqp_dispatcher_flush,
                                                   .supports_batch = amqp_dispatcher_supports_batch,
                                                   /* Extended dispatch with response capture */
                                                   .dispatch_ex = amqp_dispatcher_dispatch_ex,
                                                   .produce_ex = NULL};

/**
 * @brief Create a new AMQP dispatcher instance.
 * @param config JSONB configuration with host, exchange, routing_key, and optional settings
 * @return Dispatcher pointer on success, NULL on failure
 */
Dispatcher *amqp_dispatcher_create(Jsonb *config) {
    AmqpDispatcher *amqp;
    int batch_capacity;
    JsonbValue val;

    if (!amqp_dispatcher_validate_config(config)) {
        ulak_log("warning", "Invalid AMQP dispatcher configuration");
        return NULL;
    }

    amqp = (AmqpDispatcher *)palloc0(sizeof(AmqpDispatcher));

    /* Initialize all fields to safe defaults */
    amqp->host = NULL;
    amqp->port = AMQP_DEFAULT_PORT;
    amqp->vhost = NULL;
    amqp->username = NULL;
    amqp->password = NULL;
    amqp->exchange = NULL;
    amqp->routing_key = NULL;
    amqp->exchange_type = NULL;
    amqp->persistent = true;
    amqp->mandatory = false;
    amqp->heartbeat = ulak_amqp_heartbeat > 0 ? ulak_amqp_heartbeat : PGX_AMQP_DEFAULT_HEARTBEAT;
    amqp->frame_max = ulak_amqp_frame_max > 0 ? ulak_amqp_frame_max : AMQP_DEFAULT_FRAME_MAX;

    /* TLS defaults */
    amqp->tls = false;
    amqp->tls_ca_cert = NULL;
    amqp->tls_cert = NULL;
    amqp->tls_key = NULL;
    amqp->tls_verify_peer = ulak_amqp_ssl_verify_peer;

    /* Connection state */
    amqp->conn = NULL;
    amqp->socket = NULL;
    amqp->channel = 0;
    amqp->connected = false;
    amqp->confirm_mode = false;
    amqp->last_successful_op = 0;

    /* Initialize batch tracking - use GUC value for initial capacity */
    batch_capacity =
        ulak_amqp_batch_capacity > 0 ? ulak_amqp_batch_capacity : AMQP_PENDING_INITIAL_CAPACITY;
    amqp->pending_messages = palloc(sizeof(AmqpPendingMessage) * batch_capacity);
    amqp->pending_count = 0;
    amqp->pending_capacity = batch_capacity;
    amqp->next_delivery_tag = 1;

    /* Initialize spinlock for thread safety */
    SpinLockInit(&amqp->pending_lock);

    /* Initialize base dispatcher */
    amqp->base.protocol = PROTOCOL_TYPE_AMQP;
    amqp->base.config = config;
    amqp->base.ops = &amqp_dispatcher_ops;
    amqp->base.private_data = amqp;

    /* Parse configuration - use stack-allocated JsonbValues (no memory leak) */

    /* Get host (required) */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_HOST, &val) || val.type != jbvString) {
        ulak_log("warning", "AMQP config missing or invalid 'host'");
        goto error;
    }
    amqp->host = pnstrdup(val.val.string.val, val.val.string.len);

    /* Get exchange (required) */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_EXCHANGE, &val) || val.type != jbvString) {
        ulak_log("warning", "AMQP config missing or invalid 'exchange'");
        goto error;
    }
    amqp->exchange = pnstrdup(val.val.string.val, val.val.string.len);

    /* Get routing_key (required) */
    if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_ROUTING_KEY, &val) || val.type != jbvString) {
        ulak_log("warning", "AMQP config missing or invalid 'routing_key'");
        goto error;
    }
    amqp->routing_key = pnstrdup(val.val.string.val, val.val.string.len);

    /* Get port (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_PORT, &val) && val.type == jbvNumeric) {
        amqp->port =
            DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val.val.numeric)));
    }

    /* Get vhost (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_VHOST, &val) && val.type == jbvString) {
        amqp->vhost = pnstrdup(val.val.string.val, val.val.string.len);
    }

    /* Get username (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_USERNAME, &val) && val.type == jbvString) {
        amqp->username = pnstrdup(val.val.string.val, val.val.string.len);
    }

    /* Get password (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_PASSWORD, &val) && val.type == jbvString) {
        amqp->password = pnstrdup(val.val.string.val, val.val.string.len);
    }

    /* Get exchange_type (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_EXCHANGE_TYPE, &val) && val.type == jbvString) {
        amqp->exchange_type = pnstrdup(val.val.string.val, val.val.string.len);
    }

    /* Get persistent (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_PERSISTENT, &val) && val.type == jbvBool) {
        amqp->persistent = val.val.boolean;
    }

    /* Get mandatory (optional) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_MANDATORY, &val) && val.type == jbvBool) {
        amqp->mandatory = val.val.boolean;
    }

    /* Get heartbeat (optional - override GUC default) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_HEARTBEAT, &val) && val.type == jbvNumeric) {
        amqp->heartbeat =
            DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val.val.numeric)));
    }

    /* Get frame_max (optional - override GUC default) */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_FRAME_MAX, &val) && val.type == jbvNumeric) {
        amqp->frame_max =
            DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val.val.numeric)));
    }

    /* TLS configuration */
    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS, &val) && val.type == jbvBool) {
        amqp->tls = val.val.boolean;
        /* Update default port for TLS if not explicitly set */
        if (amqp->tls && amqp->port == AMQP_DEFAULT_PORT) {
            JsonbValue port_check;
            if (!extract_jsonb_value(config, AMQP_CONFIG_KEY_PORT, &port_check)) {
                amqp->port = AMQP_DEFAULT_TLS_PORT;
            }
        }
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_CA_CERT, &val) && val.type == jbvString) {
        amqp->tls_ca_cert = pnstrdup(val.val.string.val, val.val.string.len);
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_CERT, &val) && val.type == jbvString) {
        amqp->tls_cert = pnstrdup(val.val.string.val, val.val.string.len);
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_KEY, &val) && val.type == jbvString) {
        amqp->tls_key = pnstrdup(val.val.string.val, val.val.string.len);
    }

    if (extract_jsonb_value(config, AMQP_CONFIG_KEY_TLS_VERIFY_PEER, &val) && val.type == jbvBool) {
        amqp->tls_verify_peer = val.val.boolean;
    }

    ulak_log("info", "Created AMQP dispatcher for host: %s:%d, exchange: %s, routing_key: %s",
             amqp->host, amqp->port, amqp->exchange, amqp->routing_key);

    return (Dispatcher *)amqp;

error:
    /* Cleanup on error */
    if (amqp->host)
        pfree(amqp->host);
    if (amqp->vhost)
        pfree(amqp->vhost);
    if (amqp->username) {
        explicit_bzero(amqp->username, strlen(amqp->username));
        pfree(amqp->username);
    }
    if (amqp->password) {
        explicit_bzero(amqp->password, strlen(amqp->password));
        pfree(amqp->password);
    }
    if (amqp->exchange)
        pfree(amqp->exchange);
    if (amqp->routing_key)
        pfree(amqp->routing_key);
    if (amqp->exchange_type)
        pfree(amqp->exchange_type);
    if (amqp->tls_ca_cert)
        pfree(amqp->tls_ca_cert);
    if (amqp->tls_cert) {
        explicit_bzero(amqp->tls_cert, strlen(amqp->tls_cert));
        pfree(amqp->tls_cert);
    }
    if (amqp->tls_key) {
        explicit_bzero(amqp->tls_key, strlen(amqp->tls_key));
        pfree(amqp->tls_key);
    }
    if (amqp->pending_messages)
        pfree(amqp->pending_messages);
    pfree(amqp);
    return NULL;
}

/**
 * @brief Clean up and destroy an AMQP dispatcher.
 * @param dispatcher Dispatcher to clean up
 */
void amqp_dispatcher_cleanup(Dispatcher *dispatcher) {
    AmqpDispatcher *amqp;

    if (!dispatcher || !dispatcher->private_data)
        return;

    amqp = (AmqpDispatcher *)dispatcher->private_data;

    /* Close connection gracefully */
    amqp_disconnect_internal(amqp);

    /* Cleanup allocated strings */
    if (amqp->host)
        pfree(amqp->host);
    if (amqp->vhost)
        pfree(amqp->vhost);
    if (amqp->username) {
        explicit_bzero(amqp->username, strlen(amqp->username));
        pfree(amqp->username);
    }
    if (amqp->password) {
        explicit_bzero(amqp->password, strlen(amqp->password));
        pfree(amqp->password);
    }
    if (amqp->exchange)
        pfree(amqp->exchange);
    if (amqp->routing_key)
        pfree(amqp->routing_key);
    if (amqp->exchange_type)
        pfree(amqp->exchange_type);

    /* Cleanup TLS strings - zero sensitive material */
    if (amqp->tls_ca_cert)
        pfree(amqp->tls_ca_cert);
    if (amqp->tls_cert) {
        explicit_bzero(amqp->tls_cert, strlen(amqp->tls_cert));
        pfree(amqp->tls_cert);
    }
    if (amqp->tls_key) {
        explicit_bzero(amqp->tls_key, strlen(amqp->tls_key));
        pfree(amqp->tls_key);
    }

    /* Cleanup batch tracking */
    if (amqp->pending_messages)
        pfree(amqp->pending_messages);

    /* Note: Do NOT free base.config - it's owned by the caller (worker/queue) */

    pfree(amqp);
    ulak_log("debug", "AMQP dispatcher cleaned up");
}

/**
 * @brief Extended dispatch with response capture.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param headers Per-message JSONB headers (unused for AMQP)
 * @param metadata JSONB metadata (reserved for future use)
 * @param result Output dispatch result with delivery tag and confirm status
 * @return true on success, false on failure
 */
bool amqp_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result) {
    AmqpDispatcher *amqp;
    char *error_msg = NULL;
    bool success;

    (void)headers;  /* AMQP does not use per-message headers from ulak */
    (void)metadata; /* Reserved for future use */

    if (!dispatcher || !dispatcher->private_data) {
        if (result) {
            result->success = false;
            result->error_msg = pstrdup("Invalid AMQP dispatcher");
        }
        return false;
    }

    amqp = (AmqpDispatcher *)dispatcher->private_data;

    success = amqp_dispatcher_dispatch(dispatcher, payload, &error_msg);

    if (result) {
        result->success = success;
        if (!success && error_msg) {
            result->error_msg = error_msg;
        } else {
            if (error_msg)
                pfree(error_msg);
        }
        if (success) {
            result->amqp_delivery_tag = amqp->next_delivery_tag - 1;
            result->amqp_confirmed = true;
        }
    } else {
        if (error_msg)
            pfree(error_msg);
    }

    return success;
}
