/**
 * @file amqp_connection.c
 * @brief AMQP connection management.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles AMQP connection lifecycle: connect, disconnect, reconnect, health checks.
 */

#include "amqp_internal.h"

#include <string.h>
#include <strings.h>
#include <time.h>

#if defined(__has_include)
#if __has_include(<rabbitmq-c/ssl_socket.h>)
#include <rabbitmq-c/ssl_socket.h>
#else
#include <amqp_ssl_socket.h>
#endif
#else
#include <amqp_ssl_socket.h>
#endif

#include "config/guc.h"
#include "postgres.h"
#include "utils/logging.h"

/* Error prefix constants */
#define ERROR_PREFIX_RETRYABLE "[RETRYABLE]"
#define ERROR_PREFIX_PERMANENT "[PERMANENT]"

/* Connection health check interval (seconds) */
#define AMQP_PING_INTERVAL_SECONDS 30

/**
 * @brief Secure memory clearing for sensitive data.
 *
 * Uses explicit_bzero which is guaranteed not to be optimized away.
 *
 * @param ptr Pointer to memory to zero
 * @param len Number of bytes to zero
 */
void amqp_secure_zero_memory(void *ptr, size_t len) { explicit_bzero(ptr, len); }

/**
 * @brief Convert AMQP reply to a human-readable error string.
 * @param reply AMQP RPC reply to inspect
 * @param buffer Output buffer for error string
 * @param buflen Size of output buffer
 * @return Pointer to error description (may be buffer or a string literal)
 */
const char *amqp_reply_error_string(amqp_rpc_reply_t reply, char *buffer, size_t buflen) {
    switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return "No error";

    case AMQP_RESPONSE_NONE:
        snprintf(buffer, buflen, "Missing RPC reply");
        return buffer;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        snprintf(buffer, buflen, "Library error: %s", amqp_error_string2(reply.library_error));
        return buffer;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD && reply.reply.decoded) {
            amqp_connection_close_t *m = (amqp_connection_close_t *)reply.reply.decoded;
            snprintf(buffer, buflen, "Connection closed: %d - %.*s", m->reply_code,
                     (int)m->reply_text.len, (char *)m->reply_text.bytes);
        } else if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD && reply.reply.decoded) {
            amqp_channel_close_t *m = (amqp_channel_close_t *)reply.reply.decoded;
            snprintf(buffer, buflen, "Channel closed: %d - %.*s", m->reply_code,
                     (int)m->reply_text.len, (char *)m->reply_text.bytes);
        } else {
            snprintf(buffer, buflen, "Server exception (method: 0x%08X)", reply.reply.id);
        }
        return buffer;

    default:
        snprintf(buffer, buflen, "Unknown reply type: %d", reply.reply_type);
        return buffer;
    }
}

/**
 * @brief Check if an AMQP error is permanent (should not be retried).
 *
 * Permanent errors include authentication failures, permission issues, etc.
 *
 * @param reply AMQP RPC reply to inspect
 * @return true if the error is permanent, false if retryable
 */
bool amqp_is_permanent_error(amqp_rpc_reply_t reply) {
    if (reply.reply_type != AMQP_RESPONSE_SERVER_EXCEPTION) {
        return false;
    }

    /* Check connection close codes */
    if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD && reply.reply.decoded) {
        amqp_connection_close_t *m = (amqp_connection_close_t *)reply.reply.decoded;
        /* AMQP reply codes:
         * 403 = ACCESS_REFUSED (permissions)
         * 530 = NOT_ALLOWED (vhost access denied)
         * 402 = INVALID_PATH (bad vhost)
         */
        if (m->reply_code == 403 || m->reply_code == 530 || m->reply_code == 402) {
            return true;
        }
    }

    /* Check channel close codes */
    if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD && reply.reply.decoded) {
        amqp_channel_close_t *m = (amqp_channel_close_t *)reply.reply.decoded;
        /* 403 = ACCESS_REFUSED
         * 404 = NOT_FOUND (queue/exchange doesn't exist)
         */
        if (m->reply_code == 403 || m->reply_code == 404) {
            return true;
        }
    }

    return false;
}

/**
 * @brief Disconnect from AMQP broker (internal).
 * @param amqp AMQP dispatcher instance
 */
void amqp_disconnect_internal(AmqpDispatcher *amqp) {
    if (!amqp)
        return;

    if (amqp->conn) {
        /* Close channel if open */
        if (amqp->channel > 0) {
            amqp_channel_close(amqp->conn, amqp->channel, AMQP_REPLY_SUCCESS);
            amqp->channel = 0;
        }

        /* Close connection */
        amqp_connection_close(amqp->conn, AMQP_REPLY_SUCCESS);

        /* Destroy connection */
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
    }

    amqp->socket = NULL;
    amqp->connected = false;
    amqp->confirm_mode = false;

    ulak_log("debug", "AMQP connection closed");
}

/**
 * @brief Enable publisher confirms on the channel.
 *
 * Required for reliable delivery tracking.
 *
 * @param amqp AMQP dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if confirms enabled, false on failure
 */
bool amqp_enable_confirms(AmqpDispatcher *amqp, char **error_msg) {
    amqp_rpc_reply_t reply;
    char err_buffer[256];

    if (!amqp || !amqp->conn || amqp->channel <= 0) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP connection state for enable_confirms");
        return false;
    }

    /* Enable confirm mode */
    amqp_confirm_select(amqp->conn, amqp->channel);

    reply = amqp_get_rpc_reply(amqp->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        const char *err = amqp_reply_error_string(reply, err_buffer, sizeof(err_buffer));
        if (error_msg) {
            *error_msg = psprintf("Failed to enable publisher confirms: %s", err);
        }
        ulak_log("error", "Failed to enable AMQP publisher confirms: %s", err);
        return false;
    }

    amqp->confirm_mode = true;
    amqp->next_delivery_tag = 1; /* Delivery tags start at 1 */

    ulak_log("debug", "AMQP publisher confirms enabled on channel %d", amqp->channel);
    return true;
}

/**
 * @brief Connect to AMQP broker (internal).
 *
 * Creates socket, opens connection, authenticates, and opens channel.
 *
 * @param amqp AMQP dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected and channel opened, false on failure
 */
bool amqp_connect_internal(AmqpDispatcher *amqp, char **error_msg) {
    amqp_rpc_reply_t reply;
    int status;
    char err_buffer[256];
    int timeout_ms;
    struct timeval tv;

    if (!amqp) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP dispatcher");
        return false;
    }

    /* Disconnect existing connection if any */
    amqp_disconnect_internal(amqp);

    /* Create new connection state */
    amqp->conn = amqp_new_connection();
    if (!amqp->conn) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " Failed to create AMQP connection state");
        ulak_log("error", "Failed to create AMQP connection state");
        return false;
    }

    /* Create socket - TLS or plain TCP */
    if (amqp->tls) {
#ifdef AMQP_SSL_SOCKET_H
        amqp->socket = amqp_ssl_socket_new(amqp->conn);
        if (!amqp->socket) {
            if (error_msg)
                *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " Failed to create AMQP SSL socket");
            ulak_log("error", "Failed to create AMQP SSL socket");
            amqp_destroy_connection(amqp->conn);
            amqp->conn = NULL;
            return false;
        }

        /* Configure TLS */
        if (amqp->tls_ca_cert) {
            amqp_ssl_socket_set_cacert(amqp->socket, amqp->tls_ca_cert);
        }
        if (amqp->tls_cert && amqp->tls_key) {
            amqp_ssl_socket_set_key(amqp->socket, amqp->tls_cert, amqp->tls_key);
        }
        amqp_ssl_socket_set_verify_peer(amqp->socket, amqp->tls_verify_peer ? 1 : 0);
        amqp_ssl_socket_set_verify_hostname(amqp->socket, amqp->tls_verify_peer ? 1 : 0);

        ulak_log("debug", "AMQP SSL socket created with TLS");
#else
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " AMQP SSL support not compiled in");
        ulak_log("error", "AMQP SSL support not available");
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
        return false;
#endif
    } else {
        amqp->socket = amqp_tcp_socket_new(amqp->conn);
        if (!amqp->socket) {
            if (error_msg)
                *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " Failed to create AMQP TCP socket");
            ulak_log("error", "Failed to create AMQP TCP socket");
            amqp_destroy_connection(amqp->conn);
            amqp->conn = NULL;
            return false;
        }
        ulak_log("debug", "AMQP TCP socket created");
    }

    /* Set connection timeout */
    timeout_ms = ulak_amqp_connection_timeout > 0 ? ulak_amqp_connection_timeout * 1000 : 10000;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    /* Open socket connection */
    status = amqp_socket_open_noblock(amqp->socket, amqp->host, amqp->port, &tv);
    if (status != AMQP_STATUS_OK) {
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Failed to connect to AMQP broker %s:%d: %s",
                         amqp->host, amqp->port, amqp_error_string2(status));
        }
        ulak_log("error", "Failed to connect to AMQP broker %s:%d: %s", amqp->host, amqp->port,
                 amqp_error_string2(status));
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
        amqp->socket = NULL;
        return false;
    }

    ulak_log("debug", "AMQP socket connected to %s:%d", amqp->host, amqp->port);

    /* Login (SASL PLAIN authentication) */
    reply =
        amqp_login(amqp->conn, amqp->vhost ? amqp->vhost : AMQP_DEFAULT_VHOST, 0, /* channel_max */
                   amqp->frame_max > 0 ? amqp->frame_max : AMQP_DEFAULT_FRAME_MAX,
                   amqp->heartbeat > 0 ? amqp->heartbeat : AMQP_DEFAULT_HEARTBEAT,
                   AMQP_SASL_METHOD_PLAIN, amqp->username ? amqp->username : AMQP_DEFAULT_USERNAME,
                   amqp->password ? amqp->password : AMQP_DEFAULT_PASSWORD);

    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        const char *err = amqp_reply_error_string(reply, err_buffer, sizeof(err_buffer));
        bool permanent = amqp_is_permanent_error(reply);
        if (error_msg) {
            *error_msg = psprintf("%s AMQP login failed: %s",
                                  permanent ? ERROR_PREFIX_PERMANENT : ERROR_PREFIX_RETRYABLE, err);
        }
        ulak_log("error", "AMQP login failed to %s (vhost: %s): %s", amqp->host,
                 amqp->vhost ? amqp->vhost : "/", err);
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
        amqp->socket = NULL;
        return false;
    }

    ulak_log("debug", "AMQP login successful (vhost: %s)", amqp->vhost ? amqp->vhost : "/");

    /* Open channel */
    amqp->channel = AMQP_DEFAULT_CHANNEL;
    amqp_channel_open(amqp->conn, amqp->channel);
    reply = amqp_get_rpc_reply(amqp->conn);

    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        const char *err = amqp_reply_error_string(reply, err_buffer, sizeof(err_buffer));
        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " AMQP channel open failed: %s", err);
        }
        ulak_log("error", "AMQP channel open failed: %s", err);
        amqp_connection_close(amqp->conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
        amqp->socket = NULL;
        amqp->channel = 0;
        return false;
    }

    ulak_log("debug", "AMQP channel %d opened", amqp->channel);

    /* Enable publisher confirms for reliable delivery */
    if (!amqp_enable_confirms(amqp, error_msg)) {
        amqp_channel_close(amqp->conn, amqp->channel, AMQP_REPLY_SUCCESS);
        amqp_connection_close(amqp->conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(amqp->conn);
        amqp->conn = NULL;
        amqp->socket = NULL;
        amqp->channel = 0;
        return false;
    }

    amqp->connected = true;
    amqp->last_successful_op = time(NULL);

    ulak_log("info", "AMQP connection established to %s:%d (vhost: %s, channel: %d)", amqp->host,
             amqp->port, amqp->vhost ? amqp->vhost : "/", amqp->channel);

    return true;
}

/**
 * @brief Ensure AMQP connection is active.
 *
 * Uses lazy check: only verifies connection health if enough time has passed
 * since the last successful operation.
 *
 * @param amqp AMQP dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected, false on failure
 */
bool amqp_ensure_connected(AmqpDispatcher *amqp, char **error_msg) {
    int socket_status;

    if (!amqp) {
        if (error_msg)
            *error_msg = pstrdup("Invalid AMQP dispatcher");
        return false;
    }

    /* Check if connection exists */
    if (amqp->connected && amqp->conn && amqp->channel > 0) {
        /* Lazy check: assume connection is valid if recent operation succeeded */
        time_t now = time(NULL);
        if (amqp->last_successful_op > 0 &&
            (now - amqp->last_successful_op) < AMQP_PING_INTERVAL_SECONDS) {
            return true;
        }

        /* Check if socket is still valid - if not, we need to reconnect */
        socket_status = amqp_get_sockfd(amqp->conn);
        if (socket_status < 0) {
            ulak_log("debug", "AMQP socket invalid, reconnecting...");
            /* Fully disconnect before reconnecting to avoid resource leaks */
            amqp_disconnect_internal(amqp);
        } else {
            /* Update last successful operation time */
            amqp->last_successful_op = now;
            return true;
        }
    }

    /* Connect or reconnect -- amqp_connect_internal calls amqp_disconnect_internal first */
    return amqp_connect_internal(amqp, error_msg);
}
