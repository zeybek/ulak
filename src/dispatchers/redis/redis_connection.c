/**
 * @file redis_connection.c
 * @brief Redis connection management.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles Redis connection lifecycle: connect, disconnect, reconnect, health checks.
 */

#include "redis_internal.h"

#include <string.h>
#include <time.h>

#include "postgres.h"
#include "utils/logging.h"

/**
 * @brief Check if a Redis error is permanent (should not be retried).
 *
 * Permanent errors include authentication failures, permission issues, etc.
 *
 * @param error_str Redis error string
 * @return true if the error is permanent, false if retryable
 */
bool redis_is_permanent_error(const char *error_str) {
    if (!error_str)
        return false;

    /* Authentication/permission errors are permanent */
    if (strstr(error_str, "NOAUTH") != NULL)
        return true;
    if (strstr(error_str, "NOPERM") != NULL)
        return true;

    /* Type errors indicate wrong data structure */
    if (strstr(error_str, "WRONGTYPE") != NULL)
        return true;

    /* Server resource issues */
    if (strstr(error_str, "OOM") != NULL)
        return true;

    /* Read-only replica */
    if (strstr(error_str, "READONLY") != NULL)
        return true;

    return false;
}

/**
 * @brief Connect to Redis server (internal).
 *
 * Handles both plain TCP and TLS connections, authentication, database selection,
 * and optional consumer group creation.
 *
 * @param redis Redis dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected and authenticated, false on failure
 */
bool redis_connect_internal(RedisDispatcher *redis, char **error_msg) {
    struct timeval timeout;

    timeout.tv_sec = redis->connect_timeout;
    timeout.tv_usec = 0;

    /* Disconnect existing connection if any */
    redis_disconnect_internal(redis);

    if (redis->tls) {
        /* TLS connection */
        redisSSLContext *ssl_ctx;

        /* Create SSL context */
        ssl_ctx = redis_tls_create_context(redis->tls_ca_cert, redis->tls_cert, redis->tls_key,
                                           error_msg);
        if (!ssl_ctx) {
            return false;
        }
        redis->ssl_context = ssl_ctx;

        /* Create TCP connection first */
        redis->context = redisConnectWithTimeout(redis->host, redis->port, timeout);

        if (!redis->context || redis->context->err) {
            const char *err = redis->context ? redis->context->errstr : "Connection failed";
            if (error_msg) {
                *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis connection error: %s", err);
            }
            ulak_log("error", "Failed to connect to Redis: %s", err);

            if (redis->context) {
                redisFree(redis->context);
                redis->context = NULL;
            }
            redis_tls_cleanup(redis->ssl_context);
            redis->ssl_context = NULL;
            return false;
        }

        /* Initiate SSL handshake */
        if (!redis_tls_initiate(redis->context, redis->ssl_context, error_msg)) {
            redisFree(redis->context);
            redis->context = NULL;
            redis_tls_cleanup(redis->ssl_context);
            redis->ssl_context = NULL;
            return false;
        }

        ulak_log("debug", "Redis TLS connection established to %s:%d", redis->host, redis->port);
    } else {
        /* Plain TCP connection */
        redis->context = redisConnectWithTimeout(redis->host, redis->port, timeout);

        if (!redis->context || redis->context->err) {
            const char *err = redis->context ? redis->context->errstr : "Connection failed";
            if (error_msg) {
                *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis connection error: %s", err);
            }
            ulak_log("error", "Failed to connect to Redis: %s", err);

            if (redis->context) {
                redisFree(redis->context);
                redis->context = NULL;
            }
            return false;
        }

        ulak_log("debug", "Redis TCP connection established to %s:%d", redis->host, redis->port);
    }

    /* Set command timeout (only if context is valid) */
    if (redis->context) {
        struct timeval cmd_timeout;
        cmd_timeout.tv_sec = redis->command_timeout;
        cmd_timeout.tv_usec = 0;
        redisSetTimeout(redis->context, cmd_timeout);
    }

    /* Enable TCP keepalive for dead connection detection.
     * 15s interval matches hiredis default. Must re-apply after reconnect
     * since redisReconnect() does not preserve keepalive settings. */
    if (redis->context) {
        redisEnableKeepAliveWithInterval(redis->context, 15);
    }

    /* Authenticate if password provided (Redis 6+ ACL: AUTH username password) */
    if (redis->username && redis->password) {
        redisReply *reply = (redisReply *)redisCommand(redis->context, "AUTH %s %s",
                                                       redis->username, redis->password);

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            const char *err = reply ? reply->str : "AUTH command failed";
            if (error_msg) {
                *error_msg =
                    psprintf(ERROR_PREFIX_PERMANENT " Redis ACL authentication failed: %s", err);
            }
            ulak_log("error", "Redis ACL authentication failed: %s", err);

            if (reply)
                freeReplyObject(reply);
            redisFree(redis->context);
            redis->context = NULL;
            if (redis->ssl_context) {
                redis_tls_cleanup(redis->ssl_context);
                redis->ssl_context = NULL;
            }
            return false;
        }
        freeReplyObject(reply);
        ulak_log("debug", "Redis ACL authentication successful (user: %s)", redis->username);
    } else if (redis->password) {
        redisReply *reply = (redisReply *)redisCommand(redis->context, "AUTH %s", redis->password);

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            const char *err = reply ? reply->str : "AUTH command failed";
            if (error_msg) {
                *error_msg =
                    psprintf(ERROR_PREFIX_PERMANENT " Redis authentication failed: %s", err);
            }
            ulak_log("error", "Redis authentication failed: %s", err);

            if (reply)
                freeReplyObject(reply);
            redisFree(redis->context);
            redis->context = NULL;
            if (redis->ssl_context) {
                redis_tls_cleanup(redis->ssl_context);
                redis->ssl_context = NULL;
            }
            return false;
        }
        freeReplyObject(reply);
        ulak_log("debug", "Redis authentication successful");
    }

    /* Select database */
    if (redis->db != 0) {
        redisReply *reply = (redisReply *)redisCommand(redis->context, "SELECT %d", redis->db);

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            const char *err = reply ? reply->str : "SELECT command failed";
            if (error_msg) {
                *error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " Redis SELECT %d failed: %s", redis->db, err);
            }
            ulak_log("error", "Redis SELECT %d failed: %s", redis->db, err);

            if (reply)
                freeReplyObject(reply);
            redisFree(redis->context);
            redis->context = NULL;
            if (redis->ssl_context) {
                redis_tls_cleanup(redis->ssl_context);
                redis->ssl_context = NULL;
            }
            return false;
        }
        freeReplyObject(reply);
        ulak_log("debug", "Redis database %d selected", redis->db);
    }

    /* Auto-create consumer group if configured */
    if (redis->consumer_group && redis->stream_key) {
        redisReply *reply =
            (redisReply *)redisCommand(redis->context, "XGROUP CREATE %s %s $ MKSTREAM",
                                       redis->stream_key, redis->consumer_group);
        if (reply) {
            if (reply->type == REDIS_REPLY_ERROR && strstr(reply->str, "BUSYGROUP")) {
                /* Group already exists -- idempotent, OK */
                ulak_log("debug", "Redis consumer group '%s' already exists",
                         redis->consumer_group);
            } else if (reply->type == REDIS_REPLY_ERROR) {
                ulak_log("warning", "Failed to create consumer group '%s': %s",
                         redis->consumer_group, reply->str);
            } else {
                ulak_log("info", "Created consumer group '%s' on stream '%s'",
                         redis->consumer_group, redis->stream_key);
            }
            freeReplyObject(reply);
        }
    }

    return true;
}

/**
 * @brief Disconnect from Redis server (internal).
 * @param redis Redis dispatcher instance
 */
void redis_disconnect_internal(RedisDispatcher *redis) {
    if (redis->context) {
        redisFree(redis->context);
        redis->context = NULL;
    }
    if (redis->ssl_context) {
        redis_tls_cleanup(redis->ssl_context);
        redis->ssl_context = NULL;
    }
}

/**
 * @brief Ensure Redis connection is active.
 *
 * Uses lazy PING: only checks connection health if enough time has passed
 * since the last successful operation to reduce latency.
 *
 * @param redis Redis dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if connected, false on failure
 */
bool redis_dispatcher_ensure_connected(RedisDispatcher *redis, char **error_msg) {
    if (!redis) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis dispatcher");
        return false;
    }

    /* Check if connection exists and is healthy */
    if (redis->context && !redis->context->err) {
        /* Lazy PING: only check if enough time has passed since last successful op */
        time_t now = time(NULL);
        bool need_ping = (redis->last_successful_op == 0) ||
                         (now - redis->last_successful_op > REDIS_PING_INTERVAL_SECONDS);

        if (need_ping) {
            /* Test connection with PING */
            redisReply *reply = (redisReply *)redisCommand(redis->context, "PING");
            if (reply && reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str, "PONG") == 0) {
                freeReplyObject(reply);
                return true;
            }
            if (reply)
                freeReplyObject(reply);

            /* Connection is broken, reconnect */
            ulak_log("debug", "Redis connection broken, reconnecting...");
        } else {
            /* Assume connection is still valid based on recent success */
            return true;
        }
    }

    /* Connect or reconnect */
    return redis_connect_internal(redis, error_msg);
}

/**
 * @brief Reconnect to Redis server.
 * @param redis Redis dispatcher instance
 * @param error_msg Output error message on failure
 * @return true if reconnected, false on failure
 */
bool redis_dispatcher_reconnect(RedisDispatcher *redis, char **error_msg) {
    redis_disconnect_internal(redis);
    return redis_connect_internal(redis, error_msg);
}
