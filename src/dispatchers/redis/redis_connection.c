/**
 * @file redis_connection.c
 * @brief Redis connection management.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles Redis connection lifecycle: connect, disconnect, reconnect, health checks.
 */

#include "redis_internal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

/* ------------------------------------------------------------------------
 * Bounded host resolution
 *
 * hiredis resolves the host name with a blocking getaddrinfo() before it
 * applies the connect timeout to connect() [source: redis/hiredis net.c,
 * _redisContextConnectTcp, via deepwiki]. A broken DNS record therefore
 * stalled the worker for the resolver's own retry schedule (8-15 s in
 * Docker) no matter what connect_timeout said. We resolve the name on a
 * helper thread, wait at most connect_timeout for it, hand hiredis the
 * numeric address and cache it for REDIS_RESOLVE_TTL_SEC.
 *
 * The thread only calls getaddrinfo/inet_ntop and touches malloc'd memory
 * (palloc is not thread-safe and the batch context may be gone by the time
 * an abandoned lookup finishes). If the wait times out the job is marked
 * abandoned and the thread frees it itself when the lookup returns.
 * ------------------------------------------------------------------------ */

#define REDIS_RESOLVE_TTL_SEC 60

typedef struct RedisResolveJob {
    pthread_mutex_t lock;
    pthread_cond_t done_cv;
    char *host;     /* malloc'd copy of the name to resolve */
    bool done;      /* lookup finished, result fields valid */
    bool abandoned; /* caller stopped waiting; thread owns the job */
    int gai_rc;     /* getaddrinfo() return code */
    char ip[64];    /* numeric address, '\0' when none found */
} RedisResolveJob;

static void redis_resolve_job_free(RedisResolveJob *job) {
    pthread_mutex_destroy(&job->lock);
    pthread_cond_destroy(&job->done_cv);
    free(job->host);
    free(job);
}

static void *redis_resolve_thread(void *arg) {
    RedisResolveJob *job = (RedisResolveJob *)arg;
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    struct addrinfo *ai;
    char ip[64] = "";
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    rc = getaddrinfo(job->host, NULL, &hints, &res);
    if (rc == 0 && res != NULL) {
        /* Prefer IPv4, then the first IPv6 address */
        for (ai = res; ai != NULL; ai = ai->ai_next) {
            if (ai->ai_family == AF_INET) {
                inet_ntop(AF_INET, &((struct sockaddr_in *)ai->ai_addr)->sin_addr, ip, sizeof(ip));
                break;
            }
        }
        if (ip[0] == '\0') {
            for (ai = res; ai != NULL; ai = ai->ai_next) {
                if (ai->ai_family == AF_INET6) {
                    inet_ntop(AF_INET6, &((struct sockaddr_in6 *)ai->ai_addr)->sin6_addr, ip,
                              sizeof(ip));
                    break;
                }
            }
        }
        freeaddrinfo(res);
    }

    pthread_mutex_lock(&job->lock);
    if (job->abandoned) {
        pthread_mutex_unlock(&job->lock);
        redis_resolve_job_free(job);
        return NULL;
    }
    job->gai_rc = rc;
    strlcpy(job->ip, ip, sizeof(job->ip));
    job->done = true;
    pthread_cond_signal(&job->done_cv);
    pthread_mutex_unlock(&job->lock);
    return NULL;
}

/**
 * @brief Resolve @p host to a numeric address, waiting at most @p timeout_sec.
 *
 * @return true and fills @p ip_out; false with a retryable @p error_msg on
 *         timeout, resolver failure or thread creation failure.
 */
static bool redis_resolve_host_bounded(const char *host, int timeout_sec, char *ip_out,
                                       size_t ip_len, char **error_msg) {
    RedisResolveJob *job;
    pthread_t tid;
    pthread_attr_t attr;
    struct timespec deadline;
    struct in_addr a4;
    struct in6_addr a6;
    sigset_t block_all;
    sigset_t saved_mask;
    bool ok = false;
    int rc;

    /* Numeric address: nothing to resolve */
    if (inet_pton(AF_INET, host, &a4) == 1 || inet_pton(AF_INET6, host, &a6) == 1) {
        strlcpy(ip_out, host, ip_len);
        return true;
    }

    job = (RedisResolveJob *)calloc(1, sizeof(RedisResolveJob));
    if (job == NULL || (job->host = strdup(host)) == NULL) {
        free(job);
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " Redis resolver: out of memory");
        return false;
    }
    pthread_mutex_init(&job->lock, NULL);
    pthread_cond_init(&job->done_cv, NULL);

    /* The helper must never receive the backend's signals (SIGTERM, SIGHUP,
     * SIGURG for the latch): a new thread inherits the creator's mask, so
     * block everything while creating it and restore our own mask after. */
    sigfillset(&block_all);
    pthread_sigmask(SIG_BLOCK, &block_all, &saved_mask);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    rc = pthread_create(&tid, &attr, redis_resolve_thread, job);
    pthread_attr_destroy(&attr);
    pthread_sigmask(SIG_SETMASK, &saved_mask, NULL);
    if (rc != 0) {
        redis_resolve_job_free(job);
        if (error_msg)
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Redis resolver thread failed: %s", strerror(rc));
        return false;
    }

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_sec > 0 ? timeout_sec : 1;

    pthread_mutex_lock(&job->lock);
    while (!job->done) {
        rc = pthread_cond_timedwait(&job->done_cv, &job->lock, &deadline);
        if (rc == ETIMEDOUT)
            break;
    }
    if (!job->done) {
        /* Give up; the thread frees the job when getaddrinfo returns */
        job->abandoned = true;
        pthread_mutex_unlock(&job->lock);
        if (error_msg)
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Redis DNS resolution of %s timed out after %d s",
                         host, timeout_sec);
        return false;
    }
    if (job->gai_rc == 0 && job->ip[0] != '\0') {
        strlcpy(ip_out, job->ip, ip_len);
        ok = true;
    } else if (error_msg) {
        *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis DNS resolution of %s failed: %s", host,
                              job->gai_rc != 0 ? gai_strerror(job->gai_rc) : "no address");
    }
    pthread_mutex_unlock(&job->lock);
    redis_resolve_job_free(job);
    return ok;
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
    const char *connect_host;

    timeout.tv_sec = redis->connect_timeout;
    timeout.tv_usec = 0;

    /* Disconnect existing connection if any */
    redis_disconnect_internal(redis);

    /* Resolve the host ourselves (bounded, cached) and hand hiredis the address */
    if (redis->resolved_ip[0] == '\0' || time(NULL) - redis->resolved_at >= REDIS_RESOLVE_TTL_SEC) {
        char ip[sizeof(redis->resolved_ip)];

        if (!redis_resolve_host_bounded(redis->host, redis->connect_timeout, ip, sizeof(ip),
                                        error_msg)) {
            redis->resolved_ip[0] = '\0';
            ulak_log("error", "Failed to resolve Redis host %s: %s", redis->host,
                     error_msg && *error_msg ? *error_msg : "unknown error");
            return false;
        }
        strlcpy(redis->resolved_ip, ip, sizeof(redis->resolved_ip));
        redis->resolved_at = time(NULL);
        ulak_log("debug", "Resolved Redis host %s to %s", redis->host, redis->resolved_ip);
    }
    connect_host = redis->resolved_ip;

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
        redis->context = redisConnectWithTimeout(connect_host, redis->port, timeout);

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
            redis->resolved_ip[0] = '\0';
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
        redis->context = redisConnectWithTimeout(connect_host, redis->port, timeout);

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
            redis->resolved_ip[0] = '\0';
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
