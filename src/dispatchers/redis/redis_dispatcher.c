/**
 * @file redis_dispatcher.c
 * @brief Redis Streams protocol dispatcher implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles Redis Streams message dispatching using hiredis with TLS support.
 *
 * This file contains:
 * - Dispatcher operations struct
 * - create, dispatch, validate_config, cleanup functions
 *
 * Connection management is in redis_connection.c
 * TLS handling is in redis_tls.c
 * Config validation helpers are in redis_config.c
 */

#include "redis_internal.h"

#include <string.h>
#include <time.h>
#include <unistd.h>

#include "config/guc.h"
#include "core/entities.h"
#include "fmgr.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/** Redis Dispatcher Operations */
static DispatcherOperations redis_dispatcher_ops = {
    .dispatch = redis_dispatcher_dispatch,
    .validate_config = redis_dispatcher_validate_config,
    .cleanup = redis_dispatcher_cleanup,
    /* Pipelining batch operations */
    .produce = redis_dispatcher_produce,
    .flush = redis_dispatcher_flush,
    .supports_batch = redis_dispatcher_supports_batch,
    /* Extended dispatch with stream ID capture */
    .dispatch_ex = redis_dispatcher_dispatch_ex,
    .produce_ex = NULL};

/**
 * @brief Create a new Redis dispatcher instance.
 * @param config JSONB configuration with host, stream_key, and optional settings
 * @return Dispatcher pointer on success, NULL on failure
 */
Dispatcher *redis_dispatcher_create(Jsonb *config) {
    RedisDispatcher *redis;
    JsonbValue host_val, stream_key_val, password_val;
    JsonbValue tls_ca_val, tls_cert_val, tls_key_val;

    if (!redis_dispatcher_validate_config(config)) {
        ulak_log("error", "Invalid Redis dispatcher configuration");
        return NULL;
    }

    redis = (RedisDispatcher *)palloc0(sizeof(RedisDispatcher));

    /* Initialize base dispatcher */
    redis->base.protocol = PROTOCOL_TYPE_REDIS;
    redis->base.config = config;
    redis->base.ops = &redis_dispatcher_ops;
    redis->base.private_data = redis;

    /* Parse configuration - use stack-allocated JsonbValues */

    /* Get host (required) */
    if (!extract_jsonb_value(config, REDIS_CONFIG_KEY_HOST, &host_val) ||
        host_val.type != jbvString) {
        ulak_log("error", "Redis config missing or invalid 'host'");
        pfree(redis);
        return NULL;
    }
    redis->host = pnstrdup(host_val.val.string.val, host_val.val.string.len);

    /* Get stream_key (required) */
    if (!extract_jsonb_value(config, REDIS_CONFIG_KEY_STREAM_KEY, &stream_key_val) ||
        stream_key_val.type != jbvString) {
        ulak_log("error", "Redis config missing or invalid 'stream_key'");
        pfree(redis->host);
        pfree(redis);
        return NULL;
    }
    redis->stream_key = pnstrdup(stream_key_val.val.string.val, stream_key_val.val.string.len);

    /* Get port (optional, default 6379) */
    redis->port = jsonb_get_int32(config, REDIS_CONFIG_KEY_PORT, ulak_redis_default_port);

    /* Validate port range (1-65535) */
    if (redis->port < 1 || redis->port > 65535) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("ulak: Redis port %d out of valid range (1-65535), using "
                                 "default 6379",
                                 redis->port)));
        redis->port = 6379;
    }

    /* Get db (optional, default 0) */
    redis->db = jsonb_get_int32(config, REDIS_CONFIG_KEY_DB, ulak_redis_default_db);

    /* Validate db range (0-15) */
    if (redis->db < 0 || redis->db > 15) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("ulak: Redis db %d out of valid range (0-15), using default 0",
                                 redis->db)));
        redis->db = 0;
    }

    /* Get timeouts (optional, use GUC defaults) */
    redis->connect_timeout =
        jsonb_get_int32(config, REDIS_CONFIG_KEY_CONNECT_TIMEOUT, ulak_redis_connect_timeout);

    /* Validate connect_timeout (>= 1) */
    if (redis->connect_timeout < 1) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("ulak: Redis connect_timeout %d invalid, using default 5",
                                 redis->connect_timeout)));
        redis->connect_timeout = 5;
    }

    redis->command_timeout =
        jsonb_get_int32(config, REDIS_CONFIG_KEY_COMMAND_TIMEOUT, ulak_redis_command_timeout);

    /* Validate command_timeout (>= 1) */
    if (redis->command_timeout < 1) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("ulak: Redis command_timeout %d invalid, using default 30",
                                 redis->command_timeout)));
        redis->command_timeout = 30;
    }

    /* Get maxlen (optional, 0 = unlimited) */
    redis->maxlen = jsonb_get_int64(config, REDIS_CONFIG_KEY_MAXLEN, 0);

    /* Validate maxlen (>= 0) */
    if (redis->maxlen < 0) {
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("ulak: Redis maxlen %lld invalid (must be >= 0), using 0 (unlimited)",
                        (long long)redis->maxlen)));
        redis->maxlen = 0;
    }

    /* Get minid (optional, 0 = disabled) */
    redis->minid = jsonb_get_int64(config, REDIS_CONFIG_KEY_MINID, 0);

    /* Validate minid (>= 0) */
    if (redis->minid < 0) {
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("ulak: Redis minid %lld invalid (must be >= 0), using 0 (disabled)",
                        (long long)redis->minid)));
        redis->minid = 0;
    }

    /* Get password (optional) */
    redis->password = NULL;
    if (extract_jsonb_value(config, REDIS_CONFIG_KEY_PASSWORD, &password_val) &&
        password_val.type == jbvString && password_val.val.string.len > 0) {
        redis->password = pnstrdup(password_val.val.string.val, password_val.val.string.len);
    }

    /* Get username for Redis 6+ ACL auth (optional) */
    {
        JsonbValue username_val;
        redis->username = NULL;
        if (extract_jsonb_value(config, REDIS_CONFIG_KEY_USERNAME, &username_val) &&
            username_val.type == jbvString && username_val.val.string.len > 0) {
            redis->username = pnstrdup(username_val.val.string.val, username_val.val.string.len);
        }
    }

    /* Get TLS settings (optional) */
    redis->tls = jsonb_get_bool(config, REDIS_CONFIG_KEY_TLS, false);

    redis->tls_ca_cert = NULL;
    if (extract_jsonb_value(config, REDIS_CONFIG_KEY_TLS_CA_CERT, &tls_ca_val) &&
        tls_ca_val.type == jbvString && tls_ca_val.val.string.len > 0) {
        redis->tls_ca_cert = pnstrdup(tls_ca_val.val.string.val, tls_ca_val.val.string.len);
    }

    redis->tls_cert = NULL;
    if (extract_jsonb_value(config, REDIS_CONFIG_KEY_TLS_CERT, &tls_cert_val) &&
        tls_cert_val.type == jbvString && tls_cert_val.val.string.len > 0) {
        redis->tls_cert = pnstrdup(tls_cert_val.val.string.val, tls_cert_val.val.string.len);
    }

    redis->tls_key = NULL;
    if (extract_jsonb_value(config, REDIS_CONFIG_KEY_TLS_KEY, &tls_key_val) &&
        tls_key_val.type == jbvString && tls_key_val.val.string.len > 0) {
        redis->tls_key = pnstrdup(tls_key_val.val.string.val, tls_key_val.val.string.len);
    }

    /* Validate TLS file accessibility if TLS is enabled */
    if (redis->tls) {
        if (redis->tls_ca_cert && access(redis->tls_ca_cert, R_OK) != 0) {
            ereport(WARNING,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                     errmsg("ulak: Redis TLS CA cert file not readable: %s", redis->tls_ca_cert)));
        }
        if (redis->tls_cert && access(redis->tls_cert, R_OK) != 0) {
            ereport(WARNING,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                     errmsg("ulak: Redis TLS client cert file not readable: %s", redis->tls_cert)));
        }
        if (redis->tls_key && access(redis->tls_key, R_OK) != 0) {
            ereport(WARNING,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                     errmsg("ulak: Redis TLS client key file not readable: %s", redis->tls_key)));
        }
    }

    /* Get NOMKSTREAM setting (optional, default false) */
    redis->nomkstream = jsonb_get_bool(config, REDIS_CONFIG_KEY_NOMKSTREAM, false);

    /* Get MAXLEN_APPROXIMATE setting (optional, default true for backwards compat) */
    redis->maxlen_approximate = jsonb_get_bool(config, REDIS_CONFIG_KEY_MAXLEN_APPROXIMATE, true);

    /* Get consumer_group (optional) */
    {
        JsonbValue cg_val;
        redis->consumer_group = NULL;
        if (extract_jsonb_value(config, REDIS_CONFIG_KEY_CONSUMER_GROUP, &cg_val) &&
            cg_val.type == jbvString && cg_val.val.string.len > 0) {
            redis->consumer_group = pnstrdup(cg_val.val.string.val, cg_val.val.string.len);
        }
    }

    /* Initialize pipelining batch state */
    {
        int capacity = ulak_batch_size > 0 ? ulak_batch_size : 200;
        redis->pending_messages = palloc(sizeof(RedisPendingMessage) * capacity);
        redis->pending_count = 0;
        redis->pending_capacity = capacity;
    }

    /* Initialize connection as NULL - will connect on first dispatch */
    redis->context = NULL;
    redis->ssl_context = NULL;
    redis->last_successful_op = 0;

    ulak_log("info", "Created Redis dispatcher for %s:%d (stream: %s, db: %d, tls: %s)",
             redis->host, redis->port, redis->stream_key, redis->db,
             redis->tls ? "enabled" : "disabled");

    return (Dispatcher *)redis;
}

/**
 * @brief Dispatch a single message to Redis Streams.
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param error_msg Output error message on failure
 * @return true on successful XADD, false on failure
 */
bool redis_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    RedisDispatcher *redis;
    char timestamp[32];
    time_t now;
    struct tm tm_buf;
    struct tm *tm_info;
    redisReply *reply;

    redis = (RedisDispatcher *)dispatcher->private_data;
    if (!redis || !payload) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis dispatcher or payload");
        return false;
    }

    /* Ensure we have a valid connection */
    if (!redis_dispatcher_ensure_connected(redis, error_msg)) {
        return false;
    }

    /* Get current timestamp using thread-safe gmtime_r */
    now = time(NULL);
    tm_info = gmtime_r(&now, &tm_buf);
    if (tm_info) {
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", tm_info);
    } else {
        /* Fallback timestamp if gmtime_r fails */
        snprintf(timestamp, sizeof(timestamp), "1970-01-01T00:00:00Z");
    }

    /* Build XADD command with optional NOMKSTREAM, MINID, and MAXLEN trimming.
     * Priority: NOMKSTREAM + MINID > NOMKSTREAM + MAXLEN > MINID > MAXLEN > plain
     * maxlen_approximate: true = "MAXLEN ~" (approximate), false = "MAXLEN" (exact) */
    if (redis->nomkstream && redis->minid > 0) {
        reply = (redisReply *)redisCommand(
            redis->context, "XADD %s NOMKSTREAM MINID ~ %lld-0 * %s %s %s %s", redis->stream_key,
            (long long)redis->minid, REDIS_STREAM_FIELD_PAYLOAD, payload,
            REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
    } else if (redis->nomkstream && redis->maxlen > 0) {
        if (redis->maxlen_approximate) {
            reply = (redisReply *)redisCommand(
                redis->context, "XADD %s NOMKSTREAM MAXLEN ~ %lld * %s %s %s %s", redis->stream_key,
                (long long)redis->maxlen, REDIS_STREAM_FIELD_PAYLOAD, payload,
                REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
        } else {
            reply = (redisReply *)redisCommand(
                redis->context, "XADD %s NOMKSTREAM MAXLEN %lld * %s %s %s %s", redis->stream_key,
                (long long)redis->maxlen, REDIS_STREAM_FIELD_PAYLOAD, payload,
                REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
        }
    } else if (redis->nomkstream) {
        reply = (redisReply *)redisCommand(redis->context, "XADD %s NOMKSTREAM * %s %s %s %s",
                                           redis->stream_key, REDIS_STREAM_FIELD_PAYLOAD, payload,
                                           REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
    } else if (redis->minid > 0) {
        reply = (redisReply *)redisCommand(redis->context, "XADD %s MINID ~ %lld-0 * %s %s %s %s",
                                           redis->stream_key, (long long)redis->minid,
                                           REDIS_STREAM_FIELD_PAYLOAD, payload,
                                           REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
    } else if (redis->maxlen > 0) {
        if (redis->maxlen_approximate) {
            reply = (redisReply *)redisCommand(
                redis->context, "XADD %s MAXLEN ~ %lld * %s %s %s %s", redis->stream_key,
                (long long)redis->maxlen, REDIS_STREAM_FIELD_PAYLOAD, payload,
                REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
        } else {
            reply = (redisReply *)redisCommand(redis->context, "XADD %s MAXLEN %lld * %s %s %s %s",
                                               redis->stream_key, (long long)redis->maxlen,
                                               REDIS_STREAM_FIELD_PAYLOAD, payload,
                                               REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
        }
    } else {
        reply = (redisReply *)redisCommand(redis->context, "XADD %s * %s %s %s %s",
                                           redis->stream_key, REDIS_STREAM_FIELD_PAYLOAD, payload,
                                           REDIS_STREAM_FIELD_TIMESTAMP, timestamp);
    }

    /* Check for errors */
    if (!reply) {
        /* Connection error */
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Redis XADD failed: %s", redis->context->errstr);
        }
        ulak_log("error", "Redis XADD failed: %s", redis->context->errstr);

        /* Mark connection as broken */
        redis_disconnect_internal(redis);
        return false;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        /* Classify error as permanent or retryable */
        const char *prefix =
            redis_is_permanent_error(reply->str) ? ERROR_PREFIX_PERMANENT : ERROR_PREFIX_RETRYABLE;
        if (error_msg) {
            *error_msg = psprintf("%s Redis XADD error: %s", prefix, reply->str);
        }
        ulak_log("error", "Redis XADD error: %s", reply->str);
        freeReplyObject(reply);
        return false;
    }

    if (reply->type != REDIS_REPLY_STRING) {
        /* Unexpected reply type */
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Redis unexpected reply type: %d", reply->type);
        }
        ulak_log("error", "Redis XADD unexpected reply type: %d", reply->type);
        freeReplyObject(reply);
        return false;
    }

    /* Success - capture stream ID and update last successful operation timestamp */
    if (reply->str) {
        strlcpy(redis->last_stream_id, reply->str, sizeof(redis->last_stream_id));
    } else {
        redis->last_stream_id[0] = '\0';
    }
    redis->last_successful_op = time(NULL);

    ulak_log("debug", "Redis XADD successful: stream=%s, entry_id=%s", redis->stream_key,
             reply->str ? reply->str : "(null)");

    freeReplyObject(reply);
    return true;
}

/**
 * @brief Validate Redis configuration from JSONB.
 *
 * Performs comprehensive validation of all config fields with type and range checks.
 *
 * @param config JSONB configuration to validate
 * @return true if configuration is valid, false otherwise
 */
bool redis_dispatcher_validate_config(Jsonb *config) {
    JsonbValue val;
    char *unknown_key = NULL;

    if (!config) {
        ulak_log("error", "Redis config: NULL config provided");
        return false;
    }

    /* Strict validation: reject unknown configuration keys */
    if (!jsonb_validate_keys(config, REDIS_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        ulak_log("error",
                 "Redis config contains unknown key '%s'. "
                 "Allowed keys: host, port, db, password, username, stream_key, "
                 "connect_timeout, command_timeout, maxlen, minid, tls, tls_ca_cert, "
                 "tls_cert, tls_key, nomkstream, maxlen_approximate, consumer_group",
                 unknown_key ? unknown_key : "unknown");
        if (unknown_key) {
            pfree(unknown_key);
        }
        return false;
    }

    /* === REQUIRED FIELDS === */

    /* host (required, non-empty string) */
    if (!extract_jsonb_value(config, REDIS_CONFIG_KEY_HOST, &val) || val.type != jbvString ||
        val.val.string.len == 0) {
        ulak_log("error", "Redis config: 'host' is required and must be non-empty string");
        return false;
    }

    /* stream_key (required, non-empty string) */
    if (!extract_jsonb_value(config, REDIS_CONFIG_KEY_STREAM_KEY, &val) || val.type != jbvString ||
        val.val.string.len == 0) {
        ulak_log("error", "Redis config: 'stream_key' is required and must be non-empty string");
        return false;
    }

    /* === OPTIONAL NUMERIC FIELDS WITH RANGE VALIDATION === */

    /* port (optional, 1-65535) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_PORT, 1, 65535, false)) {
        return false;
    }

    /* db (optional, 0-15) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_DB, 0, 15, false)) {
        return false;
    }

    /* connect_timeout (optional, 1-3600) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_CONNECT_TIMEOUT, 1, 3600, false)) {
        return false;
    }

    /* command_timeout (optional, 1-3600) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_COMMAND_TIMEOUT, 1, 3600, false)) {
        return false;
    }

    /* maxlen (optional, >= 0) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_MAXLEN, 0, INT64_MAX, false)) {
        return false;
    }

    /* minid (optional, >= 0) */
    if (!redis_validate_numeric_field(config, REDIS_CONFIG_KEY_MINID, 0, INT64_MAX, false)) {
        return false;
    }

    return true;
}

/**
 * @brief Clean up and destroy a Redis dispatcher.
 * @param dispatcher Dispatcher to clean up
 */
void redis_dispatcher_cleanup(Dispatcher *dispatcher) {
    RedisDispatcher *redis = (RedisDispatcher *)dispatcher->private_data;
    if (!redis)
        return;

    /* Disconnect from Redis */
    redis_disconnect_internal(redis);

    /* Free allocated strings */
    if (redis->host) {
        pfree(redis->host);
        redis->host = NULL;
    }

    if (redis->stream_key) {
        pfree(redis->stream_key);
        redis->stream_key = NULL;
    }

    /* Securely clear password before freeing (prevents compiler optimization) */
    if (redis->password) {
        explicit_bzero(redis->password, strlen(redis->password));
        pfree(redis->password);
        redis->password = NULL;
    }

    /* Securely clear username before freeing */
    if (redis->username) {
        explicit_bzero(redis->username, strlen(redis->username));
        pfree(redis->username);
        redis->username = NULL;
    }

    if (redis->consumer_group) {
        pfree(redis->consumer_group);
        redis->consumer_group = NULL;
    }

    /* Free pipelining batch state */
    if (redis->pending_messages) {
        pfree(redis->pending_messages);
        redis->pending_messages = NULL;
        redis->pending_count = 0;
        redis->pending_capacity = 0;
    }

    if (redis->tls_ca_cert) {
        pfree(redis->tls_ca_cert);
        redis->tls_ca_cert = NULL;
    }

    if (redis->tls_cert) {
        explicit_bzero(redis->tls_cert, strlen(redis->tls_cert));
        pfree(redis->tls_cert);
        redis->tls_cert = NULL;
    }

    if (redis->tls_key) {
        explicit_bzero(redis->tls_key, strlen(redis->tls_key));
        pfree(redis->tls_key);
        redis->tls_key = NULL;
    }

    /* Zero last_stream_id in case it contains sensitive info */
    explicit_bzero(redis->last_stream_id, sizeof(redis->last_stream_id));

    pfree(redis);
    ulak_log("debug", "Redis dispatcher cleaned up");
}

/**
 * @brief Extended dispatch with headers/metadata and DispatchResult capture.
 *
 * Captures the Redis stream ID from XADD response.
 *
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param headers Per-message JSONB headers (unused for Redis)
 * @param metadata JSONB metadata (unused for Redis)
 * @param result Output dispatch result with stream ID
 * @return true on success, false on failure
 */
bool redis_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                  Jsonb *metadata, DispatchResult *result) {
    char *error_msg = NULL;
    bool success;
    RedisDispatcher *redis;

    if (!dispatcher || !dispatcher->private_data) {
        if (result) {
            result->success = false;
            result->error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis dispatcher");
        }
        return false;
    }

    redis = (RedisDispatcher *)dispatcher->private_data;

    /* Clear last stream ID before dispatch */
    redis->last_stream_id[0] = '\0';

    success = redis_dispatcher_dispatch(dispatcher, payload, &error_msg);

    if (result) {
        result->success = success;
        if (!success && error_msg) {
            result->error_msg = error_msg;
        } else if (success && redis->last_stream_id[0] != '\0') {
            result->redis_stream_id = pstrdup(redis->last_stream_id);
            if (error_msg)
                pfree(error_msg);
        } else {
            if (error_msg)
                pfree(error_msg);
        }
    } else if (error_msg) {
        pfree(error_msg);
    }

    return success;
}

/**
 * @brief Produce: Append XADD to Redis pipeline (no roundtrip until flush).
 *
 * Uses same XADD command variants as synchronous dispatch.
 *
 * @param dispatcher Dispatcher instance
 * @param payload Message payload string
 * @param msg_id Queue message ID for tracking
 * @param error_msg Output error message on failure
 * @return true if command was appended to pipeline, false on failure
 */
bool redis_dispatcher_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                              char **error_msg) {
    RedisDispatcher *redis = (RedisDispatcher *)dispatcher->private_data;
    RedisPendingMessage *pm;
    char ts[32];
    int ret;

    if (!redis || !payload) {
        if (error_msg)
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis dispatcher");
        return false;
    }

    /* For pipelining: only connect if not connected. Do NOT call
     * ensure_connected which may send PING -- that would inject a PONG
     * reply into the pipeline, corrupting redisGetReply order in flush. */
    if (!redis->context || redis->context->err) {
        if (!redis_connect_internal(redis, error_msg))
            return false;
    }

    /* Grow capacity if needed */
    if (redis->pending_count >= redis->pending_capacity) {
        int new_cap = redis->pending_capacity * 2;
        redis->pending_messages =
            repalloc(redis->pending_messages, sizeof(RedisPendingMessage) * new_cap);
        redis->pending_capacity = new_cap;
    }

    /* Generate timestamp */
    {
        time_t now = time(NULL);
        struct tm tm_buf;
        gmtime_r(&now, &tm_buf);
        snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02dZ", tm_buf.tm_year + 1900,
                 tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
    }

    /* Build pipelined XADD command -- same variant logic as sync dispatch */
    if (redis->nomkstream && redis->minid > 0)
        ret = redisAppendCommand(redis->context, "XADD %s NOMKSTREAM MINID ~ %lld-0 * %s %s %s %s",
                                 redis->stream_key, (long long)redis->minid,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else if (redis->nomkstream && redis->maxlen > 0 && redis->maxlen_approximate)
        ret = redisAppendCommand(redis->context, "XADD %s NOMKSTREAM MAXLEN ~ %lld * %s %s %s %s",
                                 redis->stream_key, (long long)redis->maxlen,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else if (redis->nomkstream && redis->maxlen > 0)
        ret = redisAppendCommand(redis->context, "XADD %s NOMKSTREAM MAXLEN %lld * %s %s %s %s",
                                 redis->stream_key, (long long)redis->maxlen,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else if (redis->nomkstream)
        ret = redisAppendCommand(redis->context, "XADD %s NOMKSTREAM * %s %s %s %s",
                                 redis->stream_key, REDIS_STREAM_FIELD_PAYLOAD, payload,
                                 REDIS_STREAM_FIELD_TIMESTAMP, ts);
    else if (redis->minid > 0)
        ret = redisAppendCommand(redis->context, "XADD %s MINID ~ %lld-0 * %s %s %s %s",
                                 redis->stream_key, (long long)redis->minid,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else if (redis->maxlen > 0 && redis->maxlen_approximate)
        ret = redisAppendCommand(redis->context, "XADD %s MAXLEN ~ %lld * %s %s %s %s",
                                 redis->stream_key, (long long)redis->maxlen,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else if (redis->maxlen > 0)
        ret = redisAppendCommand(redis->context, "XADD %s MAXLEN %lld * %s %s %s %s",
                                 redis->stream_key, (long long)redis->maxlen,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);
    else
        ret = redisAppendCommand(redis->context, "XADD %s * %s %s %s %s", redis->stream_key,
                                 REDIS_STREAM_FIELD_PAYLOAD, payload, REDIS_STREAM_FIELD_TIMESTAMP,
                                 ts);

    if (ret != REDIS_OK) {
        if (error_msg)
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Failed to pipeline XADD: %s",
                                  redis->context->errstr);
        return false;
    }

    /* Track pending message */
    pm = &redis->pending_messages[redis->pending_count];
    pm->msg_id = msg_id;
    pm->success = false;
    pm->error[0] = '\0';
    pm->stream_id[0] = '\0';
    redis->pending_count++;

    return true;
}

/**
 * @brief Flush: Read all pipelined XADD replies and report results.
 * @param dispatcher Dispatcher instance
 * @param timeout_ms Timeout (unused, Redis pipeline flush is synchronous)
 * @param failed_ids Output array of failed message IDs
 * @param failed_count Output count of failed messages
 * @param failed_errors Output array of error strings for failed messages
 * @return Number of successfully delivered messages
 */
int redis_dispatcher_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                           int *failed_count, char ***failed_errors) {
    RedisDispatcher *redis = (RedisDispatcher *)dispatcher->private_data;
    int i, success_count = 0, fail_count = 0;
    int pending;
    redisReply *reply;

    (void)timeout_ms; /* Redis pipeline flush is synchronous */

    if (!redis || !redis->context) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    pending = redis->pending_count;
    if (pending == 0) {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
        return 0;
    }

    /* Read all replies -- pipeline was already flushed by redisAppendCommand */
    for (i = 0; i < pending; i++) {
        RedisPendingMessage *pm = &redis->pending_messages[i];

        if (redisGetReply(redis->context, (void **)&reply) != REDIS_OK || !reply) {
            /* Connection dead -- mark remaining as failed */
            const char *err = redis->context ? redis->context->errstr : "connection lost";
            int j;
            snprintf(pm->error, sizeof(pm->error), ERROR_PREFIX_RETRYABLE " %s", err);
            pm->success = false;
            fail_count++;

            /* Mark all remaining as failed too */
            for (j = i + 1; j < pending; j++) {
                RedisPendingMessage *rem = &redis->pending_messages[j];
                strlcpy(rem->error, pm->error, sizeof(rem->error));
                rem->success = false;
                fail_count++;
            }
            redis_disconnect_internal(redis);
            break;
        }

        if (reply->type == REDIS_REPLY_STRING) {
            pm->success = true;
            strlcpy(pm->stream_id, reply->str, sizeof(pm->stream_id));
            success_count++;
        } else if (reply->type == REDIS_REPLY_ERROR) {
            const char *prefix = redis_is_permanent_error(reply->str) ? ERROR_PREFIX_PERMANENT
                                                                      : ERROR_PREFIX_RETRYABLE;
            pm->success = false;
            snprintf(pm->error, sizeof(pm->error), "%s %s", prefix, reply->str);
            fail_count++;
        } else {
            pm->success = false;
            snprintf(pm->error, sizeof(pm->error),
                     ERROR_PREFIX_RETRYABLE " Unexpected reply type %d", reply->type);
            fail_count++;
        }
        freeReplyObject(reply);
    }

    /* Update last_successful_op if any success */
    if (success_count > 0) {
        redis->last_successful_op = time(NULL);
    }

    /* Build failed arrays */
    if (fail_count > 0 && failed_ids && failed_count) {
        int j = 0;
        *failed_ids = palloc(sizeof(int64) * fail_count);
        *failed_count = fail_count;
        if (failed_errors)
            *failed_errors = palloc(sizeof(char *) * fail_count);

        for (i = 0; i < pending; i++) {
            RedisPendingMessage *pm = &redis->pending_messages[i];
            if (!pm->success) {
                (*failed_ids)[j] = pm->msg_id;
                if (failed_errors && *failed_errors)
                    (*failed_errors)[j] = pstrdup(
                        pm->error[0] ? pm->error : ERROR_PREFIX_RETRYABLE " Redis dispatch failed");
                j++;
            }
        }
    } else {
        if (failed_count)
            *failed_count = 0;
        if (failed_errors)
            *failed_errors = NULL;
    }

    /* Reset pending count for next batch */
    redis->pending_count = 0;

    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher Dispatcher instance (unused)
 * @return Always true (Redis pipelining is always available)
 */
bool redis_dispatcher_supports_batch(Dispatcher *dispatcher) {
    (void)dispatcher;
    return true;
}
