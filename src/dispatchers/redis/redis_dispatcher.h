/**
 * @file redis_dispatcher.h
 * @brief Redis Streams dispatcher using hiredis with TLS support.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Dispatches messages via XADD to Redis Streams. Supports TLS,
 * authentication, custom field mapping, and connection pooling.
 */

#ifndef ULAK_DISPATCHERS_REDIS_DISPATCHER_H
#define ULAK_DISPATCHERS_REDIS_DISPATCHER_H

#include "dispatchers/dispatcher.h"
#include "postgres.h"
#include "utils/jsonb.h"

#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>
#include <time.h>

/* Redis Configuration Keys */
#define REDIS_CONFIG_KEY_HOST "host"
#define REDIS_CONFIG_KEY_PORT "port"
#define REDIS_CONFIG_KEY_DB "db"
#define REDIS_CONFIG_KEY_PASSWORD "password"
#define REDIS_CONFIG_KEY_STREAM_KEY "stream_key"
#define REDIS_CONFIG_KEY_CONNECT_TIMEOUT "connect_timeout"
#define REDIS_CONFIG_KEY_COMMAND_TIMEOUT "command_timeout"
#define REDIS_CONFIG_KEY_MAXLEN "maxlen"
#define REDIS_CONFIG_KEY_TLS "tls"
#define REDIS_CONFIG_KEY_TLS_CA_CERT "tls_ca_cert"
#define REDIS_CONFIG_KEY_TLS_CERT "tls_cert"
#define REDIS_CONFIG_KEY_TLS_KEY "tls_key"
#define REDIS_CONFIG_KEY_USERNAME "username"
#define REDIS_CONFIG_KEY_NOMKSTREAM "nomkstream"
#define REDIS_CONFIG_KEY_MAXLEN_APPROXIMATE "maxlen_approximate"
#define REDIS_CONFIG_KEY_MINID "minid"
#define REDIS_CONFIG_KEY_CONSUMER_GROUP "consumer_group"

/* Redis Defaults */
#define REDIS_DEFAULT_PORT 6379
#define REDIS_DEFAULT_DB 0
#define REDIS_DEFAULT_CONNECT_TIMEOUT 5
#define REDIS_DEFAULT_COMMAND_TIMEOUT 30
#define REDIS_PING_INTERVAL_SECONDS 30 /* Interval for connection health checks */

/* Redis Stream Field Names */
#define REDIS_STREAM_FIELD_PAYLOAD "payload"
#define REDIS_STREAM_FIELD_TIMESTAMP "ts"

/** @brief Tracks a single in-flight message during pipelined batch dispatch. */
typedef struct RedisPendingMessage {
    int64 msg_id;       /**< PostgreSQL queue row ID. */
    bool success;       /**< Delivery confirmed by server. */
    char error[256];    /**< Error text (fixed buffer, no palloc). */
    char stream_id[64]; /**< Stream ID returned by XADD. */
} RedisPendingMessage;

/**
 * @brief Redis Streams protocol dispatcher state.
 *
 * Holds connection, TLS context, stream configuration,
 * authentication credentials, and pipelining batch state.
 */
typedef struct {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Connection settings (parsed from JSONB) */
    /** @{ */
    char *host;          /**< Redis server hostname. */
    int port;            /**< Redis server port. */
    int db;              /**< Redis database number (SELECT). */
    char *password;      /**< Authentication password (zeroed on cleanup). */
    char *username;      /**< ACL username for Redis 6+ AUTH. */
    char *stream_key;    /**< Target stream key for XADD. */
    int connect_timeout; /**< TCP connection timeout in seconds. */
    int command_timeout; /**< Per-command timeout in seconds. */
    int64 maxlen;        /**< Stream max length (0 = unlimited). */
    int64 minid;         /**< Stream MINID trimming threshold (0 = disabled). */
    /** @} */

    /** @name TLS configuration */
    /** @{ */
    bool tls;          /**< Enable TLS handshake. */
    char *tls_ca_cert; /**< CA certificate path. */
    char *tls_cert;    /**< Client certificate path (mTLS). */
    char *tls_key;     /**< Client key path (zeroed on cleanup). */
    /** @} */

    /** @name Stream options */
    /** @{ */
    bool nomkstream;         /**< If true, XADD fails if stream doesn't exist. */
    bool maxlen_approximate; /**< If true, use MAXLEN ~ (approximate), else exact. */
    char *consumer_group;    /**< Consumer group to auto-create on connect. */
    /** @} */

    /** @name Runtime connection state */
    /** @{ */
    redisContext *context;        /**< Active hiredis connection (NULL if disconnected). */
    redisSSLContext *ssl_context; /**< TLS context (NULL if TLS disabled). */
    /** @} */

    /** @name Connection health tracking */
    /** @{ */
    time_t last_successful_op; /**< Time of last successful Redis operation. */
    /** @} */

    /** @name Last synchronous dispatch result (for dispatch_ex) */
    /** @{ */
    char last_stream_id[64]; /**< Stream ID from most recent XADD. */
    /** @} */

    /** @name Pipelining batch state */
    /** @{ */
    struct RedisPendingMessage *pending_messages; /**< Array of in-flight messages. */
    int pending_count;                            /**< Current number of pending messages. */
    int pending_capacity;                         /**< Allocated capacity (grows via repalloc). */
    /** @} */
} RedisDispatcher;

/** @name Core dispatcher interface functions */
/** @{ */

/**
 * @brief Create a Redis dispatcher from endpoint JSONB config.
 *
 * @param config  JSONB object containing connection and stream settings.
 * @return Allocated Dispatcher, or NULL on configuration error.
 */
extern Dispatcher *redis_dispatcher_create(Jsonb *config);

/**
 * @brief Dispatch a single message via XADD to a Redis Stream.
 *
 * @param dispatcher  The Redis dispatcher.
 * @param payload     Message payload string.
 * @param error_msg   OUT - error description on failure (caller must pfree).
 * @return true on successful delivery, false on error.
 */
extern bool redis_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload,
                                      char **error_msg);

/**
 * @brief Validate Redis endpoint JSONB config.
 *
 * @param config  JSONB object to validate.
 * @return true if configuration is valid, false otherwise.
 */
extern bool redis_dispatcher_validate_config(Jsonb *config);

/**
 * @brief Release all resources held by the Redis dispatcher.
 *
 * Disconnects from Redis, frees TLS context, and zeroes sensitive data.
 *
 * @param dispatcher  The Redis dispatcher to clean up.
 */
extern void redis_dispatcher_cleanup(Dispatcher *dispatcher);

/**
 * @brief Extended dispatch with headers, metadata, and result capture.
 *
 * Captures the XADD stream ID in the DispatchResult.
 *
 * @param dispatcher  The Redis dispatcher.
 * @param payload     Message payload string.
 * @param headers     Optional JSONB headers (mapped to stream fields).
 * @param metadata    Optional JSONB metadata.
 * @param result      OUT - dispatch result with stream ID.
 * @return true on successful delivery, false on error.
 */
extern bool redis_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload,
                                         Jsonb *headers, Jsonb *metadata, DispatchResult *result);

/** @} */

/** @name Connection helper functions */
/** @{ */

/**
 * @brief Ensure connection is valid, reconnect if necessary.
 *
 * @param redis      The Redis dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true if connected (or reconnection succeeded).
 */
extern bool redis_dispatcher_ensure_connected(RedisDispatcher *redis, char **error_msg);

/**
 * @brief Force reconnection to Redis server.
 *
 * Tears down existing connection and establishes a new one.
 *
 * @param redis      The Redis dispatcher.
 * @param error_msg  OUT - error description on failure.
 * @return true on successful reconnection, false on error.
 */
extern bool redis_dispatcher_reconnect(RedisDispatcher *redis, char **error_msg);

/** @} */

#endif /* ULAK_DISPATCHERS_REDIS_DISPATCHER_H */
