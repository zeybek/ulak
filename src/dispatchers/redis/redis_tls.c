/**
 * @file redis_tls.c
 * @brief Redis TLS/SSL handling.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles TLS context creation, SSL handshake, and cleanup for Redis connections.
 */

#include "redis_internal.h"

#include "postgres.h"
#include "utils/logging.h"

/**
 * @brief Create Redis SSL context from certificate files.
 * @param ca_cert CA certificate file path, or NULL
 * @param client_cert Client certificate file path (for mTLS), or NULL
 * @param client_key Client private key file path (for mTLS), or NULL
 * @param error_msg Output error message on failure
 * @return SSL context on success, NULL on failure
 */
redisSSLContext *redis_tls_create_context(const char *ca_cert, const char *client_cert,
                                          const char *client_key, char **error_msg) {
    redisSSLContext *ssl_ctx;
    redisSSLContextError ssl_error;

    /* Initialize SSL context */
    ssl_ctx = redisCreateSSLContext(ca_cert,     /* CA cert file path */
                                    NULL,        /* CA cert directory (not used) */
                                    client_cert, /* Client cert file (mTLS) */
                                    client_key,  /* Client key file (mTLS) */
                                    NULL,        /* Server name for SNI */
                                    &ssl_error);

    if (!ssl_ctx) {
        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis TLS context error: %s",
                                  redisSSLContextGetError(ssl_error));
        }
        ulak_log("error", "Failed to create Redis SSL context: %s",
                 redisSSLContextGetError(ssl_error));
        return NULL;
    }

    return ssl_ctx;
}

/**
 * @brief Initiate TLS handshake on an existing Redis connection.
 * @param ctx Redis connection context
 * @param ssl_ctx SSL context to use for handshake
 * @param error_msg Output error message on failure
 * @return true on success, false on failure
 */
bool redis_tls_initiate(redisContext *ctx, redisSSLContext *ssl_ctx, char **error_msg) {
    if (!ctx || !ssl_ctx) {
        if (error_msg) {
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis or SSL context");
        }
        return false;
    }

    if (redisInitiateSSLWithContext(ctx, ssl_ctx) != REDIS_OK) {
        if (error_msg) {
            *error_msg =
                psprintf(ERROR_PREFIX_RETRYABLE " Redis TLS handshake failed: %s", ctx->errstr);
        }
        ulak_log("error", "Redis TLS handshake failed: %s", ctx->errstr);
        return false;
    }

    return true;
}

/**
 * @brief Clean up Redis SSL context.
 *
 * Safe to call with NULL.
 *
 * @param ssl_ctx SSL context to free, or NULL
 */
void redis_tls_cleanup(redisSSLContext *ssl_ctx) {
    if (ssl_ctx) {
        redisFreeSSLContext(ssl_ctx);
    }
}
