/**
 * @file redis_tls.c
 * @brief Redis TLS/SSL handling.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles TLS context creation, SSL handshake, and cleanup for Redis connections.
 *
 * The SSL objects are built here instead of with redisCreateSSLContext():
 * hiredis uses its server name only for SNI and checks the certificate chain,
 * not the name, so any certificate from a trusted CA would be accepted.
 * redisInitiateSSL() takes a caller-made SSL with the name check set.
 */

#include "redis_internal.h"

#include <arpa/inet.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

#include "postgres.h"
#include "utils/logging.h"

/**
 * @private
 * @brief Reason string of the most recent OpenSSL error on this thread.
 * @return Static string; never NULL.
 */
static const char *redis_tls_openssl_error(void) {
    unsigned long err = ERR_peek_last_error();
    const char *reason = err ? ERR_reason_error_string(err) : NULL;

    return reason ? reason : "unknown error";
}

/**
 * @brief Create Redis SSL context from certificate files.
 * @param ca_cert CA certificate file path, or NULL for the system store
 * @param client_cert Client certificate file path (for mTLS), or NULL
 * @param client_key Client private key file path (for mTLS), or NULL
 * @param error_msg Output error message on failure
 * @return SSL context on success, NULL on failure
 */
SSL_CTX *redis_tls_create_context(const char *ca_cert, const char *client_cert,
                                  const char *client_key, char **error_msg) {
    SSL_CTX *ssl_ctx;
    const char *failed = NULL;

    ERR_clear_error();
    ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx) {
        failed = "cannot create TLS context";
    } else {
        /* Same defaults as redisCreateSSLContext(): verify the peer's chain */
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

        if ((ca_cert ? SSL_CTX_load_verify_locations(ssl_ctx, ca_cert, NULL)
                     : SSL_CTX_set_default_verify_paths(ssl_ctx)) != 1)
            failed = "cannot load CA certificates";
        else if ((client_cert == NULL) != (client_key == NULL))
            failed = "client certificate and key must both be set";
        else if (client_cert && SSL_CTX_use_certificate_chain_file(ssl_ctx, client_cert) != 1)
            failed = "cannot load client certificate";
        else if (client_key &&
                 SSL_CTX_use_PrivateKey_file(ssl_ctx, client_key, SSL_FILETYPE_PEM) != 1)
            failed = "cannot load client private key";
    }

    if (failed) {
        const char *detail = redis_tls_openssl_error();

        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis TLS context error: %s (%s)",
                                  failed, detail);
        }
        ulak_log("error", "Failed to create Redis SSL context: %s (%s)", failed, detail);
        if (ssl_ctx)
            SSL_CTX_free(ssl_ctx);
        ERR_clear_error();
        return NULL;
    }

    return ssl_ctx;
}

/**
 * @brief Initiate TLS handshake on an existing Redis connection.
 * @param ctx Redis connection context
 * @param ssl_ctx SSL context to use for handshake
 * @param host Configured Redis host, used for SNI and the certificate name check
 * @param error_msg Output error message on failure
 * @return true on success, false on failure
 */
bool redis_tls_initiate(redisContext *ctx, SSL_CTX *ssl_ctx, const char *host, char **error_msg) {
    SSL *ssl;
    unsigned char addr[sizeof(struct in6_addr)];
    bool is_ip;
    bool name_set;

    if (!ctx || !ssl_ctx || !host) {
        if (error_msg) {
            *error_msg = pstrdup(ERROR_PREFIX_PERMANENT " Invalid Redis or SSL context");
        }
        return false;
    }

    ERR_clear_error();
    ssl = SSL_new(ssl_ctx);
    if (!ssl) {
        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis TLS handshake failed: %s",
                                  redis_tls_openssl_error());
        }
        ERR_clear_error();
        return false;
    }

    /* An IP literal is matched against the certificate's IP addresses and is
     * not sent as SNI (RFC 6066 §3 allows host names only) */
    is_ip = inet_pton(AF_INET, host, addr) == 1 || inet_pton(AF_INET6, host, addr) == 1;
    if (is_ip)
        name_set = X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), host) == 1;
    else
        name_set = SSL_set_tlsext_host_name(ssl, host) == 1 && SSL_set1_host(ssl, host) == 1;

    if (!name_set) {
        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE
                                  " Redis TLS handshake failed: cannot set server name %s",
                                  host);
        }
        SSL_free(ssl);
        ERR_clear_error();
        return false;
    }

    /* On success the context owns ssl (freed by redisFree); on failure we do */
    if (redisInitiateSSL(ctx, ssl) != REDIS_OK) {
        long verify = SSL_get_verify_result(ssl);
        const char *reason =
            verify != X509_V_OK ? X509_verify_cert_error_string(verify) : ctx->errstr;

        if (error_msg) {
            *error_msg = psprintf(ERROR_PREFIX_RETRYABLE " Redis TLS handshake failed: %s", reason);
        }
        ulak_log("error", "Redis TLS handshake failed: %s", reason);
        SSL_free(ssl);
        ERR_clear_error();
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
void redis_tls_cleanup(SSL_CTX *ssl_ctx) {
    if (ssl_ctx) {
        SSL_CTX_free(ssl_ctx);
    }
}
