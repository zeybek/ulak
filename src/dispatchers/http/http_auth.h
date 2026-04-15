/**
 * @file http_auth.h
 * @brief Pluggable HTTP authentication subsystem.
 *
 * Supports Basic, Bearer, OAuth2 client-credentials, and AWS SigV4
 * authentication methods. Parsed once at dispatcher creation and
 * applied per-request. Sensitive credentials are zeroed before free.
 */

#ifndef ULAK_HTTP_AUTH_H
#define ULAK_HTTP_AUTH_H

#include <curl/curl.h>
#include "postgres.h"
#include "utils/jsonb.h"

/** @brief Authentication type enumeration. */
typedef enum {
    HTTP_AUTH_NONE = 0, /**< No authentication. */
    HTTP_AUTH_BASIC,    /**< HTTP Basic (username:password). */
    HTTP_AUTH_BEARER,   /**< Bearer token. */
    HTTP_AUTH_OAUTH2,   /**< OAuth2 client-credentials flow. */
    HTTP_AUTH_AWS_SIGV4 /**< AWS Signature Version 4. */
} HttpAuthType;

/**
 * @brief OAuth2 token cache (worker-local).
 *
 * Caches access tokens obtained via the client-credentials flow
 * to avoid re-fetching on every request.
 */
typedef struct {
    char *access_token;   /**< Cached token (palloc'd in TopMemoryContext). */
    int access_token_len; /**< Length of cached token. */
    time_t expires_at;    /**< Absolute expiry (unix seconds, with 60s safety buffer). */
} OAuth2TokenCache;

/**
 * @brief Authentication configuration.
 *
 * Parsed once at dispatcher creation from the "auth" nested JSONB
 * config. Only one union member is active based on the type field.
 * Sensitive fields are zeroed with explicit_bzero before pfree.
 */
typedef struct {
    HttpAuthType type; /**< Active authentication method. */

    union {
        /** @name Basic authentication */
        /** @{ */
        struct {
            char *username; /**< HTTP Basic username. */
            char *password; /**< HTTP Basic password (zeroed on cleanup). */
        } basic;
        /** @} */

        /** @name Bearer token authentication */
        /** @{ */
        struct {
            char *token; /**< Bearer token (zeroed on cleanup). */
        } bearer;
        /** @} */

        /** @name OAuth2 client-credentials authentication */
        /** @{ */
        struct {
            char *token_url;        /**< Token endpoint URL. */
            char *client_id;        /**< OAuth2 client ID. */
            char *client_secret;    /**< OAuth2 client secret (zeroed on cleanup). */
            char *scope;            /**< Requested scope (may be NULL). */
            OAuth2TokenCache cache; /**< Worker-local token cache. */
        } oauth2;
        /** @} */

        /** @name AWS Signature Version 4 authentication */
        /** @{ */
        struct {
            char *access_key;    /**< AWS access key ID. */
            char *secret_key;    /**< AWS secret access key (zeroed on cleanup). */
            char *region;        /**< AWS region (e.g. "us-east-1"). */
            char *service;       /**< AWS service name (e.g. "execute-api"). */
            char *session_token; /**< STS temporary session token (may be NULL). */
            char *sigv4_param;   /**< Prebuilt "aws:amz:{region}:{service}" for CURLOPT. */
        } aws_sigv4;
        /** @} */
    } config; /**< Auth-type-specific configuration. */
} HttpAuthConfig;

/** @name Configuration parsing */
/** @{ */

/**
 * @brief Parse auth config from endpoint JSONB.
 * @param endpoint_config  Full endpoint JSONB configuration.
 * @return palloc'd HttpAuthConfig, or NULL if no "auth" key present.
 */
extern HttpAuthConfig *http_auth_parse(Jsonb *endpoint_config);

/**
 * @brief Validate auth config (called from validate_config).
 * @param endpoint_config  Full endpoint JSONB configuration.
 * @return true if valid or absent, false on error.
 */
extern bool http_auth_validate(Jsonb *endpoint_config);
/** @} */

/** @name Per-request application */
/** @{ */

/**
 * @brief Apply auth to a curl handle and header list.
 *
 * For OAuth2, may perform synchronous token fetch (network I/O).
 *
 * @param auth        Parsed auth configuration.
 * @param curl        curl easy handle to configure.
 * @param headers     Existing header list.
 * @param auth_error  Set to true on fatal auth error.
 * @return Updated curl_slist, or NULL on fatal auth error.
 */
extern struct curl_slist *http_auth_apply(HttpAuthConfig *auth, CURL *curl,
                                          struct curl_slist *headers, bool *auth_error);

/**
 * @brief Invalidate cached OAuth2 token (call on 401 for retry).
 * @param auth  Auth configuration with OAuth2 cache.
 */
extern void http_auth_invalidate_token(HttpAuthConfig *auth);
/** @} */

/** @name Lifecycle */
/** @{ */

/**
 * @brief Cleanup auth config, zeroing all sensitive data.
 * @param auth  Auth configuration to destroy.
 */
extern void http_auth_cleanup(HttpAuthConfig *auth);

/**
 * @brief Return auth type name string (for logging).
 * @param type  Auth type enumeration value.
 * @return Static string: "none", "basic", "bearer", "oauth2", or "aws_sigv4".
 */
extern const char *http_auth_type_name(HttpAuthType type);
/** @} */

/** @name Protocol-specific apply functions */
/** @{ */

/**
 * @brief Apply OAuth2 authentication (implemented in http_auth_oauth2.c).
 * @param auth        Parsed auth configuration.
 * @param curl        curl easy handle.
 * @param headers     Existing header list.
 * @param auth_error  Set to true on fatal error.
 * @return Updated curl_slist, or NULL on error.
 */
extern struct curl_slist *http_auth_oauth2_apply(HttpAuthConfig *auth, CURL *curl,
                                                 struct curl_slist *headers, bool *auth_error);

/**
 * @brief Apply AWS SigV4 authentication (implemented in http_auth_sigv4.c).
 * @param auth        Parsed auth configuration.
 * @param curl        curl easy handle.
 * @param headers     Existing header list.
 * @param auth_error  Set to true on fatal error.
 * @return Updated curl_slist, or NULL on error.
 */
extern struct curl_slist *http_auth_sigv4_apply(HttpAuthConfig *auth, CURL *curl,
                                                struct curl_slist *headers, bool *auth_error);
/** @} */

#endif /* ULAK_HTTP_AUTH_H */
