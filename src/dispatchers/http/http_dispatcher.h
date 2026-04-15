/**
 * @file http_dispatcher.h
 * @brief HTTP/HTTPS webhook dispatcher with batch support.
 *
 * Clean Architecture: Interface Adapters Layer.
 * Implements synchronous single-message dispatch via curl easy handle
 * and asynchronous batch dispatch via curl_multi. Supports Standard
 * Webhooks signing, CloudEvents envelopes, mTLS, certificate pinning,
 * proxy tunneling, and pluggable authentication (Basic, Bearer,
 * OAuth2, AWS SigV4).
 */

#ifndef ULAK_DISPATCHERS_HTTP_DISPATCHER_H
#define ULAK_DISPATCHERS_HTTP_DISPATCHER_H

#include <curl/curl.h>
#include "dispatchers/dispatcher.h"
#include "postgres.h"
#include "utils/jsonb.h"

/** @brief Error buffer size for batch request tracking. */
#define HTTP_ERROR_BUFFER_SIZE 256

/** @brief Tracks an individual in-flight request during batch produce/flush. */
typedef struct HttpPendingRequest {
    int64 msg_id;                       /**< Message ID for result tracking. */
    CURL *easy_handle;                  /**< Individual curl handle. */
    struct curl_slist *headers;         /**< Request headers (must be freed). */
    char *payload;                      /**< Request payload copy. */
    char *response_body;                /**< Response buffer (for future use). */
    size_t response_size;               /**< Response size. */
    bool completed;                     /**< Request completed flag. */
    bool success;                       /**< Request success flag. */
    long http_code;                     /**< HTTP response code. */
    char error[HTTP_ERROR_BUFFER_SIZE]; /**< Error message buffer. */
} HttpPendingRequest;

/**
 * @brief HTTP protocol dispatcher state.
 *
 * Holds curl handles, endpoint configuration, TLS credentials,
 * authentication, proxy settings, and batch tracking arrays.
 */
typedef struct {
    Dispatcher base; /**< Inherited dispatcher base. */

    /** @name Endpoint configuration (parsed from JSONB) */
    /** @{ */
    char *url;             /**< Target webhook URL. */
    char *method;          /**< HTTP method (GET/POST/PUT/PATCH/DELETE). */
    Jsonb *headers;        /**< Static headers from endpoint config. */
    int32 timeout;         /**< Request timeout in seconds. */
    int32 connect_timeout; /**< Connection timeout in seconds. */
    /** @} */

    /** @name Standard Webhooks signing (optional) */
    /** @{ */
    char *signing_secret;   /**< Base64-decoded HMAC key, NULL if unsigned. */
    int signing_secret_len; /**< Length of decoded signing secret. */
    /** @} */

    /** @name CloudEvents envelope (optional) */
    /** @{ */
    int cloudevents_mode;   /**< CE_MODE_NONE, CE_MODE_BINARY, or CE_MODE_STRUCTURED. */
    char *cloudevents_type; /**< Event type (e.g. "com.myapp.order.created"). */
    /** @} */

    /** @name mTLS and certificate pinning (optional) */
    /** @{ */
    char *tls_client_cert;       /**< Path to client certificate PEM. */
    char *tls_client_key;        /**< Path to client private key PEM. */
    char *tls_ca_bundle;         /**< Path to custom CA bundle. */
    char *tls_pinned_public_key; /**< SHA256 pin: "sha256//base64hash=". */
    /** @} */

    /** @name Authentication (optional) */
    /** @{ */
    /**
     * Parsed from "auth" nested config. Type is HttpAuthConfig* defined
     * in http_auth.h. Using void* to avoid requiring http_auth.h in
     * this public header.
     */
    void *auth; /**< Pluggable auth config (Basic/Bearer/OAuth2/SigV4). */
    /** @} */

    /** @name Proxy configuration (optional) */
    /** @{ */
    char *proxy_url;       /**< Proxy URL (http/https/socks5 scheme). */
    long proxy_type;       /**< CURLPROXY_HTTP, CURLPROXY_HTTPS, or CURLPROXY_SOCKS5_HOSTNAME. */
    char *proxy_username;  /**< Proxy auth username (NULL if no auth). */
    char *proxy_password;  /**< Proxy auth password (zeroed on cleanup). */
    char *proxy_userpwd;   /**< Prebuilt "user:password" for CURLOPT_PROXYUSERPWD. */
    char *proxy_no_proxy;  /**< CURLOPT_NOPROXY bypass patterns. */
    char *proxy_ca_bundle; /**< CA bundle path for HTTPS proxy TLS. */
    bool proxy_ssl_verify; /**< Verify proxy TLS cert (default: true). */
    /** @} */

    /** @name Sync dispatch (single-message mode) */
    /** @{ */
    /**
     * Reusable curl easy handle. curl_easy_reset() preserves: connection
     * cache, DNS cache, TLS session cache.
     * Ref: curl.se/libcurl/c/curl_easy_reset.html
     */
    CURL *sync_handle; /**< Reusable curl handle for sync dispatch. */
    /** @} */

    /** @name Batch operations */
    /** @{ */
    CURLM *multi_handle;                  /**< curl_multi handle for batch operations. */
    HttpPendingRequest *pending_requests; /**< Array of in-flight requests. */
    int pending_count;                    /**< Current number of pending requests. */
    int pending_capacity;                 /**< Allocated capacity. */
    /** @} */
} HttpDispatcher;

/** @name Core dispatcher interface */
/** @{ */

/**
 * @brief Create an HTTP dispatcher from endpoint JSONB config.
 * @param config  Endpoint configuration (url, method, headers, etc.).
 * @return Allocated Dispatcher, or NULL on error.
 */
extern Dispatcher *http_dispatcher_create(Jsonb *config);

/**
 * @brief Dispatch a single message synchronously.
 * @param dispatcher  The HTTP dispatcher.
 * @param payload     JSON payload to send.
 * @param error_msg   Set on failure; caller must pfree.
 * @return true on success, false on failure.
 */
extern bool http_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg);

/**
 * @brief Validate HTTP endpoint JSONB configuration.
 * @param config  Endpoint configuration to validate.
 * @return true if valid, false on error.
 */
extern bool http_dispatcher_validate_config(Jsonb *config);

/**
 * @brief Clean up HTTP dispatcher, freeing all resources.
 * @param dispatcher  The HTTP dispatcher to destroy.
 */
extern void http_dispatcher_cleanup(Dispatcher *dispatcher);
/** @} */

/** @name Extended dispatch */
/** @{ */

/**
 * @brief Dispatch with per-message headers/metadata and response capture.
 * @param dispatcher  The HTTP dispatcher.
 * @param payload     JSON payload to send.
 * @param headers     Per-message headers (override endpoint config).
 * @param metadata    Per-message metadata.
 * @param result      Populated with dispatch result details.
 * @return true on success, false on failure.
 */
extern bool http_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                        Jsonb *metadata, DispatchResult *result);
/** @} */

/** @name Batch operations */
/** @{ */

/**
 * @brief Enqueue a message for batch dispatch (non-blocking).
 * @param dispatcher  The HTTP dispatcher.
 * @param payload     JSON payload to send.
 * @param msg_id      PostgreSQL queue row ID.
 * @param error_msg   Set on failure; caller must pfree.
 * @return true if enqueued, false on error.
 */
extern bool http_batch_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                               char **error_msg);

/**
 * @brief Flush all pending batch requests and collect results.
 * @param dispatcher    The HTTP dispatcher.
 * @param timeout_ms    Maximum wall-clock time to wait.
 * @param[out] failed_ids     palloc'd array of failed message IDs.
 * @param[out] failed_count   Number of failed messages.
 * @param[out] failed_errors  palloc'd array of error strings.
 * @return Number of successfully delivered messages.
 */
extern int http_batch_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids,
                            int *failed_count, char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher  The HTTP dispatcher.
 * @return Always true for HTTP.
 */
extern bool http_batch_supports(Dispatcher *dispatcher);
/** @} */

#endif /* ULAK_DISPATCHERS_HTTP_DISPATCHER_H */
