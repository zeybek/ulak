/**
 * @file http_internal.h
 * @brief Internal declarations for the HTTP dispatcher module.
 *
 * @warning This header is for internal use within src/dispatchers/http/ only.
 *          Do not include from files outside this module.
 *
 * Contains SSRF protection, webhook signing, header building, curl
 * configuration, and batch operation helpers.
 */

#ifndef ULAK_HTTP_INTERNAL_H
#define ULAK_HTTP_INTERNAL_H

#include <curl/curl.h>
#include "http_auth.h"
#include "http_dispatcher.h"

/* HTTP Method Constants */
#define HTTP_METHOD_GET "GET"
#define HTTP_METHOD_POST "POST"
#define HTTP_METHOD_PUT "PUT"
#define HTTP_METHOD_PATCH "PATCH"
#define HTTP_METHOD_DELETE "DELETE"

/* Default HTTP Configuration */
#define DEFAULT_HTTP_METHOD HTTP_METHOD_POST
#define DEFAULT_CONTENT_TYPE "application/json"

/* Configuration Keys */
#define HTTP_CONFIG_KEY_URL "url"
#define HTTP_CONFIG_KEY_METHOD "method"
#define HTTP_CONFIG_KEY_HEADERS "headers"
#define HTTP_CONFIG_KEY_TIMEOUT "timeout"
#define HTTP_CONFIG_KEY_CONNECT_TIMEOUT "connect_timeout"
#define HTTP_CONFIG_KEY_SIGNING_SECRET "signing_secret"
#define HTTP_CONFIG_KEY_CLOUDEVENTS "cloudevents"
#define HTTP_CONFIG_KEY_CE_MODE "cloudevents_mode"
#define HTTP_CONFIG_KEY_CE_TYPE "cloudevents_type"
#define HTTP_CONFIG_KEY_RATE_LIMIT "rate_limit"
#define HTTP_CONFIG_KEY_AUTH "auth"
#define HTTP_CONFIG_KEY_TLS_CLIENT_CERT "tls_client_cert"
#define HTTP_CONFIG_KEY_TLS_CLIENT_KEY "tls_client_key"
#define HTTP_CONFIG_KEY_TLS_CA_BUNDLE "tls_ca_bundle"
#define HTTP_CONFIG_KEY_TLS_PINNED_KEY "tls_pinned_public_key"
#define HTTP_CONFIG_KEY_PROXY "proxy"
#define HTTP_CONFIG_KEY_AUTO_DISABLE_ON_GONE "auto_disable_on_gone"

/* Allowed configuration keys for strict validation */
static const char *HTTP_ALLOWED_CONFIG_KEYS[] __attribute__((unused)) = {
    HTTP_CONFIG_KEY_URL,
    HTTP_CONFIG_KEY_METHOD,
    HTTP_CONFIG_KEY_HEADERS,
    HTTP_CONFIG_KEY_TIMEOUT,
    HTTP_CONFIG_KEY_CONNECT_TIMEOUT,
    HTTP_CONFIG_KEY_SIGNING_SECRET,
    HTTP_CONFIG_KEY_CLOUDEVENTS,
    HTTP_CONFIG_KEY_CE_MODE,
    HTTP_CONFIG_KEY_CE_TYPE,
    HTTP_CONFIG_KEY_RATE_LIMIT,
    HTTP_CONFIG_KEY_AUTH,
    HTTP_CONFIG_KEY_TLS_CLIENT_CERT,
    HTTP_CONFIG_KEY_TLS_CLIENT_KEY,
    HTTP_CONFIG_KEY_TLS_CA_BUNDLE,
    HTTP_CONFIG_KEY_TLS_PINNED_KEY,
    HTTP_CONFIG_KEY_PROXY,
    HTTP_CONFIG_KEY_AUTO_DISABLE_ON_GONE,
    NULL /* NULL terminator */
};

/* Allowed URL schemes for SSRF protection */
#define URL_SCHEME_HTTP "http://"
#define URL_SCHEME_HTTPS "https://"

/* ============================================================================
 * Security Functions (http_security.c)
 * ============================================================================ */

/**
 * @brief Validate URL scheme for SSRF protection.
 *
 * Only http:// and https:// URLs are allowed.
 *
 * @param url      URL string to validate.
 * @param url_len  Length of the URL string.
 * @return true if URL scheme is safe, false otherwise.
 */
bool http_validate_url_scheme(const char *url, size_t url_len);

/**
 * @brief Check if URL targets an internal/private IP address (SSRF protection).
 *
 * Blocks RFC 1918, loopback, and link-local addresses unless
 * the http_allow_internal_urls GUC is enabled.
 *
 * @param url      URL string to check.
 * @param url_len  Length of the URL string.
 * @return true if the URL targets an internal address (should be blocked).
 */
bool http_is_internal_url(const char *url, size_t url_len);

/**
 * @brief Validate HTTP method.
 *
 * Only GET, POST, PUT, PATCH, DELETE are allowed.
 *
 * @param method  HTTP method string.
 * @param len     Length of the method string.
 * @return true if method is valid, false otherwise.
 */
bool http_is_valid_method(const char *method, size_t len);

/**
 * @brief Validate proxy URL scheme.
 *
 * Only http://, https://, and socks5:// are allowed.
 *
 * @param url      Proxy URL string to validate.
 * @param url_len  Length of the proxy URL string.
 * @return true if proxy scheme is valid, false otherwise.
 */
bool http_validate_proxy_url_scheme(const char *url, size_t url_len);

/* ============================================================================
 * Webhook Signing Functions (http_request.c)
 * ============================================================================ */

/**
 * @brief Add Standard Webhooks signature headers to curl_slist.
 *
 * Computes HMAC-SHA256 over "{msg_id}.{timestamp}.{payload}" and adds:
 * webhook-id, webhook-timestamp, webhook-signature.
 *
 * @param curl_headers     Existing header list.
 * @param http_dispatcher  Dispatcher with signing secret.
 * @param payload          Request payload to sign.
 * @param msg_id           Message ID for the webhook-id header.
 * @return Updated curl_slist with signature headers appended.
 */
struct curl_slist *http_add_webhook_signature(struct curl_slist *curl_headers,
                                              HttpDispatcher *http_dispatcher, const char *payload,
                                              int64 msg_id);

/* ============================================================================
 * Request Building Functions (http_request.c)
 * ============================================================================ */

/**
 * @brief Build curl headers from dispatcher configuration.
 * @param http_dispatcher  Dispatcher with header configuration.
 * @return curl_slist that must be freed with curl_slist_free_all().
 */
struct curl_slist *http_build_headers(HttpDispatcher *http_dispatcher);

/**
 * @brief Build merged curl headers from dispatcher config + per-message headers.
 *
 * Per-message headers override endpoint config headers with the same key.
 *
 * @param http_dispatcher      Dispatcher with base header configuration.
 * @param per_message_headers  Per-message header overrides.
 * @return curl_slist that must be freed with curl_slist_free_all().
 */
struct curl_slist *http_build_merged_headers(HttpDispatcher *http_dispatcher,
                                             Jsonb *per_message_headers);

/**
 * @brief Configure a curl easy handle with dispatcher settings.
 * @param curl             curl easy handle to configure.
 * @param http_dispatcher  Dispatcher with URL, method, TLS, proxy settings.
 * @param payload          Request payload.
 * @param curl_headers     Prepared header list.
 */
void http_configure_curl(CURL *curl, HttpDispatcher *http_dispatcher, const char *payload,
                         struct curl_slist *curl_headers);

/* ============================================================================
 * Batch Operations (http_batch.c)
 * ============================================================================ */

/**
 * @brief Add request to batch without sending (non-blocking).
 * @param dispatcher  The HTTP dispatcher.
 * @param payload     JSON payload to enqueue.
 * @param msg_id      PostgreSQL queue row ID.
 * @param error_msg   Set on failure; caller must pfree.
 * @return true if enqueued, false on error.
 */
bool http_batch_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                        char **error_msg);

/**
 * @brief Execute all pending requests and collect results.
 * @param dispatcher          The HTTP dispatcher.
 * @param timeout_ms          Maximum wall-clock time to wait.
 * @param[out] failed_ids     palloc'd array of failed message IDs.
 * @param[out] failed_count   Number of failed messages.
 * @param[out] failed_errors  palloc'd array of error strings.
 * @return Number of successfully delivered requests.
 */
int http_batch_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids, int *failed_count,
                     char ***failed_errors);

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher  The HTTP dispatcher.
 * @return Always true for HTTP.
 */
bool http_batch_supports(Dispatcher *dispatcher);

#endif /* ULAK_HTTP_INTERNAL_H */
