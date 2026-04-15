/**
 * @file http_request.c
 * @brief CURL handle configuration and request building.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles CURL handle setup and HTTP header construction.
 */

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <string.h>
#include "config/guc.h"
#include "http_internal.h"
#include "lib/stringinfo.h"
#include "postgres.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/**
 * @private
 * @brief Base64 encode binary data.
 * @param data Binary data to encode.
 * @param len Length of input data.
 * @return palloc'd null-terminated base64 string. Caller must pfree.
 */
static char *base64_encode(const unsigned char *data, int len) {
    static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    char *out;
    int i, o;

    out = palloc(((len + 2) / 3) * 4 + 1);
    o = 0;
    for (i = 0; i < len; i += 3) {
        unsigned int n = ((unsigned int)data[i]) << 16;
        if (i + 1 < len)
            n |= ((unsigned int)data[i + 1]) << 8;
        if (i + 2 < len)
            n |= (unsigned int)data[i + 2];

        out[o++] = b64[(n >> 18) & 0x3F];
        out[o++] = b64[(n >> 12) & 0x3F];
        out[o++] = (i + 1 < len) ? b64[(n >> 6) & 0x3F] : '=';
        out[o++] = (i + 2 < len) ? b64[n & 0x3F] : '=';
    }
    out[o] = '\0';
    return out;
}

/**
 * @brief Add Standard Webhooks signature headers to a curl request.
 *
 * Standard Webhooks spec (standardwebhooks.com):
 *   webhook-id: unique message identifier
 *   webhook-timestamp: Unix timestamp (seconds)
 *   webhook-signature: v1,<base64(HMAC-SHA256(secret, "{id}.{ts}.{body}"))>
 *
 * @param curl_headers Existing curl header list.
 * @param http_dispatcher HTTP dispatcher with signing secret.
 * @param payload Message payload to sign.
 * @param msg_id Message ID for webhook-id header.
 * @return Updated curl header list with signature headers appended.
 */
struct curl_slist *http_add_webhook_signature(struct curl_slist *curl_headers,
                                              HttpDispatcher *http_dispatcher, const char *payload,
                                              int64 msg_id) {
    unsigned char hmac_result[EVP_MAX_MD_SIZE];
    unsigned int hmac_len;
    char *sign_content;
    char *signature_b64;
    char header_buf[512];
    long timestamp;
    char msg_id_str[32];

    if (!http_dispatcher->signing_secret || http_dispatcher->signing_secret_len == 0)
        return curl_headers;

    /* Generate webhook-id and timestamp */
    snprintf(msg_id_str, sizeof(msg_id_str), "msg_%lld", (long long)msg_id);
    timestamp = (long)time(NULL);

    /* Compute HMAC-SHA256 over "{msg_id}.{timestamp}.{payload}" */
    sign_content = psprintf("%s.%ld.%s", msg_id_str, timestamp, payload);

    HMAC(EVP_sha256(), http_dispatcher->signing_secret, http_dispatcher->signing_secret_len,
         (unsigned char *)sign_content, strlen(sign_content), hmac_result, &hmac_len);

    pfree(sign_content);

    /* Base64 encode the HMAC */
    signature_b64 = base64_encode(hmac_result, hmac_len);

    /* Zero HMAC result (sensitive) */
    explicit_bzero(hmac_result, sizeof(hmac_result));

    /* Add webhook-id header */
    snprintf(header_buf, sizeof(header_buf), "webhook-id: %s", msg_id_str);
    curl_headers = curl_slist_append(curl_headers, header_buf);

    /* Add webhook-timestamp header */
    snprintf(header_buf, sizeof(header_buf), "webhook-timestamp: %ld", timestamp);
    curl_headers = curl_slist_append(curl_headers, header_buf);

    /* Add webhook-signature header (v1 prefix per Standard Webhooks spec) */
    snprintf(header_buf, sizeof(header_buf), "webhook-signature: v1,%s", signature_b64);
    curl_headers = curl_slist_append(curl_headers, header_buf);

    pfree(signature_b64);

    return curl_headers;
}

/**
 * @brief Build curl headers from dispatcher configuration.
 * @param http_dispatcher HTTP dispatcher with header configuration.
 * @return curl_slist that must be freed with curl_slist_free_all().
 */
struct curl_slist *http_build_headers(HttpDispatcher *http_dispatcher) {
    struct curl_slist *curl_headers = NULL;

    /* Set default Content-Type header */
    curl_headers = curl_slist_append(curl_headers, "Content-Type: " DEFAULT_CONTENT_TYPE);

    /* Disable Expect: 100-continue — saves a round-trip on every POST */
    curl_headers = curl_slist_append(curl_headers, "Expect:");

    /* Add custom headers from config if present.
     * http_dispatcher->headers is set during creation only when "headers" key
     * exists in config. It points to the full config Jsonb (deep-copied into
     * DispatcherCacheContext). We iterate the root to find the "headers" sub-object. */
    if (http_dispatcher->headers) {
        JsonbIterator *it = JsonbIteratorInit(&http_dispatcher->headers->root);
        JsonbValue v;
        JsonbIteratorToken tok;
        bool in_headers = false;
        char *current_key = NULL;

        while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
            if (tok == WJB_KEY && v.type == jbvString) {
                size_t headers_key_len = strlen(HTTP_CONFIG_KEY_HEADERS);
                if (v.val.string.len == headers_key_len &&
                    strncmp(v.val.string.val, HTTP_CONFIG_KEY_HEADERS, headers_key_len) == 0) {
                    in_headers = true;
                } else if (in_headers) {
                    current_key = pnstrdup(v.val.string.val, v.val.string.len);
                }
            } else if (in_headers && tok == WJB_VALUE && v.type == jbvString && current_key) {
                char *header_value = pnstrdup(v.val.string.val, v.val.string.len);
                bool header_safe = true;
                const char *p;

                /* Check for header injection (CR/LF) */
                for (p = current_key; *p && header_safe; p++) {
                    if (*p == '\r' || *p == '\n') {
                        header_safe = false;
                    }
                }
                for (p = header_value; *p && header_safe; p++) {
                    if (*p == '\r' || *p == '\n') {
                        header_safe = false;
                    }
                }

                if (header_safe) {
                    StringInfoData header_str;
                    initStringInfo(&header_str);
                    appendStringInfo(&header_str, "%s: %s", current_key, header_value);
                    curl_headers = curl_slist_append(curl_headers, header_str.data);
                    pfree(header_str.data);
                } else {
                    ulak_log("warning", "Skipping HTTP header with invalid characters (CR/LF)");
                }

                pfree(header_value);
                pfree(current_key);
                current_key = NULL;
            } else if (tok == WJB_END_OBJECT && in_headers) {
                in_headers = false;
            }
        }

        if (current_key) {
            pfree(current_key);
        }
    }

    return curl_headers;
}

/**
 * @private
 * @brief No-op write callback that discards response body.
 *
 * Default libcurl behavior writes to stdout which floods worker output.
 * dispatch_ex() overrides this with its own response capture callback.
 *
 * @param ptr Response data pointer.
 * @param size Element size (always 1).
 * @param nmemb Number of elements.
 * @param userdata User data pointer (unused).
 * @return Number of bytes consumed.
 */
static size_t discard_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    (void)ptr;
    (void)userdata;
    return size * nmemb;
}

/**
 * @brief Configure a curl easy handle with dispatcher settings.
 * @param curl CURL easy handle to configure.
 * @param http_dispatcher HTTP dispatcher with endpoint settings.
 * @param payload Request payload string.
 * @param curl_headers curl_slist of HTTP headers.
 */
void http_configure_curl(CURL *curl, HttpDispatcher *http_dispatcher, const char *payload,
                         struct curl_slist *curl_headers) {
    /* Discard response body by default (prevents stdout flood).
     * dispatch_ex() overrides with response_write_callback after this. */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_write_callback);

    /* Set URL */
    curl_easy_setopt(curl, CURLOPT_URL, http_dispatcher->url);

    /* Set HTTP method */
    if (strcmp(http_dispatcher->method, HTTP_METHOD_POST) == 0) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
    } else if (strcmp(http_dispatcher->method, HTTP_METHOD_PUT) == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, HTTP_METHOD_PUT);
    } else if (strcmp(http_dispatcher->method, HTTP_METHOD_PATCH) == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, HTTP_METHOD_PATCH);
    } else if (strcmp(http_dispatcher->method, HTTP_METHOD_DELETE) == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, HTTP_METHOD_DELETE);
    } else if (strcmp(http_dispatcher->method, HTTP_METHOD_GET) == 0) {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    } else {
        /* Default to POST for unknown methods */
        ulak_log("warning", "Unknown HTTP method '%s', defaulting to POST",
                 http_dispatcher->method);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
    }

    /* Set payload (for methods that support body) */
    if (payload && strcmp(http_dispatcher->method, HTTP_METHOD_GET) != 0) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, strlen(payload));
    }

    /* Set headers */
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers);

    /* Set timeouts from config */
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)http_dispatcher->timeout);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)http_dispatcher->connect_timeout);

    /* Follow redirects - configurable via GUC.
     * SECURITY: Restrict redirect protocols to HTTP/HTTPS only to prevent
     * SSRF via redirect to file://, gopher://, etc. schemes.
     * Also prevents redirect to internal IPs via protocol downgrade. */
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, ulak_http_max_redirects > 0 ? 1L : 0L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, (long)ulak_http_max_redirects);
#if LIBCURL_VERSION_NUM >= 0x075500 /* 7.85.0: CURLOPT_REDIR_PROTOCOLS_STR */
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS_STR, "https");
#elif LIBCURL_VERSION_NUM >= 0x071304 /* 7.19.4: CURLOPT_REDIR_PROTOCOLS */
    curl_easy_setopt(curl, CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTPS);
#endif

    /* SSL/TLS verification - configurable via GUC */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, ulak_http_ssl_verify_peer ? 1L : 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, ulak_http_ssl_verify_host ? 2L : 0L);

    /* Performance optimizations for high-throughput dispatch */

    /* TCP keepalive - reuse connections instead of opening new ones */
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 15L);

    /* Disable Nagle algorithm - send small packets immediately */
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);

    /* DNS cache - avoid repeated lookups for same host */
    curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 300L);

    /* Connection reuse - don't close after each request */
    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
    curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);

    /* HTTP/2 over TLS (h2) — safer than h2c, avoids upgrade overhead */
#if LIBCURL_VERSION_NUM >= 0x072F00 /* 7.47.0 */
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
#else
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
#endif

    /* Wait for HTTP/2 multiplexing — reuse existing connection instead of opening new */
#if LIBCURL_VERSION_NUM >= 0x072B00 /* 7.43.0 */
    curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
#endif

    /* mTLS: client certificate authentication */
    if (http_dispatcher->tls_client_cert)
        curl_easy_setopt(curl, CURLOPT_SSLCERT, http_dispatcher->tls_client_cert);
    if (http_dispatcher->tls_client_key)
        curl_easy_setopt(curl, CURLOPT_SSLKEY, http_dispatcher->tls_client_key);

    /* Custom CA bundle (overrides system default) */
    if (http_dispatcher->tls_ca_bundle)
        curl_easy_setopt(curl, CURLOPT_CAINFO, http_dispatcher->tls_ca_bundle);

    /* Certificate pinning — reject if server cert doesn't match pin */
    if (http_dispatcher->tls_pinned_public_key)
        curl_easy_setopt(curl, CURLOPT_PINNEDPUBLICKEY, http_dispatcher->tls_pinned_public_key);

    /* Proxy configuration */
    if (http_dispatcher->proxy_url) {
        curl_easy_setopt(curl, CURLOPT_PROXY, http_dispatcher->proxy_url);
        curl_easy_setopt(curl, CURLOPT_PROXYTYPE, http_dispatcher->proxy_type);

        /* Proxy authentication (credentials separate from URL for security) */
        if (http_dispatcher->proxy_userpwd) {
            curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, http_dispatcher->proxy_userpwd);
            curl_easy_setopt(curl, CURLOPT_PROXYAUTH, (long)(CURLAUTH_BASIC | CURLAUTH_DIGEST));
        }

        /* No-proxy bypass patterns */
        if (http_dispatcher->proxy_no_proxy)
            curl_easy_setopt(curl, CURLOPT_NOPROXY, http_dispatcher->proxy_no_proxy);

        /* HTTPS proxy TLS settings */
        if (http_dispatcher->proxy_ca_bundle)
            curl_easy_setopt(curl, CURLOPT_PROXY_CAINFO, http_dispatcher->proxy_ca_bundle);

        curl_easy_setopt(curl, CURLOPT_PROXY_SSL_VERIFYPEER,
                         http_dispatcher->proxy_ssl_verify ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_PROXY_SSL_VERIFYHOST,
                         http_dispatcher->proxy_ssl_verify ? 2L : 0L);
    }

    /* Safe for multi-threaded background workers — disable signal-based timeouts */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
}

/**
 * @brief Build merged curl headers from dispatcher config and per-message headers.
 *
 * Per-message headers override endpoint config headers with the same key.
 *
 * @param http_dispatcher HTTP dispatcher with base header configuration.
 * @param per_message_headers Per-message JSONB headers (may be NULL).
 * @return curl_slist that must be freed with curl_slist_free_all().
 */
struct curl_slist *http_build_merged_headers(HttpDispatcher *http_dispatcher,
                                             Jsonb *per_message_headers) {
    struct curl_slist *curl_headers = NULL;

    /* Set default Content-Type header */
    curl_headers = curl_slist_append(curl_headers, "Content-Type: " DEFAULT_CONTENT_TYPE);

    /* Disable Expect: 100-continue — saves a round-trip on every POST */
    curl_headers = curl_slist_append(curl_headers, "Expect:");

    /* First, add headers from endpoint config (base headers) */
    if (http_dispatcher->headers) {
        JsonbIterator *it = JsonbIteratorInit(&http_dispatcher->headers->root);
        JsonbValue v;
        JsonbIteratorToken tok;
        bool in_headers = false;
        char *current_key = NULL;

        while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
            if (tok == WJB_KEY && v.type == jbvString) {
                size_t headers_key_len = strlen(HTTP_CONFIG_KEY_HEADERS);
                if (v.val.string.len == headers_key_len &&
                    strncmp(v.val.string.val, HTTP_CONFIG_KEY_HEADERS, headers_key_len) == 0) {
                    in_headers = true;
                } else if (in_headers) {
                    current_key = pnstrdup(v.val.string.val, v.val.string.len);
                }
            } else if (in_headers && tok == WJB_VALUE && v.type == jbvString && current_key) {
                /* Check if per-message headers has this key - if so, skip the endpoint header */
                bool override_found = false;
                if (per_message_headers) {
                    JsonbValue check_val;
                    if (extract_jsonb_value(per_message_headers, current_key, &check_val)) {
                        override_found = true; /* Per-message will override this */
                    }
                }

                if (!override_found) {
                    char *header_value = pnstrdup(v.val.string.val, v.val.string.len);
                    bool header_safe = true;
                    const char *p;

                    /* Check for header injection (CR/LF) */
                    for (p = current_key; *p && header_safe; p++) {
                        if (*p == '\r' || *p == '\n')
                            header_safe = false;
                    }
                    for (p = header_value; *p && header_safe; p++) {
                        if (*p == '\r' || *p == '\n')
                            header_safe = false;
                    }

                    if (header_safe) {
                        StringInfoData header_str;
                        initStringInfo(&header_str);
                        appendStringInfo(&header_str, "%s: %s", current_key, header_value);
                        curl_headers = curl_slist_append(curl_headers, header_str.data);
                        pfree(header_str.data);
                    }
                    pfree(header_value);
                }

                pfree(current_key);
                current_key = NULL;
            } else if (tok == WJB_END_OBJECT && in_headers) {
                in_headers = false;
            }
        }
        if (current_key)
            pfree(current_key);
    }

    /* Then, add per-message headers (override) */
    if (per_message_headers) {
        JsonbIterator *it = JsonbIteratorInit(&per_message_headers->root);
        JsonbValue v;
        JsonbIteratorToken tok;
        char *current_key = NULL;

        while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
            if (tok == WJB_KEY && v.type == jbvString) {
                current_key = pnstrdup(v.val.string.val, v.val.string.len);
            } else if (tok == WJB_VALUE && v.type == jbvString && current_key) {
                char *header_value = pnstrdup(v.val.string.val, v.val.string.len);
                bool header_safe = true;
                const char *p;

                /* Check for header injection (CR/LF) */
                for (p = current_key; *p && header_safe; p++) {
                    if (*p == '\r' || *p == '\n')
                        header_safe = false;
                }
                for (p = header_value; *p && header_safe; p++) {
                    if (*p == '\r' || *p == '\n')
                        header_safe = false;
                }

                if (header_safe) {
                    StringInfoData header_str;
                    initStringInfo(&header_str);
                    appendStringInfo(&header_str, "%s: %s", current_key, header_value);
                    curl_headers = curl_slist_append(curl_headers, header_str.data);
                    pfree(header_str.data);
                } else {
                    ulak_log("warning",
                             "Skipping per-message header with invalid characters (CR/LF)");
                }

                pfree(header_value);
                pfree(current_key);
                current_key = NULL;
            }
        }
        if (current_key)
            pfree(current_key);
    }

    return curl_headers;
}
