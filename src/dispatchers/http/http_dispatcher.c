/**
 * @file http_dispatcher.c
 * @brief HTTP protocol dispatcher implementation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles HTTP/HTTPS webhook message dispatching using libcurl.
 * Supports both synchronous (dispatch) and batch (produce/flush) modes.
 *
 * This is the main dispatcher entry point. Implementation details are in:
 *   - http_security.c: SSRF protection and URL validation
 *   - http_request.c: CURL handle configuration and header building
 *   - http_sync.c: Synchronous single message dispatch
 *   - http_batch.c: Batch operations using curl_multi
 */

#include <string.h>
#include "config/guc.h"
#include "core/entities.h"
#include "http_internal.h"
#include "postgres.h"
#include "utils/cloudevents.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/** @brief HTTP Dispatcher Operations - with batch support and dispatch_ex. */
static DispatcherOperations http_dispatcher_ops = {.dispatch = http_dispatcher_dispatch,
                                                   .validate_config =
                                                       http_dispatcher_validate_config,
                                                   .cleanup = http_dispatcher_cleanup,
                                                   /* Batch operations enabled */
                                                   .produce = http_batch_produce,
                                                   .flush = http_batch_flush,
                                                   .supports_batch = http_batch_supports,
                                                   /* Extended dispatch with response capture */
                                                   .dispatch_ex = http_dispatcher_dispatch_ex,
                                                   .produce_ex = NULL};

/**
 * @brief Create an HTTP dispatcher from JSONB endpoint configuration.
 * @param config JSONB endpoint configuration containing url, method, headers, etc.
 * @return Newly allocated Dispatcher, or NULL on invalid configuration.
 */
Dispatcher *http_dispatcher_create(Jsonb *config) {
    HttpDispatcher *http_dispatcher;
    JsonbValue url_val, method_val, headers_val;
    int initial_capacity;

    if (!http_dispatcher_validate_config(config)) {
        ulak_log("error", "Invalid HTTP dispatcher configuration");
        return NULL;
    }

    /* palloc0 never returns NULL — it ereport(ERROR)s on failure */
    http_dispatcher = (HttpDispatcher *)palloc0(sizeof(HttpDispatcher));

    /* Initialize base dispatcher */
    http_dispatcher->base.protocol = PROTOCOL_TYPE_HTTP;
    http_dispatcher->base.config = config;
    http_dispatcher->base.ops = &http_dispatcher_ops;
    http_dispatcher->base.private_data = http_dispatcher;

    /* Parse configuration - use stack-allocated JsonbValues (no memory leak) */

    /* Get URL (required) */
    if (!extract_jsonb_value(config, HTTP_CONFIG_KEY_URL, &url_val) || url_val.type != jbvString) {
        ulak_log("error", "HTTP config missing or invalid 'url'");
        pfree(http_dispatcher);
        return NULL;
    }
    http_dispatcher->url = pnstrdup(url_val.val.string.val, url_val.val.string.len);

    /* Get method (optional, default to POST) */
    http_dispatcher->method = pstrdup(DEFAULT_HTTP_METHOD);
    if (extract_jsonb_value(config, HTTP_CONFIG_KEY_METHOD, &method_val) &&
        method_val.type == jbvString) {
        pfree(http_dispatcher->method);
        http_dispatcher->method = pnstrdup(method_val.val.string.val, method_val.val.string.len);
    }

    /* Get timeout values from config (endpoint config overrides GUC) */
    http_dispatcher->timeout = jsonb_get_int32(config, HTTP_CONFIG_KEY_TIMEOUT, ulak_http_timeout);
    http_dispatcher->connect_timeout =
        jsonb_get_int32(config, HTTP_CONFIG_KEY_CONNECT_TIMEOUT, ulak_http_connect_timeout);

    /* Get headers (optional) - reference to parent config (which is deep-copied
     * into DispatcherCacheContext by worker.c:dispatcher_cache_get_or_create).
     * findJsonbValueFromContainer returns jbvBinary for nested objects. */
    http_dispatcher->headers = NULL;
    if (extract_jsonb_value(config, HTTP_CONFIG_KEY_HEADERS, &headers_val) &&
        headers_val.type == jbvBinary) {
        http_dispatcher->headers = config;
    }

    /* Get signing secret (optional — enables Standard Webhooks signing) */
    http_dispatcher->signing_secret = NULL;
    http_dispatcher->signing_secret_len = 0;
    {
        JsonbValue secret_val;
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_SIGNING_SECRET, &secret_val) &&
            secret_val.type == jbvString && secret_val.val.string.len > 0) {
            char *raw_secret = pnstrdup(secret_val.val.string.val, secret_val.val.string.len);
            /* Store raw secret — the signing function handles encoding */
            http_dispatcher->signing_secret = raw_secret;
            http_dispatcher->signing_secret_len = strlen(raw_secret);
            ulak_log("info", "Standard Webhooks signing enabled for %s", http_dispatcher->url);
        }
    }

    /* Get CloudEvents configuration (optional) */
    http_dispatcher->cloudevents_mode = CE_MODE_NONE;
    http_dispatcher->cloudevents_type = NULL;
    {
        JsonbValue ce_enabled_val;
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_CLOUDEVENTS, &ce_enabled_val) &&
            ce_enabled_val.type == jbvBool && ce_enabled_val.val.boolean) {
            /* CloudEvents enabled — parse mode and type */
            JsonbValue ce_mode_val, ce_type_val;

            if (extract_jsonb_value(config, HTTP_CONFIG_KEY_CE_MODE, &ce_mode_val) &&
                ce_mode_val.type == jbvString) {
                char *mode_str = pnstrdup(ce_mode_val.val.string.val, ce_mode_val.val.string.len);
                http_dispatcher->cloudevents_mode = cloudevents_parse_mode(mode_str);
                pfree(mode_str);
            } else {
                http_dispatcher->cloudevents_mode = CE_MODE_BINARY; /* Default */
            }

            if (extract_jsonb_value(config, HTTP_CONFIG_KEY_CE_TYPE, &ce_type_val) &&
                ce_type_val.type == jbvString) {
                http_dispatcher->cloudevents_type =
                    pnstrdup(ce_type_val.val.string.val, ce_type_val.val.string.len);
            }

            ulak_log("info", "CloudEvents %s mode enabled for %s (type: %s)",
                     http_dispatcher->cloudevents_mode == CE_MODE_BINARY ? "binary" : "structured",
                     http_dispatcher->url,
                     http_dispatcher->cloudevents_type ? http_dispatcher->cloudevents_type
                                                       : "ulak.message");
        }
    }

    /* Parse auth configuration (optional) */
    http_dispatcher->auth = (void *)http_auth_parse(config);
    if (http_dispatcher->auth) {
        ulak_log("info", "HTTP auth type '%s' configured for %s",
                 http_auth_type_name(((HttpAuthConfig *)http_dispatcher->auth)->type),
                 http_dispatcher->url);
    }

    /* Get TLS/mTLS configuration (optional) */
    http_dispatcher->tls_client_cert = NULL;
    http_dispatcher->tls_client_key = NULL;
    http_dispatcher->tls_ca_bundle = NULL;
    http_dispatcher->tls_pinned_public_key = NULL;
    {
        JsonbValue tls_val;
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_TLS_CLIENT_CERT, &tls_val) &&
            tls_val.type == jbvString && tls_val.val.string.len > 0) {
            http_dispatcher->tls_client_cert =
                pnstrdup(tls_val.val.string.val, tls_val.val.string.len);
        }
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_TLS_CLIENT_KEY, &tls_val) &&
            tls_val.type == jbvString && tls_val.val.string.len > 0) {
            http_dispatcher->tls_client_key =
                pnstrdup(tls_val.val.string.val, tls_val.val.string.len);
        }
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_TLS_CA_BUNDLE, &tls_val) &&
            tls_val.type == jbvString && tls_val.val.string.len > 0) {
            http_dispatcher->tls_ca_bundle =
                pnstrdup(tls_val.val.string.val, tls_val.val.string.len);
        }
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_TLS_PINNED_KEY, &tls_val) &&
            tls_val.type == jbvString && tls_val.val.string.len > 0) {
            http_dispatcher->tls_pinned_public_key =
                pnstrdup(tls_val.val.string.val, tls_val.val.string.len);
        }
        if (http_dispatcher->tls_client_cert)
            ulak_log("info", "mTLS enabled for %s", http_dispatcher->url);
        if (http_dispatcher->tls_pinned_public_key)
            ulak_log("info", "Certificate pinning enabled for %s", http_dispatcher->url);
    }

    /* Parse proxy configuration (optional) */
    http_dispatcher->proxy_url = NULL;
    http_dispatcher->proxy_type = CURLPROXY_HTTP;
    http_dispatcher->proxy_username = NULL;
    http_dispatcher->proxy_password = NULL;
    http_dispatcher->proxy_userpwd = NULL;
    http_dispatcher->proxy_no_proxy = NULL;
    http_dispatcher->proxy_ca_bundle = NULL;
    http_dispatcher->proxy_ssl_verify = true;
    {
        Jsonb *proxy_obj = jsonb_get_nested(config, HTTP_CONFIG_KEY_PROXY);
        if (proxy_obj) {
            char *purl = jsonb_get_string(proxy_obj, "url", NULL);
            if (!purl) {
                ulak_log("error", "Proxy config requires 'url'");
                pfree(proxy_obj);
                pfree(http_dispatcher->url);
                pfree(http_dispatcher->method);
                pfree(http_dispatcher);
                return NULL;
            }
            http_dispatcher->proxy_url = purl;

            /* Map type string to CURL proxy type constant */
            {
                char *ptype = jsonb_get_string(proxy_obj, "type", NULL);
                if (ptype) {
                    if (strcmp(ptype, "https") == 0)
                        http_dispatcher->proxy_type = CURLPROXY_HTTPS;
                    else if (strcmp(ptype, "socks5") == 0)
                        http_dispatcher->proxy_type = CURLPROXY_SOCKS5_HOSTNAME;
                    else
                        http_dispatcher->proxy_type = CURLPROXY_HTTP;
                    pfree(ptype);
                }
            }

            /* Proxy auth: username + password must be both or neither */
            http_dispatcher->proxy_username = jsonb_get_string(proxy_obj, "username", NULL);
            http_dispatcher->proxy_password = jsonb_get_string(proxy_obj, "password", NULL);
            if (http_dispatcher->proxy_username && http_dispatcher->proxy_password) {
                http_dispatcher->proxy_userpwd = psprintf("%s:%s", http_dispatcher->proxy_username,
                                                          http_dispatcher->proxy_password);
            }

            http_dispatcher->proxy_no_proxy = jsonb_get_string(proxy_obj, "no_proxy", NULL);
            http_dispatcher->proxy_ca_bundle = jsonb_get_string(proxy_obj, "ca_bundle", NULL);
            http_dispatcher->proxy_ssl_verify = jsonb_get_bool(proxy_obj, "ssl_verify", true);

            ulak_log("info", "Proxy configured for %s: %s", http_dispatcher->url,
                     http_dispatcher->proxy_url);

            pfree(proxy_obj);
        }
    }

    /* Initialize curl_multi handle for batch operations */
    http_dispatcher->multi_handle = curl_multi_init();
    if (!http_dispatcher->multi_handle) {
        ulak_log("error", "Failed to initialize curl_multi handle");
        pfree(http_dispatcher->url);
        pfree(http_dispatcher->method);
        pfree(http_dispatcher);
        return NULL;
    }

    /* Configure curl_multi connection limits from GUC */
    curl_multi_setopt(http_dispatcher->multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS,
                      (long)ulak_http_max_connections_per_host);
    curl_multi_setopt(http_dispatcher->multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                      (long)ulak_http_max_total_connections);

    /* Enable pipelining/multiplexing if configured */
    if (ulak_http_enable_pipelining) {
        curl_multi_setopt(http_dispatcher->multi_handle, CURLMOPT_PIPELINING,
                          CURLPIPE_HTTP1 | CURLPIPE_MULTIPLEX);
    }

    /* Bound connection cache size to prevent unbounded growth.
     * Default is 4x easy handles which fluctuates with batch size.
     * Fixed cap: 25 connections (sufficient for most endpoints). */
    curl_multi_setopt(http_dispatcher->multi_handle, CURLMOPT_MAXCONNECTS, 25L);

    /* Initialize reusable sync handle — curl_easy_reset() preserves
     * connection cache, DNS cache, TLS session cache across requests.
     * Ref: everything.curl.dev/libcurl/caches.html */
    http_dispatcher->sync_handle = curl_easy_init();
    if (!http_dispatcher->sync_handle)
        ulak_log("warning", "Failed to create reusable sync handle, will create per-request");

    /* Initialize pending requests array */
    initial_capacity = ulak_http_batch_capacity > 0 ? ulak_http_batch_capacity : 64;
    http_dispatcher->pending_requests = palloc0(sizeof(HttpPendingRequest) * initial_capacity);
    http_dispatcher->pending_count = 0;
    http_dispatcher->pending_capacity = initial_capacity;

    ulak_log("info",
             "Created HTTP batch dispatcher for URL: %s (batch_capacity: %d, "
             "connections_per_host: %d, total_connections: %d)",
             http_dispatcher->url, initial_capacity, ulak_http_max_connections_per_host,
             ulak_http_max_total_connections);
    return (Dispatcher *)http_dispatcher;
}

/**
 * @brief Validate HTTP endpoint configuration.
 * @param config JSONB configuration to validate.
 * @return true if configuration is valid.
 */
bool http_dispatcher_validate_config(Jsonb *config) {
    JsonbValue url_val;
    JsonbValue method_val;
    JsonbValue timeout_val;
    JsonbValue connect_timeout_val;
    int timeout;
    int connect_timeout;
    char *unknown_key = NULL;

    if (!config) {
        return false;
    }

    /* Strict validation: reject unknown configuration keys */
    if (!jsonb_validate_keys(config, HTTP_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        ulak_log("error",
                 "HTTP config contains unknown key '%s'. "
                 "Allowed keys: url, method, headers, timeout, connect_timeout, "
                 "signing_secret, cloudevents, ce_mode, ce_type, rate_limit, "
                 "auth, tls_client_cert, tls_client_key, tls_ca_bundle, "
                 "tls_pinned_public_key, proxy, auto_disable_on_gone",
                 unknown_key ? unknown_key : "unknown");
        if (unknown_key) {
            pfree(unknown_key);
        }
        return false;
    }

    /* HTTP requires URL - use stack allocation */
    if (!extract_jsonb_value(config, HTTP_CONFIG_KEY_URL, &url_val) || url_val.type != jbvString ||
        url_val.val.string.len == 0) {
        return false;
    }

    /* SSRF Protection: Validate URL scheme
     * Only allow http:// and https:// to prevent attacks via:
     *   - file:// (local file disclosure)
     *   - gopher:// (protocol smuggling)
     *   - dict://, ldap://, ftp://, etc.
     */
    if (!http_validate_url_scheme(url_val.val.string.val, url_val.val.string.len)) {
        ulak_log("error",
                 "HTTP config URL has invalid scheme. Only http:// and https:// are allowed "
                 "(SSRF protection)");
        return false;
    }

    /* SSRF Protection: Block internal/private IP addresses
     * Prevents requests to localhost, private networks, and cloud metadata endpoints
     */
    if (http_is_internal_url(url_val.val.string.val, url_val.val.string.len)) {
        ulak_log("error", "HTTP config URL targets internal/private network address "
                          "(SSRF protection)");
        return false;
    }

    /* Validate HTTP method if provided */
    if (extract_jsonb_value(config, HTTP_CONFIG_KEY_METHOD, &method_val)) {
        if (method_val.type != jbvString) {
            ulak_log("error", "HTTP config method must be a string");
            return false;
        }
        if (!http_is_valid_method(method_val.val.string.val, method_val.val.string.len)) {
            ulak_log("error",
                     "HTTP config method '%.*s' is invalid. Allowed: GET, POST, PUT, PATCH, DELETE",
                     (int)method_val.val.string.len, method_val.val.string.val);
            return false;
        }
    }

    /* Validate timeout range if provided (1-300 seconds) */
    if (extract_jsonb_value(config, HTTP_CONFIG_KEY_TIMEOUT, &timeout_val)) {
        if (timeout_val.type == jbvNumeric) {
            timeout = jsonb_get_int32(config, HTTP_CONFIG_KEY_TIMEOUT, 0);
            if (timeout < 1 || timeout > 300) {
                ulak_log("error", "HTTP config timeout %d is out of range (1-300 seconds)",
                         timeout);
                return false;
            }
        }
    }

    /* Validate connect_timeout range if provided (1-60 seconds) */
    if (extract_jsonb_value(config, HTTP_CONFIG_KEY_CONNECT_TIMEOUT, &connect_timeout_val)) {
        if (connect_timeout_val.type == jbvNumeric) {
            connect_timeout = jsonb_get_int32(config, HTTP_CONFIG_KEY_CONNECT_TIMEOUT, 0);
            if (connect_timeout < 1 || connect_timeout > 60) {
                ulak_log("error", "HTTP config connect_timeout %d is out of range (1-60 seconds)",
                         connect_timeout);
                return false;
            }
        }
    }

    /* Validate auth configuration if present */
    {
        Jsonb *auth_check = jsonb_get_nested(config, HTTP_CONFIG_KEY_AUTH);
        if (auth_check) {
            pfree(auth_check);
            if (!http_auth_validate(config)) {
                ulak_log("error", "Invalid HTTP auth configuration");
                return false;
            }
        }
    }

    /* Validate proxy configuration if present */
    {
        Jsonb *proxy_check = jsonb_get_nested(config, HTTP_CONFIG_KEY_PROXY);
        if (proxy_check) {
            /* proxy.url is required */
            if (!jsonb_has_key(proxy_check, "url")) {
                ulak_log("error", "Proxy config requires 'url'");
                pfree(proxy_check);
                return false;
            }

            /* Validate proxy URL scheme (http, https, socks5 only) */
            {
                char *proxy_url = jsonb_get_string(proxy_check, "url", NULL);
                if (proxy_url) {
                    if (!http_validate_proxy_url_scheme(proxy_url, strlen(proxy_url))) {
                        ulak_log("error", "Proxy URL has invalid scheme. "
                                          "Only http://, https://, socks5:// are allowed");
                        pfree(proxy_url);
                        pfree(proxy_check);
                        return false;
                    }
                    /* Reject embedded credentials in proxy URL */
                    if (strchr(proxy_url, '@') != NULL) {
                        ulak_log("error", "Proxy URL must not contain embedded credentials. "
                                          "Use separate 'username' and 'password' fields");
                        pfree(proxy_url);
                        pfree(proxy_check);
                        return false;
                    }
                    pfree(proxy_url);
                }
            }

            /* Validate proxy type if specified */
            {
                char *ptype = jsonb_get_string(proxy_check, "type", NULL);
                if (ptype) {
                    if (strcmp(ptype, "http") != 0 && strcmp(ptype, "https") != 0 &&
                        strcmp(ptype, "socks5") != 0) {
                        ulak_log("error", "Proxy type '%s' invalid. Allowed: http, https, socks5",
                                 ptype);
                        pfree(ptype);
                        pfree(proxy_check);
                        return false;
                    }
                    pfree(ptype);
                }
            }

            /* username and password must be both present or both absent */
            {
                bool has_user = jsonb_has_key(proxy_check, "username");
                bool has_pass = jsonb_has_key(proxy_check, "password");
                if (has_user != has_pass) {
                    ulak_log("error",
                             "Proxy config requires both 'username' and 'password' or neither");
                    pfree(proxy_check);
                    return false;
                }
            }

            pfree(proxy_check);
        }
    }

    /* Validate auto_disable_on_gone if present (must be boolean) */
    {
        JsonbValue adg_val;
        if (extract_jsonb_value(config, HTTP_CONFIG_KEY_AUTO_DISABLE_ON_GONE, &adg_val)) {
            if (adg_val.type != jbvBool) {
                ulak_log("error", "HTTP config auto_disable_on_gone must be a boolean");
                return false;
            }
        }
    }

    return true;
}

/**
 * @brief Clean up and free an HTTP dispatcher and all its resources.
 * @param dispatcher Base dispatcher to clean up.
 */
void http_dispatcher_cleanup(Dispatcher *dispatcher) {
    HttpDispatcher *http_dispatcher = (HttpDispatcher *)dispatcher->private_data;
    int i;

    if (!http_dispatcher)
        return;

    /* Cleanup any pending requests first */
    if (http_dispatcher->pending_requests) {
        for (i = 0; i < http_dispatcher->pending_count; i++) {
            HttpPendingRequest *req = &http_dispatcher->pending_requests[i];
            if (req->easy_handle) {
                if (http_dispatcher->multi_handle) {
                    curl_multi_remove_handle(http_dispatcher->multi_handle, req->easy_handle);
                }
                curl_easy_cleanup(req->easy_handle);
            }
            if (req->headers)
                curl_slist_free_all(req->headers);
            if (req->payload)
                pfree(req->payload);
            if (req->response_body)
                pfree(req->response_body);
        }
        pfree(http_dispatcher->pending_requests);
    }

    /* Cleanup reusable sync handle */
    if (http_dispatcher->sync_handle) {
        curl_easy_cleanup(http_dispatcher->sync_handle);
        http_dispatcher->sync_handle = NULL;
    }

    /* Cleanup multi handle */
    if (http_dispatcher->multi_handle) {
        curl_multi_cleanup(http_dispatcher->multi_handle);
    }

    if (http_dispatcher->url) {
        pfree(http_dispatcher->url);
    }

    if (http_dispatcher->method) {
        pfree(http_dispatcher->method);
    }

    /* Note: headers points to config which is owned elsewhere, don't free */

    /* Free CloudEvents type string */
    if (http_dispatcher->cloudevents_type)
        pfree(http_dispatcher->cloudevents_type);

    /* Free TLS config strings (zero sensitive paths before freeing) */
    if (http_dispatcher->tls_client_cert) {
        explicit_bzero(http_dispatcher->tls_client_cert, strlen(http_dispatcher->tls_client_cert));
        pfree(http_dispatcher->tls_client_cert);
    }
    if (http_dispatcher->tls_client_key) {
        explicit_bzero(http_dispatcher->tls_client_key, strlen(http_dispatcher->tls_client_key));
        pfree(http_dispatcher->tls_client_key);
    }
    if (http_dispatcher->tls_ca_bundle)
        pfree(http_dispatcher->tls_ca_bundle);
    if (http_dispatcher->tls_pinned_public_key)
        pfree(http_dispatcher->tls_pinned_public_key);

    /* Cleanup auth config (zeros sensitive data) */
    if (http_dispatcher->auth) {
        http_auth_cleanup((HttpAuthConfig *)http_dispatcher->auth);
        http_dispatcher->auth = NULL;
    }

    /* Free proxy config (zero sensitive credentials) */
    if (http_dispatcher->proxy_password) {
        explicit_bzero(http_dispatcher->proxy_password, strlen(http_dispatcher->proxy_password));
        pfree(http_dispatcher->proxy_password);
    }
    if (http_dispatcher->proxy_userpwd) {
        explicit_bzero(http_dispatcher->proxy_userpwd, strlen(http_dispatcher->proxy_userpwd));
        pfree(http_dispatcher->proxy_userpwd);
    }
    if (http_dispatcher->proxy_username)
        pfree(http_dispatcher->proxy_username);
    if (http_dispatcher->proxy_url)
        pfree(http_dispatcher->proxy_url);
    if (http_dispatcher->proxy_no_proxy)
        pfree(http_dispatcher->proxy_no_proxy);
    if (http_dispatcher->proxy_ca_bundle)
        pfree(http_dispatcher->proxy_ca_bundle);

    /* Zero and free signing secret (sensitive data) */
    if (http_dispatcher->signing_secret) {
        explicit_bzero(http_dispatcher->signing_secret, http_dispatcher->signing_secret_len);
        pfree(http_dispatcher->signing_secret);
    }

    pfree(http_dispatcher);
    ulak_log("debug", "HTTP dispatcher cleaned up");
}
