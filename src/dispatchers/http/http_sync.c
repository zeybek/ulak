/**
 * @file http_sync.c
 * @brief Synchronous single message dispatch.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles synchronous HTTP message dispatching.
 */

#include "ulak.h"

#include <string.h>
#include <time.h>
#include "config/guc.h"
#include "core/entities.h"
#include "http_internal.h"
#include "utils/builtins.h"
#include "utils/cloudevents.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "utils/numeric.h"

/** @brief Response body buffer for dispatch_ex. */
typedef struct {
    char *data;
    size_t size;
    size_t capacity;
} ResponseBuffer;

/**
 * @private
 * @brief Parse Retry-After header value from HTTP response.
 *
 * Handles delay-seconds (integer) format per RFC 7231 Section 7.1.3.
 * HTTP-date format is intentionally not supported: integer-seconds covers
 * the vast majority of real-world 429/503 responses, and a full RFC 7231
 * date parser would add significant complexity for marginal benefit.
 * Cap at 86400 seconds (24h) to prevent abuse from malicious servers.
 *
 * @param curl CURL handle with completed response.
 * @return Delay in seconds, or 0 if absent/unparseable.
 */
#if LIBCURL_VERSION_NUM >= 0x075400 /* 7.84.0: curl_easy_header */
static int32 parse_retry_after(CURL *curl) {
    struct curl_header *header = NULL;

    if (curl_easy_header(curl, "Retry-After", 0, CURLH_HEADER, -1, &header) == CURLHE_OK &&
        header->value) {
        char *endptr;
        long val = strtol(header->value, &endptr, 10);
        if (*endptr == '\0' && val > 0 && val <= 86400)
            return (int32)val;
    }
    return 0;
}
#endif

/**
 * @brief Dispatch HTTP message synchronously (single message).
 * @param dispatcher Base dispatcher instance.
 * @param payload Message payload string.
 * @param error_msg Output error message on failure (caller must pfree).
 * @return true on success, false on failure.
 */
bool http_dispatcher_dispatch(Dispatcher *dispatcher, const char *payload, char **error_msg) {
    HttpDispatcher *http_dispatcher;
    CURL *curl;
    CURLcode res;
    bool success = false;
    bool owns_handle = false;
    struct curl_slist *curl_headers = NULL;
    char *wrapped_payload = NULL;     /* CloudEvents structured envelope (owned, must pfree) */
    const char *final_payload = NULL; /* Points to payload or wrapped_payload */

    http_dispatcher = (HttpDispatcher *)dispatcher->private_data;
    if (!http_dispatcher || !payload) {
        if (error_msg)
            *error_msg = pstrdup("Invalid HTTP dispatcher or payload");
        return false;
    }

    final_payload = payload;

    /* Reuse sync handle if available (preserves connection/DNS/TLS cache),
     * otherwise create a temporary handle as fallback. */
    if (http_dispatcher->sync_handle) {
        curl = http_dispatcher->sync_handle;
        curl_easy_reset(curl);
    } else {
        curl = curl_easy_init();
        owns_handle = true;
        if (!curl) {
            ulak_log("error", "Failed to initialize curl");
            if (error_msg)
                *error_msg = pstrdup("Failed to initialize HTTP client");
            return false;
        }
    }

    /* Build headers */
    curl_headers = http_build_headers(http_dispatcher);

    /* Apply authentication (may do network I/O for OAuth2 token fetch) */
    if (http_dispatcher->auth) {
        bool auth_error = false;
        curl_headers = http_auth_apply(http_dispatcher->auth, curl, curl_headers, &auth_error);
        if (auth_error) {
            if (error_msg)
                *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " HTTP auth failed");
            curl_slist_free_all(curl_headers);
            if (owns_handle)
                curl_easy_cleanup(curl);
            return false;
        }
    }

    /* CloudEvents + Webhook signing integration.
     * Order: wrap payload first (structured mode), then sign the final payload. */
    {
        int64 fallback_id = (int64)time(NULL);

        /* CloudEvents wrapping must happen before signing */
        if (http_dispatcher->cloudevents_mode == CE_MODE_BINARY) {
            curl_headers = cloudevents_add_binary_headers(curl_headers, fallback_id,
                                                          http_dispatcher->cloudevents_type,
                                                          ulak_cloudevents_source);
        } else if (http_dispatcher->cloudevents_mode == CE_MODE_STRUCTURED) {
            wrapped_payload = cloudevents_wrap_structured(
                payload, fallback_id, http_dispatcher->cloudevents_type, ulak_cloudevents_source);
            final_payload = wrapped_payload;
        }

        /* Sign the final payload (after wrapping) */
        curl_headers =
            http_add_webhook_signature(curl_headers, http_dispatcher, final_payload, fallback_id);
    }

    /* Configure curl */
    http_configure_curl(curl, http_dispatcher, final_payload, curl_headers);

    /* Perform request */
    res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

        if (response_code >= 200 && response_code < 300) {
            success = true;
            ulak_log("debug", "HTTP request successful: %ld", response_code);
        } else if (response_code >= 500 && response_code < 600) {
            /* 5xx Server Errors - retryable */
            ulak_log("warning", "HTTP request failed with server error: %ld (retryable)",
                     response_code);
            if (error_msg) {
                *error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Server error", response_code);
            }
        } else if (response_code >= 400 && response_code < 500) {
            /* 4xx Client Errors */
            if (response_code == 429) {
                /* Rate limited - retryable */
                ulak_log("warning", "HTTP request rate limited: %ld (retryable)", response_code);
                if (error_msg) {
                    *error_msg =
                        psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Rate limited", response_code);
                }
            } else if (response_code == 410) {
                /* 410 Gone - permanent failure, signal endpoint disable */
                ulak_log("warning", "HTTP 410 Gone: endpoint permanently unavailable");
                if (error_msg) {
                    *error_msg =
                        psprintf(ERROR_PREFIX_PERMANENT " " ERROR_PREFIX_DISABLE " HTTP 410: Gone");
                }
            } else {
                /* Other 4xx - permanent failure */
                ulak_log("warning", "HTTP request failed with client error: %ld (permanent)",
                         response_code);
                if (error_msg) {
                    *error_msg =
                        psprintf(ERROR_PREFIX_PERMANENT " HTTP %ld: Client error", response_code);
                }
            }
        } else {
            /* Other status codes */
            ulak_log("warning", "HTTP request returned unexpected status: %ld", response_code);
            if (error_msg) {
                *error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Unexpected status", response_code);
            }
        }
    } else {
        /* Network/connection/proxy errors */
        if (res == CURLE_COULDNT_RESOLVE_PROXY) {
            ulak_log("error", "Proxy DNS resolution failed: %s (retryable)",
                     curl_easy_strerror(res));
            if (error_msg)
                *error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " Proxy error: %s", curl_easy_strerror(res));
        }
#if LIBCURL_VERSION_NUM >= 0x074900 /* 7.73.0: CURLE_PROXY */
        else if (res == CURLE_PROXY) {
            ulak_log("error", "Proxy handshake/auth failure: %s (permanent)",
                     curl_easy_strerror(res));
            if (error_msg)
                *error_msg =
                    psprintf(ERROR_PREFIX_PERMANENT " Proxy error: %s", curl_easy_strerror(res));
        }
#endif
        else {
            ulak_log("error", "HTTP request failed: %s (retryable)", curl_easy_strerror(res));
            if (error_msg)
                *error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP error: %s", curl_easy_strerror(res));
        }
    }

    /* Cleanup */
    if (wrapped_payload)
        pfree(wrapped_payload);
    curl_slist_free_all(curl_headers);
    if (owns_handle)
        curl_easy_cleanup(curl);

    return success;
}

/**
 * @private
 * @brief CURL write callback for capturing response body.
 * @param ptr Response data pointer.
 * @param size Element size (always 1).
 * @param nmemb Number of elements.
 * @param userdata Pointer to ResponseBuffer.
 * @return Number of bytes consumed.
 */
static size_t response_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    ResponseBuffer *buf = (ResponseBuffer *)userdata;
    size_t total_size = size * nmemb;
    size_t max_size = (size_t)ulak_response_body_max_size;
    size_t remaining;
    size_t to_copy;

    if (buf->size >= max_size) {
        /* Already at max, just consume data without storing */
        return total_size;
    }

    remaining = max_size - buf->size;
    to_copy = (total_size < remaining) ? total_size : remaining;

    /* Expand buffer if needed */
    if (buf->size + to_copy >= buf->capacity) {
        size_t new_capacity = buf->capacity * 2;
        if (new_capacity < buf->size + to_copy + 1)
            new_capacity = buf->size + to_copy + 1;
        if (new_capacity > max_size + 1)
            new_capacity = max_size + 1;

        buf->data = repalloc(buf->data, new_capacity);
        buf->capacity = new_capacity;
    }

    memcpy(buf->data + buf->size, ptr, to_copy);
    buf->size += to_copy;
    buf->data[buf->size] = '\0';

    return total_size;
}

/**
 * @brief Extended HTTP dispatch with per-message headers/metadata and response capture.
 * @param dispatcher Base dispatcher instance.
 * @param payload Message payload string.
 * @param headers Per-message JSONB headers (may be NULL).
 * @param metadata Per-message JSONB metadata (may be NULL).
 * @param result Output dispatch result with response details (may be NULL).
 * @return true on success, false on failure.
 */
bool http_dispatcher_dispatch_ex(Dispatcher *dispatcher, const char *payload, Jsonb *headers,
                                 Jsonb *metadata, DispatchResult *result) {
    HttpDispatcher *http_dispatcher;
    CURL *curl;
    CURLcode res;
    bool success = false;
    bool owns_handle = false;
    struct curl_slist *curl_headers = NULL;
    char *wrapped_payload_ex = NULL;
    const char *final_payload_ex = NULL;
    char *url_suffix_buf = NULL;
    ResponseBuffer response_buf = {0};
    int timeout_override = 0;

    http_dispatcher = (HttpDispatcher *)dispatcher->private_data;
    if (!http_dispatcher || !payload) {
        if (result) {
            result->success = false;
            result->error_msg = pstrdup("Invalid HTTP dispatcher or payload");
        }
        return false;
    }

    /* Reuse sync handle if available (preserves connection/DNS/TLS cache) */
    if (http_dispatcher->sync_handle) {
        curl = http_dispatcher->sync_handle;
        curl_easy_reset(curl);
    } else {
        curl = curl_easy_init();
        owns_handle = true;
        if (!curl) {
            ulak_log("error", "Failed to initialize curl");
            if (result) {
                result->success = false;
                result->error_msg = pstrdup("Failed to initialize HTTP client");
            }
            return false;
        }
    }

    /* Initialize response buffer */
    response_buf.capacity = 1024;
    response_buf.data = palloc(response_buf.capacity);
    response_buf.data[0] = '\0';
    response_buf.size = 0;

    /* Build merged headers (endpoint config + per-message) */
    curl_headers = http_build_merged_headers(http_dispatcher, headers);

    /* Apply authentication (may do network I/O for OAuth2 token fetch) */
    if (http_dispatcher->auth) {
        bool auth_error = false;
        curl_headers = http_auth_apply(http_dispatcher->auth, curl, curl_headers, &auth_error);
        if (auth_error) {
            if (result) {
                result->success = false;
                result->error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " HTTP auth failed");
            }
            if (response_buf.data)
                pfree(response_buf.data);
            curl_slist_free_all(curl_headers);
            if (owns_handle)
                curl_easy_cleanup(curl);
            return false;
        }
    }

    /* CloudEvents + Webhook signing for dispatch_ex.
     * Order: wrap payload (structured mode), then sign the final payload. */
    final_payload_ex = payload;
    {
        int64 ex_id = (int64)time(NULL);

        if (http_dispatcher->cloudevents_mode == CE_MODE_BINARY) {
            curl_headers = cloudevents_add_binary_headers(
                curl_headers, ex_id, http_dispatcher->cloudevents_type, ulak_cloudevents_source);
        } else if (http_dispatcher->cloudevents_mode == CE_MODE_STRUCTURED) {
            wrapped_payload_ex = cloudevents_wrap_structured(
                payload, ex_id, http_dispatcher->cloudevents_type, ulak_cloudevents_source);
            final_payload_ex = wrapped_payload_ex;
        }

        curl_headers =
            http_add_webhook_signature(curl_headers, http_dispatcher, final_payload_ex, ex_id);

        /* Configure curl with final payload */
        http_configure_curl(curl, http_dispatcher, final_payload_ex, curl_headers);
        /* NOTE: Do NOT free wrapped_payload_ex here — CURLOPT_POSTFIELDS stores
         * the pointer without copying. Must remain valid through curl_easy_perform(). */
    }

    /* Apply metadata overrides if present */
    if (metadata) {
        JsonbValue val;

        /* Timeout override */
        if (extract_jsonb_value(metadata, "timeout", &val) && val.type == jbvNumeric) {
            timeout_override =
                DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val.val.numeric)));
            if (timeout_override > 0) {
                curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)timeout_override);
            }
        }

        /* URL suffix override (append to base URL).
         * SECURITY: Reject suffixes containing scheme-changing or credential-injection
         * characters to prevent SSRF (e.g. "@evil.com" changes the destination). */
        if (extract_jsonb_value(metadata, "url_suffix", &val) && val.type == jbvString) {
            char *suffix = pnstrdup(val.val.string.val, val.val.string.len);

            if (strstr(suffix, "://") != NULL || strchr(suffix, '@') != NULL) {
                ulak_log("warning", "Rejected url_suffix containing '://' or '@': SSRF risk");
                pfree(suffix);
            } else {
                StringInfoData full_url;
                initStringInfo(&full_url);
                appendStringInfo(&full_url, "%s%s", http_dispatcher->url, suffix);

                /* Validate concatenated URL against internal IP blocklist */
                if (!ulak_http_allow_internal_urls &&
                    http_is_internal_url(full_url.data, full_url.len)) {
                    ulak_log("warning", "Rejected url_suffix leading to internal URL");
                    pfree(full_url.data);
                } else {
                    curl_easy_setopt(curl, CURLOPT_URL, full_url.data);
                    /* NOTE: full_url.data must survive until after curl_easy_perform().
                     * CURLOPT_URL does NOT copy the string in all curl versions.
                     * Store pointer for cleanup after perform. */
                    url_suffix_buf = full_url.data;
                }
                pfree(suffix);
            }
        }
    }

    /* Set up response capture */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, response_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buf);

    /* Perform request */
    res = curl_easy_perform(curl);

    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

        /* Populate result */
        if (result) {
            char *content_type = NULL;
            result->http_status_code = response_code;
            result->http_response_size = response_buf.size;

            /* Get content type */
            curl_easy_getinfo(curl, CURLINFO_CONTENT_TYPE, &content_type);
            if (content_type) {
                result->http_content_type = pstrdup(content_type);
            }

            /* Copy response body (truncated if needed) */
            if (response_buf.size > 0 && ulak_response_body_max_size > 0) {
                result->http_response_body = pstrdup(response_buf.data);
            }
        }

        if (response_code >= 200 && response_code < 300) {
            success = true;
            if (result)
                result->success = true;
            ulak_log("debug", "HTTP request successful: %ld", response_code);
        } else if (response_code >= 500 && response_code < 600) {
            /* 5xx Server Errors - retryable */
            ulak_log("warning", "HTTP request failed with server error: %ld (retryable)",
                     response_code);
            if (result) {
                result->success = false;
                result->error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Server error", response_code);
#if LIBCURL_VERSION_NUM >= 0x075400
                if (response_code == 503) {
                    result->retry_after_seconds = parse_retry_after(curl);
                }
#endif
            }
        } else if (response_code >= 400 && response_code < 500) {
            /* 4xx Client Errors */
            if (response_code == 429) {
                /* Rate limited - retryable */
                ulak_log("warning", "HTTP request rate limited: %ld (retryable)", response_code);
                if (result) {
                    result->success = false;
                    result->error_msg =
                        psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Rate limited", response_code);
#if LIBCURL_VERSION_NUM >= 0x075400
                    result->retry_after_seconds = parse_retry_after(curl);
#endif
                }
            } else if (response_code == 410) {
                /* 410 Gone - permanent, signal endpoint disable */
                ulak_log("warning", "HTTP 410 Gone: endpoint permanently unavailable");
                if (result) {
                    result->success = false;
                    result->error_msg =
                        psprintf(ERROR_PREFIX_PERMANENT " " ERROR_PREFIX_DISABLE " HTTP 410: Gone");
                    result->should_disable_endpoint = true;
                }
            } else {
                /* Other 4xx - permanent failure */
                ulak_log("warning", "HTTP request failed with client error: %ld (permanent)",
                         response_code);
                if (result) {
                    result->success = false;
                    result->error_msg =
                        psprintf(ERROR_PREFIX_PERMANENT " HTTP %ld: Client error", response_code);
                }
            }
        } else {
            /* Other status codes */
            ulak_log("warning", "HTTP request returned unexpected status: %ld", response_code);
            if (result) {
                result->success = false;
                result->error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Unexpected status", response_code);
            }
        }
    } else {
        /* Network/connection/proxy errors */
        if (res == CURLE_COULDNT_RESOLVE_PROXY) {
            ulak_log("error", "Proxy DNS resolution failed: %s (retryable)",
                     curl_easy_strerror(res));
            if (result) {
                result->success = false;
                result->error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " Proxy error: %s", curl_easy_strerror(res));
            }
        }
#if LIBCURL_VERSION_NUM >= 0x074900 /* 7.73.0: CURLE_PROXY */
        else if (res == CURLE_PROXY) {
            ulak_log("error", "Proxy handshake/auth failure: %s (permanent)",
                     curl_easy_strerror(res));
            if (result) {
                result->success = false;
                result->error_msg =
                    psprintf(ERROR_PREFIX_PERMANENT " Proxy error: %s", curl_easy_strerror(res));
            }
        }
#endif
        else {
            ulak_log("error", "HTTP request failed: %s (retryable)", curl_easy_strerror(res));
            if (result) {
                result->success = false;
                result->error_msg =
                    psprintf(ERROR_PREFIX_RETRYABLE " HTTP error: %s", curl_easy_strerror(res));
            }
        }
    }

    /* Cleanup — free AFTER curl_easy_perform() since CURLOPT_POSTFIELDS/URL store pointers */
    if (wrapped_payload_ex)
        pfree(wrapped_payload_ex);
    if (url_suffix_buf)
        pfree(url_suffix_buf);
    if (response_buf.data)
        pfree(response_buf.data);
    curl_slist_free_all(curl_headers);
    if (owns_handle)
        curl_easy_cleanup(curl);

    return success;
}
