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
#include "mb/pg_wchar.h"
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
 * @brief Process-unique fallback message id for callers that do not know the
 * queue row id (legacy dispatch() path, or DispatchResult.message_id == 0).
 *
 * Never use wall-clock seconds here: every message dispatched in the same
 * second would share one webhook-id / ce-id and receivers that de-duplicate on
 * that id would silently drop them.
 */
static int64 http_fallback_msg_id(void) {
    static uint32 counter = 0;

    counter++;
    return ((int64)MyProcPid << 32) | (int64)counter;
}

/**
 * @private
 * @brief Classify a 4xx response into an error message (sync paths).
 *
 * 401 with OAuth2 auth invalidates the cached token and is retryable so the
 * next attempt fetches a fresh token; every other 401/403/4xx is permanent.
 */
static char *http_classify_4xx(HttpDispatcher *http_dispatcher, long response_code,
                               bool *should_disable) {
    if (should_disable)
        *should_disable = false;

    if (response_code == 429)
        return psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Rate limited", response_code);

    if (http_status_is_retryable_4xx(response_code))
        return psprintf(ERROR_PREFIX_RETRYABLE " HTTP %ld: Client error (transient)",
                        response_code);

    if (response_code == 410) {
        if (should_disable)
            *should_disable = true;
        return psprintf(ERROR_PREFIX_PERMANENT " " ERROR_PREFIX_DISABLE " HTTP 410: Gone");
    }

    if (response_code == 401 &&
        http_auth_handle_unauthorized((HttpAuthConfig *)http_dispatcher->auth))
        return psprintf(ERROR_PREFIX_RETRYABLE
                        " HTTP 401: Unauthorized (OAuth2 token invalidated, will refresh)");

    return psprintf(ERROR_PREFIX_PERMANENT " HTTP %ld: Client error", response_code);
}

bool http_status_is_retryable_4xx(long response_code) {
    return response_code == 408 || response_code == 425 || response_code == 429;
}

/**
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
int32 http_parse_retry_after(CURL *curl) {
#if LIBCURL_VERSION_NUM >= 0x075400 /* 7.84.0: curl_easy_header */
    struct curl_header *header = NULL;

    if (curl_easy_header(curl, "Retry-After", 0, CURLH_HEADER, -1, &header) == CURLHE_OK &&
        header->value) {
        char *endptr;
        long val = strtol(header->value, &endptr, 10);
        if (*endptr == '\0' && val > 0 && val <= 86400)
            return (int32)val;
    }
#endif
    return 0;
}

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
        int64 fallback_id = http_fallback_msg_id();

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
            /* 4xx Client Errors (429 retryable, 410 permanent+disable,
             * 401 retryable for OAuth2 after token invalidation, rest permanent) */
            char *classified = http_classify_4xx(http_dispatcher, response_code, NULL);
            ulak_log("warning", "HTTP request failed with client error %ld: %s", response_code,
                     classified);
            if (error_msg)
                *error_msg = classified;
            else
                pfree(classified);
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
 * @private
 * @brief Join the endpoint URL and a per-message url_suffix.
 *
 * Exactly one '/' goes between them, so a suffix such as ".evil.com" or
 * ":8080/x" lands in the path and can never extend the host or the port.
 *
 * @param base Endpoint URL.
 * @param suffix Non-empty url_suffix.
 * @return palloc'd joined URL.
 */
static char *http_join_url_suffix(const char *base, const char *suffix) {
    StringInfoData buf;
    size_t base_len = strlen(base);

    while (base_len > 0 && base[base_len - 1] == '/')
        base_len--;

    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, base, (int)base_len);
    if (suffix[0] != '/')
        appendStringInfoChar(&buf, '/');
    appendStringInfoString(&buf, suffix);
    return buf.data;
}

/**
 * @private
 * @brief Whether a joined URL still points at the endpoint's own origin.
 *
 * Both URLs are parsed with libcurl's URL API (the parser curl itself will use
 * for the request); scheme, host and effective port must match and the joined
 * URL must carry no userinfo.
 *
 * @param base Endpoint URL.
 * @param url Joined URL.
 * @return true if both parse and scheme, host and port are those of @p base.
 */
static bool http_url_same_origin(const char *base, const char *url) {
    static const CURLUPart parts[] = {CURLUPART_SCHEME, CURLUPART_HOST, CURLUPART_PORT};
    CURLU *base_h = curl_url();
    CURLU *url_h = curl_url();
    bool same = false;
    char *user = NULL;
    int i;

    if (!base_h || !url_h)
        goto done;
    if (curl_url_set(base_h, CURLUPART_URL, base, 0) != CURLUE_OK ||
        curl_url_set(url_h, CURLUPART_URL, url, 0) != CURLUE_OK)
        goto done;
    if (curl_url_get(url_h, CURLUPART_USER, &user, 0) != CURLUE_NO_USER)
        goto done;

    same = true;
    for (i = 0; i < (int)lengthof(parts) && same; i++) {
        char *a = NULL;
        char *b = NULL;

        if (curl_url_get(base_h, parts[i], &a, CURLU_DEFAULT_PORT) != CURLUE_OK ||
            curl_url_get(url_h, parts[i], &b, CURLU_DEFAULT_PORT) != CURLUE_OK ||
            pg_strcasecmp(a, b) != 0)
            same = false;
        curl_free(a);
        curl_free(b);
    }

done:
    curl_free(user);
    curl_url_cleanup(base_h);
    curl_url_cleanup(url_h);
    return same;
}

/**
 * @private
 * @brief Copy of the part of a server-sent string that is valid text.
 *
 * Captured values end up in the jsonb response column, and jsonb input does
 * not check the encoding: a body that is not text, or one the size cap cut in
 * the middle of a character, would store bytes that are invalid in the
 * database encoding. Such a row breaks conversions and makes a dump of the
 * table unrestorable (COPY FROM rejects it). Keep the valid prefix only.
 *
 * @param data Bytes received from the server.
 * @param len Number of bytes.
 * @return palloc'd NUL-terminated copy of the longest valid prefix.
 */
static char *http_valid_text_prefix(const char *data, size_t len) {
    int valid = pg_encoding_verifymbstr(GetDatabaseEncoding(), data, (int)len);

    return pnstrdup(data, valid);
}

/**
 * @private
 * @brief metadata.timeout in seconds, clamped to 0..300.
 *
 * numeric_int4 would raise an ERROR for a value outside int4 and take the
 * whole batch down; strtol saturates instead.
 *
 * @param num The numeric metadata value.
 * @return Seconds in 1..300, or 0 when the value is not positive.
 */
static int http_metadata_timeout(Numeric num) {
    char *str = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
    long secs = strtol(str, NULL, 10);

    pfree(str);
    if (secs > 300)
        return 300;
    return secs > 0 ? (int)secs : 0;
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
     * Order: wrap payload (structured mode), then sign the final payload.
     * The id is the ulak.queue row id (unique, stable across retries) when the
     * caller provided it; the fallback is process-unique, never wall-clock. */
    final_payload_ex = payload;
    {
        int64 ex_id =
            (result && result->message_id > 0) ? result->message_id : http_fallback_msg_id();

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

        /* Timeout override — clamped to the same 1..300s range the endpoint
         * config allows, so a single message cannot stall the worker. */
        if (extract_jsonb_value(metadata, "timeout", &val) && val.type == jbvNumeric) {
            timeout_override = http_metadata_timeout(val.val.numeric);
            if (timeout_override > 0)
                curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)timeout_override);
        }

        /* URL suffix override (append to base URL).
         * SECURITY: Reject suffixes containing scheme-changing or credential-injection
         * characters to prevent SSRF (e.g. "@evil.com" changes the destination). */
        if (extract_jsonb_value(metadata, "url_suffix", &val) && val.type == jbvString &&
            val.val.string.len > 0) {
            char *suffix = pnstrdup(val.val.string.val, val.val.string.len);

            if (strstr(suffix, "://") != NULL || strchr(suffix, '@') != NULL) {
                /* SECURITY/CORRECTNESS: do NOT fall through to curl_easy_perform().
                 * curl is still configured with the base URL, so continuing would
                 * silently misdeliver this message to {base} instead of
                 * {base}{suffix} (false success on 2xx, false-permanent DLQ on 404).
                 * Reject the dispatch permanently — a retry would reproduce the
                 * same deterministic rejection. */
                ulak_log("warning", "Rejected url_suffix containing '://' or '@': SSRF risk");
                pfree(suffix);
                if (result) {
                    result->success = false;
                    result->error_msg = pstrdup(
                        ERROR_PREFIX_PERMANENT
                        " Rejected url_suffix ('://' or '@'): would misdeliver to base URL");
                }
                success = false;
                goto cleanup;
            } else {
                char *full_url = http_join_url_suffix(http_dispatcher->url, suffix);

                if (!http_url_same_origin(http_dispatcher->url, full_url)) {
                    /* Same as above: never fall through to the base URL */
                    ulak_log("warning",
                             "Rejected url_suffix: invalid URL or other scheme, host or port");
                    pfree(full_url);
                    pfree(suffix);
                    if (result) {
                        result->success = false;
                        result->error_msg =
                            pstrdup(ERROR_PREFIX_PERMANENT " Rejected url_suffix: joined URL is "
                                                           "invalid or leaves the endpoint's "
                                                           "scheme, host or port");
                    }
                    success = false;
                    goto cleanup;
                }

                /* Validate concatenated URL against internal IP blocklist */
                if (!ulak_http_allow_internal_urls &&
                    http_is_internal_url(full_url, strlen(full_url))) {
                    /* SECURITY/CORRECTNESS: same as the '://'/'@' branch above —
                     * reject the dispatch instead of falling through to the base URL. */
                    ulak_log("warning", "Rejected url_suffix leading to internal URL");
                    pfree(full_url);
                    pfree(suffix);
                    if (result) {
                        result->success = false;
                        result->error_msg = pstrdup(ERROR_PREFIX_PERMANENT
                                                    " Rejected url_suffix leading to internal URL");
                    }
                    success = false;
                    goto cleanup;
                } else {
                    curl_easy_setopt(curl, CURLOPT_URL, full_url);
                    /* NOTE: full_url must survive until after curl_easy_perform().
                     * CURLOPT_URL does NOT copy the string in all curl versions.
                     * Store pointer for cleanup after perform. */
                    url_suffix_buf = full_url;
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
                result->http_content_type =
                    http_valid_text_prefix(content_type, strlen(content_type));
            }

            /* Copy response body (truncated if needed) */
            if (response_buf.size > 0 && ulak_response_body_max_size > 0) {
                result->http_response_body =
                    http_valid_text_prefix(response_buf.data, response_buf.size);
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
                if (response_code == 503) {
                    result->retry_after_seconds = http_parse_retry_after(curl);
                }
            }
        } else if (response_code >= 400 && response_code < 500) {
            /* 4xx Client Errors (429 retryable, 410 permanent+disable,
             * 401 retryable for OAuth2 after token invalidation, rest permanent) */
            bool should_disable = false;
            char *classified = http_classify_4xx(http_dispatcher, response_code, &should_disable);
            ulak_log("warning", "HTTP request failed with client error %ld: %s", response_code,
                     classified);
            if (result) {
                result->success = false;
                result->error_msg = classified;
                result->should_disable_endpoint = should_disable;
                if (response_code == 429)
                    result->retry_after_seconds = http_parse_retry_after(curl);
            } else {
                pfree(classified);
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

cleanup:
    /* Cleanup — free AFTER curl_easy_perform() since CURLOPT_POSTFIELDS/URL store pointers.
     * Also reached early via `goto cleanup` from the url_suffix rejection branches,
     * where url_suffix_buf is NULL and curl_easy_perform() was never called. */
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
