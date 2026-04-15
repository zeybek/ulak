/**
 * @file http_batch.c
 * @brief Batch operations for HTTP dispatcher.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles asynchronous batch HTTP requests using curl_multi.
 */

#include "ulak.h"

#include <string.h>
#include <time.h>
#include "config/guc.h"
#include "core/entities.h"
#include "http_internal.h"
#include "utils/cloudevents.h"
#include "utils/logging.h"

/**
 * @brief Add a request to the batch without sending (non-blocking).
 * @param dispatcher Base dispatcher instance.
 * @param payload Message payload string.
 * @param msg_id Message ID for tracking and webhook signing.
 * @param error_msg Output error message on failure (caller must pfree).
 * @return true on success, false on failure.
 */
bool http_batch_produce(Dispatcher *dispatcher, const char *payload, int64 msg_id,
                        char **error_msg) {
    HttpDispatcher *http_dispatcher;
    CURL *easy;
    int index;
    HttpPendingRequest *req;
    CURLMcode mrc;

    http_dispatcher = (HttpDispatcher *)dispatcher->private_data;

    if (!http_dispatcher || !payload || !http_dispatcher->multi_handle) {
        if (error_msg)
            *error_msg = pstrdup("Invalid HTTP dispatcher or payload");
        return false;
    }

    /* Check capacity */
    if (http_dispatcher->pending_count >= http_dispatcher->pending_capacity) {
        if (error_msg)
            *error_msg = pstrdup("HTTP batch capacity reached, flush before producing more");
        return false;
    }

    /* Create easy handle for this request */
    easy = curl_easy_init();
    if (!easy) {
        if (error_msg)
            *error_msg = pstrdup("Failed to initialize curl handle");
        return false;
    }

    /* Get slot in pending array */
    index = http_dispatcher->pending_count;
    req = &http_dispatcher->pending_requests[index];

    /* Initialize request tracking */
    req->msg_id = msg_id;
    req->easy_handle = easy;
    req->payload = pstrdup(payload);
    req->response_body = NULL;
    req->response_size = 0;
    req->completed = false;
    req->success = false;
    req->http_code = 0;
    req->error[0] = '\0';

    /* Build headers */
    req->headers = http_build_headers(http_dispatcher);

    /* Apply authentication */
    if (http_dispatcher->auth) {
        bool auth_error = false;
        req->headers = http_auth_apply(http_dispatcher->auth, easy, req->headers, &auth_error);
        if (auth_error) {
            curl_slist_free_all(req->headers);
            curl_easy_cleanup(easy);
            pfree(req->payload);
            req->headers = NULL;
            req->easy_handle = NULL;
            req->payload = NULL;
            if (error_msg)
                *error_msg = pstrdup(ERROR_PREFIX_RETRYABLE " HTTP auth failed");
            return false;
        }
    }

    /* CloudEvents wrapping must happen before signing so signature covers final payload */
    if (http_dispatcher->cloudevents_mode == CE_MODE_BINARY) {
        req->headers = cloudevents_add_binary_headers(
            req->headers, msg_id, http_dispatcher->cloudevents_type, ulak_cloudevents_source);
    } else if (http_dispatcher->cloudevents_mode == CE_MODE_STRUCTURED) {
        char *wrapped = cloudevents_wrap_structured(
            req->payload, msg_id, http_dispatcher->cloudevents_type, ulak_cloudevents_source);
        pfree(req->payload);
        req->payload = wrapped;
    }

    /* Add Standard Webhooks signature after wrapping (signs the final payload) */
    req->headers = http_add_webhook_signature(req->headers, http_dispatcher, req->payload, msg_id);

    /* Configure curl options */
    http_configure_curl(easy, http_dispatcher, req->payload, req->headers);

    /* Store index in private data for result matching */
    curl_easy_setopt(easy, CURLOPT_PRIVATE, (void *)(intptr_t)index);

    /* Add to multi handle */
    mrc = curl_multi_add_handle(http_dispatcher->multi_handle, easy);
    if (mrc != CURLM_OK) {
        /* Cleanup on failure */
        curl_slist_free_all(req->headers);
        curl_easy_cleanup(easy);
        pfree(req->payload);
        req->headers = NULL;
        req->easy_handle = NULL;
        req->payload = NULL;
        if (error_msg)
            *error_msg = psprintf("Failed to add handle to multi: %s", curl_multi_strerror(mrc));
        return false;
    }

    http_dispatcher->pending_count++;
    return true;
}

/**
 * @brief Execute all pending requests and collect results.
 * @param dispatcher Base dispatcher instance.
 * @param timeout_ms Timeout in milliseconds (0 uses GUC default).
 * @param failed_ids Output array of failed message IDs (caller must pfree).
 * @param failed_count Output number of failed messages.
 * @param failed_errors Output array of error strings (caller must pfree each).
 * @return Number of successful requests.
 */
int http_batch_flush(Dispatcher *dispatcher, int timeout_ms, int64 **failed_ids, int *failed_count,
                     char ***failed_errors) {
    HttpDispatcher *http_dispatcher = (HttpDispatcher *)dispatcher->private_data;
    int still_running = 0;
    int elapsed_ms = 0;
    int poll_interval_ms = 100;
    int success_count = 0;
    int fail_count = 0;
    int i, j;
    CURLMsg *msg;
    int msgs_left;

    if (!http_dispatcher || !http_dispatcher->multi_handle) {
        if (failed_count)
            *failed_count = 0;
        return 0;
    }

    if (http_dispatcher->pending_count == 0) {
        if (failed_count)
            *failed_count = 0;
        return 0;
    }

    /* Use GUC timeout if not specified */
    if (timeout_ms <= 0) {
        timeout_ms = ulak_http_flush_timeout;
    }

    ulak_log("debug", "HTTP batch flush starting: %d requests, timeout: %d ms",
             http_dispatcher->pending_count, timeout_ms);

    /* Main polling loop — use real wall-clock time to prevent premature timeout.
     * curl_multi_poll may return early, so elapsed_ms += poll_interval is inaccurate. */
    {
        struct timespec start_time, now_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);

        do {
            CURLMcode mc = curl_multi_perform(http_dispatcher->multi_handle, &still_running);

            if (mc != CURLM_OK) {
                ulak_log("warning", "curl_multi_perform failed: %s", curl_multi_strerror(mc));
                break;
            }

            if (still_running > 0) {
                int numfds;
                mc = curl_multi_poll(http_dispatcher->multi_handle, NULL, 0, poll_interval_ms,
                                     &numfds);
                if (mc != CURLM_OK) {
                    ulak_log("warning", "curl_multi_poll failed: %s", curl_multi_strerror(mc));
                    break;
                }
            }

            clock_gettime(CLOCK_MONOTONIC, &now_time);
            elapsed_ms = (int)((now_time.tv_sec - start_time.tv_sec) * 1000 +
                               (now_time.tv_nsec - start_time.tv_nsec) / 1000000);
        } while (still_running > 0 && elapsed_ms < timeout_ms);
    }

    /* Collect results from completed transfers */
    while ((msg = curl_multi_info_read(http_dispatcher->multi_handle, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            CURL *easy = msg->easy_handle;

            /* Get request index from private data */
            intptr_t index;
            curl_easy_getinfo(easy, CURLINFO_PRIVATE, (char **)&index);

            if (index >= 0 && index < http_dispatcher->pending_count) {
                HttpPendingRequest *req = &http_dispatcher->pending_requests[index];
                req->completed = true;

                if (msg->data.result == CURLE_OK) {
                    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &req->http_code);

                    if (req->http_code >= 200 && req->http_code < 300) {
                        req->success = true;
                    } else if (req->http_code == 410) {
                        /* 410 Gone - permanent failure, signal endpoint disable */
                        snprintf(req->error, HTTP_ERROR_BUFFER_SIZE,
                                 ERROR_PREFIX_PERMANENT " " ERROR_PREFIX_DISABLE " HTTP 410: Gone");
                    } else if (req->http_code >= 400 && req->http_code < 500 &&
                               req->http_code != 429) {
                        /* 4xx (except 429 and 410) are permanent */
                        snprintf(req->error, HTTP_ERROR_BUFFER_SIZE,
                                 ERROR_PREFIX_PERMANENT " HTTP %ld", req->http_code);
                    } else {
                        /*
                         * 5xx, 429, 1xx, 3xx are all retryable.
                         * Note: Retry-After header is intentionally not parsed in batch
                         * mode. Batch dispatch prioritizes throughput; per-request
                         * Retry-After overrides are only available in sync mode
                         * (dispatch_ex with DispatchResult).
                         */
                        snprintf(req->error, HTTP_ERROR_BUFFER_SIZE,
                                 ERROR_PREFIX_RETRYABLE " HTTP %ld", req->http_code);
                    }
                } else {
                    /* Network/connection error - retryable */
                    snprintf(req->error, HTTP_ERROR_BUFFER_SIZE, ERROR_PREFIX_RETRYABLE " %s",
                             curl_easy_strerror(msg->data.result));
                }
            }

            /* Remove from multi handle */
            curl_multi_remove_handle(http_dispatcher->multi_handle, easy);
        }
    }

    /* Count successes and failures */
    for (i = 0; i < http_dispatcher->pending_count; i++) {
        HttpPendingRequest *req = &http_dispatcher->pending_requests[i];

        if (!req->completed) {
            /* Timeout - mark as retryable */
            snprintf(req->error, HTTP_ERROR_BUFFER_SIZE, ERROR_PREFIX_RETRYABLE " Request timeout");
            fail_count++;
        } else if (req->success) {
            success_count++;
        } else {
            fail_count++;
        }
    }

    /* Return failed IDs and error strings if requested */
    if (fail_count > 0 && failed_ids && failed_count) {
        *failed_ids = palloc(sizeof(int64) * fail_count);
        *failed_count = fail_count;
        if (failed_errors)
            *failed_errors = palloc(sizeof(char *) * fail_count);
        j = 0;
        for (i = 0; i < http_dispatcher->pending_count; i++) {
            HttpPendingRequest *req = &http_dispatcher->pending_requests[i];
            if (!req->success) {
                (*failed_ids)[j] = req->msg_id;
                if (failed_errors && *failed_errors) {
                    (*failed_errors)[j] =
                        pstrdup(req->error[0] ? req->error : "[RETRYABLE] unknown batch error");
                }
                j++;
            }
        }
    } else if (failed_count) {
        *failed_count = fail_count;
        if (failed_errors)
            *failed_errors = NULL;
    }

    /* Cleanup all pending requests */
    for (i = 0; i < http_dispatcher->pending_count; i++) {
        HttpPendingRequest *req = &http_dispatcher->pending_requests[i];
        if (req->easy_handle) {
            curl_easy_cleanup(req->easy_handle);
            req->easy_handle = NULL;
        }
        if (req->headers) {
            curl_slist_free_all(req->headers);
            req->headers = NULL;
        }
        if (req->payload) {
            pfree(req->payload);
            req->payload = NULL;
        }
        if (req->response_body) {
            pfree(req->response_body);
            req->response_body = NULL;
        }
    }

    /* Reset pending count for next batch */
    http_dispatcher->pending_count = 0;

    ulak_log("debug", "HTTP batch flush completed: %d success, %d failed in %d ms", success_count,
             fail_count, elapsed_ms);
    return success_count;
}

/**
 * @brief Check if this dispatcher supports batch operations.
 * @param dispatcher Base dispatcher instance.
 * @return true if batch operations are available.
 */
bool http_batch_supports(Dispatcher *dispatcher) {
    HttpDispatcher *http_dispatcher = (HttpDispatcher *)dispatcher->private_data;
    return http_dispatcher && http_dispatcher->multi_handle != NULL;
}
