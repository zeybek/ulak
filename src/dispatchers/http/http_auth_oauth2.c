/**
 * @file http_auth_oauth2.c
 * @brief OAuth2 client credentials flow.
 *
 * Fetches access tokens from OAuth2 token endpoint, caches them
 * with a 60-second safety buffer before expiry.
 * Worker-local cache -- no shared memory coordination needed.
 */

#include <curl/curl.h>
#include <string.h>
#include <time.h>
#include "postgres.h"
#include "utils/memutils.h"

#include "config/guc.h"
#include "http_auth.h"
#include "utils/json_utils.h"

/* Safety buffer: refresh token 60 seconds before expiry */
#define TOKEN_EXPIRY_BUFFER_SECS 60

/* Default timeout for token endpoint requests */
#define TOKEN_FETCH_TIMEOUT_SECS 10

/** @brief Response buffer for token fetch. */
typedef struct {
    char *data;
    size_t size;
    size_t capacity;
} ResponseBuffer;

/**
 * @private
 * @brief Curl write callback for token response.
 * @param contents Response data pointer.
 * @param size Element size (always 1).
 * @param nmemb Number of elements.
 * @param userp Pointer to ResponseBuffer.
 * @return Number of bytes consumed.
 */
static size_t token_write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t total = size * nmemb;
    ResponseBuffer *buf = (ResponseBuffer *)userp;

    if (buf->size + total >= buf->capacity) {
        /* Cap at 64KB to prevent abuse */
        if (buf->capacity >= 65536)
            return 0;
        buf->capacity = (buf->capacity == 0) ? 4096 : buf->capacity * 2;
        if (buf->capacity > 65536)
            buf->capacity = 65536;
        buf->data = repalloc(buf->data, buf->capacity);
    }

    memcpy(buf->data + buf->size, contents, total);
    buf->size += total;
    buf->data[buf->size] = '\0';
    return total;
}

/**
 * @private
 * @brief Fetch a new OAuth2 access token from the token endpoint.
 * @param auth Auth configuration with OAuth2 credentials.
 * @return true on success, false on failure.
 */
static bool oauth2_fetch_token(HttpAuthConfig *auth) {
    CURL *curl;
    CURLcode res;
    long http_code;
    ResponseBuffer response;
    char *post_body;
    char *escaped_id;
    char *escaped_secret;
    Jsonb *json_response;
    char *access_token;
    int32 expires_in;
    MemoryContext old_ctx;

    curl = curl_easy_init();
    if (!curl) {
        elog(WARNING, "[ulak] OAuth2: failed to create curl handle for token fetch");
        return false;
    }

    /* URL-encode client_id and client_secret for form body */
    escaped_id = curl_easy_escape(curl, auth->config.oauth2.client_id, 0);
    escaped_secret = curl_easy_escape(curl, auth->config.oauth2.client_secret, 0);

    if (auth->config.oauth2.scope) {
        char *escaped_scope = curl_easy_escape(curl, auth->config.oauth2.scope, 0);
        post_body = psprintf("grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
                             escaped_id, escaped_secret, escaped_scope);
        curl_free(escaped_scope);
    } else {
        post_body = psprintf("grant_type=client_credentials&client_id=%s&client_secret=%s",
                             escaped_id, escaped_secret);
    }
    curl_free(escaped_id);
    curl_free(escaped_secret);

    /* Setup response buffer */
    response.data = palloc(4096);
    response.data[0] = '\0';
    response.size = 0;
    response.capacity = 4096;

    /* Configure curl for token fetch */
    curl_easy_setopt(curl, CURLOPT_URL, auth->config.oauth2.token_url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, token_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)TOKEN_FETCH_TIMEOUT_SECS);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)5);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L); /* No redirects for security */
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    /* Perform token request */
    res = curl_easy_perform(curl);

    /* Zero post_body (contains client_secret) */
    explicit_bzero(post_body, strlen(post_body));
    pfree(post_body);

    if (res != CURLE_OK) {
        elog(WARNING, "[ulak] OAuth2 token fetch failed: %s", curl_easy_strerror(res));
        explicit_bzero(response.data, response.size);
        pfree(response.data);
        curl_easy_cleanup(curl);
        return false;
    }

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    if (http_code != 200) {
        elog(WARNING, "[ulak] OAuth2 token endpoint returned HTTP %ld", http_code);
        explicit_bzero(response.data, response.size);
        pfree(response.data);
        return false;
    }

    /* Parse JSON response */
    json_response = string_to_jsonb(response.data);
    explicit_bzero(response.data, response.size);
    pfree(response.data);

    if (!json_response) {
        elog(WARNING, "[ulak] OAuth2: failed to parse token response as JSON");
        return false;
    }

    access_token = jsonb_get_string(json_response, "access_token", NULL);
    expires_in = jsonb_get_int32(json_response, "expires_in", 3600);

    if (!access_token) {
        elog(WARNING, "[ulak] OAuth2: token response missing 'access_token'");
        pfree(json_response);
        return false;
    }

    /* Invalidate old token if any */
    http_auth_invalidate_token(auth);

    /* Store new token in TopMemoryContext (survives transactions) */
    old_ctx = MemoryContextSwitchTo(TopMemoryContext);
    auth->config.oauth2.cache.access_token = pstrdup(access_token);
    auth->config.oauth2.cache.access_token_len = strlen(access_token);
    MemoryContextSwitchTo(old_ctx);

    auth->config.oauth2.cache.expires_at = time(NULL) + expires_in - TOKEN_EXPIRY_BUFFER_SECS;

    elog(DEBUG1, "[ulak] OAuth2: token fetched, expires in %d seconds", expires_in);

    pfree(access_token);
    pfree(json_response);
    return true;
}

/**
 * @brief Apply OAuth2 auth -- fetch token if needed, add Authorization header.
 * @param auth Auth configuration with OAuth2 credentials and token cache.
 * @param curl CURL easy handle (unused, reserved for future use).
 * @param headers Existing curl header list.
 * @param auth_error Output flag set to true on failure.
 * @return Updated header list with Authorization header appended.
 */
struct curl_slist *http_auth_oauth2_apply(HttpAuthConfig *auth, CURL *curl,
                                          struct curl_slist *headers, bool *auth_error) {
    char *auth_header;

    /* Check if cached token is still valid */
    if (!auth->config.oauth2.cache.access_token ||
        time(NULL) >= auth->config.oauth2.cache.expires_at) {
        if (!oauth2_fetch_token(auth)) {
            elog(WARNING, "[ulak] OAuth2: token fetch failed, cannot authenticate");
            *auth_error = true;
            return headers;
        }
    }

    /* Add Authorization header */
    auth_header = psprintf("Authorization: Bearer %s", auth->config.oauth2.cache.access_token);
    headers = curl_slist_append(headers, auth_header);
    /* Zero sensitive token data before freeing */
    explicit_bzero(auth_header, strlen(auth_header));
    pfree(auth_header);

    return headers;
}
