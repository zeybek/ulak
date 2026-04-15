/**
 * @file http_auth_sigv4.c
 * @brief AWS SigV4 signing via CURLOPT_AWS_SIGV4.
 *
 * Uses libcurl's native AWS SigV4 support (curl >= 7.75.0).
 * Handles access key, secret key, region, service, and optional session token.
 */

#include <curl/curl.h>
#include <string.h>
#include "postgres.h"

#include "http_auth.h"

/**
 * @brief Apply AWS SigV4 signing to a curl handle.
 *
 * CURLOPT_AWS_SIGV4 requires libcurl >= 7.75.0 (version number 0x074B00).
 *
 * @param auth Auth configuration with AWS credentials.
 * @param curl CURL easy handle to sign.
 * @param headers Existing curl header list.
 * @param auth_error Output flag set to true on failure.
 * @return Updated header list (session token header may be appended).
 */
struct curl_slist *http_auth_sigv4_apply(HttpAuthConfig *auth, CURL *curl,
                                         struct curl_slist *headers, bool *auth_error) {
#if LIBCURL_VERSION_NUM < 0x074B00
    /* curl too old — no SigV4 support */
    elog(WARNING,
         "[ulak] AWS SigV4 requires libcurl >= 7.75.0, "
         "current version: %s",
         curl_version());
    *auth_error = true;
    return headers;
#else
    char *userpwd;

    /* Set SigV4 signing parameters */
    curl_easy_setopt(curl, CURLOPT_AWS_SIGV4, auth->config.aws_sigv4.sigv4_param);

    /* Set credentials */
    userpwd =
        psprintf("%s:%s", auth->config.aws_sigv4.access_key, auth->config.aws_sigv4.secret_key);
    curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd);
    /* Zero after curl copies internally */
    explicit_bzero(userpwd, strlen(userpwd));
    pfree(userpwd);

    /* Add session token header if present (for STS temporary credentials) */
    if (auth->config.aws_sigv4.session_token) {
        char *token_header =
            psprintf("x-amz-security-token: %s", auth->config.aws_sigv4.session_token);
        headers = curl_slist_append(headers, token_header);
        pfree(token_header);
    }

    return headers;
#endif
}
