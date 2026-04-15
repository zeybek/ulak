/**
 * @file http_auth.c
 * @brief HTTP auth router, basic/bearer, parse/validate/cleanup.
 *
 * Central auth subsystem for HTTP dispatcher.
 * Parses "auth" nested JSONB config, validates, and routes to type-specific handlers.
 */

#include <string.h>
#include "postgres.h"
#include "utils/memutils.h"

#include "http_auth.h"
#include "http_internal.h"
#include "utils/json_utils.h"

/* Auth type string constants */
#define AUTH_TYPE_BASIC "basic"
#define AUTH_TYPE_BEARER "bearer"
#define AUTH_TYPE_OAUTH2 "oauth2"
#define AUTH_TYPE_AWS_SIGV4 "aws_sigv4"

/* Auth config keys */
#define AUTH_KEY_TYPE "type"
#define AUTH_KEY_USERNAME "username"
#define AUTH_KEY_PASSWORD "password"
#define AUTH_KEY_TOKEN "token"
#define AUTH_KEY_TOKEN_URL "token_url"
#define AUTH_KEY_CLIENT_ID "client_id"
#define AUTH_KEY_CLIENT_SECRET "client_secret"
#define AUTH_KEY_SCOPE "scope"
#define AUTH_KEY_ACCESS_KEY "access_key"
#define AUTH_KEY_SECRET_KEY "secret_key"
#define AUTH_KEY_REGION "region"
#define AUTH_KEY_SERVICE "service"
#define AUTH_KEY_SESSION_TOKEN "session_token"

/**
 * @brief Get human-readable name for an auth type.
 * @param type Auth type enum value.
 * @return Static string name of the auth type.
 */
const char *http_auth_type_name(HttpAuthType type) {
    switch (type) {
    case HTTP_AUTH_BASIC:
        return AUTH_TYPE_BASIC;
    case HTTP_AUTH_BEARER:
        return AUTH_TYPE_BEARER;
    case HTTP_AUTH_OAUTH2:
        return AUTH_TYPE_OAUTH2;
    case HTTP_AUTH_AWS_SIGV4:
        return AUTH_TYPE_AWS_SIGV4;
    default:
        return "none";
    }
}

/**
 * @brief Parse auth config from endpoint JSONB.
 * @param endpoint_config JSONB endpoint configuration containing "auth" key.
 * @return Newly allocated HttpAuthConfig, or NULL if no auth or invalid.
 */
HttpAuthConfig *http_auth_parse(Jsonb *endpoint_config) {
    Jsonb *auth_obj;
    char *type_str;
    HttpAuthConfig *auth;

    if (!endpoint_config)
        return NULL;

    auth_obj = jsonb_get_nested(endpoint_config, "auth");
    if (!auth_obj)
        return NULL;

    type_str = jsonb_get_string(auth_obj, AUTH_KEY_TYPE, NULL);
    if (!type_str) {
        elog(WARNING, "[ulak] Auth config missing 'type' field");
        pfree(auth_obj);
        return NULL;
    }

    auth = palloc0(sizeof(HttpAuthConfig));

    if (strcmp(type_str, AUTH_TYPE_BASIC) == 0) {
        auth->type = HTTP_AUTH_BASIC;
        auth->config.basic.username = jsonb_get_string(auth_obj, AUTH_KEY_USERNAME, NULL);
        auth->config.basic.password = jsonb_get_string(auth_obj, AUTH_KEY_PASSWORD, NULL);
        if (!auth->config.basic.username || !auth->config.basic.password) {
            elog(WARNING, "[ulak] Basic auth requires 'username' and 'password'");
            pfree(type_str);
            pfree(auth_obj);
            pfree(auth);
            return NULL;
        }
    } else if (strcmp(type_str, AUTH_TYPE_BEARER) == 0) {
        auth->type = HTTP_AUTH_BEARER;
        auth->config.bearer.token = jsonb_get_string(auth_obj, AUTH_KEY_TOKEN, NULL);
        if (!auth->config.bearer.token) {
            elog(WARNING, "[ulak] Bearer auth requires 'token'");
            pfree(type_str);
            pfree(auth_obj);
            pfree(auth);
            return NULL;
        }
    } else if (strcmp(type_str, AUTH_TYPE_OAUTH2) == 0) {
        auth->type = HTTP_AUTH_OAUTH2;
        auth->config.oauth2.token_url = jsonb_get_string(auth_obj, AUTH_KEY_TOKEN_URL, NULL);
        auth->config.oauth2.client_id = jsonb_get_string(auth_obj, AUTH_KEY_CLIENT_ID, NULL);
        auth->config.oauth2.client_secret =
            jsonb_get_string(auth_obj, AUTH_KEY_CLIENT_SECRET, NULL);
        auth->config.oauth2.scope = jsonb_get_string(auth_obj, AUTH_KEY_SCOPE, NULL);
        auth->config.oauth2.cache.access_token = NULL;
        auth->config.oauth2.cache.expires_at = 0;
        if (!auth->config.oauth2.token_url || !auth->config.oauth2.client_id ||
            !auth->config.oauth2.client_secret) {
            elog(WARNING, "[ulak] OAuth2 auth requires 'token_url', 'client_id', 'client_secret'");
            pfree(type_str);
            pfree(auth_obj);
            pfree(auth);
            return NULL;
        }
    } else if (strcmp(type_str, AUTH_TYPE_AWS_SIGV4) == 0) {
        auth->type = HTTP_AUTH_AWS_SIGV4;
        auth->config.aws_sigv4.access_key = jsonb_get_string(auth_obj, AUTH_KEY_ACCESS_KEY, NULL);
        auth->config.aws_sigv4.secret_key = jsonb_get_string(auth_obj, AUTH_KEY_SECRET_KEY, NULL);
        auth->config.aws_sigv4.region = jsonb_get_string(auth_obj, AUTH_KEY_REGION, NULL);
        auth->config.aws_sigv4.service = jsonb_get_string(auth_obj, AUTH_KEY_SERVICE, NULL);
        auth->config.aws_sigv4.session_token =
            jsonb_get_string(auth_obj, AUTH_KEY_SESSION_TOKEN, NULL);
        if (!auth->config.aws_sigv4.access_key || !auth->config.aws_sigv4.secret_key ||
            !auth->config.aws_sigv4.region || !auth->config.aws_sigv4.service) {
            elog(WARNING, "[ulak] AWS SigV4 auth requires 'access_key', 'secret_key', "
                          "'region', 'service'");
            pfree(type_str);
            pfree(auth_obj);
            pfree(auth);
            return NULL;
        }
        /* Prebuilt sigv4 param string */
        auth->config.aws_sigv4.sigv4_param = psprintf(
            "aws:amz:%s:%s", auth->config.aws_sigv4.region, auth->config.aws_sigv4.service);
    } else {
        elog(WARNING, "[ulak] Unknown auth type: '%s'", type_str);
        pfree(type_str);
        pfree(auth_obj);
        pfree(auth);
        return NULL;
    }

    pfree(type_str);
    pfree(auth_obj);
    return auth;
}

/**
 * @brief Validate auth config structure (no network I/O).
 * @param endpoint_config JSONB endpoint configuration containing "auth" key.
 * @return true if auth config is valid or absent.
 */
bool http_auth_validate(Jsonb *endpoint_config) {
    Jsonb *auth_obj;
    char *type_str;
    bool valid = true;

    if (!endpoint_config)
        return true;

    auth_obj = jsonb_get_nested(endpoint_config, "auth");
    if (!auth_obj)
        return true; /* No auth = valid */

    type_str = jsonb_get_string(auth_obj, AUTH_KEY_TYPE, NULL);
    if (!type_str) {
        elog(WARNING, "[ulak] Auth config missing 'type' field");
        pfree(auth_obj);
        return false;
    }

    if (strcmp(type_str, AUTH_TYPE_BASIC) == 0) {
        if (!jsonb_has_key(auth_obj, AUTH_KEY_USERNAME) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_PASSWORD)) {
            elog(WARNING, "[ulak] Basic auth requires 'username' and 'password'");
            valid = false;
        }
    } else if (strcmp(type_str, AUTH_TYPE_BEARER) == 0) {
        if (!jsonb_has_key(auth_obj, AUTH_KEY_TOKEN)) {
            elog(WARNING, "[ulak] Bearer auth requires 'token'");
            valid = false;
        }
    } else if (strcmp(type_str, AUTH_TYPE_OAUTH2) == 0) {
        if (!jsonb_has_key(auth_obj, AUTH_KEY_TOKEN_URL) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_CLIENT_ID) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_CLIENT_SECRET)) {
            elog(WARNING, "[ulak] OAuth2 auth requires 'token_url', 'client_id', 'client_secret'");
            valid = false;
        }
        /* Validate token_url scheme */
        if (valid) {
            char *token_url = jsonb_get_string(auth_obj, AUTH_KEY_TOKEN_URL, NULL);
            if (token_url) {
                if (!http_validate_url_scheme(token_url, strlen(token_url))) {
                    elog(WARNING, "[ulak] OAuth2 token_url has invalid scheme");
                    valid = false;
                }
                /* SSRF protection: block internal/private network token URLs */
                if (valid && http_is_internal_url(token_url, strlen(token_url))) {
                    elog(WARNING,
                         "[ulak] OAuth2 token_url targets internal/private network address "
                         "(SSRF protection)");
                    valid = false;
                }
                pfree(token_url);
            }
        }
    } else if (strcmp(type_str, AUTH_TYPE_AWS_SIGV4) == 0) {
        if (!jsonb_has_key(auth_obj, AUTH_KEY_ACCESS_KEY) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_SECRET_KEY) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_REGION) ||
            !jsonb_has_key(auth_obj, AUTH_KEY_SERVICE)) {
            elog(WARNING, "[ulak] AWS SigV4 auth requires 'access_key', 'secret_key', "
                          "'region', 'service'");
            valid = false;
        }
    } else {
        elog(WARNING, "[ulak] Unknown auth type: '%s'", type_str);
        valid = false;
    }

    pfree(type_str);
    pfree(auth_obj);
    return valid;
}

/**
 * @brief Apply auth to a curl handle.
 * @param auth Auth configuration.
 * @param curl CURL easy handle.
 * @param headers Existing curl header list.
 * @param auth_error Output flag set to true on failure.
 * @return Updated header list (may have Authorization header appended).
 */
struct curl_slist *http_auth_apply(HttpAuthConfig *auth, CURL *curl, struct curl_slist *headers,
                                   bool *auth_error) {
    *auth_error = false;

    if (!auth || auth->type == HTTP_AUTH_NONE)
        return headers;

    switch (auth->type) {
    case HTTP_AUTH_BASIC: {
        char *userpwd = psprintf("%s:%s", auth->config.basic.username, auth->config.basic.password);
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd);
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
        /* Zero after curl copies the string internally */
        explicit_bzero(userpwd, strlen(userpwd));
        pfree(userpwd);
    } break;

    case HTTP_AUTH_BEARER: {
        char *auth_header = psprintf("Authorization: Bearer %s", auth->config.bearer.token);
        headers = curl_slist_append(headers, auth_header);
        /* Zero sensitive token data before freeing */
        explicit_bzero(auth_header, strlen(auth_header));
        pfree(auth_header);
    } break;

    case HTTP_AUTH_OAUTH2:
        return http_auth_oauth2_apply(auth, curl, headers, auth_error);

    case HTTP_AUTH_AWS_SIGV4:
        return http_auth_sigv4_apply(auth, curl, headers, auth_error);

    default:
        break;
    }

    return headers;
}

/**
 * @brief Invalidate cached OAuth2 token.
 * @param auth Auth configuration (must be OAuth2 type).
 */
void http_auth_invalidate_token(HttpAuthConfig *auth) {
    if (!auth || auth->type != HTTP_AUTH_OAUTH2)
        return;

    if (auth->config.oauth2.cache.access_token) {
        explicit_bzero(auth->config.oauth2.cache.access_token,
                       auth->config.oauth2.cache.access_token_len);
        pfree(auth->config.oauth2.cache.access_token);
        auth->config.oauth2.cache.access_token = NULL;
        auth->config.oauth2.cache.access_token_len = 0;
        auth->config.oauth2.cache.expires_at = 0;
    }
}

/**
 * @private
 * @brief Zero and free a sensitive string.
 * @param str String to securely free (may be NULL).
 */
static void secure_free(char *str) {
    if (str) {
        explicit_bzero(str, strlen(str));
        pfree(str);
    }
}

/**
 * @brief Clean up auth config, zeroing all sensitive data.
 * @param auth Auth configuration to free (may be NULL).
 */
void http_auth_cleanup(HttpAuthConfig *auth) {
    if (!auth)
        return;

    switch (auth->type) {
    case HTTP_AUTH_BASIC:
        pfree(auth->config.basic.username);
        secure_free(auth->config.basic.password);
        break;

    case HTTP_AUTH_BEARER:
        secure_free(auth->config.bearer.token);
        break;

    case HTTP_AUTH_OAUTH2:
        pfree(auth->config.oauth2.token_url);
        pfree(auth->config.oauth2.client_id);
        secure_free(auth->config.oauth2.client_secret);
        if (auth->config.oauth2.scope)
            pfree(auth->config.oauth2.scope);
        http_auth_invalidate_token(auth);
        break;

    case HTTP_AUTH_AWS_SIGV4:
        pfree(auth->config.aws_sigv4.access_key);
        secure_free(auth->config.aws_sigv4.secret_key);
        pfree(auth->config.aws_sigv4.region);
        pfree(auth->config.aws_sigv4.service);
        if (auth->config.aws_sigv4.session_token)
            secure_free(auth->config.aws_sigv4.session_token);
        if (auth->config.aws_sigv4.sigv4_param)
            pfree(auth->config.aws_sigv4.sigv4_param);
        break;

    default:
        break;
    }

    pfree(auth);
}
