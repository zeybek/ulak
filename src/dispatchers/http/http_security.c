/**
 * @file http_security.c
 * @brief SSRF protection and security validation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles URL validation to prevent Server-Side Request Forgery attacks.
 */

#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include "config/guc.h"
#include "http_internal.h"
#include "postgres.h"

/**
 * @brief Validate URL scheme for SSRF protection.
 *
 * Only http:// and https:// URLs are allowed to prevent:
 *   - file:// scheme (local file access)
 *   - gopher:// scheme (protocol smuggling)
 *   - dict:// scheme (service probing)
 *   - ftp:// scheme (unintended FTP access)
 *   - ldap:// scheme (LDAP injection)
 *   - other dangerous schemes
 *
 * @param url URL string to validate.
 * @param url_len Length of the URL string.
 * @return true if URL scheme is safe, false otherwise.
 */
bool http_validate_url_scheme(const char *url, size_t url_len) {
    size_t http_len;
    size_t https_len;

    if (url == NULL || url_len == 0) {
        return false;
    }

    /* Check for http:// prefix (case-insensitive) */
    http_len = strlen(URL_SCHEME_HTTP);
    if (url_len >= http_len && pg_strncasecmp(url, URL_SCHEME_HTTP, http_len) == 0) {
        return true;
    }

    /* Check for https:// prefix (case-insensitive) */
    https_len = strlen(URL_SCHEME_HTTPS);
    if (url_len >= https_len && pg_strncasecmp(url, URL_SCHEME_HTTPS, https_len) == 0) {
        return true;
    }

    return false;
}

/**
 * @brief Validate proxy URL scheme.
 *
 * Only http://, https://, and socks5:// are allowed for proxy connections.
 *
 * @param url Proxy URL string to validate.
 * @param url_len Length of the proxy URL string.
 * @return true if URL scheme is valid for a proxy, false otherwise.
 */
bool http_validate_proxy_url_scheme(const char *url, size_t url_len) {
    if (url == NULL || url_len == 0)
        return false;

    if (url_len >= 7 && pg_strncasecmp(url, "http://", 7) == 0)
        return true;
    if (url_len >= 8 && pg_strncasecmp(url, "https://", 8) == 0)
        return true;
    if (url_len >= 9 && pg_strncasecmp(url, "socks5://", 9) == 0)
        return true;

    return false;
}

/**
 * @brief Check if URL targets an internal/private IP address (SSRF protection).
 *
 * Blocks requests to:
 *   - localhost (127.0.0.1, ::1)
 *   - Private networks (10.x.x.x, 172.16-31.x.x, 192.168.x.x)
 *   - Link-local / AWS metadata (169.254.x.x)
 *   - IPv6 local addresses
 *
 * Can be bypassed by setting ulak.http_allow_internal_urls = true.
 *
 * @param url URL string to check.
 * @param url_len Length of the URL string.
 * @return true if the URL targets an internal address (should be blocked).
 */
bool http_is_internal_url(const char *url, size_t url_len) {
    const char *host_start;
    const char *host_end;
    size_t remaining;
    size_t host_len;
    char hostname[256];

    /* Check if internal URLs are explicitly allowed (for testing) */
    if (ulak_http_allow_internal_urls) {
        return false; /* Allow all URLs, skip SSRF check */
    }

    if (url == NULL || url_len == 0) {
        return false;
    }

    /* Find hostname start (after ://) */
    host_start = strstr(url, "://");
    if (!host_start) {
        return false;
    }
    host_start += 3;

    /* Find end of hostname (port, path, or query) */
    host_end = host_start;
    remaining = url_len - (host_start - url);
    while (remaining > 0 && *host_end && *host_end != '/' && *host_end != ':' && *host_end != '?') {
        host_end++;
        remaining--;
    }

    host_len = host_end - host_start;
    if (host_len == 0 || host_len > 255) {
        return false;
    }

    /* Copy hostname to null-terminated buffer */
    memcpy(hostname, host_start, host_len);
    hostname[host_len] = '\0';

    /* Check for localhost patterns */
    if (pg_strcasecmp(hostname, "localhost") == 0 || strcmp(hostname, "127.0.0.1") == 0 ||
        strcmp(hostname, "::1") == 0 || strcmp(hostname, "0.0.0.0") == 0 ||
        strncmp(hostname, "127.", 4) == 0) {
        return true;
    }

    /* Check for private IP ranges */
    /* 10.0.0.0/8 */
    if (strncmp(hostname, "10.", 3) == 0) {
        return true;
    }

    /* 192.168.0.0/16 */
    if (strncmp(hostname, "192.168.", 8) == 0) {
        return true;
    }

    /* 172.16.0.0/12 (172.16.x.x - 172.31.x.x) */
    if (strncmp(hostname, "172.", 4) == 0) {
        int second_octet = 0;
        if (sscanf(hostname + 4, "%d", &second_octet) == 1) {
            if (second_octet >= 16 && second_octet <= 31) {
                return true;
            }
        }
    }

    /* 169.254.0.0/16 (link-local, includes AWS metadata 169.254.169.254) */
    if (strncmp(hostname, "169.254.", 8) == 0) {
        return true;
    }

    /* IPv6 link-local and site-local */
    if (pg_strncasecmp(hostname, "fe80:", 5) == 0 || pg_strncasecmp(hostname, "fc00:", 5) == 0 ||
        pg_strncasecmp(hostname, "fd00:", 5) == 0) {
        return true;
    }

    /*
     * DNS rebinding protection: resolve hostname and check resolved IP.
     * Without this, an attacker can use a domain that resolves to a private IP
     * (e.g., 169-254-169-254.nip.io -> 169.254.169.254) to bypass string-based checks.
     */
    {
        struct addrinfo hints, *result, *rp;
        int gai_ret;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 */
        hints.ai_socktype = SOCK_STREAM;

        gai_ret = getaddrinfo(hostname, NULL, &hints, &result);
        if (gai_ret != 0) {
            /* DNS resolution failed — block as suspicious */
            elog(DEBUG1, "[ulak] DNS resolution failed for '%s': %s", hostname,
                 gai_strerror(gai_ret));
            return true;
        }

        for (rp = result; rp != NULL; rp = rp->ai_next) {
            if (rp->ai_family == AF_INET) {
                struct sockaddr_in *ipv4 = (struct sockaddr_in *)rp->ai_addr;
                unsigned char *ip = (unsigned char *)&ipv4->sin_addr;

                /* 127.0.0.0/8 (loopback) */
                if (ip[0] == 127) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 127.x.x.x",
                         hostname);
                    return true;
                }
                /* 10.0.0.0/8 (private) */
                if (ip[0] == 10) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 10.x.x.x",
                         hostname);
                    return true;
                }
                /* 172.16.0.0/12 (private) */
                if (ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 172.16-31.x.x",
                         hostname);
                    return true;
                }
                /* 192.168.0.0/16 (private) */
                if (ip[0] == 192 && ip[1] == 168) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 192.168.x.x",
                         hostname);
                    return true;
                }
                /* 169.254.0.0/16 (link-local, AWS metadata) */
                if (ip[0] == 169 && ip[1] == 254) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 169.254.x.x",
                         hostname);
                    return true;
                }
                /* 0.0.0.0 */
                if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to 0.0.0.0",
                         hostname);
                    return true;
                }
            } else if (rp->ai_family == AF_INET6) {
                struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)rp->ai_addr;
                unsigned char *ip6 = ipv6->sin6_addr.s6_addr;

                /* ::1 (loopback) */
                if (IN6_IS_ADDR_LOOPBACK(&ipv6->sin6_addr)) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to ::1", hostname);
                    return true;
                }
                /* fe80::/10 (link-local) */
                if (IN6_IS_ADDR_LINKLOCAL(&ipv6->sin6_addr)) {
                    freeaddrinfo(result);
                    elog(WARNING, "[ulak] DNS rebinding blocked: '%s' resolves to link-local IPv6",
                         hostname);
                    return true;
                }
                /* fc00::/7 (unique local) */
                if (ip6[0] == 0xfc || ip6[0] == 0xfd) {
                    freeaddrinfo(result);
                    elog(WARNING,
                         "[ulak] DNS rebinding blocked: '%s' resolves to unique-local IPv6",
                         hostname);
                    return true;
                }
            }
        }

        freeaddrinfo(result);
    }

    return false;
}

/**
 * @brief Validate HTTP method string.
 *
 * Only GET, POST, PUT, PATCH, DELETE are allowed.
 *
 * @param method HTTP method string.
 * @param len Length of the method string.
 * @return true if method is valid, false otherwise.
 */
bool http_is_valid_method(const char *method, size_t len) {
    if (method == NULL || len == 0) {
        return false;
    }

    /* Check against allowed methods */
    if ((len == 3 && pg_strncasecmp(method, HTTP_METHOD_GET, 3) == 0) ||
        (len == 4 && pg_strncasecmp(method, HTTP_METHOD_POST, 4) == 0) ||
        (len == 3 && pg_strncasecmp(method, HTTP_METHOD_PUT, 3) == 0) ||
        (len == 5 && pg_strncasecmp(method, HTTP_METHOD_PATCH, 5) == 0) ||
        (len == 6 && pg_strncasecmp(method, HTTP_METHOD_DELETE, 6) == 0)) {
        return true;
    }

    return false;
}
