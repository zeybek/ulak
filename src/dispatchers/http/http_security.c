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
#include <sys/socket.h>
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
 * @brief Check whether a URL carries userinfo ("user:pass@host") in its authority.
 *
 * Userinfo is rejected outright: it lets "http://trusted.example:80@10.0.0.1/"
 * fool naive host parsing (the part before ':' looks like a public host while
 * curl connects to 10.0.0.1), and credentials belong in the auth config, not
 * the URL.
 *
 * @param url      URL string.
 * @param url_len  Length of the URL string.
 * @return true if an '@' appears inside the authority component.
 */
bool http_url_has_userinfo(const char *url, size_t url_len) {
    const char *p;
    const char *end;

    if (url == NULL || url_len == 0)
        return false;

    p = strstr(url, "://");
    if (p == NULL)
        return false;
    p += 3;
    end = url + url_len;

    for (; p < end && *p; p++) {
        if (*p == '/' || *p == '?' || *p == '#')
            return false;
        if (*p == '@')
            return true;
    }
    return false;
}

/**
 * @brief Check whether an IPv4 address (network byte order) is internal.
 *
 * Loopback 127.0.0.0/8, RFC 1918 (10/8, 172.16/12, 192.168/16), link-local and
 * cloud metadata 169.254.0.0/16, CGNAT 100.64.0.0/10, "this network" 0.0.0.0/8
 * (Linux connects 0.0.0.0 to the local host) and the limited broadcast
 * address.
 *
 * @param ip  Four address bytes.
 * @return true if the address is internal (should be blocked).
 */
static bool ulak_ipv4_is_internal(const unsigned char *ip) {
    if (ip[0] == 127 || ip[0] == 10 || ip[0] == 0) /* loopback, 10/8, 0/8 */
        return true;
    if (ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) /* 172.16.0.0/12 */
        return true;
    if (ip[0] == 192 && ip[1] == 168) /* 192.168.0.0/16 */
        return true;
    if (ip[0] == 169 && ip[1] == 254) /* 169.254.0.0/16 link-local / metadata */
        return true;
    if (ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127) /* 100.64.0.0/10 CGNAT */
        return true;
    if (ip[0] == 255 && ip[1] == 255 && ip[2] == 255 && ip[3] == 255) /* broadcast */
        return true;
    return false;
}

/**
 * @brief Check whether a resolved socket address is internal/private.
 *
 * The single range table for every SSRF check: the getaddrinfo pass at
 * validation time and the connect-time guard below use it, so the two cannot
 * drift. IPv6: the unspecified address (Linux connects a TCP socket to :: as
 * ::1, net/ipv6/tcp_ipv6.c tcp_v6_connect), loopback, link-local fe80::/10,
 * unique-local fc00::/7, and the IPv4 address carried by IPv4-mapped
 * (::ffff:a.b.c.d) and NAT64 well-known-prefix (64:ff9b::a.b.c.d, RFC 6052)
 * addresses, so a rebind to ::ffff:169.254.169.254 or a DNS64 answer cannot
 * bypass the IPv4 ranges.
 *
 * @param sa  Resolved socket address.
 * @return true if the address is internal/private (should be blocked).
 */
static bool ulak_sockaddr_is_internal(const struct sockaddr *sa) {
    static const unsigned char nat64_prefix[12] = {0x00, 0x64, 0xff, 0x9b};

    if (sa == NULL) {
        return false;
    }

    if (sa->sa_family == AF_INET) {
        const struct sockaddr_in *ipv4 = (const struct sockaddr_in *)sa;

        return ulak_ipv4_is_internal((const unsigned char *)&ipv4->sin_addr);
    } else if (sa->sa_family == AF_INET6) {
        const struct sockaddr_in6 *ipv6 = (const struct sockaddr_in6 *)sa;
        const unsigned char *ip6 = ipv6->sin6_addr.s6_addr;

        if (IN6_IS_ADDR_UNSPECIFIED(&ipv6->sin6_addr)) /* :: */
            return true;
        if (IN6_IS_ADDR_LOOPBACK(&ipv6->sin6_addr)) /* ::1 */
            return true;
        if (IN6_IS_ADDR_LINKLOCAL(&ipv6->sin6_addr)) /* fe80::/10 */
            return true;
        if ((ip6[0] & 0xfe) == 0xfc) /* fc00::/7 unique-local */
            return true;
        if (IN6_IS_ADDR_V4MAPPED(&ipv6->sin6_addr) ||
            memcmp(ip6, nat64_prefix, sizeof(nat64_prefix)) == 0)
            return ulak_ipv4_is_internal(&ip6[12]);
    }

    return false;
}

/**
 * @brief Extract the host curl will connect to from an http(s) URL.
 *
 * The host is the authority after the last '@' (userinfo is rejected by
 * validation, but curl would connect to what follows it), without the port; a
 * bracketed IPv6 literal ("http://[2001:db8::1]:8080/") is returned without
 * its brackets.
 *
 * @param url            URL string (need not be NUL-terminated).
 * @param url_len        Length of the URL string.
 * @param hostname       Output buffer.
 * @param hostname_size  Size of the output buffer.
 * @return true if a non-empty host that fits the buffer was found.
 */
static bool http_url_host(const char *url, size_t url_len, char *hostname, size_t hostname_size) {
    const char *end = url + url_len;
    const char *p = url;
    const char *authority;
    const char *host_start;
    const char *host_end;
    size_t host_len;

    /* Authority starts after "://" */
    while (p + 3 <= end && !(p[0] == ':' && p[1] == '/' && p[2] == '/'))
        p++;
    if (p + 3 > end)
        return false;
    authority = p + 3;

    /* Authority ends at the path, query or fragment */
    p = authority;
    while (p < end && *p != '/' && *p != '?' && *p != '#')
        p++;
    end = p;

    host_start = authority;
    for (p = authority; p < end; p++) {
        if (*p == '@')
            host_start = p + 1;
    }

    host_end = host_start;
    if (host_start < end && *host_start == '[') {
        host_start++;
        host_end = host_start;
        while (host_end < end && *host_end != ']')
            host_end++;
        if (host_end == end)
            return false; /* unterminated IPv6 literal */
    } else {
        while (host_end < end && *host_end != ':')
            host_end++;
    }

    host_len = host_end - host_start;
    if (host_len == 0 || host_len >= hostname_size)
        return false;

    memcpy(hostname, host_start, host_len);
    hostname[host_len] = '\0';
    return true;
}

/**
 * @brief Classify the host an http(s) URL points at (SSRF protection).
 *
 * Blocks localhost, every address literal in the internal ranges of
 * ulak_sockaddr_is_internal(), and names that resolve to one of them (DNS
 * rebinding: 169-254-169-254.nip.io -> 169.254.169.254). getaddrinfo also
 * turns numeric forms such as "2130706433" or "0x7f.1" into the address they
 * denote, so no string matching on the host is needed. A host that does not
 * resolve is reported separately: it is not known to be internal, and the
 * connect-time guard still checks whatever curl resolves later.
 *
 * Returns HTTP_URL_TARGET_PUBLIC for everything when
 * ulak.http_allow_internal_urls is on.
 *
 * @param url      URL string (need not be NUL-terminated).
 * @param url_len  Length of the URL string.
 * @return HTTP_URL_TARGET_PUBLIC, HTTP_URL_TARGET_INTERNAL or HTTP_URL_TARGET_UNRESOLVED.
 */
HttpUrlTarget http_classify_url_target(const char *url, size_t url_len) {
    char hostname[256];
    struct in_addr addr4;
    struct sockaddr_in6 literal6;
    struct addrinfo hints, *result, *rp;
    int gai_ret;

    /* Check if internal URLs are explicitly allowed (for testing) */
    if (ulak_http_allow_internal_urls) {
        return HTTP_URL_TARGET_PUBLIC;
    }

    if (url == NULL || url_len == 0 || !http_url_host(url, url_len, hostname, sizeof(hostname))) {
        return HTTP_URL_TARGET_UNRESOLVED;
    }

    if (pg_strcasecmp(hostname, "localhost") == 0) {
        return HTTP_URL_TARGET_INTERNAL;
    }

    /* Address literals: no lookup, no warning */
    if (inet_pton(AF_INET, hostname, &addr4) == 1) {
        return ulak_ipv4_is_internal((const unsigned char *)&addr4) ? HTTP_URL_TARGET_INTERNAL
                                                                    : HTTP_URL_TARGET_PUBLIC;
    }
    memset(&literal6, 0, sizeof(literal6));
    if (inet_pton(AF_INET6, hostname, &literal6.sin6_addr) == 1) {
        literal6.sin6_family = AF_INET6;
        return ulak_sockaddr_is_internal((const struct sockaddr *)&literal6)
                   ? HTTP_URL_TARGET_INTERNAL
                   : HTTP_URL_TARGET_PUBLIC;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;

    gai_ret = getaddrinfo(hostname, NULL, &hints, &result);
    if (gai_ret != 0) {
        elog(DEBUG1, "[ulak] DNS resolution failed for '%s': %s", hostname, gai_strerror(gai_ret));
        return HTTP_URL_TARGET_UNRESOLVED;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        if (ulak_sockaddr_is_internal(rp->ai_addr)) {
            char addr_text[INET6_ADDRSTRLEN] = "?";
            const void *raw = rp->ai_family == AF_INET6
                                  ? (const void *)&((struct sockaddr_in6 *)rp->ai_addr)->sin6_addr
                                  : (const void *)&((struct sockaddr_in *)rp->ai_addr)->sin_addr;

            inet_ntop(rp->ai_family, raw, addr_text, sizeof(addr_text));
            elog(WARNING, "[ulak] SSRF blocked: '%s' resolves to internal/private address %s",
                 hostname, addr_text);
            freeaddrinfo(result);
            return HTTP_URL_TARGET_INTERNAL;
        }
    }

    freeaddrinfo(result);
    return HTTP_URL_TARGET_PUBLIC;
}

/**
 * @brief Check if URL targets an internal/private IP address (SSRF protection).
 *
 * True only when the host is known to be internal; a host that does not
 * resolve is not (use http_classify_url_target() to tell the two apart).
 * Always false when ulak.http_allow_internal_urls is on.
 *
 * @param url URL string to check.
 * @param url_len Length of the URL string.
 * @return true if the URL targets an internal address (should be blocked).
 */
bool http_is_internal_url(const char *url, size_t url_len) {
    return http_classify_url_target(url, url_len) == HTTP_URL_TARGET_INTERNAL;
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

/**
 * @brief curl CURLOPT_OPENSOCKETFUNCTION callback enforcing SSRF protection.
 *
 * curl resolves DNS independently at transfer time, so a string/validate-time
 * check (http_is_internal_url) cannot prevent DNS rebinding: a hostname can
 * resolve to a public IP at endpoint-create time and to 127.0.0.1 /
 * 169.254.169.254 at dispatch time. This callback runs for every connection
 * curl actually makes (including redirects) and inspects the real resolved
 * address before connecting, aborting the connect when it is internal — closing
 * the TOCTOU. Honors the ulak.http_allow_internal_urls bypass GUC.
 *
 * @param clientp  Unused (CURLOPT_OPENSOCKETDATA not set).
 * @param purpose  Connection purpose; only IP connections are guarded.
 * @param address  Resolved address curl is about to connect to.
 * @return An open socket fd on success, or CURL_SOCKET_BAD to abort the connect.
 */
curl_socket_t ulak_http_opensocket_guard(void *clientp, curlsocktype purpose,
                                         struct curl_sockaddr *address) {
    (void)clientp;

    if (purpose == CURLSOCKTYPE_IPCXN && !ulak_http_allow_internal_urls &&
        ulak_sockaddr_is_internal(&address->addr)) {
        elog(WARNING, "[ulak] SSRF blocked: connection to internal/private address refused "
                      "(connect-time DNS-rebinding guard)");
        return CURL_SOCKET_BAD;
    }

    return socket(address->family, address->socktype, address->protocol);
}
