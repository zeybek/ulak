/**
 * @file cloudevents.c
 * @brief CloudEvents v1.0 envelope support
 *
 * Implements CloudEvents binary content mode (HTTP headers) and
 * structured content mode (JSON envelope) per the CNCF spec.
 */

#include "utils/cloudevents.h"
#include <string.h>
#include <time.h>
#include "lib/stringinfo.h"
#include "postgres.h"

/**
 * @brief Generate an RFC3339 timestamp string for the current time.
 * @private
 * @return palloc'd string (caller must pfree)
 */
static char *ce_rfc3339_now(void) {
    char *buf = palloc(32);
    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    if (!tm) {
        strlcpy(buf, "1970-01-01T00:00:00Z", 32);
        return buf;
    }
    strftime(buf, 32, "%Y-%m-%dT%H:%M:%SZ", tm);
    return buf;
}

/**
 * @brief Escape a string for safe JSON interpolation.
 * @private
 *
 * Escapes: " -> \", \ -> \\, control chars -> \\uXXXX
 *
 * @param str String to escape (NULL yields empty string)
 * @return palloc'd escaped string (caller must pfree)
 */
static char *ce_json_escape(const char *str) {
    StringInfoData buf;
    const char *p;

    if (!str)
        return pstrdup("");

    initStringInfo(&buf);
    for (p = str; *p; p++) {
        switch (*p) {
        case '"':
            appendStringInfoString(&buf, "\\\"");
            break;
        case '\\':
            appendStringInfoString(&buf, "\\\\");
            break;
        case '\n':
            appendStringInfoString(&buf, "\\n");
            break;
        case '\r':
            appendStringInfoString(&buf, "\\r");
            break;
        case '\t':
            appendStringInfoString(&buf, "\\t");
            break;
        default:
            if ((unsigned char)*p < 0x20)
                appendStringInfo(&buf, "\\u%04x", (unsigned char)*p);
            else
                appendStringInfoChar(&buf, *p);
            break;
        }
    }
    return buf.data;
}

/**
 * @brief Add CloudEvents binary content mode headers to a curl_slist.
 *
 * Each curl_slist_append is checked; on failure returns what we have so far.
 *
 * @param headers Existing header list (may be NULL)
 * @param msg_id Message ID for ce-id
 * @param event_type CloudEvents type attribute (defaults to "ulak.message")
 * @param source CloudEvents source attribute (defaults to "/ulak")
 * @return Updated curl_slist with CE headers appended
 */
struct curl_slist *cloudevents_add_binary_headers(struct curl_slist *headers, int64 msg_id,
                                                  const char *event_type, const char *source) {
    struct curl_slist *tmp;
    char *time_str;
    StringInfoData hdr;

    initStringInfo(&hdr);

    /* ce-specversion (required) */
    appendStringInfo(&hdr, "ce-specversion: %s", CE_SPECVERSION);
    tmp = curl_slist_append(headers, hdr.data);
    if (!tmp) {
        pfree(hdr.data);
        return headers;
    }
    headers = tmp;

    /* ce-id (required) */
    resetStringInfo(&hdr);
    appendStringInfo(&hdr, "ce-id: msg_%lld", (long long)msg_id);
    tmp = curl_slist_append(headers, hdr.data);
    if (!tmp) {
        pfree(hdr.data);
        return headers;
    }
    headers = tmp;

    /* ce-type (required) */
    resetStringInfo(&hdr);
    appendStringInfo(&hdr, "ce-type: %s", event_type ? event_type : "ulak.message");
    tmp = curl_slist_append(headers, hdr.data);
    if (!tmp) {
        pfree(hdr.data);
        return headers;
    }
    headers = tmp;

    /* ce-source (required) */
    resetStringInfo(&hdr);
    appendStringInfo(&hdr, "ce-source: %s", source ? source : "/ulak");
    tmp = curl_slist_append(headers, hdr.data);
    if (!tmp) {
        pfree(hdr.data);
        return headers;
    }
    headers = tmp;

    /* ce-time (optional but recommended) */
    time_str = ce_rfc3339_now();
    resetStringInfo(&hdr);
    appendStringInfo(&hdr, "ce-time: %s", time_str);
    pfree(time_str);
    tmp = curl_slist_append(headers, hdr.data);
    if (!tmp) {
        pfree(hdr.data);
        return headers;
    }
    headers = tmp;

    pfree(hdr.data);
    return headers;
}

/**
 * @brief Wrap a payload in a CloudEvents structured content mode JSON envelope.
 * @param payload JSON payload string to wrap
 * @param msg_id Message ID for the "id" field
 * @param event_type CloudEvents type (defaults to "ulak.message")
 * @param source CloudEvents source (defaults to "/ulak")
 * @return palloc'd JSON string (caller must pfree)
 */
char *cloudevents_wrap_structured(const char *payload, int64 msg_id, const char *event_type,
                                  const char *source) {
    StringInfoData buf;
    char *escaped_type;
    char *escaped_source;
    char *time_str;

    escaped_type = ce_json_escape(event_type ? event_type : "ulak.message");
    escaped_source = ce_json_escape(source ? source : "/ulak");
    time_str = ce_rfc3339_now();

    initStringInfo(&buf);

    appendStringInfo(&buf,
                     "{\"specversion\":\"%s\""
                     ",\"id\":\"msg_%lld\""
                     ",\"type\":\"%s\""
                     ",\"source\":\"%s\""
                     ",\"time\":\"%s\""
                     ",\"datacontenttype\":\"application/json\""
                     ",\"data\":%s}",
                     CE_SPECVERSION, (long long)msg_id, escaped_type, escaped_source, time_str,
                     payload);

    pfree(escaped_type);
    pfree(escaped_source);
    pfree(time_str);

    return buf.data;
}

/**
 * @brief Parse a CloudEvents mode string to a constant.
 * @param mode_str Mode string ("binary", "structured", or NULL for default)
 * @return CE_MODE_BINARY, CE_MODE_STRUCTURED, or CE_MODE_NONE
 */
int cloudevents_parse_mode(const char *mode_str) {
    if (!mode_str || mode_str[0] == '\0')
        return CE_MODE_BINARY; /* Default to binary */

    if (pg_strcasecmp(mode_str, "binary") == 0)
        return CE_MODE_BINARY;
    if (pg_strcasecmp(mode_str, "structured") == 0)
        return CE_MODE_STRUCTURED;

    elog(WARNING, "[ulak] Unknown CloudEvents mode '%s', disabling CloudEvents", mode_str);
    return CE_MODE_NONE;
}
