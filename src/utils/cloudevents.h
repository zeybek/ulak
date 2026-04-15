/**
 * @file cloudevents.h
 * @brief CloudEvents v1.0 envelope support (binary and structured modes).
 *
 * Provides functions to add CloudEvents attributes to HTTP headers
 * (binary content mode) or wrap payloads in a CloudEvents JSON envelope
 * (structured content mode).
 *
 * @see https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
 */

#ifndef ULAK_UTILS_CLOUDEVENTS_H
#define ULAK_UTILS_CLOUDEVENTS_H

#include <curl/curl.h>
#include "postgres.h"

/* CloudEvents modes */
#define CE_MODE_NONE 0
#define CE_MODE_BINARY 1     /**< Attributes as HTTP headers (ce-*). */
#define CE_MODE_STRUCTURED 2 /**< Full JSON envelope. */

/* CloudEvents spec version */
#define CE_SPECVERSION "1.0"

/* CloudEvents config keys */
#define CE_CONFIG_KEY_ENABLED "cloudevents"
#define CE_CONFIG_KEY_MODE "cloudevents_mode"
#define CE_CONFIG_KEY_TYPE "cloudevents_type"

/** @name Binary content mode */
/** @{ */

/**
 * @brief Add CloudEvents binary content mode headers to a curl_slist.
 *
 * Adds: ce-specversion, ce-id, ce-type, ce-source, ce-time.
 *
 * @param headers     Existing header list.
 * @param msg_id      Message ID for ce-id header.
 * @param event_type  CloudEvents type attribute.
 * @param source      CloudEvents source attribute.
 * @return Updated curl_slist with CloudEvents headers appended.
 */
struct curl_slist *cloudevents_add_binary_headers(struct curl_slist *headers, int64 msg_id,
                                                  const char *event_type, const char *source);
/** @} */

/** @name Structured content mode */
/** @{ */

/**
 * @brief Wrap a JSON payload in CloudEvents structured content mode envelope.
 *
 * Output format:
 * @code{.json}
 * {
 *   "specversion": "1.0",
 *   "id": "msg_12345",
 *   "type": "com.example.event",
 *   "source": "/ulak",
 *   "time": "2026-04-13T12:00:00Z",
 *   "datacontenttype": "application/json",
 *   "data": { ... original payload ... }
 * }
 * @endcode
 *
 * @param payload     Original JSON payload.
 * @param msg_id      Message ID for the "id" field.
 * @param event_type  CloudEvents type attribute.
 * @param source      CloudEvents source attribute.
 * @return palloc'd JSON string; caller must pfree.
 */
char *cloudevents_wrap_structured(const char *payload, int64 msg_id, const char *event_type,
                                  const char *source);
/** @} */

/** @name Mode parsing */
/** @{ */

/**
 * @brief Parse cloudevents_mode string to integer constant.
 * @param mode_str  Mode string ("binary" or "structured").
 * @return CE_MODE_BINARY, CE_MODE_STRUCTURED, or CE_MODE_NONE.
 */
int cloudevents_parse_mode(const char *mode_str);
/** @} */

#endif /* ULAK_UTILS_CLOUDEVENTS_H */
