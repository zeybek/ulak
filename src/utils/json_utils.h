/**
 * @file json_utils.h
 * @brief JSONB extraction, conversion, iteration, and validation utilities.
 *
 * Clean Architecture: Infrastructure Layer.
 * Wraps the PostgreSQL JSONB API with convenient typed accessors
 * (string, bool, int32, int64, double, nested) and a key-validation
 * helper used by dispatcher config validators.
 */

#ifndef ULAK_UTILS_JSON_UTILS_H
#define ULAK_UTILS_JSON_UTILS_H

#include "postgres.h"
#include "utils/jsonb.h"

/** @name Core extraction */
/** @{ */

/**
 * @brief Extract a value from JSONB object by key.
 *
 * The value is copied into the provided buffer; no heap allocation occurs.
 *
 * @param jsonb  The JSONB object to search.
 * @param key    The key to look for.
 * @param value  Pointer to caller-allocated JsonbValue (stack allocation recommended).
 * @return true if key found, false otherwise.
 */
extern bool extract_jsonb_value(Jsonb *jsonb, const char *key, JsonbValue *value);
/** @} */

/** @name Conversion utilities */
/** @{ */

/**
 * @brief Serialize JSONB to a palloc'd text string.
 * @param jsonb  JSONB value to serialize.
 * @return palloc'd string; caller must pfree.
 */
extern char *jsonb_to_string(Jsonb *jsonb);

/**
 * @brief Parse a JSON text string into a JSONB datum.
 * @param str  JSON text to parse.
 * @return palloc'd Jsonb; caller must pfree.
 */
extern Jsonb *string_to_jsonb(const char *str);

/**
 * @brief Parse JSON text that came from outside (a server reply) into JSONB.
 * @param str  JSON text to parse.
 * @return palloc'd Jsonb, or NULL when the text is not valid JSON (no ERROR).
 */
extern Jsonb *string_to_jsonb_safe(const char *str);
/** @} */

/** @name Typed accessors */
/** @{ */

/**
 * @brief Check if a key exists in the JSONB object.
 * @param jsonb  JSONB object to search.
 * @param key    Key to check for.
 * @return true if key exists, false otherwise.
 */
extern bool jsonb_has_key(Jsonb *jsonb, const char *key);

/**
 * @brief Get a raw JsonbValue for a key.
 * @param jsonb      JSONB object to search.
 * @param key        Key to look for.
 * @param out_value  Output JsonbValue buffer.
 * @return true if key found, false otherwise.
 */
extern bool jsonb_get_value(Jsonb *jsonb, const char *key, JsonbValue *out_value);

/**
 * @brief Get a string value, returning default if absent.
 * @param jsonb          JSONB object to search.
 * @param key            Key to look for.
 * @param default_value  Returned when key is absent.
 * @return palloc'd string or default_value.
 */
extern char *jsonb_get_string(Jsonb *jsonb, const char *key, const char *default_value);

/**
 * @brief Get a boolean value, returning default if absent.
 * @param jsonb          JSONB object to search.
 * @param key            Key to look for.
 * @param default_value  Returned when key is absent.
 * @return Boolean value or default_value.
 */
extern bool jsonb_get_bool(Jsonb *jsonb, const char *key, bool default_value);

/**
 * @brief Get a 32-bit integer value, returning default if absent.
 * @param jsonb          JSONB object to search.
 * @param key            Key to look for.
 * @param default_value  Returned when key is absent.
 * @return int32 value or default_value.
 */
extern int32 jsonb_get_int32(Jsonb *jsonb, const char *key, int32 default_value);

/**
 * @brief Convert a JSONB numeric to int32 without raising an ERROR.
 * @param num  Numeric value (JsonbValue.val.numeric).
 * @param out  Set to the value on success (may be NULL).
 * @return true if @p num is an integer within int32, false otherwise.
 */
extern bool jsonb_numeric_to_int32(Numeric num, int32 *out);

/**
 * @brief Get a 64-bit integer value, returning default if absent.
 * @param jsonb          JSONB object to search.
 * @param key            Key to look for.
 * @param default_value  Returned when key is absent.
 * @return int64 value or default_value.
 */
extern int64 jsonb_get_int64(Jsonb *jsonb, const char *key, int64 default_value);

/**
 * @brief Get a double value, returning default if absent.
 * @param jsonb          JSONB object to search.
 * @param key            Key to look for.
 * @param default_value  Returned when key is absent.
 * @return double value or default_value.
 */
extern double jsonb_get_double(Jsonb *jsonb, const char *key, double default_value);

/**
 * @brief Get a nested JSONB object for a key.
 * @param jsonb  JSONB object to search.
 * @param key    Key whose value is a nested object.
 * @return palloc'd Jsonb, or NULL if key absent or not an object.
 */
extern Jsonb *jsonb_get_nested(Jsonb *jsonb, const char *key);
/** @} */

/** @name Key validation */
/** @{ */

/**
 * @brief Validate that all keys in JSONB object are in the allowed list.
 * @param jsonb         JSONB object to validate.
 * @param allowed_keys  NULL-terminated array of allowed key names.
 * @param unknown_key   If not NULL, set to the first unknown key found (caller must pfree).
 * @return true if all keys are allowed, false if an unknown key is found.
 */
extern bool jsonb_validate_keys(Jsonb *jsonb, const char **allowed_keys, char **unknown_key);
/** @} */

#endif /* ULAK_UTILS_JSON_UTILS_H */
