/**
 * @file json_utils.c
 * @brief JSON utility functions implementation
 *
 * Clean Architecture: Infrastructure Layer
 * Provides utilities for working with PostgreSQL JSONB data type
 */

#include "utils/json_utils.h"
#include <errno.h>
#if PG_VERSION_NUM >= 160000
#include "nodes/miscnodes.h"
#endif
#include <stdlib.h>
#include <string.h>
#include "utils/logging.h"

/**
 * @brief Extract a value from a JSONB object by key.
 *
 * Uses findJsonbValueFromContainer for direct O(log n) lookup. Handles ALL
 * value types including nested objects (returned as jbvBinary) and scalars
 * (jbvString, jbvNumeric, jbvBool, jbvNull).
 *
 * @param jsonb JSONB object to search
 * @param key Key string to look up
 * @param value Output JsonbValue populated on success
 * @return true if key was found
 */
bool extract_jsonb_value(Jsonb *jsonb, const char *key, JsonbValue *value) {
    JsonbValue kval;
    JsonbValue *result;

    if (!jsonb || !key || !value) {
        return false;
    }

    kval.type = jbvString;
    kval.val.string.val = (char *)key;
    kval.val.string.len = strlen(key);

    result = findJsonbValueFromContainer(&jsonb->root, JB_FOBJECT, &kval);
    if (result) {
        *value = *result;
        pfree(result);
        return true;
    }

    return false;
}

/**
 * @brief Convert a JSONB value to a C string.
 * @param jsonb JSONB value to convert
 * @return palloc'd string, or NULL if jsonb is NULL
 */
char *jsonb_to_string(Jsonb *jsonb) {
    if (!jsonb) {
        return NULL;
    }

    return JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
}

/**
 * @brief Convert a C string to a JSONB value.
 * @param str JSON string to parse
 * @return palloc'd Jsonb, or NULL if str is NULL
 */
Jsonb *string_to_jsonb(const char *str) {
    if (!str) {
        return NULL;
    }

    /* Use PostgreSQL's built-in jsonb_in function */
    return (Jsonb *)DirectFunctionCall1(jsonb_in, CStringGetDatum(str));
}

/**
 * @brief Convert a C string to a JSONB value without raising on bad input.
 *
 * For text that did not come from ulak itself (a remote server's reply), where
 * a syntax error must become a delivery failure instead of an ERROR that aborts
 * the whole batch. The parser's message is discarded: it quotes the input.
 *
 * @param str JSON string to parse
 * @return palloc'd Jsonb, or NULL if str is NULL or not valid JSON
 */
Jsonb *string_to_jsonb_safe(const char *str) {
#if PG_VERSION_NUM >= 160000
    ErrorSaveContext escontext = {T_ErrorSaveContext};
    Datum result;

    if (!str) {
        return NULL;
    }

    if (!DirectInputFunctionCallSafe(jsonb_in, (char *)str, InvalidOid, -1, (Node *)&escontext,
                                     &result)) {
        return NULL;
    }
    return DatumGetJsonbP(result);
#else
    /* Soft input errors (ErrorSaveContext) only exist from PG16 on */
    MemoryContext saved_ctx = CurrentMemoryContext;
    Jsonb *volatile result = NULL;

    if (!str) {
        return NULL;
    }

    PG_TRY();
    {
        result = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(str)));
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(saved_ctx);
        FlushErrorState();
        result = NULL;
    }
    PG_END_TRY();

    return result;
#endif
}

/**
 * @brief Check if a JSONB object contains a specific key.
 * @param jsonb JSONB object to check
 * @param key Key to look for
 * @return true if key exists
 */
bool jsonb_has_key(Jsonb *jsonb, const char *key) {
    JsonbValue value;

    if (!jsonb || !key) {
        return false;
    }

    return extract_jsonb_value(jsonb, key, &value);
}

/**
 * @brief Get a value from a JSONB object using a caller-provided buffer.
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param out_value Caller-provided output buffer
 * @return true if key was found
 */
bool jsonb_get_value(Jsonb *jsonb, const char *key, JsonbValue *out_value) {
    if (!jsonb || !key || !out_value) {
        return false;
    }

    return extract_jsonb_value(jsonb, key, out_value);
}

/**
 * @brief Get an int32 value from a JSONB object by key.
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param default_value Value returned if key is missing or not numeric
 * @return Extracted int32 or default_value
 */
int32 jsonb_get_int32(Jsonb *jsonb, const char *key, int32 default_value) {
    JsonbValue value;
    if (jsonb_get_value(jsonb, key, &value) && value.type == jbvNumeric) {
        /* Use PostgreSQL's built-in numeric to string conversion */
        Datum numeric_datum = PointerGetDatum(value.val.numeric);
        char *num_str = DatumGetCString(DirectFunctionCall1(numeric_out, numeric_datum));
        if (num_str) {
            int32 result = default_value;
            if (sscanf(num_str, "%d", &result) == 1) {
                pfree(num_str);
                return result;
            }
            pfree(num_str);
        }
    }

    return default_value;
}

/**
 * @brief Convert a JSONB numeric to int32 without raising on bad input.
 *
 * numeric_int4 raises an ERROR for a value outside int4; called while a worker
 * builds a dispatcher, that ERROR takes the whole batch down. This returns
 * false instead, for fractions too.
 *
 * @param num Numeric value
 * @param out Set to the value on success (may be NULL)
 * @return true if num is an integer within int32
 */
bool jsonb_numeric_to_int32(Numeric num, int32 *out) {
    char *str = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
    char *end;
    long long val;
    bool ok;

    errno = 0;
    val = strtoll(str, &end, 10);
    ok = errno == 0 && end != str && *end == '\0' && val >= PG_INT32_MIN && val <= PG_INT32_MAX;
    pfree(str);

    if (ok && out)
        *out = (int32)val;
    return ok;
}

/**
 * @brief Get a string value from a JSONB object by key.
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param default_value Value returned if key is missing or not a string (may be NULL)
 * @return palloc'd string copy, or NULL if not found and no default
 */
char *jsonb_get_string(Jsonb *jsonb, const char *key, const char *default_value) {
    JsonbValue value;
    if (jsonb_get_value(jsonb, key, &value) && value.type == jbvString) {
        return pnstrdup(value.val.string.val, value.val.string.len);
    }

    if (default_value) {
        return pstrdup(default_value);
    }

    return NULL;
}

/**
 * @brief Get a boolean value from a JSONB object by key.
 *
 * Supports native jbvBool and string representations
 * ("true"/"false", "1"/"0", "yes"/"no", "on"/"off").
 *
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param default_value Value returned if key is missing or unrecognized
 * @return Extracted boolean or default_value
 */
bool jsonb_get_bool(Jsonb *jsonb, const char *key, bool default_value) {
    JsonbValue value;
    if (!jsonb_get_value(jsonb, key, &value)) {
        return default_value;
    }

    if (value.type == jbvBool) {
        return value.val.boolean;
    }

    /* Also support parsing boolean from string values */
    if (value.type == jbvString) {
        char *str_val = pnstrdup(value.val.string.val, value.val.string.len);
        if (str_val) {
            /* Check for common boolean string representations */
            if (strcmp(str_val, "true") == 0 || strcmp(str_val, "1") == 0 ||
                strcmp(str_val, "yes") == 0 || strcmp(str_val, "on") == 0) {
                pfree(str_val);
                return true;
            } else if (strcmp(str_val, "false") == 0 || strcmp(str_val, "0") == 0 ||
                       strcmp(str_val, "no") == 0 || strcmp(str_val, "off") == 0) {
                pfree(str_val);
                return false;
            }
            pfree(str_val);
        }
    }

    return default_value;
}

/**
 * @brief Get an int64 value from a JSONB object by key.
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param default_value Value returned if key is missing or not numeric
 * @return Extracted int64 or default_value
 */
int64 jsonb_get_int64(Jsonb *jsonb, const char *key, int64 default_value) {
    JsonbValue value;
    if (jsonb_get_value(jsonb, key, &value) && value.type == jbvNumeric) {
        /* Use PostgreSQL's built-in numeric to string conversion */
        Datum numeric_datum = PointerGetDatum(value.val.numeric);
        char *num_str = DatumGetCString(DirectFunctionCall1(numeric_out, numeric_datum));
        if (num_str) {
            int64 result = default_value;
            if (sscanf(num_str, "%lld", (long long *)&result) == 1) {
                pfree(num_str);
                return result;
            }
            pfree(num_str);
        }
    }

    return default_value;
}

/**
 * @brief Get a double value from a JSONB object by key.
 * @param jsonb JSONB object to search
 * @param key Key to look up
 * @param default_value Value returned if key is missing or not numeric
 * @return Extracted double or default_value
 */
double jsonb_get_double(Jsonb *jsonb, const char *key, double default_value) {
    JsonbValue value;
    if (jsonb_get_value(jsonb, key, &value) && value.type == jbvNumeric) {
        /* Use PostgreSQL's built-in numeric to string conversion */
        Datum numeric_datum = PointerGetDatum(value.val.numeric);
        char *num_str = DatumGetCString(DirectFunctionCall1(numeric_out, numeric_datum));
        if (num_str) {
            double result = default_value;
            if (sscanf(num_str, "%lf", &result) == 1) {
                pfree(num_str);
                return result;
            }
            pfree(num_str);
        }
    }

    return default_value;
}

/**
 * @brief Get a nested JSONB object by key.
 *
 * Uses findJsonbValueFromContainer for direct lookup including nested objects.
 *
 * @param jsonb Parent JSONB object
 * @param key Key of the nested object
 * @return palloc'd Jsonb or NULL if not found or not an object/array
 */
Jsonb *jsonb_get_nested(Jsonb *jsonb, const char *key) {
    JsonbValue kval;
    JsonbValue *result;

    if (!jsonb || !key)
        return NULL;

    kval.type = jbvString;
    kval.val.string.val = (char *)key;
    kval.val.string.len = strlen(key);

    result = findJsonbValueFromContainer(&jsonb->root, JB_FOBJECT, &kval);
    if (result && result->type == jbvBinary) {
        Jsonb *nested = palloc(VARHDRSZ + result->val.binary.len);
        SET_VARSIZE(nested, VARHDRSZ + result->val.binary.len);
        memcpy(&nested->root, result->val.binary.data, result->val.binary.len);
        pfree(result);
        return nested;
    }
    if (result)
        pfree(result);
    return NULL;
}

/**
 * @brief Validate that all top-level keys in a JSONB object are in an allowed list.
 * @param jsonb JSONB object to validate
 * @param allowed_keys NULL-terminated array of allowed key strings
 * @param unknown_key Output: first unknown key found (palloc'd), or NULL
 * @return true if all keys are allowed
 */
bool jsonb_validate_keys(Jsonb *jsonb, const char **allowed_keys, char **unknown_key) {
    JsonbIterator *it;
    JsonbIteratorToken tok;
    JsonbValue v;
    int depth = 0;

    if (!jsonb || !allowed_keys) {
        return false;
    }

    if (unknown_key) {
        *unknown_key = NULL;
    }

    it = JsonbIteratorInit(&jsonb->root);

    while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        /* Track depth to only check top-level keys */
        if (tok == WJB_BEGIN_OBJECT) {
            depth++;
            continue;
        }
        if (tok == WJB_END_OBJECT) {
            depth--;
            continue;
        }
        if (tok == WJB_BEGIN_ARRAY || tok == WJB_END_ARRAY) {
            continue;
        }

        /* Only validate top-level keys (depth == 1) */
        if (tok == WJB_KEY && depth == 1 && v.type == jbvString) {
            const char **allowed;
            bool found = false;

            /* Check if this key is in allowed list */
            for (allowed = allowed_keys; *allowed != NULL; allowed++) {
                size_t allowed_len = strlen(*allowed);
                if (v.val.string.len == allowed_len &&
                    strncmp(v.val.string.val, *allowed, allowed_len) == 0) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                /* Unknown key found */
                if (unknown_key) {
                    *unknown_key = pnstrdup(v.val.string.val, v.val.string.len);
                }
                return false;
            }
        }
    }

    return true;
}
