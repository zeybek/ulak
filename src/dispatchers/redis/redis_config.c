/**
 * @file redis_config.c
 * @brief Redis dispatcher configuration validation.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles configuration validation for Redis dispatcher.
 */

#include "redis_internal.h"

#include "fmgr.h"
#include "postgres.h"
#include "utils/json_utils.h"
#include "utils/logging.h"
#include "utils/numeric.h"

const char *REDIS_ALLOWED_CONFIG_KEYS[] = {REDIS_CONFIG_KEY_HOST,
                                           REDIS_CONFIG_KEY_PORT,
                                           REDIS_CONFIG_KEY_DB,
                                           REDIS_CONFIG_KEY_PASSWORD,
                                           REDIS_CONFIG_KEY_STREAM_KEY,
                                           REDIS_CONFIG_KEY_CONNECT_TIMEOUT,
                                           REDIS_CONFIG_KEY_COMMAND_TIMEOUT,
                                           REDIS_CONFIG_KEY_MAXLEN,
                                           REDIS_CONFIG_KEY_TLS,
                                           REDIS_CONFIG_KEY_TLS_CA_CERT,
                                           REDIS_CONFIG_KEY_TLS_CERT,
                                           REDIS_CONFIG_KEY_TLS_KEY,
                                           REDIS_CONFIG_KEY_USERNAME,
                                           REDIS_CONFIG_KEY_NOMKSTREAM,
                                           REDIS_CONFIG_KEY_MAXLEN_APPROXIMATE,
                                           REDIS_CONFIG_KEY_MINID,
                                           REDIS_CONFIG_KEY_CONSUMER_GROUP,
                                           NULL};

/**
 * @brief Validate a numeric field with type and range check.
 * @param config JSONB configuration to inspect
 * @param field_name Name of the field to validate
 * @param min_val Minimum allowed value (inclusive)
 * @param max_val Maximum allowed value (inclusive)
 * @param required Whether the field is required
 * @return true if valid, false otherwise with error message logged
 */
bool redis_validate_numeric_field(Jsonb *config, const char *field_name, int64 min_val,
                                  int64 max_val, bool required) {
    JsonbValue val;
    Datum num_datum;
    int64 num_val;
    const char *type_name;

    if (!extract_jsonb_value(config, field_name, &val)) {
        if (required) {
            ulak_log("error", "Redis config: missing required field '%s'", field_name);
            return false;
        }
        return true; /* Optional field not present = OK */
    }

    /* Type check: must be numeric */
    if (val.type != jbvNumeric) {
        type_name = "unknown";
        switch (val.type) {
        case jbvString:
            type_name = "string";
            break;
        case jbvBool:
            type_name = "boolean";
            break;
        case jbvNull:
            type_name = "null";
            break;
        case jbvArray:
            type_name = "array";
            break;
        case jbvObject:
            type_name = "object";
            break;
        default:
            break;
        }
        ulak_log("error", "Redis config: '%s' must be a number, got %s", field_name, type_name);
        return false;
    }

    /* Range check */
    num_datum = DirectFunctionCall1(numeric_int8, NumericGetDatum(val.val.numeric));
    num_val = DatumGetInt64(num_datum);

    if (num_val < min_val || num_val > max_val) {
        ulak_log("error", "Redis config: '%s' value %lld out of range [%lld-%lld]", field_name,
                 (long long)num_val, (long long)min_val, (long long)max_val);
        return false;
    }

    return true;
}
