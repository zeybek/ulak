/**
 * @file retry_policy.c
 * @brief Retry policy parsing and backoff calculation.
 *
 * Clean Architecture: Utility Layer.
 * Extracted from worker.c: these helpers have no SPI / transaction
 * dependencies and are candidates for unit testing.
 */

#include "utils/retry_policy.h"

#include <string.h>

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#include "config/guc.h"
#include "core/entities.h"
#include "utils/json_utils.h"

int get_max_retries_from_policy(Jsonb *retry_policy) {
    JsonbValue max_retries_val;
    int max_retries;

    /* Default max retries if no policy specified */
    if (!retry_policy)
        return ulak_default_max_retries;

    /* Parse retry_policy JSON to extract max_retries */
    if (extract_jsonb_value(retry_policy, "max_retries", &max_retries_val)) {
        if (max_retries_val.type == jbvNumeric) {
            max_retries = DatumGetInt32(
                DirectFunctionCall1(numeric_int4, NumericGetDatum(max_retries_val.val.numeric)));
            if (max_retries > 0)
                return max_retries;
        }
    }

    /* Fallback to default */
    return ulak_default_max_retries;
}

const char *get_backoff_type_from_policy(Jsonb *retry_policy) {
    JsonbValue backoff_val;

    /* Default backoff type if no policy specified */
    if (!retry_policy)
        return "exponential";

    /* Parse retry_policy JSON to extract backoff type */
    if (extract_jsonb_value(retry_policy, "backoff", &backoff_val)) {
        if (backoff_val.type == jbvString) {
            /* Compare against known backoff types and return static string */
            if (backoff_val.val.string.len == 5 &&
                strncmp(backoff_val.val.string.val, "fixed", 5) == 0) {
                return "fixed";
            } else if (backoff_val.val.string.len == 6 &&
                       strncmp(backoff_val.val.string.val, "linear", 6) == 0) {
                return "linear";
            }
            /* Unknown backoff type - fall through to default */
        }
    }

    return "exponential"; /* Default */
}

int calculate_exponential_backoff(int retry_count) {
    int multiplier;
    int delay;

    /* Simple exponential backoff: base_delay * 2^retry_count */
    /* Max delay capped at ulak_retry_max_delay */

    /* Overflow protection: cap retry_count to prevent overflow */
    if (retry_count > MAX_RETRY_COUNT_FOR_EXPONENTIAL)
        return ulak_retry_max_delay;

    multiplier = (1 << retry_count); /* 2^retry_count */
    delay = ulak_retry_base_delay * multiplier;

    /* Check for overflow (delay should be >= base_delay) */
    if (delay < ulak_retry_base_delay)
        return ulak_retry_max_delay;

    return (delay > ulak_retry_max_delay) ? ulak_retry_max_delay : delay;
}

int calculate_delay_from_policy(Jsonb *retry_policy, int retry_count) {
    const char *backoff_type = get_backoff_type_from_policy(retry_policy);

    if (strcmp(backoff_type, "fixed") == 0) {
        /* Fixed delay - use base_delay as fixed value */
        JsonbValue delay_val;
        if (extract_jsonb_value(retry_policy, "delay", &delay_val) &&
            delay_val.type == jbvNumeric) {
            return ulak_retry_base_delay;
        }
        return ulak_retry_base_delay;
    } else if (strcmp(backoff_type, "linear") == 0) {
        /* Linear backoff: base_delay + (retry_count * increment) */
        int delay = ulak_retry_base_delay + (retry_count * ulak_retry_increment);
        return (delay > ulak_retry_max_delay) ? ulak_retry_max_delay : delay;
    } else {
        /* Exponential backoff (default) */
        return calculate_exponential_backoff(retry_count);
    }
}
