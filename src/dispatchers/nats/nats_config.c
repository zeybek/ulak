/**
 * @file nats_config.c
 * @brief NATS configuration parsing.
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "nats_dispatcher.h"
#include "nats_internal.h"
#include "utils/json_utils.h"

/**
 * @private
 * @brief Parse a NATS option value as a 32-bit integer.
 * @param key Option key name (for error messages)
 * @param value String value to parse
 * @return Parsed integer value (ereports on invalid input)
 */
static int32 parse_nats_option_int(const char *key, const char *value) {
    int32 parsed;

    if (value == NULL || *value == '\0') {
        elog(ERROR, "[ulak] NATS option '%s' must be a non-empty integer string", key);
    }

    parsed = pg_strtoint32(value);
    return parsed;
}

/**
 * @brief Apply custom options from JSONB config to NATS connection options.
 * @param opts NATS options object to populate
 * @param config JSONB configuration containing the "options" field
 */
void nats_apply_options(natsOptions *opts, Jsonb *config) {
    JsonbIterator *it;
    JsonbValue key_val, val;
    JsonbIteratorToken tok;
    bool in_options = false;

    if (config == NULL)
        return;

    it = JsonbIteratorInit(&config->root);
    while ((tok = JsonbIteratorNext(&it, &key_val, false)) != WJB_DONE) {
        if (tok == WJB_KEY) {
            char *key = pnstrdup(key_val.val.string.val, key_val.val.string.len);

            if (strcmp(key, NATS_CONFIG_KEY_OPTIONS) == 0) {
                in_options = true;
                pfree(key);
                continue;
            }

            if (in_options) {
                tok = JsonbIteratorNext(&it, &val, true);
                if (tok == WJB_VALUE && val.type == jbvString) {
                    char *sval = pnstrdup(val.val.string.val, val.val.string.len);
                    int32 parsed;

                    parsed = parse_nats_option_int(key, sval);

                    if (strcmp(key, "max_reconnect") == 0)
                        natsOptions_SetMaxReconnect(opts, parsed);
                    else if (strcmp(key, "reconnect_wait") == 0)
                        natsOptions_SetReconnectWait(opts, parsed);
                    else if (strcmp(key, "ping_interval") == 0)
                        natsOptions_SetPingInterval(opts, parsed * 1000);
                    else if (strcmp(key, "max_pings_out") == 0)
                        natsOptions_SetMaxPingsOut(opts, parsed);
                    else if (strcmp(key, "io_buf_size") == 0)
                        natsOptions_SetIOBufSize(opts, parsed);

                    pfree(sval);
                }
            }

            pfree(key);
        }

        if (tok == WJB_END_OBJECT && in_options) {
            in_options = false;
        }
    }
}
