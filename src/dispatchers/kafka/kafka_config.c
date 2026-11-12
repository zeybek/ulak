/**
 * @file kafka_config.c
 * @brief Kafka configuration parsing.
 *
 * Clean Architecture: Interface Adapters Layer
 * Handles parsing and validation of Kafka configuration options.
 */

#include <string.h>
#include "config/guc.h"
#include "kafka_internal.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/json_utils.h"
#include "utils/logging.h"

/**
 * @private
 * @brief Check if a Kafka config key is sensitive (contains credentials).
 * @param key Configuration key name to check
 * @return true if the value should be masked in logs
 *
 * THREAD SAFETY: Safe to call from any thread. Uses only stack-local data.
 */
static bool kafka_is_sensitive_key(const char *key) {
    /* Kafka config keys that may contain sensitive data */
    static const char *sensitive_patterns[] = {"password",   "secret", "key.pem", "certificate",
                                               "credential", "token",  NULL};
    const char **pattern;
    char key_lower[256];
    char *p;

    /* Lowercase a bounded copy for case-insensitive matching (ASCII only,
     * no locale lookup) */
    strlcpy(key_lower, key, sizeof(key_lower));
    for (p = key_lower; *p; p++) {
        *p = pg_ascii_tolower((unsigned char)*p);
    }

    for (pattern = sensitive_patterns; *pattern != NULL; pattern++) {
        if (strstr(key_lower, *pattern) != NULL) {
            return true;
        }
    }
    return false;
}

/**
 * @private
 * @brief Set one librdkafka property, logging a rejected value.
 * @param conf librdkafka configuration object
 * @param key Property name
 * @param value Property value
 * @return true if librdkafka accepted it
 */
static bool kafka_conf_set_logged(rd_kafka_conf_t *conf, const char *key, const char *value) {
    char errstr[512];

    if (rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr)) == RD_KAFKA_CONF_OK)
        return true;
    ulak_log("warning", "Failed to set Kafka option %s=%s: %s", key,
             kafka_is_sensitive_key(key) ? "***" : value, errstr);
    return false;
}

/**
 * @private
 * @brief Whether an acks value is the one the idempotent producer requires.
 * @param acks acks value
 * @return true for "all" and "-1"
 */
static bool kafka_acks_is_all(const char *acks) {
    return strcmp(acks, "all") == 0 || strcmp(acks, "-1") == 0;
}

/**
 * @brief Parse Kafka options from JSONB configuration.
 *
 * Sets librdkafka configuration options from the "options" field.
 * Also applies GUC-based defaults for acks and compression.
 *
 * @param conf librdkafka configuration object to populate
 * @param config JSONB configuration containing the "options" field
 */
void kafka_parse_options(rd_kafka_conf_t *conf, Jsonb *config) {
    JsonbValue options_val;
    const char *acks;
    const char *compression;
    char *option_acks = NULL;
    bool option_idempotence = false;
    char timeout_str[16];
    JsonbContainer *container;
    JsonbIterator *it;
    JsonbValue key_jv, val_jv;
    JsonbIteratorToken token;

    /* Set default configuration from GUC values (or compile-time fallbacks) */
    acks = ulak_kafka_acks && ulak_kafka_acks[0] ? ulak_kafka_acks : "all";
    compression = ulak_kafka_compression ? ulak_kafka_compression : "snappy";

    /* librdkafka keeps an undelivered message for message.timeout.ms (300 s by
     * default) and may still send it after ulak has given up waiting and
     * scheduled a retry; give up at the same time instead */
    snprintf(timeout_str, sizeof(timeout_str), "%d",
             ulak_kafka_delivery_timeout > 0 ? ulak_kafka_delivery_timeout : 30000);

    kafka_conf_set_logged(conf, "acks", acks);
    kafka_conf_set_logged(conf, "retries", DEFAULT_RETRIES);
    kafka_conf_set_logged(conf, "batch.size", DEFAULT_BATCH_SIZE);
    kafka_conf_set_logged(conf, "linger.ms", DEFAULT_LINGER_MS);
    kafka_conf_set_logged(conf, "compression.type", compression);
    kafka_conf_set_logged(conf, "message.timeout.ms", timeout_str);
    kafka_conf_set_logged(conf, "enable.idempotence", "true");

    /* Check if options exist in config */
    if (config && extract_jsonb_value(config, CONFIG_KEY_OPTIONS, &options_val) &&
        options_val.type == jbvBinary) {
        /* Parse options from JSONB object */
        container = options_val.val.binary.data;
        it = JsonbIteratorInit(container);

        while ((token = JsonbIteratorNext(&it, &key_jv, false)) != WJB_DONE) {
            if (token == WJB_KEY && key_jv.type == jbvString) {
                /* Extract key using dynamic allocation */
                char *key_str = pnstrdup(key_jv.val.string.val, key_jv.val.string.len);
                char *val_str = NULL;

                /* Get the value (next token should be WJB_VALUE). librdkafka takes
                 * every property as a string; accept JSON numbers and booleans
                 * too ("linger.ms": 10) instead of dropping them. */
                token = JsonbIteratorNext(&it, &val_jv, false);
                if (token == WJB_VALUE && val_jv.type == jbvString)
                    val_str = pnstrdup(val_jv.val.string.val, val_jv.val.string.len);
                else if (token == WJB_VALUE && val_jv.type == jbvNumeric)
                    val_str = DatumGetCString(
                        DirectFunctionCall1(numeric_out, NumericGetDatum(val_jv.val.numeric)));
                else if (token == WJB_VALUE && val_jv.type == jbvBool)
                    val_str = pstrdup(val_jv.val.boolean ? "true" : "false");
                else
                    ulak_log("warning",
                             "Ignoring Kafka option %s: value must be a string, "
                             "number or boolean",
                             key_str);

                if (val_str) {
                    if (kafka_conf_set_logged(conf, key_str, val_str)) {
                        if (strcmp(key_str, "acks") == 0 ||
                            strcmp(key_str, "request.required.acks") == 0) {
                            if (option_acks)
                                pfree(option_acks);
                            option_acks = pstrdup(val_str);
                        } else if (strcmp(key_str, "enable.idempotence") == 0) {
                            option_idempotence = true;
                        }
                    }

                    /* Zero sensitive values before freeing */
                    if (kafka_is_sensitive_key(key_str)) {
                        explicit_bzero(val_str, strlen(val_str));
                    }
                    pfree(val_str);
                }
                pfree(key_str);
            }
        }
    }

    /*
     * The idempotent producer requires acks=all; with "0" or "1" (from
     * ulak.kafka_acks or the endpoint options) rd_kafka_new() would refuse the
     * configuration and the endpoint could never deliver. A lower acks level is
     * an explicit trade of durability for latency, so turn idempotence off for
     * it, unless the endpoint asked for idempotence itself: then the conflict
     * is left for rd_kafka_new() to report.
     */
    if (!kafka_acks_is_all(option_acks ? option_acks : acks) && !option_idempotence) {
        ulak_log("debug", "Kafka acks=%s: idempotent producer disabled",
                 option_acks ? option_acks : acks);
        kafka_conf_set_logged(conf, "enable.idempotence", "false");
    }
    if (option_acks)
        pfree(option_acks);
}
