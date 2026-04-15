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
    size_t i;

    /* Convert to lowercase for case-insensitive matching */
    for (i = 0; i < sizeof(key_lower) - 1 && key[i]; i++) {
        key_lower[i] = (key[i] >= 'A' && key[i] <= 'Z') ? key[i] + 32 : key[i];
    }
    key_lower[i] = '\0';

    for (pattern = sensitive_patterns; *pattern != NULL; pattern++) {
        if (strstr(key_lower, *pattern) != NULL) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Validate SASL/SSL configuration consistency.
 *
 * Checks that when SASL is configured, required fields are present.
 *
 * @param config JSONB configuration to validate
 * @return true if SASL/SSL configuration is consistent
 */
bool kafka_validate_sasl_ssl_config(Jsonb *config) {
    JsonbValue options_val;
    JsonbContainer *container;
    JsonbIterator *it;
    JsonbValue key_jv, val_jv;
    JsonbIteratorToken token;
    bool has_sasl_mechanism = false;
    bool has_sasl_username = false;
    bool has_sasl_password = false;
    bool has_security_protocol = false;

    if (!config || !extract_jsonb_value(config, CONFIG_KEY_OPTIONS, &options_val) ||
        options_val.type != jbvBinary) {
        return true; /* No options = no SASL config to validate */
    }

    container = options_val.val.binary.data;
    it = JsonbIteratorInit(container);

    while ((token = JsonbIteratorNext(&it, &key_jv, false)) != WJB_DONE) {
        if (token == WJB_KEY && key_jv.type == jbvString) {
            char *key = pnstrdup(key_jv.val.string.val, key_jv.val.string.len);

            if (strcmp(key, "sasl.mechanism") == 0 || strcmp(key, "sasl.mechanisms") == 0)
                has_sasl_mechanism = true;
            else if (strcmp(key, "sasl.username") == 0)
                has_sasl_username = true;
            else if (strcmp(key, "sasl.password") == 0)
                has_sasl_password = true;
            else if (strcmp(key, "security.protocol") == 0)
                has_security_protocol = true;

            pfree(key);

            /* Consume the value token */
            token = JsonbIteratorNext(&it, &val_jv, false);
        }
    }

    /* If SASL mechanism is set, require username and password for PLAIN/SCRAM */
    if (has_sasl_mechanism && (!has_sasl_username || !has_sasl_password)) {
        ulak_log("warning", "Kafka SASL mechanism configured but missing username or password");
        return false;
    }

    /* If SASL is configured, security.protocol should be set */
    if (has_sasl_mechanism && !has_security_protocol) {
        ulak_log("warning", "Kafka SASL configured but security.protocol not set "
                            "(should be SASL_PLAINTEXT or SASL_SSL)");
    }

    return true;
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
    char errstr[512];
    const char *acks;
    const char *compression;
    JsonbContainer *container;
    JsonbIterator *it;
    JsonbValue key_jv, val_jv;
    JsonbIteratorToken token;

    /* Set default configuration from GUC values (or compile-time fallbacks) */
    acks = ulak_kafka_acks ? ulak_kafka_acks : "all";
    compression = ulak_kafka_compression ? ulak_kafka_compression : "snappy";

    rd_kafka_conf_set(conf, "acks", acks, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "retries", DEFAULT_RETRIES, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.size", DEFAULT_BATCH_SIZE, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "linger.ms", DEFAULT_LINGER_MS, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "compression.type", compression, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.idempotence", "true", errstr, sizeof(errstr));

    /* Check if options exist in config */
    if (!config || !extract_jsonb_value(config, CONFIG_KEY_OPTIONS, &options_val)) {
        return;
    }

    if (options_val.type != jbvBinary) {
        return;
    }

    /* Parse options from JSONB object */
    container = options_val.val.binary.data;
    it = JsonbIteratorInit(container);

    while ((token = JsonbIteratorNext(&it, &key_jv, false)) != WJB_DONE) {
        if (token == WJB_KEY && key_jv.type == jbvString) {
            /* Extract key using dynamic allocation */
            char *key_str = pnstrdup(key_jv.val.string.val, key_jv.val.string.len);

            /* Get the value (next token should be WJB_VALUE) */
            token = JsonbIteratorNext(&it, &val_jv, false);
            if (token == WJB_VALUE && val_jv.type == jbvString) {
                char *val_str = pnstrdup(val_jv.val.string.val, val_jv.val.string.len);

                if (rd_kafka_conf_set(conf, key_str, val_str, errstr, sizeof(errstr)) !=
                    RD_KAFKA_CONF_OK) {
                    ulak_log("warning", "Failed to set Kafka option %s=%s: %s", key_str,
                             kafka_is_sensitive_key(key_str) ? "***" : val_str, errstr);
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
