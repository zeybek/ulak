/**
 * @file nats_dispatcher.c
 * @brief NATS dispatcher factory, create, and cleanup.
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

#include "config/guc.h"
#include "nats_dispatcher.h"
#include "nats_internal.h"
#include "utils/json_utils.h"

#include <string.h>

/** Allowed config keys for validation */
static const char *NATS_ALLOWED_CONFIG_KEYS[] = {
    NATS_CONFIG_KEY_URL,         NATS_CONFIG_KEY_SUBJECT,
    NATS_CONFIG_KEY_JETSTREAM,   NATS_CONFIG_KEY_STREAM,
    NATS_CONFIG_KEY_TOKEN,       NATS_CONFIG_KEY_USERNAME,
    NATS_CONFIG_KEY_PASSWORD,    NATS_CONFIG_KEY_NKEY_SEED,
    NATS_CONFIG_KEY_CREDENTIALS, NATS_CONFIG_KEY_TLS,
    NATS_CONFIG_KEY_TLS_CA_CERT, NATS_CONFIG_KEY_TLS_CERT,
    NATS_CONFIG_KEY_TLS_KEY,     NATS_CONFIG_KEY_HEADERS,
    NATS_CONFIG_KEY_OPTIONS,     NULL};

/** Operations struct */
static DispatcherOperations nats_ops = {
    .dispatch = nats_dispatcher_dispatch,
    .validate_config = nats_dispatcher_validate_config,
    .cleanup = NULL, /* Set in create */
    .produce = nats_dispatcher_produce,
    .flush = nats_dispatcher_flush,
    .supports_batch = nats_dispatcher_supports_batch,
    .dispatch_ex = nats_dispatcher_dispatch_ex,
    .produce_ex = nats_dispatcher_produce_ex,
};

/**
 * @private
 * @brief Clean up and destroy a NATS dispatcher.
 * @param self Dispatcher to clean up
 */
static void nats_dispatcher_cleanup(Dispatcher *self) {
    NatsDispatcher *nats = (NatsDispatcher *)self->private_data;

    if (nats == NULL)
        return;

    /* Destroy JetStream context */
    if (nats->js != NULL) {
        jsCtx_Destroy(nats->js);
        nats->js = NULL;
    }

    /* Destroy connection */
    if (nats->conn != NULL) {
        natsConnection_Destroy(nats->conn);
        nats->conn = NULL;
    }

    /* Destroy options */
    if (nats->opts != NULL) {
        natsOptions_Destroy(nats->opts);
        nats->opts = NULL;
    }

    /* Release cnats thread-local memory */
    nats_ReleaseThreadMemory();

    /* Free config strings -- zero sensitive fields first */
    if (nats->token) {
        explicit_bzero(nats->token, strlen(nats->token));
        pfree(nats->token);
    }
    if (nats->password) {
        explicit_bzero(nats->password, strlen(nats->password));
        pfree(nats->password);
    }
    if (nats->nkey_seed) {
        explicit_bzero(nats->nkey_seed, strlen(nats->nkey_seed));
        pfree(nats->nkey_seed);
    }
    if (nats->credentials_file) {
        explicit_bzero(nats->credentials_file, strlen(nats->credentials_file));
        pfree(nats->credentials_file);
    }
    if (nats->tls_key) {
        explicit_bzero(nats->tls_key, strlen(nats->tls_key));
        pfree(nats->tls_key);
    }
    if (nats->url)
        pfree(nats->url);
    if (nats->subject)
        pfree(nats->subject);
    if (nats->stream)
        pfree(nats->stream);
    if (nats->username)
        pfree(nats->username);
    if (nats->tls_ca_cert)
        pfree(nats->tls_ca_cert);
    if (nats->tls_cert)
        pfree(nats->tls_cert);

    /* Free batch tracking */
    if (nats->pending_messages)
        pfree(nats->pending_messages);

    pfree(nats);
    self->private_data = NULL;
}

/**
 * @brief Validate NATS configuration from JSONB.
 * @param config JSONB configuration to validate
 * @return true if configuration is valid, false otherwise
 */
bool nats_dispatcher_validate_config(Jsonb *config) {
    char *unknown_key = NULL;
    JsonbValue val;
    JsonbValue opt_key_val, opt_val;
    JsonbIterator *it;
    JsonbIteratorToken tok;

    if (config == NULL) {
        elog(WARNING, "[ulak] NATS config is NULL");
        return false;
    }

    /* Validate keys using utility function (only checks top-level) */
    if (!jsonb_validate_keys(config, NATS_ALLOWED_CONFIG_KEYS, &unknown_key)) {
        elog(WARNING,
             "[ulak] ERROR: NATS config contains unknown key '%s'. "
             "Allowed keys: url, subject, jetstream, stream, token, username, "
             "password, nkey_seed, credentials_file, tls, tls_ca_cert, tls_cert, "
             "tls_key, headers, options",
             unknown_key ? unknown_key : "?");
        if (unknown_key)
            pfree(unknown_key);
        return false;
    }

    if (!extract_jsonb_value(config, NATS_CONFIG_KEY_URL, &val) || val.type != jbvString ||
        val.val.string.len == 0) {
        elog(WARNING, "[ulak] ERROR: NATS config missing required key 'url'");
        return false;
    }
    if (!extract_jsonb_value(config, NATS_CONFIG_KEY_SUBJECT, &val) || val.type != jbvString ||
        val.val.string.len == 0) {
        elog(WARNING, "[ulak] ERROR: NATS config missing required key 'subject'");
        return false;
    }
    /* NATS publish subjects must not contain wildcards ('*' token wildcard,
     * '>' full wildcard). These are only valid for subscribe operations. */
    if (memchr(val.val.string.val, '*', val.val.string.len) != NULL ||
        memchr(val.val.string.val, '>', val.val.string.len) != NULL) {
        elog(WARNING,
             "[ulak] ERROR: NATS 'subject' must not contain wildcard characters ('*' or '>')");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_JETSTREAM, &val) && val.type != jbvBool) {
        elog(WARNING, "[ulak] ERROR: NATS 'jetstream' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_TLS, &val) && val.type != jbvBool) {
        elog(WARNING, "[ulak] ERROR: NATS 'tls' must be a boolean");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_STREAM, &val) &&
        (val.type != jbvString || val.val.string.len == 0)) {
        elog(WARNING, "[ulak] ERROR: NATS 'stream' must be a non-empty string");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_TOKEN, &val) &&
        (val.type != jbvString || val.val.string.len == 0)) {
        elog(WARNING, "[ulak] ERROR: NATS 'token' must be a non-empty string");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_USERNAME, &val) &&
        (val.type != jbvString || val.val.string.len == 0)) {
        elog(WARNING, "[ulak] ERROR: NATS 'username' must be a non-empty string");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_PASSWORD, &val) &&
        (val.type != jbvString || val.val.string.len == 0)) {
        elog(WARNING, "[ulak] ERROR: NATS 'password' must be a non-empty string");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_NKEY_SEED, &val) &&
        (val.type != jbvString || val.val.string.len == 0)) {
        elog(WARNING, "[ulak] ERROR: NATS 'nkey_seed' must be a non-empty string");
        return false;
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_CREDENTIALS, &val)) {
        if (val.type != jbvString || val.val.string.len == 0) {
            elog(WARNING, "[ulak] ERROR: NATS 'credentials_file' must be a non-empty string");
            return false;
        }
        /* Reject directory traversal and non-absolute paths for security.
         * Only allow absolute paths with .creds extension to prevent
         * arbitrary file access via the PG server OS user. */
        {
            char *cpath = pnstrdup(val.val.string.val, val.val.string.len);
            if (strstr(cpath, "..") != NULL) {
                elog(WARNING, "[ulak] ERROR: NATS 'credentials_file' must not contain '..'");
                pfree(cpath);
                return false;
            }
            if (cpath[0] != '/') {
                elog(WARNING, "[ulak] ERROR: NATS 'credentials_file' must be an absolute path");
                pfree(cpath);
                return false;
            }
            if (val.val.string.len < 6 || strcmp(cpath + val.val.string.len - 6, ".creds") != 0) {
                elog(WARNING, "[ulak] ERROR: NATS 'credentials_file' must have .creds extension");
                pfree(cpath);
                return false;
            }
            pfree(cpath);
        }
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_HEADERS, &val)) {
        if (val.type != jbvBinary && val.type != jbvObject) {
            elog(WARNING, "[ulak] ERROR: NATS 'headers' must be an object");
            return false;
        }
    }

    if (extract_jsonb_value(config, NATS_CONFIG_KEY_OPTIONS, &val)) {
        if (val.type != jbvBinary && val.type != jbvObject) {
            elog(WARNING, "[ulak] ERROR: NATS 'options' must be an object");
            return false;
        }

        it = JsonbIteratorInit(val.val.binary.data);
        while ((tok = JsonbIteratorNext(&it, &opt_key_val, false)) != WJB_DONE) {
            if (tok != WJB_KEY)
                continue;

            if ((opt_key_val.val.string.len == 13 &&
                 strncmp(opt_key_val.val.string.val, "max_reconnect", 13) == 0) ||
                (opt_key_val.val.string.len == 14 &&
                 strncmp(opt_key_val.val.string.val, "reconnect_wait", 14) == 0) ||
                (opt_key_val.val.string.len == 13 &&
                 strncmp(opt_key_val.val.string.val, "ping_interval", 13) == 0) ||
                (opt_key_val.val.string.len == 13 &&
                 strncmp(opt_key_val.val.string.val, "max_pings_out", 13) == 0) ||
                (opt_key_val.val.string.len == 11 &&
                 strncmp(opt_key_val.val.string.val, "io_buf_size", 11) == 0)) {
                tok = JsonbIteratorNext(&it, &opt_val, true);
                if (tok != WJB_VALUE || opt_val.type != jbvString) {
                    elog(WARNING, "[ulak] ERROR: NATS option values must be strings");
                    return false;
                }

                {
                    char *option_value = pnstrdup(opt_val.val.string.val, opt_val.val.string.len);

                    if (option_value[0] == '\0') {
                        pfree(option_value);
                        elog(WARNING,
                             "[ulak] ERROR: NATS option values must be non-empty integer strings");
                        return false;
                    }

                    PG_TRY();
                    {
                        (void)pg_strtoint32(option_value);
                    }
                    PG_CATCH();
                    {
                        ErrorData *edata;

                        MemoryContextSwitchTo(ErrorContext);
                        edata = CopyErrorData();
                        FlushErrorState();
                        pfree(option_value);
                        elog(WARNING,
                             "[ulak] ERROR: NATS option values must be valid integer strings");
                        FreeErrorData(edata);
                        return false;
                    }
                    PG_END_TRY();

                    pfree(option_value);
                }
            } else {
                elog(WARNING, "[ulak] ERROR: NATS options contains unsupported key");
                return false;
            }
        }
    }

    return true;
}

/**
 * @brief Create a new NATS dispatcher instance.
 * @param config JSONB configuration with url, subject, and optional settings
 * @return Dispatcher pointer on success, NULL on failure
 */
Dispatcher *nats_dispatcher_create(Jsonb *config) {
    MemoryContext old_ctx;
    MemoryContext cache_ctx;
    Dispatcher *dispatcher;
    NatsDispatcher *nats;
    natsStatus s;

    /* Allocate in dispatcher cache context for lifecycle management */
    cache_ctx = AllocSetContextCreate(CurrentMemoryContext, "NatsDispatcherContext",
                                      ALLOCSET_DEFAULT_SIZES);
    old_ctx = MemoryContextSwitchTo(cache_ctx);

    dispatcher = palloc0(sizeof(Dispatcher));
    nats = palloc0(sizeof(NatsDispatcher));

    dispatcher->protocol = PROTOCOL_TYPE_NATS;
    dispatcher->config = config;
    dispatcher->ops = &nats_ops;
    dispatcher->ops->cleanup = nats_dispatcher_cleanup;
    dispatcher->private_data = nats;

    /* Parse config using extract_jsonb_value pattern */
    {
        JsonbValue val;

        if (extract_jsonb_value(config, NATS_CONFIG_KEY_URL, &val) && val.type == jbvString)
            nats->url = pnstrdup(val.val.string.val, val.val.string.len);
        else
            nats->url = pstrdup(NATS_DEFAULT_URL);

        if (extract_jsonb_value(config, NATS_CONFIG_KEY_SUBJECT, &val) && val.type == jbvString)
            nats->subject = pnstrdup(val.val.string.val, val.val.string.len);

        /* JetStream defaults to true */
        nats->jetstream = true;
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_JETSTREAM, &val) && val.type == jbvBool)
            nats->jetstream = val.val.boolean;

        if (extract_jsonb_value(config, NATS_CONFIG_KEY_STREAM, &val) && val.type == jbvString)
            nats->stream = pnstrdup(val.val.string.val, val.val.string.len);

        /* Auth */
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_TOKEN, &val) && val.type == jbvString)
            nats->token = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_USERNAME, &val) && val.type == jbvString)
            nats->username = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_PASSWORD, &val) && val.type == jbvString)
            nats->password = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_NKEY_SEED, &val) && val.type == jbvString)
            nats->nkey_seed = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_CREDENTIALS, &val) && val.type == jbvString)
            nats->credentials_file = pnstrdup(val.val.string.val, val.val.string.len);

        /* TLS */
        nats->tls_enabled = false;
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_TLS, &val) && val.type == jbvBool)
            nats->tls_enabled = val.val.boolean;
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_TLS_CA_CERT, &val) && val.type == jbvString)
            nats->tls_ca_cert = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_TLS_CERT, &val) && val.type == jbvString)
            nats->tls_cert = pnstrdup(val.val.string.val, val.val.string.len);
        if (extract_jsonb_value(config, NATS_CONFIG_KEY_TLS_KEY, &val) && val.type == jbvString)
            nats->tls_key = pnstrdup(val.val.string.val, val.val.string.len);
    }

    /* Static headers -- store reference to config (deep-copied in cache context) */
    nats->static_headers = config;

    /* Batch tracking */
    nats->pending_capacity = ulak_nats_batch_capacity;
    nats->pending_messages = palloc0(sizeof(NatsPendingMessage) * nats->pending_capacity);
    nats->pending_count = 0;

    /* Create NATS options */
    s = natsOptions_Create(&nats->opts);
    if (s != NATS_OK) {
        elog(WARNING, "[ulak] Failed to create NATS options: %s", natsStatus_GetText(s));
        goto error;
    }

    /* Set URL */
    natsOptions_SetURL(nats->opts, nats->url);

    /* Set reconnect behavior */
    natsOptions_SetAllowReconnect(nats->opts, true);
    natsOptions_SetMaxReconnect(nats->opts, ulak_nats_max_reconnect);
    natsOptions_SetReconnectWait(nats->opts, ulak_nats_reconnect_wait);
    natsOptions_SetTimeout(nats->opts, ulak_nats_delivery_timeout);

    /* Apply auth */
    if (nats->credentials_file)
        natsOptions_SetUserCredentialsFromFiles(nats->opts, nats->credentials_file, NULL);
    else if (nats->nkey_seed)
        natsOptions_SetNKeyFromSeed(nats->opts, NULL, nats->nkey_seed);
    else if (nats->token)
        natsOptions_SetToken(nats->opts, nats->token);
    else if (nats->username && nats->password)
        natsOptions_SetUserInfo(nats->opts, nats->username, nats->password);

    /* Apply TLS */
    if (nats->tls_enabled) {
        natsOptions_SetSecure(nats->opts, true);
        if (nats->tls_ca_cert)
            natsOptions_LoadCATrustedCertificates(nats->opts, nats->tls_ca_cert);
        if (nats->tls_cert && nats->tls_key)
            natsOptions_LoadCertificatesChain(nats->opts, nats->tls_cert, nats->tls_key);
    }

    /* Apply custom options from config */
    nats_apply_options(nats->opts, config);

    /* Connect */
    s = natsConnection_Connect(&nats->conn, nats->opts);
    if (s != NATS_OK) {
        elog(WARNING, "[ulak] NATS connection failed to %s: %s", nats->url, natsStatus_GetText(s));
        goto error;
    }

    /* Create JetStream context if enabled */
    if (nats->jetstream) {
        jsOptions jsOpts;
        jsOptions_Init(&jsOpts);
        jsOpts.PublishAsync.MaxPending = nats->pending_capacity;

        s = natsConnection_JetStream(&nats->js, nats->conn, &jsOpts);
        if (s != NATS_OK) {
            elog(WARNING, "[ulak] JetStream context creation failed: %s", natsStatus_GetText(s));
            goto error;
        }
    }

    MemoryContextSwitchTo(old_ctx);
    return dispatcher;

error:
    MemoryContextSwitchTo(old_ctx);
    if (nats->conn) {
        natsConnection_Destroy(nats->conn);
        nats->conn = NULL;
    }
    if (nats->opts) {
        natsOptions_Destroy(nats->opts);
        nats->opts = NULL;
    }
    nats_ReleaseThreadMemory();
    MemoryContextDelete(cache_ctx);
    return NULL;
}
