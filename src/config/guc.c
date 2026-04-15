/**
 * @file guc.c
 * @brief Configuration management implementation
 *
 * Clean Architecture: Infrastructure Layer
 * Manages PostgreSQL GUC (Grand Unified Configuration) variables
 */

#include "config/guc.h"
#include <string.h>
#include "miscadmin.h"
#include "utils/logging.h"

/**
 * Worker & Core Configuration
 * Defaults optimized for production throughput.
 * workers=4: good balance for most setups, increase for high-volume
 * poll_interval=500ms: responsive without excessive CPU
 * batch_size=200: matches HTTP batch capacity for maximum throughput
 */
int ulak_workers = 4;
int ulak_worker_restart_delay = 5;
int ulak_poll_interval = 500;
int ulak_batch_size = 200;
int ulak_log_level = LOG_LEVEL_WARNING;
int ulak_default_max_retries = 10;
int ulak_stale_recovery_timeout = 300;

/** Retry Configuration */
int ulak_retry_base_delay = 10;
int ulak_retry_max_delay = 300;
int ulak_retry_increment = 30;

char *ulak_database = NULL;

/**
 * HTTP Dispatcher Configuration
 * timeout=10s: reasonable for webhooks, 30s was too generous
 * connect_timeout=5s: fail fast on unreachable endpoints
 */
int ulak_http_timeout = 10;
int ulak_http_connect_timeout = 5;
int ulak_http_max_redirects = 0;
bool ulak_http_ssl_verify_peer = true;
bool ulak_http_ssl_verify_host = true;

bool ulak_http_allow_internal_urls = false;

/**
 * HTTP Batch Configuration
 * batch_capacity=200: matches batch_size for full utilization
 * max_connections_per_host=10: safe default for Docker/cloud. Higher values
 *   (50+) can cause connection timeouts under sustained load. With 4 workers
 *   that's 40 concurrent connections -- sufficient for most endpoints.
 * max_total_connections=25: bounded per-worker connection pool
 */
int ulak_http_batch_capacity = 200;
int ulak_http_max_connections_per_host = 10;
int ulak_http_max_total_connections = 25;
int ulak_http_flush_timeout = 30000;
bool ulak_http_enable_pipelining = true;

#ifdef ENABLE_KAFKA
int ulak_kafka_delivery_timeout = 30000;
int ulak_kafka_poll_interval = 100;
int ulak_kafka_flush_timeout = 1000;
int ulak_kafka_batch_capacity = 64;
char *ulak_kafka_acks = NULL;
char *ulak_kafka_compression = NULL;
#endif /* ENABLE_KAFKA */

#ifdef ENABLE_MQTT
int ulak_mqtt_keepalive = 60;
int ulak_mqtt_timeout = 5000;
int ulak_mqtt_default_qos = 0;
int ulak_mqtt_default_port = 1883;
#endif /* ENABLE_MQTT */

#ifdef ENABLE_REDIS
int ulak_redis_connect_timeout = 5;
int ulak_redis_command_timeout = 30;
int ulak_redis_default_db = 0;
int ulak_redis_default_port = 6379;
#endif

#ifdef ENABLE_AMQP
int ulak_amqp_connection_timeout = 10;
int ulak_amqp_heartbeat = 60;
int ulak_amqp_frame_max = 131072;
int ulak_amqp_delivery_timeout = 5000;
int ulak_amqp_batch_capacity = 64;
bool ulak_amqp_ssl_verify_peer = true;
#endif /* ENABLE_AMQP */

#ifdef ENABLE_NATS
int ulak_nats_delivery_timeout = 5000;
int ulak_nats_flush_timeout = 10000;
int ulak_nats_batch_capacity = 64;
int ulak_nats_reconnect_wait = 2000;
int ulak_nats_max_reconnect = 60;
#endif /* ENABLE_NATS */

int ulak_max_payload_size = 1048576;

/**
 * Circuit Breaker Configuration
 * threshold=10: tolerates transient errors with multi-worker setup
 * cooldown=30: faster recovery from open state
 */
int ulak_circuit_breaker_threshold = 10;
int ulak_circuit_breaker_cooldown = 30;

/** Response Tracking Configuration */
bool ulak_capture_response = false;
int ulak_response_body_max_size = 65536;

/** Event Log Configuration */
int ulak_event_log_retention_days = 30; /** Days to retain event log entries */
int ulak_dlq_retention_days = 30;       /** Days to retain DLQ messages */
int ulak_archive_retention_months = 6;  /** Months to retain archive partitions */
int ulak_max_queue_size = 1000000;      /** Max pending messages, 0=unlimited */

/** Notification Configuration */
bool ulak_enable_notify = true; /** Enable LISTEN/NOTIFY for new messages */

/** Archive Partition Configuration */
int ulak_archive_premake_months = 3; /** Months of partitions to pre-create */

/** CloudEvents Configuration */
char *ulak_cloudevents_source = NULL;

/** GUC Enum Options */
static const struct config_enum_entry ulak_log_level_options[] = {
    {"error", LOG_LEVEL_ERROR, false},
    {"warning", LOG_LEVEL_WARNING, false},
    {"info", LOG_LEVEL_INFO, false},
    {"debug", LOG_LEVEL_DEBUG, false},
    {NULL, 0, false}};

#ifdef ENABLE_KAFKA
/**
 * @brief Check hook for kafka_acks: valid values "0", "1", "-1", "all".
 * @private
 * @param newval Proposed new value
 * @param extra Unused
 * @param source GUC source context
 * @return true if value is valid
 */
static bool check_kafka_acks(char **newval, void **extra, GucSource source) {
    (void)extra;
    (void)source;
    if (*newval == NULL || **newval == '\0')
        return true; /* NULL/empty uses default */
    if (strcmp(*newval, "0") == 0 || strcmp(*newval, "1") == 0 || strcmp(*newval, "-1") == 0 ||
        strcmp(*newval, "all") == 0)
        return true;
    GUC_check_errdetail("Valid values are: \"0\", \"1\", \"-1\", \"all\".");
    return false;
}

/**
 * @brief Check hook for kafka_compression: valid values "none", "gzip", "snappy", "lz4", "zstd".
 * @private
 * @param newval Proposed new value
 * @param extra Unused
 * @param source GUC source context
 * @return true if value is valid
 */
static bool check_kafka_compression(char **newval, void **extra, GucSource source) {
    (void)extra;
    (void)source;
    if (*newval == NULL || **newval == '\0')
        return true;
    if (strcmp(*newval, "none") == 0 || strcmp(*newval, "gzip") == 0 ||
        strcmp(*newval, "snappy") == 0 || strcmp(*newval, "lz4") == 0 ||
        strcmp(*newval, "zstd") == 0)
        return true;
    GUC_check_errdetail("Valid values are: \"none\", \"gzip\", \"snappy\", \"lz4\", \"zstd\".");
    return false;
}
#endif /* ENABLE_KAFKA */

/**
 * @brief Register all ulak GUC variables with PostgreSQL.
 */
void config_init_guc_variables(void) {
    /* PGC_POSTMASTER GUCs — evaluated at extension load time, require restart */
    DefineCustomIntVariable("ulak.workers", "Number of background worker processes",
                            "Number of background worker processes for message dispatching",
                            &ulak_workers, 4, 1, 32, PGC_POSTMASTER, GUC_NOT_IN_SAMPLE, NULL, NULL,
                            NULL);

    DefineCustomIntVariable(
        "ulak.worker_restart_delay", "Delay before restarting a crashed worker (seconds)",
        "Delay in seconds before restarting a crashed background worker",
        &ulak_worker_restart_delay, 5, 1, 300, PGC_POSTMASTER, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* PGC_SIGHUP GUCs — can be changed with SIGHUP reload */
    DefineCustomIntVariable("ulak.poll_interval", "Poll interval for message queue (ms)",
                            "Interval in milliseconds for worker to poll the message queue",
                            &ulak_poll_interval, 500, 100, 60000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.batch_size", "Number of messages to process per batch",
                            "Number of messages to process in each batch", &ulak_batch_size, 200, 1,
                            1000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomEnumVariable("ulak.log_level", "Log level for ulak extension",
                             "Logging level: error, warning, info, debug", &ulak_log_level,
                             LOG_LEVEL_WARNING, ulak_log_level_options, PGC_SIGHUP,
                             GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomStringVariable("ulak.database", "Database for worker connection",
                               "The database name that the background worker connects to. "
                               "Must be set to the database where CREATE EXTENSION ulak was run.",
                               &ulak_database, "postgres", PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL,
                               NULL, NULL);

    DefineCustomIntVariable(
        "ulak.default_max_retries", "Default maximum retry attempts for failed messages",
        "Default maximum number of retry attempts before a message is marked as failed",
        &ulak_default_max_retries, 10, 0, 1000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "ulak.stale_recovery_timeout",
        "Timeout before recovering stale processing messages (seconds)",
        "Timeout in seconds before recovering stale processing messages back to pending",
        &ulak_stale_recovery_timeout, 300, 60, 3600, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL,
        NULL);

    DefineCustomIntVariable("ulak.retry_base_delay", "Base delay for retry backoff (seconds)",
                            "Base delay in seconds for retry backoff calculation",
                            &ulak_retry_base_delay, 10, 1, 3600, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.retry_max_delay", "Maximum delay for retry backoff (seconds)",
                            "Maximum delay in seconds for retry backoff", &ulak_retry_max_delay,
                            300, 1, 86400, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.retry_increment", "Increment for linear retry backoff (seconds)",
                            "Increment in seconds for linear retry backoff", &ulak_retry_increment,
                            30, 1, 3600, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* HTTP Configuration */
    DefineCustomIntVariable("ulak.http_timeout", "HTTP request timeout (seconds)",
                            "Maximum time to wait for HTTP request completion", &ulak_http_timeout,
                            10, 1, 300, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_connect_timeout", "HTTP connection timeout (seconds)",
                            "Maximum time to wait for HTTP connection establishment",
                            &ulak_http_connect_timeout, 5, 1, 60, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_max_redirects", "Maximum HTTP redirects to follow",
                            "Maximum redirects (0 to disable for SSRF protection)",
                            &ulak_http_max_redirects, 0, 0, 20, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL,
                            NULL, NULL);

    DefineCustomBoolVariable("ulak.http_ssl_verify_peer", "Verify SSL/TLS peer certificate",
                             "Enable SSL/TLS peer certificate verification",
                             &ulak_http_ssl_verify_peer, true, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL,
                             NULL, NULL);

    DefineCustomBoolVariable("ulak.http_ssl_verify_host", "Verify SSL/TLS hostname",
                             "Enable SSL/TLS hostname verification", &ulak_http_ssl_verify_host,
                             true, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomBoolVariable("ulak.http_allow_internal_urls", "Allow internal/localhost URLs",
                             "SECURITY: Disables SSRF protection. Only enable for testing.",
                             &ulak_http_allow_internal_urls, false, PGC_SUSET, GUC_NOT_IN_SAMPLE,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_batch_capacity", "HTTP batch capacity",
                            "Maximum pending HTTP requests in a batch", &ulak_http_batch_capacity,
                            200, 1, 1000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_max_connections_per_host", "Max connections per host",
                            "Maximum concurrent connections per target host",
                            &ulak_http_max_connections_per_host, 10, 1, 100, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_max_total_connections", "Max total connections",
                            "Maximum total concurrent HTTP connections",
                            &ulak_http_max_total_connections, 25, 1, 200, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.http_flush_timeout", "HTTP batch flush timeout (ms)",
                            "Maximum time to wait for batch flush", &ulak_http_flush_timeout, 30000,
                            1000, 300000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomBoolVariable("ulak.http_enable_pipelining", "Enable HTTP pipelining",
                             "Enable HTTP/1.1 pipelining and HTTP/2 multiplexing",
                             &ulak_http_enable_pipelining, true, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                             NULL, NULL, NULL);

#ifdef ENABLE_KAFKA
    /* Kafka Configuration */
    DefineCustomIntVariable("ulak.kafka_delivery_timeout", "Kafka delivery timeout (ms)",
                            "Maximum time to wait for Kafka delivery confirmation",
                            &ulak_kafka_delivery_timeout, 30000, 5000, 300000, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.kafka_poll_interval", "Kafka poll interval (ms)",
                            "Interval for polling Kafka producer", &ulak_kafka_poll_interval, 100,
                            10, 1000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.kafka_flush_timeout", "Kafka flush timeout (ms)",
                            "Maximum time to wait for Kafka flush", &ulak_kafka_flush_timeout, 5000,
                            1000, 60000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.kafka_batch_capacity", "Kafka batch capacity",
                            "Initial capacity for pending messages array",
                            &ulak_kafka_batch_capacity, 64, 16, 1024, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomStringVariable("ulak.kafka_acks", "Default Kafka acks",
                               "Acknowledgment setting ('0', '1', '-1', or 'all')",
                               &ulak_kafka_acks, "all", PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                               check_kafka_acks, NULL, NULL);

    DefineCustomStringVariable("ulak.kafka_compression", "Default Kafka compression",
                               "Compression type (none, gzip, snappy, lz4, zstd)",
                               &ulak_kafka_compression, "snappy", PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                               check_kafka_compression, NULL, NULL);
#endif /* ENABLE_KAFKA */

#ifdef ENABLE_MQTT
    /* MQTT Configuration */
    DefineCustomIntVariable("ulak.mqtt_keepalive", "MQTT keepalive (seconds)",
                            "Keep-alive interval for MQTT connection", &ulak_mqtt_keepalive, 60, 10,
                            600, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.mqtt_timeout", "MQTT timeout (ms)", "Timeout for MQTT operations",
                            &ulak_mqtt_timeout, 5000, 1000, 60000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.mqtt_default_qos", "Default MQTT QoS",
                            "Default QoS level (0, 1, or 2)", &ulak_mqtt_default_qos, 0, 0, 2,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.mqtt_default_port", "Default MQTT port",
                            "Default MQTT broker port", &ulak_mqtt_default_port, 1883, 1, 65535,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
#endif /* ENABLE_MQTT */

#ifdef ENABLE_REDIS
    /* Redis Configuration */
    DefineCustomIntVariable("ulak.redis_connect_timeout", "Redis connect timeout (seconds)",
                            "Maximum time for Redis connection", &ulak_redis_connect_timeout, 5, 1,
                            60, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.redis_command_timeout", "Redis command timeout (seconds)",
                            "Maximum time for Redis commands", &ulak_redis_command_timeout, 30, 1,
                            300, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.redis_default_db", "Default Redis database",
                            "Default database index (0-15)", &ulak_redis_default_db, 0, 0, 15,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.redis_default_port", "Default Redis port",
                            "Default Redis server port", &ulak_redis_default_port, 6379, 1, 65535,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
#endif

#ifdef ENABLE_AMQP
    /* AMQP Configuration */
    DefineCustomIntVariable("ulak.amqp_connection_timeout", "AMQP connection timeout (seconds)",
                            "Maximum time for AMQP connection", &ulak_amqp_connection_timeout, 10,
                            1, 300, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.amqp_heartbeat", "AMQP heartbeat (seconds)",
                            "Heartbeat interval (0 to disable)", &ulak_amqp_heartbeat, 60, 0, 600,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.amqp_frame_max", "AMQP max frame size (bytes)",
                            "Maximum frame size", &ulak_amqp_frame_max, 131072, 4096, 2097152,
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.amqp_delivery_timeout", "AMQP delivery timeout (ms)",
                            "Maximum time for publisher confirm", &ulak_amqp_delivery_timeout,
                            30000, 5000, 300000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.amqp_batch_capacity", "AMQP batch capacity",
                            "Initial capacity for pending messages", &ulak_amqp_batch_capacity, 64,
                            16, 1024, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomBoolVariable("ulak.amqp_ssl_verify_peer", "Verify AMQP SSL certificate",
                             "Enable SSL certificate verification", &ulak_amqp_ssl_verify_peer,
                             true, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
#endif /* ENABLE_AMQP */

#ifdef ENABLE_NATS
    /* NATS Configuration */
    DefineCustomIntVariable("ulak.nats_delivery_timeout", "NATS delivery timeout (ms)",
                            "Maximum time to wait for JetStream publish ack",
                            &ulak_nats_delivery_timeout, 5000, 1000, 60000, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.nats_flush_timeout", "NATS flush timeout (ms)",
                            "Maximum time to wait for async publish completion",
                            &ulak_nats_flush_timeout, 10000, 1000, 300000, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.nats_batch_capacity", "NATS batch capacity",
                            "Initial capacity for pending messages in batch mode",
                            &ulak_nats_batch_capacity, 64, 16, 1024, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.nats_reconnect_wait", "NATS reconnect wait (ms)",
                            "Time between reconnection attempts", &ulak_nats_reconnect_wait, 2000,
                            100, 60000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    DefineCustomIntVariable("ulak.nats_max_reconnect", "NATS max reconnect attempts",
                            "Maximum reconnection attempts (-1 for infinite)",
                            &ulak_nats_max_reconnect, 60, -1, 10000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                            NULL, NULL, NULL);
#endif /* ENABLE_NATS */

    /* System Configuration */
    DefineCustomIntVariable("ulak.max_payload_size", "Max payload size (bytes)",
                            "Maximum message payload size", &ulak_max_payload_size, 1048576, 1024,
                            104857600, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* Circuit Breaker Configuration */
    DefineCustomIntVariable(
        "ulak.circuit_breaker_threshold", "Consecutive failures before circuit opens",
        "Number of consecutive dispatch failures before circuit breaker trips to OPEN",
        &ulak_circuit_breaker_threshold, 10, 1, 1000, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL,
        NULL);

    DefineCustomIntVariable("ulak.circuit_breaker_cooldown",
                            "Circuit breaker cooldown period (seconds)",
                            "Seconds to wait before transitioning from OPEN to HALF_OPEN",
                            &ulak_circuit_breaker_cooldown, 30, 5, 3600, PGC_SIGHUP,
                            GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* Response Tracking Configuration */
    DefineCustomBoolVariable("ulak.capture_response", "Capture dispatch response data",
                             "Store dispatch response (HTTP status, body, etc.) on the message row",
                             &ulak_capture_response, false, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL,
                             NULL, NULL);

    DefineCustomIntVariable(
        "ulak.response_body_max_size", "Maximum response body capture size (bytes)",
        "Maximum bytes of response body to capture per dispatch", &ulak_response_body_max_size,
        65536, 0, 10485760, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* Event Log Configuration */
    DefineCustomIntVariable("ulak.event_log_retention_days",
                            "Number of days to retain event log entries",
                            "Event log entries older than this will be purged by maintenance tasks",
                            &ulak_event_log_retention_days, 30, /* default 30 days */
                            1,                                  /* min 1 day */
                            365,                                /* max 1 year */
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* DLQ Retention Configuration */
    DefineCustomIntVariable("ulak.dlq_retention_days",
                            "Number of days to retain DLQ messages before automatic cleanup",
                            "DLQ messages older than this will be purged by maintenance tasks",
                            &ulak_dlq_retention_days, 30, 1, /* min 1 day */
                            3650,                            /* max 10 years */
                            PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* Archive Retention Configuration */
    DefineCustomIntVariable(
        "ulak.archive_retention_months",
        "Number of months to retain archive partitions before auto-drop",
        "Archive partitions older than this will be dropped by maintenance tasks",
        &ulak_archive_retention_months, 6, 1, /* min 1 month */
        120,                                  /* max 10 years */
        PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* Queue Backpressure Configuration */
    DefineCustomIntVariable("ulak.max_queue_size",
                            "Maximum pending messages before send() rejects new messages",
                            "Set to 0 to disable backpressure (unlimited queue). "
                            "When limit is reached, send() raises an error with SQLSTATE 53400.",
                            &ulak_max_queue_size, 1000000, 0, /* min 0 = unlimited */
                            INT_MAX, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    /* CloudEvents Configuration */
    DefineCustomStringVariable("ulak.cloudevents_source", "CloudEvents source attribute",
                               "Default source URI for CloudEvents envelope (ce-source header)",
                               &ulak_cloudevents_source, "/ulak", PGC_SIGHUP, GUC_NOT_IN_SAMPLE,
                               NULL, NULL, NULL);

    /* Notification Configuration */
    DefineCustomBoolVariable("ulak.enable_notify", "Enable LISTEN/NOTIFY for new messages",
                             "When off, the notify_new_message trigger skips pg_notify(). "
                             "Reduces lock contention at high throughput.",
                             &ulak_enable_notify, true, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL,
                             NULL);

    /* Archive Partition Configuration */
    DefineCustomIntVariable(
        "ulak.archive_premake_months", "Months of archive partitions to pre-create",
        "Number of monthly partitions to create ahead of current month",
        &ulak_archive_premake_months, 3, 1, 24, PGC_SIGHUP, GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

    ulak_log("info", "GUC variables initialized");
}

/**
 * @brief Validate all current GUC settings and emit warnings for invalid values.
 */
void config_validate_settings(void) {
    if (!config_is_valid_poll_interval(ulak_poll_interval)) {
        ereport(WARNING, (errmsg("[ulak] Invalid poll_interval: %d", ulak_poll_interval)));
    }

    if (!config_is_valid_batch_size(ulak_batch_size)) {
        ereport(WARNING, (errmsg("[ulak] Invalid batch_size: %d", ulak_batch_size)));
    }

    if (!config_is_valid_log_level(ulak_log_level)) {
        ereport(WARNING, (errmsg("[ulak] Invalid log_level: %d", ulak_log_level)));
    }

    if (!config_is_valid_worker_count(ulak_workers)) {
        ereport(WARNING, (errmsg("[ulak] Invalid workers: %d", ulak_workers)));
    }

    if (!config_is_valid_max_retries(ulak_default_max_retries)) {
        ereport(WARNING,
                (errmsg("[ulak] Invalid default_max_retries: %d", ulak_default_max_retries)));
    }

    if (!config_is_valid_stale_recovery_timeout(ulak_stale_recovery_timeout)) {
        ereport(WARNING,
                (errmsg("[ulak] Invalid stale_recovery_timeout: %d", ulak_stale_recovery_timeout)));
    }
}

/**
 * @brief Validate poll interval range (100-60000 ms).
 * @param interval Value to validate
 * @return true if within valid range
 */
bool config_is_valid_poll_interval(int interval) { return interval >= 100 && interval <= 60000; }

/**
 * @brief Validate batch size range (1-1000).
 * @param batch_size Value to validate
 * @return true if within valid range
 */
bool config_is_valid_batch_size(int batch_size) { return batch_size >= 1 && batch_size <= 1000; }

/**
 * @brief Validate log level enum range.
 * @param log_level Value to validate
 * @return true if within valid range
 */
bool config_is_valid_log_level(int log_level) {
    return log_level >= LOG_LEVEL_ERROR && log_level <= LOG_LEVEL_DEBUG;
}

/**
 * @brief Validate worker count range (1-32).
 * @param workers Value to validate
 * @return true if within valid range
 */
bool config_is_valid_worker_count(int workers) { return workers >= 1 && workers <= 32; }

/**
 * @brief Validate max retries range (0-1000).
 * @param max_retries Value to validate
 * @return true if within valid range
 */
bool config_is_valid_max_retries(int max_retries) {
    return max_retries >= 0 && max_retries <= 1000;
}

/**
 * @brief Validate stale recovery timeout range (60-3600 seconds).
 * @param timeout Value to validate
 * @return true if within valid range
 */
bool config_is_valid_stale_recovery_timeout(int timeout) {
    return timeout >= 60 && timeout <= 3600;
}

/**
 * @brief Get the current poll interval.
 * @return Poll interval in milliseconds
 */
int config_get_poll_interval(void) { return ulak_poll_interval; }

/**
 * @brief Get the current batch size.
 * @return Batch size
 */
int config_get_batch_size(void) { return ulak_batch_size; }

/**
 * @brief Get the current log level.
 * @return Current LogLevel enum value
 */
LogLevel config_get_log_level(void) { return (LogLevel)ulak_log_level; }

/**
 * @brief Get the configured worker count.
 * @return Number of workers
 */
int config_get_workers(void) { return ulak_workers; }

/**
 * @brief Get the default max retries setting.
 * @return Max retries count
 */
int config_get_default_max_retries(void) { return ulak_default_max_retries; }

/**
 * @brief Get the stale recovery timeout.
 * @return Timeout in seconds
 */
int config_get_stale_recovery_timeout(void) { return ulak_stale_recovery_timeout; }

/**
 * @brief Set the poll interval if valid.
 * @param interval New interval in milliseconds
 */
void config_set_poll_interval(int interval) {
    if (config_is_valid_poll_interval(interval)) {
        ulak_poll_interval = interval;
    }
}

/**
 * @brief Set the batch size if valid.
 * @param batch_size New batch size
 */
void config_set_batch_size(int batch_size) {
    if (config_is_valid_batch_size(batch_size)) {
        ulak_batch_size = batch_size;
    }
}

/**
 * @brief Set the log level if valid.
 * @param log_level New log level
 */
void config_set_log_level(LogLevel log_level) {
    if (config_is_valid_log_level(log_level)) {
        ulak_log_level = log_level;
    }
}

/**
 * @brief Set the default max retries if valid.
 * @param max_retries New max retries count
 */
void config_set_default_max_retries(int max_retries) {
    if (config_is_valid_max_retries(max_retries)) {
        ulak_default_max_retries = max_retries;
    }
}

/**
 * @brief Get whether response capture is enabled.
 * @return true if response capture is on
 */
bool config_get_capture_response(void) { return ulak_capture_response; }

/**
 * @brief Get the maximum response body capture size.
 * @return Max size in bytes
 */
int config_get_response_body_max_size(void) { return ulak_response_body_max_size; }

/**
 * @brief Get the archive retention period.
 * @return Retention in months
 */
int config_get_archive_retention_months(void) { return ulak_archive_retention_months; }

/**
 * @brief Get the event log retention period.
 * @return Retention in days
 */
int config_get_event_log_retention_days(void) { return ulak_event_log_retention_days; }
