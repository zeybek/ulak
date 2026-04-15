/**
 * @file guc.h
 * @brief GUC (Grand Unified Configuration) variable declarations and helpers.
 *
 * Clean Architecture: Infrastructure Layer.
 * Declares all ulak.* GUC parameters, validation functions,
 * getters, and setters. Parameters are grouped by subsystem: core,
 * HTTP, Kafka, MQTT, Redis, AMQP, NATS, circuit breaker, retention,
 * backpressure, and response capture.
 */

#ifndef ULAK_CONFIG_GUC_H
#define ULAK_CONFIG_GUC_H

#include "ulak.h"

/* GUC Variable Declarations */
extern int ulak_workers;
extern int ulak_poll_interval;
extern int ulak_batch_size;
extern int ulak_default_max_retries;
extern int ulak_log_level;
extern int ulak_stale_recovery_timeout;

/* Worker & Core Configuration */
extern char *ulak_database;
extern int ulak_worker_restart_delay;
extern int ulak_retry_base_delay;
extern int ulak_retry_max_delay;
extern int ulak_retry_increment;

/* HTTP Dispatcher Configuration */
extern int ulak_http_timeout;
extern int ulak_http_connect_timeout;
extern int ulak_http_max_redirects;
extern bool ulak_http_ssl_verify_peer;
extern bool ulak_http_ssl_verify_host;

/* HTTP Security Configuration */
extern bool ulak_http_allow_internal_urls;

/* HTTP Batch Configuration */
extern int ulak_http_batch_capacity;
extern int ulak_http_max_connections_per_host;
extern int ulak_http_max_total_connections;
extern int ulak_http_flush_timeout;
extern bool ulak_http_enable_pipelining;

/* Kafka Dispatcher Configuration */
#ifdef ENABLE_KAFKA
extern int ulak_kafka_delivery_timeout;
extern int ulak_kafka_poll_interval;
extern int ulak_kafka_flush_timeout;
extern int ulak_kafka_batch_capacity;
extern char *ulak_kafka_acks;
extern char *ulak_kafka_compression;
#endif /* ENABLE_KAFKA */

/* MQTT Dispatcher Configuration */
#ifdef ENABLE_MQTT
extern int ulak_mqtt_keepalive;
extern int ulak_mqtt_timeout;
extern int ulak_mqtt_default_qos;
extern int ulak_mqtt_default_port;
#endif /* ENABLE_MQTT */

#ifdef ENABLE_REDIS
/* Redis Dispatcher Configuration */
extern int ulak_redis_connect_timeout;
extern int ulak_redis_command_timeout;
extern int ulak_redis_default_db;
extern int ulak_redis_default_port;
#endif

/* AMQP Dispatcher Configuration */
#ifdef ENABLE_AMQP
extern int ulak_amqp_connection_timeout;
extern int ulak_amqp_heartbeat;
extern int ulak_amqp_frame_max;
extern int ulak_amqp_delivery_timeout;
extern int ulak_amqp_batch_capacity;
extern bool ulak_amqp_ssl_verify_peer;
#endif /* ENABLE_AMQP */

/* NATS Dispatcher Configuration */
#ifdef ENABLE_NATS
extern int ulak_nats_delivery_timeout;
extern int ulak_nats_flush_timeout;
extern int ulak_nats_batch_capacity;
extern int ulak_nats_reconnect_wait;
extern int ulak_nats_max_reconnect;
#endif /* ENABLE_NATS */

/* Response Tracking Configuration */
extern bool ulak_capture_response;
extern int ulak_response_body_max_size;

/* System Configuration */
extern int ulak_max_payload_size;

/* Circuit Breaker Configuration */
extern int ulak_circuit_breaker_threshold;
extern int ulak_circuit_breaker_cooldown;

/* Event Log Configuration */
extern int ulak_event_log_retention_days;

/* DLQ & Archive Retention Configuration */
extern int ulak_dlq_retention_days;
extern int ulak_archive_retention_months;

/* Queue Backpressure Configuration */
extern int ulak_max_queue_size;

/* Notification Configuration */
extern bool ulak_enable_notify;

/* Archive Partition Configuration */
extern int ulak_archive_premake_months;

/* CloudEvents Configuration */
extern char *ulak_cloudevents_source;

/* Configuration Enums - Public for other modules */
typedef enum { LOG_LEVEL_ERROR = 0, LOG_LEVEL_WARNING, LOG_LEVEL_INFO, LOG_LEVEL_DEBUG } LogLevel;

/* Configuration Management Functions */
extern void config_init_guc_variables(void);
extern void config_validate_settings(void);
extern bool config_is_valid_worker_count(int workers);
extern bool config_is_valid_poll_interval(int interval);
extern bool config_is_valid_batch_size(int batch_size);
extern bool config_is_valid_max_retries(int max_retries);
extern bool config_is_valid_log_level(int log_level);
extern bool config_is_valid_stale_recovery_timeout(int timeout);

/* Configuration Getters */
extern int config_get_workers(void);
extern int config_get_poll_interval(void);
extern int config_get_batch_size(void);
extern int config_get_default_max_retries(void);
extern LogLevel config_get_log_level(void);
extern int config_get_stale_recovery_timeout(void);

/* Configuration Setters */
extern void config_set_poll_interval(int interval);
extern void config_set_batch_size(int batch_size);
extern void config_set_default_max_retries(int max_retries);
extern void config_set_log_level(LogLevel log_level);

/* Response Tracking Configuration Getters */
extern bool config_get_capture_response(void);
extern int config_get_response_body_max_size(void);
extern int config_get_archive_retention_months(void);

/* Event Log Configuration Getters */
extern int config_get_event_log_retention_days(void);

#endif /* ULAK_CONFIG_GUC_H */
