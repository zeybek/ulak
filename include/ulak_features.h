/**
 * @file ulak_features.h
 * @brief Feature flags for conditional protocol compilation.
 *
 * Protocol flags (ENABLE_KAFKA, ENABLE_MQTT, etc.) are set via
 * Makefile arguments and control which dispatchers are compiled in.
 */

#ifndef ULAK_FEATURES_H
#define ULAK_FEATURES_H

/** @name Protocol feature flags
 *
 * Set via Makefile build arguments (e.g. make ENABLE_KAFKA=1).
 * Each flag expands to a ULAK_HAS_xxx 0/1 constant for
 * use in `#if` conditionals throughout the codebase.
 * @{ */

/* #define ENABLE_KAFKA */ /**< Define via Makefile to enable Kafka dispatcher */
/* #define ENABLE_MQTT */  /**< Define via Makefile to enable MQTT dispatcher */

#define ENABLE_HTTP 1 /**< HTTP is always enabled -- core protocol */

#ifdef ENABLE_KAFKA
#define ULAK_HAS_KAFKA 1 /**< Kafka dispatcher compiled in */
#else
#define ULAK_HAS_KAFKA 0 /**< Kafka dispatcher not available */
#endif

#ifdef ENABLE_MQTT
#define ULAK_HAS_MQTT 1 /**< MQTT dispatcher compiled in */
#else
#define ULAK_HAS_MQTT 0 /**< MQTT dispatcher not available */
#endif

#ifdef ENABLE_REDIS
#define ULAK_HAS_REDIS 1 /**< Redis dispatcher compiled in */
#else
#define ULAK_HAS_REDIS 0 /**< Redis dispatcher not available */
#endif

#ifdef ENABLE_AMQP
#define ULAK_HAS_AMQP 1 /**< AMQP dispatcher compiled in */
#else
#define ULAK_HAS_AMQP 0 /**< AMQP dispatcher not available */
#endif

#ifdef ENABLE_NATS
#define ULAK_HAS_NATS 1 /**< NATS dispatcher compiled in */
#else
#define ULAK_HAS_NATS 0 /**< NATS dispatcher not available */
#endif

/** @} */

#endif /* ULAK_FEATURES_H */
