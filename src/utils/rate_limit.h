/**
 * @file rate_limit.h
 * @brief Per-endpoint token-bucket rate limiter using shared memory.
 *
 * Global rate limiting across all workers via shared memory token buckets.
 * Each endpoint has one bucket shared by all workers, protected by a
 * per-bucket spinlock for minimal contention.
 *
 * Configuration via nested JSONB in endpoint config:
 * @code{.json}
 * "rate_limit": { "limit": 10, "interval": "minute", "burst": 5 }
 * @endcode
 */

#ifndef ULAK_UTILS_RATE_LIMIT_H
#define ULAK_UTILS_RATE_LIMIT_H

#include "postgres.h"
#include "utils/jsonb.h"

/* Top-level config key for the rate_limit object */
#define RL_CONFIG_KEY "rate_limit"

/* Keys inside the rate_limit object */
#define RL_KEY_LIMIT "limit"
#define RL_KEY_INTERVAL "interval"
#define RL_KEY_BURST "burst"

/* Supported interval values */
#define RL_INTERVAL_SECOND "second"
#define RL_INTERVAL_MINUTE "minute"
#define RL_INTERVAL_HOUR "hour"

/** @name Token bucket operations */
/** @{ */

/**
 * @brief Acquire a token for the given endpoint from the shared memory bucket.
 * @param endpoint_id       Endpoint to rate limit.
 * @param tokens_per_second Normalized rate (tokens/sec). 0 = no limit.
 * @param burst             Burst capacity (0 = use tokens_per_second).
 * @return true if token acquired (dispatch allowed), false if rate limited.
 */
bool rate_limit_acquire(int64 endpoint_id, double tokens_per_second, int burst);
/** @} */

/** @name Configuration */
/** @{ */

/**
 * @brief Parse rate limit config from endpoint JSONB.
 *
 * Looks for "rate_limit" nested object with "limit", "interval", "burst".
 * Sets tokens_per_second (normalized) and burst. Both 0 if not configured.
 *
 * @param config             Endpoint JSONB configuration.
 * @param tokens_per_second  Output: normalized tokens per second.
 * @param burst              Output: burst capacity.
 */
void rate_limit_parse_config(Jsonb *config, double *tokens_per_second, int *burst);
/** @} */

/** @name Maintenance */
/** @{ */

/**
 * @brief Reset all shared rate limit buckets.
 */
void rate_limit_reset(void);
/** @} */

#endif /* ULAK_UTILS_RATE_LIMIT_H */
