/**
 * @file rate_limit.c
 * @brief Per-endpoint token bucket rate limiter (shared memory)
 *
 * Global rate limiting using shared memory token buckets.
 * All workers share the same bucket per endpoint, ensuring accurate
 * rate enforcement regardless of worker count.
 *
 * Each bucket has its own spinlock for minimal contention.
 * Token refill is based on elapsed microseconds since last check.
 *
 * Creation of new buckets is serialized via the global LWLock to prevent
 * duplicate buckets for the same endpoint (TOCTOU protection).
 */

#include "utils/rate_limit.h"
#include "postgres.h"
#include "shmem.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "utils/json_utils.h"
#include "utils/timestamp.h"

/** Stale bucket threshold: 10 minutes without any refill = evictable */
#define RL_STALE_THRESHOLD_US (10LL * 60 * 1000000)

/**
 * @brief Find an existing bucket for the given endpoint.
 * @private
 *
 * Scans under per-bucket spinlocks (fast, no global lock needed).
 *
 * @param endpoint_id Endpoint to look up
 * @return Pointer to bucket or NULL if not found
 */
static RateLimitShmemBucket *find_existing_bucket(int64 endpoint_id) {
    int i;
    RateLimitShmemBucket *buckets;

    if (ulak_shmem == NULL)
        return NULL;

    buckets = ulak_shmem->rate_limit_buckets;

    for (i = 0; i < RL_SHMEM_MAX_ENDPOINTS; i++) {
        SpinLockAcquire(&buckets[i].mutex);
        if (buckets[i].active && buckets[i].endpoint_id == endpoint_id) {
            SpinLockRelease(&buckets[i].mutex);
            return &buckets[i];
        }
        SpinLockRelease(&buckets[i].mutex);
    }

    return NULL;
}

/**
 * @brief Create a new bucket for the given endpoint.
 * @private
 *
 * Must be called under the global LWLock (LW_EXCLUSIVE) to prevent
 * duplicate buckets for the same endpoint (TOCTOU protection).
 *
 * @param endpoint_id Endpoint to create a bucket for
 * @param tokens_per_second Refill rate in tokens per second
 * @param burst Maximum burst size (0 uses tokens_per_second)
 * @param now Current timestamp for initialization
 * @return Pointer to bucket or NULL if all slots are full
 */
static RateLimitShmemBucket *create_bucket(int64 endpoint_id, double tokens_per_second, int burst,
                                           TimestampTz now) {
    int i;
    int first_empty = -1;
    int oldest_stale = -1;
    int64 oldest_stale_age = 0;
    RateLimitShmemBucket *buckets = ulak_shmem->rate_limit_buckets;

    /* Re-check: another worker may have created it while we waited for LWLock */
    for (i = 0; i < RL_SHMEM_MAX_ENDPOINTS; i++) {
        SpinLockAcquire(&buckets[i].mutex);
        if (buckets[i].active && buckets[i].endpoint_id == endpoint_id) {
            SpinLockRelease(&buckets[i].mutex);
            return &buckets[i];
        }
        if (!buckets[i].active && first_empty < 0) {
            first_empty = i;
        } else if (buckets[i].active) {
            /* Track oldest stale bucket for eviction */
            int64 age = now - buckets[i].last_refill;
            if (age > RL_STALE_THRESHOLD_US && age > oldest_stale_age) {
                oldest_stale = i;
                oldest_stale_age = age;
            }
        }
        SpinLockRelease(&buckets[i].mutex);
    }

    /* If no empty slot, try evicting oldest stale bucket */
    if (first_empty < 0 && oldest_stale >= 0) {
        SpinLockAcquire(&buckets[oldest_stale].mutex);
        /* Re-verify staleness under lock */
        if (buckets[oldest_stale].active &&
            (now - buckets[oldest_stale].last_refill) > RL_STALE_THRESHOLD_US) {
            elog(DEBUG1, "[ulak] Rate limit: evicting stale bucket for endpoint %lld",
                 (long long)buckets[oldest_stale].endpoint_id);
            buckets[oldest_stale].active = false;
            first_empty = oldest_stale;
        }
        SpinLockRelease(&buckets[oldest_stale].mutex);
    }

    if (first_empty < 0) {
        elog(WARNING, "[ulak] Rate limit: max endpoints (%d) reached, no stale buckets to evict",
             RL_SHMEM_MAX_ENDPOINTS);
        return NULL;
    }

    /* Initialize new bucket under per-bucket spinlock */
    SpinLockAcquire(&buckets[first_empty].mutex);
    {
        double max_tokens = (burst > 0) ? (double)burst : tokens_per_second;
        buckets[first_empty].endpoint_id = endpoint_id;
        buckets[first_empty].max_tokens = max_tokens;
        buckets[first_empty].tokens = max_tokens; /* Start full */
        buckets[first_empty].refill_rate = tokens_per_second / 1000000.0;
        buckets[first_empty].last_refill = now;
        buckets[first_empty].active = true;
    }
    SpinLockRelease(&buckets[first_empty].mutex);

    return &buckets[first_empty];
}

/**
 * @brief Find or create a shared memory bucket for the given endpoint.
 * @private
 *
 * Fast path: scan for existing (per-bucket spinlocks only).
 * Slow path: create under global LWLock to prevent duplicates.
 *
 * @param endpoint_id Endpoint to look up or create
 * @param tokens_per_second Refill rate for new bucket creation
 * @param burst Burst size for new bucket creation
 * @return Pointer to bucket or NULL on failure
 */
static RateLimitShmemBucket *find_or_create_bucket(int64 endpoint_id, double tokens_per_second,
                                                   int burst) {
    RateLimitShmemBucket *bucket;
    TimestampTz now;

    if (ulak_shmem == NULL || ulak_shmem->lock == NULL)
        return NULL;

    /* Fast path: existing bucket */
    bucket = find_existing_bucket(endpoint_id);
    if (bucket)
        return bucket;

    /* Slow path: create under global LWLock to prevent duplicate creation */
    now = GetCurrentTimestamp();
    LWLockAcquire(ulak_shmem->lock, LW_EXCLUSIVE);
    bucket = create_bucket(endpoint_id, tokens_per_second, burst, now);
    LWLockRelease(ulak_shmem->lock);

    return bucket;
}

/**
 * @brief Acquire a token for dispatch from the shared memory bucket.
 *
 * GetCurrentTimestamp() is called BEFORE acquiring the spinlock
 * to avoid syscalls under spinlock.
 *
 * @param endpoint_id Endpoint to rate-limit
 * @param tokens_per_second Configured rate (0 disables rate limiting)
 * @param burst Maximum burst size
 * @return true if allowed, false if rate limited
 */
bool rate_limit_acquire(int64 endpoint_id, double tokens_per_second, int burst) {
    RateLimitShmemBucket *bucket;
    TimestampTz now;
    int64 elapsed_us;
    bool acquired = false;

    /* No rate limit configured */
    if (tokens_per_second <= 0.0)
        return true;

    bucket = find_or_create_bucket(endpoint_id, tokens_per_second, burst);
    if (!bucket)
        return true; /* Can't track -- allow */

    /* Get timestamp BEFORE spinlock (syscall not allowed under spinlock) */
    now = GetCurrentTimestamp();

    SpinLockAcquire(&bucket->mutex);

    /* Re-check bucket validity (may have been reset/evicted) */
    if (!bucket->active || bucket->endpoint_id != endpoint_id) {
        SpinLockRelease(&bucket->mutex);
        return true; /* Bucket was cleared -- allow this dispatch */
    }

    /* Update config if changed (hot reload) */
    bucket->refill_rate = tokens_per_second / 1000000.0;
    bucket->max_tokens = (burst > 0) ? (double)burst : tokens_per_second;

    /* Refill tokens based on elapsed time */
    elapsed_us = now - bucket->last_refill;
    if (elapsed_us > 0) {
        bucket->tokens += (double)elapsed_us * bucket->refill_rate;
        if (bucket->tokens > bucket->max_tokens)
            bucket->tokens = bucket->max_tokens;
        bucket->last_refill = now;
    }

    /* Try to consume a token */
    if (bucket->tokens >= 1.0) {
        bucket->tokens -= 1.0;
        acquired = true;
    }

    SpinLockRelease(&bucket->mutex);
    return acquired;
}

/**
 * @brief Parse rate limit config from endpoint JSONB.
 *
 * Expected format: "rate_limit": { "limit": N, "interval": "second|minute|hour", "burst": N }
 * Normalizes limit to tokens_per_second regardless of interval.
 * burst defaults to limit if not specified.
 *
 * @param config Endpoint JSONB configuration
 * @param tokens_per_second Output: normalized rate in tokens/second
 * @param burst Output: burst size (0 if not specified)
 */
void rate_limit_parse_config(Jsonb *config, double *tokens_per_second, int *burst) {
    Jsonb *rl_obj;
    int limit;
    char *interval;

    *tokens_per_second = 0.0;
    *burst = 0;

    if (!config)
        return;

    /* Extract nested "rate_limit" object */
    rl_obj = jsonb_get_nested(config, RL_CONFIG_KEY);
    if (!rl_obj)
        return;

    limit = jsonb_get_int32(rl_obj, RL_KEY_LIMIT, 0);
    if (limit <= 0) {
        pfree(rl_obj);
        return;
    }
    if (limit > 1000000) {
        elog(WARNING, "[ulak] Rate limit: limit %d exceeds max 1000000, clamping", limit);
        limit = 1000000;
    }

    interval = jsonb_get_string(rl_obj, RL_KEY_INTERVAL, RL_INTERVAL_SECOND);
    *burst = jsonb_get_int32(rl_obj, RL_KEY_BURST, 0);
    if (*burst < 0)
        *burst = 0;
    if (*burst > 1000000) {
        elog(WARNING, "[ulak] Rate limit: burst %d exceeds max 1000000, clamping", *burst);
        *burst = 1000000;
    }

    /* Normalize to tokens per second */
    if (strcmp(interval, RL_INTERVAL_SECOND) == 0) {
        *tokens_per_second = (double)limit;
    } else if (strcmp(interval, RL_INTERVAL_MINUTE) == 0) {
        *tokens_per_second = (double)limit / 60.0;
    } else if (strcmp(interval, RL_INTERVAL_HOUR) == 0) {
        *tokens_per_second = (double)limit / 3600.0;
    } else {
        elog(WARNING, "[ulak] Rate limit: unknown interval '%s', using 'second'", interval);
        *tokens_per_second = (double)limit;
    }

    pfree(interval);
    pfree(rl_obj);
}

/**
 * @brief Reset all shared rate limit buckets.
 */
void rate_limit_reset(void) {
    int i;
    RateLimitShmemBucket *buckets;

    if (ulak_shmem == NULL)
        return;

    buckets = ulak_shmem->rate_limit_buckets;

    for (i = 0; i < RL_SHMEM_MAX_ENDPOINTS; i++) {
        SpinLockAcquire(&buckets[i].mutex);
        buckets[i].endpoint_id = 0;
        buckets[i].tokens = 0;
        buckets[i].max_tokens = 0;
        buckets[i].refill_rate = 0;
        buckets[i].last_refill = 0;
        buckets[i].active = false;
        SpinLockRelease(&buckets[i].mutex);
    }
}
