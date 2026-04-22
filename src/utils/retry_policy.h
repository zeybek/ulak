/**
 * @file retry_policy.h
 * @brief Retry policy parsing and backoff delay calculation.
 *
 * Clean Architecture: Utility Layer.
 * Pure helpers over the retry_policy JSONB attached to each message.
 * No SPI, no transaction context — safe to call from anywhere the GUC
 * values (ulak_default_max_retries, ulak_retry_base_delay,
 * ulak_retry_max_delay, ulak_retry_increment) are initialized.
 */

#ifndef ULAK_UTILS_RETRY_POLICY_H
#define ULAK_UTILS_RETRY_POLICY_H

#include "postgres.h"
#include "utils/jsonb.h"

/**
 * @brief Extract max_retries from a retry policy JSONB.
 *
 * Falls back to ulak_default_max_retries when the policy is NULL or does
 * not contain a valid max_retries field.
 *
 * @param retry_policy Retry policy JSONB, or NULL.
 * @return Maximum number of retries.
 */
extern int get_max_retries_from_policy(Jsonb *retry_policy);

/**
 * @brief Extract backoff type from retry policy.
 *
 * Returns a static string constant -- no memory allocation, no leak possible.
 *
 * @param retry_policy Retry policy JSONB, or NULL.
 * @return One of "exponential", "fixed", or "linear".
 */
extern const char *get_backoff_type_from_policy(Jsonb *retry_policy);

/**
 * @brief Calculate delay for exponential backoff.
 *
 * Uses configurable GUC values: ulak.retry_base_delay, retry_max_delay.
 * Includes overflow protection for large retry counts.
 *
 * @param retry_count Current retry attempt number.
 * @return Delay in seconds, capped at ulak_retry_max_delay.
 */
extern int calculate_exponential_backoff(int retry_count);

/**
 * @brief Calculate retry delay based on retry policy.
 *
 * Supports fixed, linear, and exponential backoff strategies.
 * Uses configurable GUC values for defaults.
 *
 * @param retry_policy Retry policy JSONB, or NULL for default exponential.
 * @param retry_count  Current retry attempt number.
 * @return Delay in seconds.
 */
extern int calculate_delay_from_policy(Jsonb *retry_policy, int retry_count);

#endif /* ULAK_UTILS_RETRY_POLICY_H */
