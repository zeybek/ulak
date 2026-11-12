/**
 * @file circuit_breaker.h
 * @brief Circuit breaker SPI wrappers.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Thin wrappers around the SQL-side circuit breaker surface
 * (ulak.endpoints.circuit_state, ulak.update_circuit_breaker()).
 *
 * The in-loop decision logic (skip vs. half-open probe vs. dispatch)
 * lives in batch_claim() in batch_processor.c and runs inside the claim
 * transaction; cb_update_after_dispatch() runs in the result transaction.
 */

#ifndef ULAK_WORKER_CIRCUIT_BREAKER_H
#define ULAK_WORKER_CIRCUIT_BREAKER_H

#include "postgres.h"

/**
 * @brief Take the half_open probe for an endpoint.
 *
 * CAS-style UPDATE: succeeds when the state is 'open' with its cooldown
 * over, or 'half_open' whose probe deadline has passed (the earlier probe
 * never reported, e.g. it was rate limited or its worker died), so at most
 * one worker wins. The winner sets circuit_half_open_at to now +
 * ulak.circuit_breaker_cooldown as the new probe deadline. Callers that lose
 * the race should defer their batch.
 *
 * Must be invoked inside an active SPI transaction (SPI_connect() already
 * called). Does not manage its own transaction lifecycle.
 *
 * @param endpoint_id Endpoint to transition.
 * @return true if this caller won the transition, false otherwise.
 */
extern bool cb_try_half_open_transition(int64 endpoint_id);

/**
 * @brief Record the outcome of a dispatch attempt against the endpoint.
 *
 * Delegates to the SQL function ulak.update_circuit_breaker(endpoint_id,
 * success); the SQL side owns the state-machine (failure count, open
 * threshold, cooldown). Logs a WARNING on SPI failure but never aborts.
 *
 * Must be invoked inside an active SPI transaction.
 *
 * @param endpoint_id Endpoint identifier.
 * @param success     Whether the most recent dispatch succeeded.
 */
extern void cb_update_after_dispatch(int64 endpoint_id, bool success);

#endif /* ULAK_WORKER_CIRCUIT_BREAKER_H */
