/**
 * @file circuit_breaker.h
 * @brief Circuit breaker SPI wrappers.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Thin wrappers around the SQL-side circuit breaker surface
 * (ulak.endpoints.circuit_state, ulak.update_circuit_breaker()).
 *
 * The in-loop decision logic (skip vs. half-open probe vs. dispatch)
 * still lives inside process_pending_messages_batch() in worker.c and
 * will be migrated with the rest of the batch processor.
 */

#ifndef ULAK_WORKER_CIRCUIT_BREAKER_H
#define ULAK_WORKER_CIRCUIT_BREAKER_H

#include "postgres.h"

/**
 * @brief Attempt to transition an endpoint from 'open' to 'half_open'.
 *
 * CAS-style UPDATE: only succeeds when the current state is still 'open',
 * so at most one worker wins the transition. Callers that lose the race
 * should defer their batch.
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
