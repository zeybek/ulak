/**
 * @file circuit_breaker.c
 * @brief Circuit breaker SPI wrappers.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * Extracted from src/worker.c. Behavior unchanged.
 */

#include "worker/circuit_breaker.h"

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"

bool cb_try_half_open_transition(int64 endpoint_id) {
    Oid argtypes[1] = {INT8OID};
    Datum values[1];
    int ret;

    values[0] = Int64GetDatum(endpoint_id);

    ret = SPI_execute_with_args("UPDATE ulak.endpoints SET circuit_state = 'half_open' "
                                "WHERE id = $1 AND circuit_state = 'open'",
                                1, argtypes, values, NULL, false, 0);

    return ret == SPI_OK_UPDATE && SPI_processed > 0;
}

void cb_update_after_dispatch(int64 endpoint_id, bool success) {
    Oid argtypes[2] = {INT8OID, BOOLOID};
    Datum values[2];
    char nulls[2] = {' ', ' '};
    int ret;

    values[0] = Int64GetDatum(endpoint_id);
    values[1] = BoolGetDatum(success);

    ret = SPI_execute_with_args("SELECT ulak.update_circuit_breaker($1, $2)", 2, argtypes, values,
                                nulls, false, 0);
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "[ulak] Failed to update circuit breaker for endpoint %lld: SPI error %d",
             (long long)endpoint_id, ret);
    }
}
