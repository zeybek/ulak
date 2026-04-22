/**
 * @file batch_types.h
 * @brief Worker batch processing types shared across worker-private modules.
 *
 * Clean Architecture: Infrastructure Layer (worker-private).
 * MessageBatchInfo mirrors a single row of the pending-messages SELECT
 * joined with endpoint state. Circuit breaker, batch processor, and
 * response capture code all read/write this struct.
 */

#ifndef ULAK_WORKER_BATCH_TYPES_H
#define ULAK_WORKER_BATCH_TYPES_H

#include "postgres.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"

#include "dispatchers/dispatcher.h"

typedef struct {
    int64 message_id;
    int64 endpoint_id;
    char *payload_str;
    int32 retry_count;
    Jsonb *retry_policy;
    char *protocol; /* Copied from endpoint */
    Jsonb *config;  /* Copied from endpoint */
    bool processed;
    bool success;
    char *error_message;
    /* Additional fields */
    int16 priority;
    TimestampTz scheduled_at;
    TimestampTz expires_at;
    char *correlation_id;
    bool endpoint_enabled;
    int32 endpoint_failure_count;
    /* discrete circuit breaker fields (no jsonb) */
    char circuit_state[16];
    int32 circuit_failure_count;
    TimestampTz circuit_opened_at;
    TimestampTz circuit_half_open_at;
    DispatchResult *result; /* Dispatch result for response capture */
    Jsonb *headers;         /* Per-message headers */
    Jsonb *metadata;        /* Per-message metadata */
    bool rate_limited;      /* Deferred by rate limiter — skip dispatch */
} MessageBatchInfo;

#endif /* ULAK_WORKER_BATCH_TYPES_H */
