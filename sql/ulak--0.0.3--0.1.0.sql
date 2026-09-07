-- ulak 0.0.3 -> 0.1.0 upgrade migration
--
-- Procedure (the C library changed too):
--   1. Install the new ulak.so and this script (make install).
--   2. Restart PostgreSQL. ulak is in shared_preload_libraries and its shared
--      memory layout changed; backends keep the old .so until restart, and the
--      new ulak._active_queue_count() symbol only exists in the new library.
--      The old 0.0.3 SQL keeps working against the new library in between.
--   3. In every database with the extension: ALTER EXTENSION ulak UPDATE;
--
-- Behaviour changes to check before upgrading (see CHANGELOG / wiki):
--   * ulak_application / ulak_monitor lose SELECT on ulak.endpoints (its config
--     column holds credentials). Use ulak.endpoint_status or
--     ulak.get_endpoint_health() instead.
--   * A signing_secret of the form whsec_<base64> is now base64-decoded before
--     signing (Standard Webhooks); receivers that verified with the raw string
--     must switch to the decoded key. Non-base64 whsec_ secrets are rejected.
--   * ulak.redrive_message() raises an error for DLQ rows that are not 'failed'.
--   * ulak.metrics() no longer emits spawns_total / spawn_failures_total /
--     restarts_total (the launcher they described never existed).
--   * ulak.database is now PGC_POSTMASTER (a restart, not a reload, applies it).
--   * webhook-id / ce-id are msg_<queue id> on every delivery path.
--
-- Schema changes:
--   * ulak.queue: drop idx_queue_next_retry / idx_queue_created_at (unused by the
--     worker fetch plan), add idx_queue_expires_pending for TTL expiry.
--   * ulak.queue: the two BEFORE UPDATE row triggers are merged into one
--     (queue_before_update) to halve per-row trigger overhead on the hottest table.
--   * ulak.endpoint_status view: endpoints without the credential-bearing config
--     column. ulak_application / ulak_monitor lose SELECT on ulak.endpoints.
--   * ulak._active_queue_count(): C-backed, shared-memory cached count used by
--     backpressure instead of an O(queue) OFFSET probe per send().
--   * ulak.get_endpoint_health() and ulak.metrics() become SECURITY DEFINER and
--     are granted to ulak_admin / ulak_application as documented.
--   * ulak.redrive_message() locks the DLQ row and rejects non-failed rows.
--   * ulak.update_circuit_breaker() fallbacks now match the GUC defaults (10/30).
--   * ulak.enable_fast_mode() is granted to ulak_admin / ulak_application.
--
-- Function bodies below are copied verbatim from sql/ulak.sql so that an
-- upgraded database is catalog-identical to a fresh 0.1.0 install
-- (verified with a pg_catalog diff during development).

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS ulak.idx_queue_next_retry;
DROP INDEX IF EXISTS ulak.idx_queue_created_at;

CREATE INDEX IF NOT EXISTS idx_queue_expires_pending ON ulak.queue(expires_at)
WHERE status = 'pending' AND expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Queue row trigger (updated_at + payload immutability in one trigger)
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS update_queue_updated_at ON ulak.queue;
DROP TRIGGER IF EXISTS enforce_payload_immutability ON ulak.queue;
DROP FUNCTION IF EXISTS ulak.prevent_payload_modification();

CREATE OR REPLACE FUNCTION ulak.queue_before_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.payload IS DISTINCT FROM NEW.payload THEN
        RAISE EXCEPTION 'payload modification not allowed after creation'
            USING HINT = 'Message payloads are immutable for audit integrity';
    END IF;
    IF OLD.headers IS DISTINCT FROM NEW.headers THEN
        RAISE EXCEPTION 'headers modification not allowed after creation'
            USING HINT = 'Message headers are immutable for audit integrity';
    END IF;
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION ulak.queue_before_update() FROM PUBLIC;

CREATE TRIGGER queue_before_update
    BEFORE UPDATE ON ulak.queue
    FOR EACH ROW EXECUTE FUNCTION ulak.queue_before_update();

-- ---------------------------------------------------------------------------
-- Endpoint status view; credentials in ulak.endpoints.config are admin-only
-- ---------------------------------------------------------------------------
CREATE VIEW ulak.endpoint_status AS
SELECT id, name, protocol, enabled, description,
       circuit_state, circuit_failure_count, circuit_opened_at, circuit_half_open_at,
       last_success_at, last_failure_at, created_at, updated_at
FROM ulak.endpoints;

COMMENT ON VIEW ulak.endpoint_status IS
'Endpoint identity and health without the config column (which holds credentials).
Readable by ulak_admin, ulak_application and ulak_monitor.';

GRANT SELECT ON ulak.endpoint_status TO ulak_admin, ulak_application, ulak_monitor;
REVOKE SELECT ON ulak.endpoints FROM ulak_application, ulak_monitor;

-- ---------------------------------------------------------------------------
-- Backpressure: cached active count
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ulak._active_queue_count(
    p_reserve bigint DEFAULT 0
)
RETURNS bigint
LANGUAGE c VOLATILE
AS 'ulak', 'ulak_active_queue_count';

COMMENT ON FUNCTION ulak._active_queue_count(bigint) IS
'Internal: number of pending/processing messages, cached in shared memory for
about one second so backpressure checks do not scan the queue on every send().
p_reserve rows are added to the cached count after it is read, so back-to-back
sends within the cache window still count against the limit.';

REVOKE ALL ON FUNCTION ulak._active_queue_count(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ulak._active_queue_count(bigint) TO ulak_admin;

CREATE OR REPLACE FUNCTION ulak._check_backpressure(
    p_projected_additional bigint DEFAULT 0
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_size integer;
    v_active bigint;
    v_projected bigint;
BEGIN
    v_max_size := current_setting('ulak.max_queue_size', true)::int;
    IF v_max_size IS NULL OR v_max_size <= 0 THEN
        RETURN; -- backpressure disabled
    END IF;

    v_projected := GREATEST(COALESCE(p_projected_additional, 0), 0);

    IF v_projected > v_max_size THEN
        RAISE EXCEPTION '[ulak] Queue backpressure: % projected messages would exceed limit % even with an empty queue. Increase ulak.max_queue_size or reduce batch size.',
            v_projected, v_max_size
        USING ERRCODE = '53400';
    END IF;

    -- Cached count (refreshed at most once per second in shared memory), so
    -- this is O(1) per call instead of a queue scan per send(). The rows we
    -- are about to insert are reserved in the cache so a burst of sends
    -- within the cache window still counts against the limit.
    v_active := ulak._active_queue_count(GREATEST(v_projected, 1));

    IF v_active + GREATEST(v_projected, 1) > v_max_size THEN
        RAISE EXCEPTION '[ulak] Queue backpressure: pending/processing queue (%) plus % projected additional messages would exceed limit %. Increase ulak.max_queue_size or wait for messages to be processed.',
            v_active, v_projected, v_max_size
        USING ERRCODE = '53400'; -- configuration_limit_exceeded
    END IF;
END;
$$;

COMMENT ON FUNCTION ulak._check_backpressure(bigint) IS
'Internal: Check queue size against ulak.max_queue_size limit, including optional projected additional rows.
Uses the shared-memory cached active count (ulak._active_queue_count, refreshed about once per second,
with the rows being inserted reserved in the cache in between).
Raises exception with SQLSTATE 53400 if the current queue or the projected total would exceed the limit.
Set max_queue_size=0 to disable.';

-- ---------------------------------------------------------------------------
-- Monitoring functions: SECURITY DEFINER + grants
-- ---------------------------------------------------------------------------
ALTER FUNCTION ulak.get_endpoint_health(text) SECURITY DEFINER SET search_path = pg_catalog, ulak;
ALTER FUNCTION ulak.metrics() SECURITY DEFINER SET search_path = pg_catalog, ulak;

GRANT EXECUTE ON FUNCTION ulak.get_endpoint_health(text) TO ulak_admin, ulak_application;
GRANT EXECUTE ON FUNCTION ulak.enable_fast_mode() TO ulak_admin, ulak_application;

-- ---------------------------------------------------------------------------
-- Circuit breaker fallbacks aligned with GUC defaults (10 failures / 30s)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ulak.update_circuit_breaker(
    p_endpoint_id bigint,
    p_success boolean
) RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_state text;
    v_failures int;
    v_threshold int;
    v_cooldown int;
    v_old_state text;
BEGIN
    -- Get current circuit breaker configuration from GUC (defaults)
    -- Fallbacks mirror the GUC defaults in src/config/guc.c (10 failures, 30s)
    v_threshold := current_setting('ulak.circuit_breaker_threshold', true)::int;
    IF v_threshold IS NULL THEN v_threshold := 10; END IF;

    v_cooldown := current_setting('ulak.circuit_breaker_cooldown', true)::int;
    IF v_cooldown IS NULL THEN v_cooldown := 30; END IF;

    -- Use advisory lock to serialize circuit breaker updates per endpoint.
    -- This prevents deadlocks when multiple workers concurrently update the same
    -- endpoint's circuit breaker state. The lock is released at transaction end.
    PERFORM pg_advisory_xact_lock(hashtext('ulak_cb'), p_endpoint_id::int);

    SELECT circuit_state, circuit_failure_count INTO v_state, v_failures
    FROM ulak.endpoints
    WHERE id = p_endpoint_id;

    IF v_state IS NULL THEN
        RETURN; -- Endpoint not found
    END IF;

    v_old_state := v_state;

    -- Check if open circuit should transition to half_open (cooldown expired)
    IF v_state = 'open' THEN
        DECLARE
            v_half_open_at timestamptz;
        BEGIN
            SELECT circuit_half_open_at INTO v_half_open_at
            FROM ulak.endpoints WHERE id = p_endpoint_id;

            IF v_half_open_at IS NOT NULL AND NOW() >= v_half_open_at THEN
                v_state := 'half_open';
                UPDATE ulak.endpoints
                SET circuit_state = 'half_open',
                    updated_at = NOW()
                WHERE id = p_endpoint_id;

                INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
                VALUES ('circuit.half_open', 'endpoint', p_endpoint_id,
                        jsonb_build_object('state', 'open', 'failures', v_failures),
                        jsonb_build_object('state', 'half_open', 'failures', v_failures));
            END IF;
        END;
    END IF;

    IF p_success THEN
        -- Success: reset to closed state
        UPDATE ulak.endpoints
        SET circuit_state = 'closed',
            circuit_failure_count = 0,
            circuit_opened_at = NULL,
            circuit_half_open_at = NULL,
            last_success_at = NOW(),
            updated_at = NOW()
        WHERE id = p_endpoint_id;

        -- Log state transition if circuit was not already closed
        IF v_old_state != 'closed' THEN
            INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
            VALUES ('circuit.closed', 'endpoint', p_endpoint_id,
                    jsonb_build_object('state', v_old_state, 'failures', v_failures),
                    jsonb_build_object('state', 'closed', 'failures', 0));
        END IF;
    ELSE
        -- Failure handling depends on current state
        IF v_state = 'half_open' THEN
            -- Half-open probe failed: go back to open with reset timer
            UPDATE ulak.endpoints
            SET circuit_state = 'open',
                circuit_failure_count = v_failures + 1,
                circuit_opened_at = NOW(),
                circuit_half_open_at = NOW() + (v_cooldown || ' seconds')::interval,
                last_failure_at = NOW(),
                updated_at = NOW()
            WHERE id = p_endpoint_id;

            INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
            VALUES ('circuit.opened', 'endpoint', p_endpoint_id,
                    jsonb_build_object('state', 'half_open', 'failures', v_failures),
                    jsonb_build_object('state', 'open', 'failures', v_failures + 1));
        ELSE
            -- Closed or already open: increment counter and potentially open circuit
            v_failures := v_failures + 1;

            IF v_failures >= v_threshold THEN
                -- Open the circuit
                UPDATE ulak.endpoints
                SET circuit_state = 'open',
                    circuit_failure_count = v_failures,
                    circuit_opened_at = NOW(),
                    circuit_half_open_at = NOW() + (v_cooldown || ' seconds')::interval,
                    last_failure_at = NOW(),
                    updated_at = NOW()
                WHERE id = p_endpoint_id;

                -- Log circuit opened event
                IF v_old_state != 'open' THEN
                    INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
                    VALUES ('circuit.opened', 'endpoint', p_endpoint_id,
                            jsonb_build_object('state', v_old_state, 'failures', v_failures - 1),
                            jsonb_build_object('state', 'open', 'failures', v_failures));
                END IF;
            ELSE
                -- Just increment failure count
                UPDATE ulak.endpoints
                SET circuit_state = v_state,
                    circuit_failure_count = v_failures,
                    last_failure_at = NOW(),
                    updated_at = NOW()
                WHERE id = p_endpoint_id;
            END IF;
        END IF;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- DLQ redrive: row lock, status check, ordering_key preserved
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ulak.redrive_message(
    p_dlq_id bigint
) RETURNS bigint
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_new_id bigint;
    v_original_id bigint;
    v_endpoint_name text;
    v_status text;
BEGIN
    -- Lock the DLQ row so two concurrent redrives of the same message cannot
    -- both insert a copy into the queue; the second caller sees 'redriven'.
    SELECT original_message_id, endpoint_name, status
    INTO v_original_id, v_endpoint_name, v_status
    FROM ulak.dlq WHERE id = p_dlq_id
    FOR UPDATE;

    IF v_original_id IS NULL THEN
        RAISE EXCEPTION 'DLQ message % not found', p_dlq_id;
    END IF;

    IF v_status <> 'failed' THEN
        RAISE EXCEPTION 'DLQ message % is not in failed state (status: %)', p_dlq_id, v_status;
    END IF;

    -- Insert back into queue as pending with reset retry count
    INSERT INTO ulak.queue (
        endpoint_id, payload, status, retry_count, priority,
        scheduled_at, idempotency_key, correlation_id, expires_at,
        payload_hash, ordering_key, headers, metadata
    )
    SELECT
        endpoint_id, payload, 'pending', 0, priority,
        NULL, NULL, correlation_id, NULL,
        payload_hash, ordering_key, headers, metadata
    FROM ulak.dlq
    WHERE id = p_dlq_id
    RETURNING id INTO v_new_id;

    -- Mark DLQ entry as redriven (don't delete — keep audit trail)
    UPDATE ulak.dlq
    SET status = 'redriven',
        last_error = format('Redriven as message %s at %s', v_new_id, NOW())
    WHERE id = p_dlq_id;

    -- Log the redrive event
    INSERT INTO ulak.event_log (event_type, entity_type, entity_id, metadata)
    VALUES ('redrive', 'message', v_new_id,
            jsonb_build_object(
                'dlq_id', p_dlq_id,
                'original_message_id', v_original_id,
                'endpoint', v_endpoint_name
            ));

    RAISE LOG '[ulak] Redrove DLQ message % (original %) as new message %',
               p_dlq_id, v_original_id, v_new_id;

    RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION ulak.redrive_message(bigint) IS
'Redrive a single message from DLQ back to queue.
Retry count is reset to 0 and ordering_key is preserved. DLQ entry is marked as redriven (not deleted).
The DLQ row is locked for the duration of the call; a message that is not in failed state raises an error.
Returns the new message ID.';
