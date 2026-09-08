-- ulak 0.1.2 -> 0.2.0 upgrade migration
-- Workers are woken through shared memory at commit instead of a NOTIFY per
-- insert (src/wake.c): adds ulak._wake_workers() and routes the queue trigger,
-- send_with_options(), send_batch*(), publish*() through it.
-- Function bodies are copied verbatim from sql/ulak.sql.

CREATE OR REPLACE FUNCTION ulak._wake_workers(
    p_id bigint,
    p_ordering_key text
) RETURNS void
LANGUAGE c
VOLATILE PARALLEL UNSAFE
AS 'ulak', 'ulak_wake_workers_sql';

CREATE OR REPLACE FUNCTION ulak.notify_new_message()
RETURNS TRIGGER AS $$
BEGIN
    -- Skip if suppressed (bulk COPY, or a caller that wakes the exact partition itself)
    IF current_setting('ulak.suppress_notify', true) = 'on' THEN
        RETURN NULL;
    END IF;
    -- Statement-level trigger: the ids are unknown here, wake every worker
    PERFORM ulak._wake_workers(NULL, NULL);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ulak.send_with_options(
    p_endpoint_name text,
    p_payload jsonb,
    p_priority smallint DEFAULT 0,
    p_scheduled_at timestamptz DEFAULT NULL,
    p_idempotency_key text DEFAULT NULL,
    p_correlation_id uuid DEFAULT NULL,
    p_expires_at timestamptz DEFAULT NULL,
    p_ordering_key text DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_endpoint_id bigint;
    v_message_id bigint;
    v_payload_hash text;
    v_existing_hash text;
BEGIN
    -- Backpressure check
    PERFORM ulak._check_backpressure();

    -- Validate endpoint exists and is enabled
    SELECT id INTO v_endpoint_id
    FROM ulak.endpoints
    WHERE name = p_endpoint_name AND enabled = true;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist or is disabled', p_endpoint_name;
    END IF;

    -- Validate priority range
    IF p_priority < 0 OR p_priority > 10 THEN
        RAISE EXCEPTION 'Priority must be between 0 and 10, got %', p_priority;
    END IF;

    -- Compute payload hash when idempotency key is provided
    v_payload_hash := CASE WHEN p_idempotency_key IS NOT NULL
                          THEN md5(p_payload::text) ELSE NULL END;

    -- Insert message with all options. The statement trigger would wake every
    -- worker; suppress it and wake the one partition this row hashes to.
    PERFORM set_config('ulak.suppress_notify', 'on', true);
    INSERT INTO ulak.queue (
        endpoint_id, payload, status, retry_count, next_retry_at,
        priority, scheduled_at, idempotency_key, correlation_id, expires_at,
        payload_hash, ordering_key
    )
    VALUES (
        v_endpoint_id, p_payload, 'pending', 0,
        COALESCE(p_scheduled_at, NOW()),
        p_priority, p_scheduled_at, p_idempotency_key, p_correlation_id, p_expires_at,
        v_payload_hash, p_ordering_key
    )
    RETURNING id INTO v_message_id;
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    PERFORM ulak._wake_workers(v_message_id, p_ordering_key);

    RETURN v_message_id;
EXCEPTION
    WHEN unique_violation THEN
        -- Idempotency key collision - check payload hash match
        SELECT id, payload_hash INTO v_message_id, v_existing_hash
        FROM ulak.queue
        WHERE idempotency_key = p_idempotency_key;

        IF v_existing_hash IS DISTINCT FROM v_payload_hash THEN
            RAISE EXCEPTION 'Idempotency key ''%'' conflict: same key with different payload', p_idempotency_key;
        END IF;

        RETURN v_message_id;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.send_batch(
    p_endpoint_name text,
    p_payloads jsonb[]
) RETURNS bigint[]
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_endpoint_id bigint;
    v_ids bigint[];
BEGIN
    -- Backpressure check
    PERFORM ulak._check_backpressure();

    -- Validate endpoint exists and is enabled
    SELECT id INTO v_endpoint_id
    FROM ulak.endpoints
    WHERE name = p_endpoint_name AND enabled = true;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist or is disabled', p_endpoint_name;
    END IF;

    -- Suppress the trigger during bulk insert (workers are woken once at the end)
    PERFORM set_config('ulak.suppress_notify', 'on', true);

    -- Bulk insert all payloads in a single INSERT using unnest
    WITH inserted AS (
        INSERT INTO ulak.queue (endpoint_id, payload, status, next_retry_at, created_at, updated_at)
        SELECT v_endpoint_id, p, 'pending', NOW(), NOW(), NOW()
        FROM unnest(p_payloads) AS p
        RETURNING id
    )
    SELECT array_agg(id) INTO v_ids FROM inserted;

    -- Re-enable the trigger and wake the workers once for the whole batch
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    PERFORM ulak._wake_workers(NULL, NULL);

    RETURN v_ids;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.send_batch_with_priority(
    p_endpoint_name text,
    p_payloads jsonb[],
    p_priority smallint DEFAULT 0
) RETURNS bigint[]
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_endpoint_id bigint;
    v_ids bigint[];
BEGIN
    -- Backpressure check
    PERFORM ulak._check_backpressure();

    SELECT id INTO v_endpoint_id
    FROM ulak.endpoints
    WHERE name = p_endpoint_name AND enabled = true;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist or is disabled', p_endpoint_name;
    END IF;

    PERFORM set_config('ulak.suppress_notify', 'on', true);

    WITH inserted AS (
        INSERT INTO ulak.queue (endpoint_id, payload, status, priority, next_retry_at, created_at, updated_at)
        SELECT v_endpoint_id, p, 'pending', p_priority, NOW(), NOW(), NOW()
        FROM unnest(p_payloads) AS p
        RETURNING id
    )
    SELECT array_agg(id) INTO v_ids FROM inserted;

    PERFORM set_config('ulak.suppress_notify', 'off', true);
    PERFORM ulak._wake_workers(NULL, NULL);

    RETURN v_ids;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.publish(
    p_event_type text,
    p_payload jsonb
) RETURNS integer
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_event_type_id bigint;
    v_count integer;
    v_projected_count bigint;
BEGIN
    SELECT id INTO v_event_type_id FROM ulak.event_types WHERE name = p_event_type;
    IF v_event_type_id IS NULL THEN
        RAISE EXCEPTION 'Event type ''%'' does not exist', p_event_type;
    END IF;

    SELECT count(*) INTO v_projected_count
    FROM ulak.subscriptions s
    JOIN ulak.endpoints e ON e.id = s.endpoint_id
    WHERE s.event_type_id = v_event_type_id
      AND s.enabled = true
      AND e.enabled = true
      AND (s.filter IS NULL OR p_payload @> s.filter);

    -- Backpressure check with exact projected fan-out row count
    PERFORM ulak._check_backpressure(v_projected_count);

    -- Suppress per-row NOTIFY during fan-out (same pattern as send_batch)
    PERFORM set_config('ulak.suppress_notify', 'on', true);

    -- Fan-out: INSERT...SELECT from active subscriptions for enabled endpoints
    WITH inserted AS (
        INSERT INTO ulak.queue (endpoint_id, payload, status, next_retry_at, created_at, updated_at)
        SELECT s.endpoint_id, p_payload, 'pending', NOW(), NOW(), NOW()
        FROM ulak.subscriptions s
        JOIN ulak.endpoints e ON e.id = s.endpoint_id
        WHERE s.event_type_id = v_event_type_id
          AND s.enabled = true
          AND e.enabled = true
          AND (s.filter IS NULL OR p_payload @> s.filter)
        RETURNING id
    )
    SELECT count(*) INTO v_count FROM inserted;

    -- Re-enable the trigger and wake the workers once for the entire fan-out
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    IF v_count > 0 THEN
        PERFORM ulak._wake_workers(NULL, NULL);
    END IF;

    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.publish_batch(
    p_events jsonb
) RETURNS integer
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_event record;
    v_event_type_id bigint;
    v_total integer := 0;
    v_count integer;
    v_projected_count bigint;
    v_missing_event_type text;
BEGIN
    WITH batch_events AS (
        SELECT e->>'event_type' AS event_type
        FROM jsonb_array_elements(p_events) AS e
    )
    SELECT be.event_type INTO v_missing_event_type
    FROM batch_events be
    LEFT JOIN ulak.event_types et ON et.name = be.event_type
    WHERE et.id IS NULL
    LIMIT 1;

    IF v_missing_event_type IS NOT NULL THEN
        RAISE EXCEPTION 'Event type ''%'' does not exist', v_missing_event_type;
    END IF;

    WITH batch_events AS (
        SELECT e->>'event_type' AS event_type, e->'payload' AS payload
        FROM jsonb_array_elements(p_events) AS e
    )
    SELECT COALESCE(count(*), 0) INTO v_projected_count
    FROM batch_events be
    JOIN ulak.event_types et ON et.name = be.event_type
    JOIN ulak.subscriptions s ON s.event_type_id = et.id
    JOIN ulak.endpoints ep ON ep.id = s.endpoint_id
    WHERE s.enabled = true
      AND ep.enabled = true
      AND (s.filter IS NULL OR be.payload @> s.filter);

    -- Backpressure check with exact projected fan-out row count for the entire batch
    PERFORM ulak._check_backpressure(v_projected_count);

    PERFORM set_config('ulak.suppress_notify', 'on', true);

    FOR v_event IN
        SELECT e->>'event_type' AS event_type, e->'payload' AS payload
        FROM jsonb_array_elements(p_events) AS e
    LOOP
        SELECT id INTO v_event_type_id FROM ulak.event_types WHERE name = v_event.event_type;
        IF v_event_type_id IS NULL THEN
            RAISE EXCEPTION 'Event type ''%'' does not exist', v_event.event_type;
        END IF;

        WITH inserted AS (
            INSERT INTO ulak.queue (endpoint_id, payload, status, next_retry_at, created_at, updated_at)
            SELECT s.endpoint_id, v_event.payload, 'pending', NOW(), NOW(), NOW()
            FROM ulak.subscriptions s
            JOIN ulak.endpoints e ON e.id = s.endpoint_id
            WHERE s.event_type_id = v_event_type_id
              AND s.enabled = true
              AND e.enabled = true
              AND (s.filter IS NULL OR v_event.payload @> s.filter)
            RETURNING id
        )
        SELECT count(*) INTO v_count FROM inserted;
        v_total := v_total + v_count;
    END LOOP;

    PERFORM set_config('ulak.suppress_notify', 'off', true);
    IF v_total > 0 THEN
        PERFORM ulak._wake_workers(NULL, NULL);
    END IF;

    RETURN v_total;
END;
$$;

COMMENT ON FUNCTION ulak._wake_workers(bigint, text) IS
'Internal: wake the delivery worker(s) responsible for a queued row once the current transaction commits.
The extension sets worker latches through shared memory (no NOTIFY lock); NULL id wakes every worker.';

COMMENT ON FUNCTION ulak.send_batch(text, jsonb[]) IS
'High-throughput batch send: inserts multiple messages in a single SQL statement.
Uses unnest pattern for 10-15x higher throughput than individual send() calls.
Workers are woken once for the entire batch.
Example: SELECT ulak.send_batch(''webhook'', ARRAY[''{...}''::jsonb, ''{...}''::jsonb])';

-- New functions start with EXECUTE for PUBLIC; the fresh schema revokes it
REVOKE ALL ON FUNCTION ulak._wake_workers(bigint, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ulak._wake_workers(bigint, text) TO ulak_admin;
