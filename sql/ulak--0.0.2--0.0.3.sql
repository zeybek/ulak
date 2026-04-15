-- ulak extension upgrade script: 0.0.2 -> 0.0.3
-- Competitive analysis improvements (24-item roadmap, items 1-19)
--
-- This script transforms a 0.0.2 installation into a 0.0.3 installation.
-- Every statement is idempotent where possible (IF NOT EXISTS, CREATE OR REPLACE,
-- DO blocks with existence checks).

-- ============================================================================
-- 1. ALTER TABLE: Add new columns
-- ============================================================================

ALTER TABLE ulak.endpoints
    ADD COLUMN IF NOT EXISTS concurrency_limit integer DEFAULT NULL
    CHECK (concurrency_limit IS NULL OR concurrency_limit > 0);

ALTER TABLE ulak.subscriptions
    ADD COLUMN IF NOT EXISTS transform_fn text DEFAULT NULL;

-- ============================================================================
-- 2. COMMENT ON COLUMN for new columns
-- ============================================================================

COMMENT ON COLUMN ulak.endpoints.concurrency_limit IS
'Maximum number of messages that can be processing simultaneously for this endpoint. NULL means unlimited.';

COMMENT ON TABLE ulak.subscriptions IS
'Maps event types to endpoints for fan-out delivery.
When publish() is called, messages are created for all active subscriptions.
Filter uses JSONB containment (@>) for payload matching.
transform_fn: optional qualified function name (schema.func) to transform payload before dispatch.';

-- ============================================================================
-- 3. CREATE OR REPLACE trigger functions
-- ============================================================================

-- Config change notification trigger for dispatcher cache invalidation
CREATE OR REPLACE FUNCTION ulak.notify_config_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('ulak_config_change', COALESCE(OLD.id, NEW.id)::text);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Notify trigger for new messages
-- Uses current_setting to allow suppression during batch operations
CREATE OR REPLACE FUNCTION ulak.notify_new_message()
RETURNS TRIGGER AS $$
BEGIN
    -- Skip NOTIFY if suppressed (e.g., during bulk COPY or send_batch)
    IF current_setting('ulak.suppress_notify', true) = 'on' THEN
        RETURN NULL;
    END IF;
    -- Skip NOTIFY if disabled via GUC (reduces global lock contention at high throughput)
    IF current_setting('ulak.enable_notify', true) = 'off' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('ulak_new_msg', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Payload immutability trigger with internal_update bypass
CREATE OR REPLACE FUNCTION ulak.prevent_payload_modification()
RETURNS TRIGGER AS $$
BEGIN
    -- Allow internal updates from send_with_options SECURITY DEFINER (replace/debounce).
    -- The GUC is set transaction-local by send_with_options and cleared immediately after.
    -- Note: ulak_application has only SELECT on queue (no UPDATE), so they cannot exploit
    -- this bypass. ulak_admin already has full DML access.
    IF current_setting('ulak.internal_update', true) = 'on' THEN
        RETURN NEW;
    END IF;
    IF OLD.payload IS DISTINCT FROM NEW.payload THEN
        RAISE EXCEPTION 'payload modification not allowed after creation'
            USING HINT = 'Message payloads are immutable for audit integrity';
    END IF;
    IF OLD.headers IS DISTINCT FROM NEW.headers THEN
        RAISE EXCEPTION 'headers modification not allowed after creation'
            USING HINT = 'Message headers are immutable for audit integrity';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. CREATE OR REPLACE new functions
-- ============================================================================

-- Queue health (unified health snapshot)
CREATE OR REPLACE FUNCTION ulak.queue_health()
RETURNS TABLE (
    total_pending bigint,
    total_processing bigint,
    oldest_pending_age_seconds double precision,
    oldest_processing_age_seconds double precision,
    endpoints_with_open_circuit integer,
    dlq_depth bigint,
    archive_default_rows bigint
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
    SELECT
        (SELECT count(*) FROM ulak.queue WHERE status = 'pending'),
        (SELECT count(*) FROM ulak.queue WHERE status = 'processing'),
        (SELECT EXTRACT(EPOCH FROM NOW() - min(created_at)) FROM ulak.queue WHERE status = 'pending'),
        (SELECT EXTRACT(EPOCH FROM NOW() - min(processing_started_at)) FROM ulak.queue WHERE status = 'processing'),
        (SELECT count(*)::integer FROM ulak.endpoints WHERE circuit_state = 'open'),
        (SELECT count(*) FROM ulak.dlq WHERE status = 'failed'),
        (SELECT count(*) FROM ulak.archive_default)
$$;

COMMENT ON FUNCTION ulak.queue_health() IS
'Returns a unified queue health snapshot: pending/processing counts, oldest message ages,
open circuit breakers, DLQ depth, and archive default partition rows.';

-- Purge all pending messages for an endpoint
CREATE OR REPLACE FUNCTION ulak.purge_endpoint(
    p_endpoint_name text
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_endpoint_id bigint;
    v_deleted integer;
BEGIN
    SELECT id INTO v_endpoint_id
    FROM ulak.endpoints
    WHERE name = p_endpoint_name;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist', p_endpoint_name;
    END IF;

    DELETE FROM ulak.queue
    WHERE endpoint_id = v_endpoint_id AND status = 'pending';

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    IF v_deleted > 0 THEN
        RAISE LOG '[ulak] Purged % pending message(s) for endpoint ''%''', v_deleted, p_endpoint_name;

        INSERT INTO ulak.event_log (event_type, entity_type, entity_id, metadata)
        VALUES ('purge', 'endpoint', v_endpoint_id,
                jsonb_build_object('endpoint_name', p_endpoint_name, 'messages_purged', v_deleted));
    END IF;

    RETURN v_deleted;
END;
$$;

COMMENT ON FUNCTION ulak.purge_endpoint(text) IS
'Delete all pending messages for an endpoint. Returns the number of messages purged.
Processing and completed messages are not affected.';

-- Default partition monitoring
CREATE OR REPLACE FUNCTION ulak.check_archive_default()
RETURNS integer
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_count integer;
BEGIN
    SELECT count(*)::integer INTO v_count
    FROM ulak.archive_default;

    IF v_count > 0 THEN
        RAISE WARNING '[ulak] % row(s) found in archive_default partition — missing monthly partitions may need to be created', v_count;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.check_archive_default() IS
'Check if any rows landed in the archive default partition (indicating missing monthly partitions).
Emits a WARNING if rows are found. Called during periodic maintenance.';

-- Payload schema validation
CREATE OR REPLACE FUNCTION ulak.validate_payload_schema(
    p_payload jsonb,
    p_schema jsonb
) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_type text;
    v_required jsonb;
    v_properties jsonb;
    v_key text;
    v_prop_schema jsonb;
    v_prop_value jsonb;
    v_prop_type text;
BEGIN
    IF p_schema IS NULL THEN
        RETURN true;
    END IF;

    -- Check top-level type
    v_type := p_schema->>'type';
    IF v_type IS NOT NULL THEN
        IF v_type = 'object' AND jsonb_typeof(p_payload) != 'object' THEN
            RAISE EXCEPTION 'Schema validation failed: expected object, got %', jsonb_typeof(p_payload);
        ELSIF v_type = 'array' AND jsonb_typeof(p_payload) != 'array' THEN
            RAISE EXCEPTION 'Schema validation failed: expected array, got %', jsonb_typeof(p_payload);
        ELSIF v_type = 'string' AND jsonb_typeof(p_payload) != 'string' THEN
            RAISE EXCEPTION 'Schema validation failed: expected string, got %', jsonb_typeof(p_payload);
        ELSIF v_type = 'number' AND jsonb_typeof(p_payload) != 'number' THEN
            RAISE EXCEPTION 'Schema validation failed: expected number, got %', jsonb_typeof(p_payload);
        ELSIF v_type = 'boolean' AND jsonb_typeof(p_payload) != 'boolean' THEN
            RAISE EXCEPTION 'Schema validation failed: expected boolean, got %', jsonb_typeof(p_payload);
        END IF;
    END IF;

    -- Check required fields
    v_required := p_schema->'required';
    IF v_required IS NOT NULL AND jsonb_typeof(v_required) = 'array' THEN
        FOR v_key IN SELECT jsonb_array_elements_text(v_required) LOOP
            IF NOT p_payload ? v_key THEN
                RAISE EXCEPTION 'Schema validation failed: required field ''%'' is missing', v_key;
            END IF;
        END LOOP;
    END IF;

    -- Check property types
    v_properties := p_schema->'properties';
    IF v_properties IS NOT NULL AND jsonb_typeof(v_properties) = 'object' THEN
        FOR v_key IN SELECT jsonb_object_keys(v_properties) LOOP
            IF p_payload ? v_key THEN
                v_prop_schema := v_properties->v_key;
                v_prop_value := p_payload->v_key;
                v_prop_type := v_prop_schema->>'type';

                IF v_prop_type IS NOT NULL AND v_prop_value IS NOT NULL THEN
                    IF v_prop_type = 'string' AND jsonb_typeof(v_prop_value) != 'string' THEN
                        RAISE EXCEPTION 'Schema validation failed: field ''%'' expected string, got %', v_key, jsonb_typeof(v_prop_value);
                    ELSIF v_prop_type = 'number' AND jsonb_typeof(v_prop_value) != 'number' THEN
                        RAISE EXCEPTION 'Schema validation failed: field ''%'' expected number, got %', v_key, jsonb_typeof(v_prop_value);
                    ELSIF v_prop_type = 'boolean' AND jsonb_typeof(v_prop_value) != 'boolean' THEN
                        RAISE EXCEPTION 'Schema validation failed: field ''%'' expected boolean, got %', v_key, jsonb_typeof(v_prop_value);
                    ELSIF v_prop_type = 'object' AND jsonb_typeof(v_prop_value) != 'object' THEN
                        RAISE EXCEPTION 'Schema validation failed: field ''%'' expected object, got %', v_key, jsonb_typeof(v_prop_value);
                    ELSIF v_prop_type = 'array' AND jsonb_typeof(v_prop_value) != 'array' THEN
                        RAISE EXCEPTION 'Schema validation failed: field ''%'' expected array, got %', v_key, jsonb_typeof(v_prop_value);
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END IF;

    RETURN true;
END;
$$;

COMMENT ON FUNCTION ulak.validate_payload_schema(jsonb, jsonb) IS
'Validate a JSONB payload against a JSON Schema subset.
Supports: type (object/array/string/number/boolean), required fields, property type checks.';

-- Payload transformation
CREATE OR REPLACE FUNCTION ulak._apply_transform(
    p_transform_fn text,
    p_payload jsonb
) RETURNS jsonb
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_result jsonb;
BEGIN
    IF p_transform_fn IS NULL THEN
        RETURN p_payload;
    END IF;

    -- Validate function name: reject multiple dots (unsupported)
    IF p_transform_fn LIKE '%.%.%' THEN
        RAISE EXCEPTION 'Invalid transform function name ''%'': only schema.function format is supported', p_transform_fn;
    END IF;

    -- Split schema.function and quote each part for SQL injection safety
    IF p_transform_fn LIKE '%.%' THEN
        EXECUTE format('SELECT %I.%I($1)',
            split_part(p_transform_fn, '.', 1),
            split_part(p_transform_fn, '.', 2))
        INTO v_result USING p_payload;
    ELSE
        EXECUTE format('SELECT %I($1)', p_transform_fn)
        INTO v_result USING p_payload;
    END IF;

    IF v_result IS NULL THEN
        RAISE EXCEPTION 'Transform function ''%'' returned NULL', p_transform_fn;
    END IF;

    RETURN v_result;
EXCEPTION
    WHEN undefined_function THEN
        RAISE EXCEPTION 'Transform function ''%'' does not exist', p_transform_fn;
    WHEN others THEN
        RAISE EXCEPTION 'Transform function ''%'' failed: %', p_transform_fn, SQLERRM;
END;
$$;

COMMENT ON FUNCTION ulak._apply_transform(text, jsonb) IS
'Internal: apply a user-defined transform function to a payload.
The function must accept (jsonb) and return jsonb.';

-- Index maintenance
CREATE OR REPLACE FUNCTION ulak.reindex_queue()
RETURNS integer
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_index record;
    v_count integer := 0;
BEGIN
    FOR v_index IN
        SELECT c.relname AS indexname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'ulak'
          AND t.relname IN ('queue', 'endpoints', 'dlq')
          AND i.indisvalid = true
    LOOP
        BEGIN
            EXECUTE format('REINDEX INDEX ulak.%I', v_index.indexname);
            v_count := v_count + 1;
        EXCEPTION WHEN others THEN
            RAISE WARNING '[ulak] Failed to reindex %: %', v_index.indexname, SQLERRM;
        END;
    END LOOP;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.reindex_queue() IS
'Rebuild all ulak queue/endpoint/DLQ indexes using REINDEX INDEX.
Reduces index bloat from frequent status UPDATEs. Schedule via pg_cron.
For non-blocking reindex, run REINDEX INDEX CONCURRENTLY manually outside a transaction.';

-- ============================================================================
-- 5. DROP old function signatures + CREATE OR REPLACE modified functions
-- ============================================================================

-- send_with_options: old 8-param signature -> new 10-param signature
-- (adds p_on_conflict text, p_debounce_seconds integer)
DROP FUNCTION IF EXISTS ulak.send_with_options(text, jsonb, smallint, timestamptz, text, uuid, timestamptz, text);

CREATE OR REPLACE FUNCTION ulak.send_with_options(
    p_endpoint_name text,
    p_payload jsonb,
    p_priority smallint DEFAULT 0,
    p_scheduled_at timestamptz DEFAULT NULL,
    p_idempotency_key text DEFAULT NULL,
    p_correlation_id uuid DEFAULT NULL,
    p_expires_at timestamptz DEFAULT NULL,
    p_ordering_key text DEFAULT NULL,
    p_on_conflict text DEFAULT 'raise',
    p_debounce_seconds integer DEFAULT NULL
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
    v_existing_created_at timestamptz;
BEGIN
    -- Validate on_conflict parameter
    IF p_on_conflict NOT IN ('raise', 'skip', 'replace') THEN
        RAISE EXCEPTION 'on_conflict must be ''raise'', ''skip'', or ''replace'', got ''%''', p_on_conflict;
    END IF;

    -- Debounce requires idempotency_key
    IF p_debounce_seconds IS NOT NULL AND p_idempotency_key IS NULL THEN
        RAISE EXCEPTION 'debounce_seconds requires idempotency_key to be set';
    END IF;

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

    -- Debounce check: if a pending message with same key exists within the window, skip
    IF p_debounce_seconds IS NOT NULL THEN
        SELECT id, created_at INTO v_message_id, v_existing_created_at
        FROM ulak.queue
        WHERE idempotency_key = p_idempotency_key
          AND status IN ('pending', 'processing');

        IF v_message_id IS NOT NULL THEN
            IF v_existing_created_at + (p_debounce_seconds || ' seconds')::interval > NOW() THEN
                -- Within debounce window: skip (return existing)
                RETURN v_message_id;
            ELSE
                -- Outside debounce window: replace payload of existing message
                PERFORM set_config('ulak.internal_update', 'on', true);
                UPDATE ulak.queue SET payload = p_payload, payload_hash = v_payload_hash,
                       updated_at = NOW()
                WHERE id = v_message_id AND status = 'pending';
                PERFORM set_config('ulak.internal_update', 'off', true);
                RETURN v_message_id;
            END IF;
        END IF;
    END IF;

    -- Insert message with all options
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

    RETURN v_message_id;
EXCEPTION
    WHEN unique_violation THEN
        -- Idempotency key collision - handle based on on_conflict mode
        SELECT id, payload_hash INTO v_message_id, v_existing_hash
        FROM ulak.queue
        WHERE idempotency_key = p_idempotency_key
          AND status IN ('pending', 'processing');

        IF p_on_conflict = 'skip' THEN
            RETURN v_message_id;
        ELSIF p_on_conflict = 'replace' THEN
            -- Replace payload of existing pending message
            PERFORM set_config('ulak.internal_update', 'on', true);
            UPDATE ulak.queue SET payload = p_payload, payload_hash = v_payload_hash,
                   updated_at = NOW()
            WHERE id = v_message_id AND status = 'pending';
            PERFORM set_config('ulak.internal_update', 'off', true);
            RETURN v_message_id;
        ELSE
            -- 'raise' mode: check payload hash match
            IF v_existing_hash IS DISTINCT FROM v_payload_hash THEN
                RAISE EXCEPTION 'Idempotency key ''%'' conflict: same key with different payload', p_idempotency_key;
            END IF;
            RETURN v_message_id;
        END IF;
END;
$$;

COMMENT ON FUNCTION ulak.send_with_options(text, jsonb, smallint, timestamptz, text, uuid, timestamptz, text, text, integer) IS
'Extended send function with additional options:
- priority: 0-10 (higher = processed first)
- scheduled_at: delay delivery until this time
- idempotency_key: unique key for deduplication
- correlation_id: UUID for distributed tracing
- expires_at: TTL - message skipped if expired
- on_conflict: raise (default), skip, or replace on idempotency collision
- debounce_seconds: time window for dedup (requires idempotency_key)';

-- subscribe: old 3-param signature -> new 4-param signature
-- (adds p_transform_fn text, now SECURITY DEFINER)
DROP FUNCTION IF EXISTS ulak.subscribe(text, text, jsonb);

CREATE OR REPLACE FUNCTION ulak.subscribe(
    p_event_type text,
    p_endpoint_name text,
    p_filter jsonb DEFAULT NULL,
    p_transform_fn text DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_event_type_id bigint;
    v_endpoint_id bigint;
    v_sub_id bigint;
BEGIN
    SELECT id INTO v_event_type_id FROM ulak.event_types WHERE name = p_event_type;
    IF v_event_type_id IS NULL THEN
        RAISE EXCEPTION 'Event type ''%'' does not exist', p_event_type;
    END IF;

    SELECT id INTO v_endpoint_id FROM ulak.endpoints WHERE name = p_endpoint_name;
    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist', p_endpoint_name;
    END IF;

    -- Validate transform function exists if provided
    IF p_transform_fn IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE format('%I.%I', n.nspname, p.proname) = p_transform_fn
               OR p.proname = p_transform_fn
        ) THEN
            RAISE EXCEPTION 'Transform function ''%'' does not exist', p_transform_fn;
        END IF;
    END IF;

    INSERT INTO ulak.subscriptions (event_type_id, endpoint_id, filter, transform_fn)
    VALUES (v_event_type_id, v_endpoint_id, p_filter, p_transform_fn)
    RETURNING id INTO v_sub_id;

    RETURN v_sub_id;
END;
$$;

-- publish: now includes schema validation and transform support
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
    v_event_schema jsonb;
    v_count integer;
    v_projected_count bigint;
BEGIN
    SELECT id, schema INTO v_event_type_id, v_event_schema
    FROM ulak.event_types WHERE name = p_event_type;
    IF v_event_type_id IS NULL THEN
        RAISE EXCEPTION 'Event type ''%'' does not exist', p_event_type;
    END IF;

    -- Validate payload against event type schema (if defined)
    IF v_event_schema IS NOT NULL THEN
        PERFORM ulak.validate_payload_schema(p_payload, v_event_schema);
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
    -- Apply per-subscription transform functions when configured
    WITH inserted AS (
        INSERT INTO ulak.queue (endpoint_id, payload, status, next_retry_at, created_at, updated_at)
        SELECT s.endpoint_id,
               CASE WHEN s.transform_fn IS NOT NULL
                    THEN ulak._apply_transform(s.transform_fn, p_payload)
                    ELSE p_payload
               END,
               'pending', NOW(), NOW(), NOW()
        FROM ulak.subscriptions s
        JOIN ulak.endpoints e ON e.id = s.endpoint_id
        WHERE s.event_type_id = v_event_type_id
          AND s.enabled = true
          AND e.enabled = true
          AND (s.filter IS NULL OR p_payload @> s.filter)
        RETURNING id
    )
    SELECT count(*) INTO v_count FROM inserted;

    -- Re-enable NOTIFY and send single notification for entire fan-out
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    IF v_count > 0 THEN
        PERFORM pg_notify('ulak_new_msg', '');
    END IF;

    RETURN v_count;
END;
$$;

-- cleanup_old_archive_partitions: detach-before-drop pattern
CREATE OR REPLACE FUNCTION ulak.cleanup_old_archive_partitions(
    p_retention_months integer DEFAULT 6
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_cutoff date;
    v_partition record;
    v_dropped integer := 0;
BEGIN
    v_cutoff := date_trunc('month', CURRENT_DATE)::date - (p_retention_months || ' months')::interval;

    FOR v_partition IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE n.nspname = 'ulak'
          AND parent.relname = 'archive'
          AND c.relname LIKE 'archive_%'
          AND c.relname != 'archive_default'
    LOOP
        -- Extract date from partition name (archive_YYYY_MM)
        DECLARE
            v_part_date date;
        BEGIN
            v_part_date := to_date(substring(v_partition.relname from 'archive_(\d{4}_\d{2})'), 'YYYY_MM');
            IF v_part_date < v_cutoff THEN
                -- Two-phase removal (pg_partman pattern): detach first, then drop
                EXECUTE format('ALTER TABLE ulak.archive DETACH PARTITION ulak.%I', v_partition.relname);
                EXECUTE format('DROP TABLE IF EXISTS ulak.%I', v_partition.relname);
                v_dropped := v_dropped + 1;
                RAISE LOG '[ulak] Detached and dropped old archive partition: %', v_partition.relname;
            END IF;
        EXCEPTION WHEN others THEN
            -- Skip partitions with unparseable names
            CONTINUE;
        END;
    END LOOP;

    RETURN v_dropped;
END;
$$;

COMMENT ON FUNCTION ulak.cleanup_old_archive_partitions(integer) IS
'Drop archive partitions older than the specified retention period.
Default retention is 6 months. Returns number of partitions dropped.';

-- ============================================================================
-- 6. DO block for trigger creation (IF NOT EXISTS)
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'notify_endpoint_config_change'
          AND tgrelid = 'ulak.endpoints'::regclass
    ) THEN
        CREATE TRIGGER notify_endpoint_config_change
            AFTER UPDATE OR DELETE ON ulak.endpoints
            FOR EACH ROW EXECUTE FUNCTION ulak.notify_config_change();
    END IF;
END
$$;

-- ============================================================================
-- 7. Updated COMMENTs
-- ============================================================================

COMMENT ON TABLE ulak.subscriptions IS
'Maps event types to endpoints for fan-out delivery.
When publish() is called, messages are created for all active subscriptions.
Filter uses JSONB containment (@>) for payload matching.
transform_fn: optional qualified function name (schema.func) to transform payload before dispatch.';

-- ============================================================================
-- 8. REVOKE ALL + complete re-GRANTs for ALL functions
-- ============================================================================

-- Strip all function permissions (PostgreSQL defaults EXECUTE TO PUBLIC)
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ulak FROM PUBLIC;

-- Admin functions: endpoint management
GRANT EXECUTE ON FUNCTION ulak.create_endpoint(text, text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.drop_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.alter_endpoint(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.validate_endpoint_config(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.enable_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.disable_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.purge_endpoint(text) TO ulak_admin;

-- Admin functions: circuit breaker and maintenance
GRANT EXECUTE ON FUNCTION ulak.update_circuit_breaker(bigint, boolean) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.reset_circuit_breaker(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.mark_expired_messages() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.maintain_archive_partitions(integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_event_log() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_dlq() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak._check_backpressure(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.archive_single_to_dlq(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.archive_completed_messages(integer, integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_old_archive_partitions(integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.check_archive_default() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.reindex_queue() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.validate_payload_schema(jsonb, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak._apply_transform(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.replay_message(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.replay_range(bigint, timestamptz, timestamptz, text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_message(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_all() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.dlq_summary() TO ulak_admin;

-- Admin functions: pub/sub management
GRANT EXECUTE ON FUNCTION ulak.create_event_type(text, text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.drop_event_type(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.subscribe(text, text, jsonb, text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.unsubscribe(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.publish(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.publish_batch(jsonb) TO ulak_admin;

-- Admin functions: monitoring
GRANT EXECUTE ON FUNCTION ulak.get_worker_status() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.health_check() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.enable_fast_mode() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.enable_fast_mode() TO ulak_application;

-- Application functions
GRANT EXECUTE ON FUNCTION ulak.send(text, jsonb) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.send_with_options(text, jsonb, smallint, timestamptz, text, uuid, timestamptz, text, text, integer) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.send_batch(text, jsonb[]) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.send_batch_with_priority(text, jsonb[], smallint) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.publish(text, jsonb) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.publish_batch(jsonb) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.get_worker_status() TO ulak_application;

-- Monitor functions: read-only monitoring
GRANT EXECUTE ON FUNCTION ulak.get_worker_status() TO ulak_monitor;
GRANT EXECUTE ON FUNCTION ulak.health_check() TO ulak_monitor;
GRANT EXECUTE ON FUNCTION ulak.get_endpoint_health(text) TO ulak_monitor;
GRANT EXECUTE ON FUNCTION ulak.dlq_summary() TO ulak_monitor;
GRANT EXECUTE ON FUNCTION ulak._shmem_metrics() TO ulak_monitor;
GRANT EXECUTE ON FUNCTION ulak.metrics() TO ulak_monitor;

-- Metrics accessible to all roles
GRANT EXECUTE ON FUNCTION ulak._shmem_metrics() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.metrics() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.queue_health() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak._shmem_metrics() TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.metrics() TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.queue_health() TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.queue_health() TO ulak_monitor;
