-- ulak extension - Authoritative SQL installation script
-- Native transactional ulak with multi-protocol async dispatch
-- Version: 0.0.1

-- ============================================================================
-- SCHEMA NOTE
-- ============================================================================
-- Schema ulak is created automatically by the extension
-- (defined in ulak.control with 'schema = ulak')

-- Grant usage on schema
GRANT USAGE ON SCHEMA ulak TO PUBLIC;

-- ============================================================================
-- TABLE CREATION
-- ============================================================================

-- Create endpoints table
CREATE TABLE ulak.endpoints (
    id bigserial PRIMARY KEY,
    name text UNIQUE NOT NULL,
    protocol text NOT NULL CHECK (protocol IN ('http', 'kafka', 'mqtt', 'redis', 'amqp', 'nats')),
    config jsonb NOT NULL,
    retry_policy jsonb DEFAULT '{"max_retries": 10, "backoff": "exponential"}'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    description text,
    -- Circuit breaker state (discrete columns)
    circuit_state text NOT NULL DEFAULT 'closed' CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    circuit_failure_count integer NOT NULL DEFAULT 0 CHECK (circuit_failure_count >= 0),
    circuit_opened_at timestamptz,
    circuit_half_open_at timestamptz,
    last_success_at timestamptz,
    last_failure_at timestamptz,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);

COMMENT ON TABLE ulak.endpoints IS
'Message delivery endpoints with protocol configuration.
Each endpoint represents a destination for async message delivery.';

COMMENT ON COLUMN ulak.endpoints.name IS
'Unique human-readable endpoint name used in send() calls';

COMMENT ON COLUMN ulak.endpoints.protocol IS
'Message delivery protocol: http, kafka, mqtt, redis, or amqp';

COMMENT ON COLUMN ulak.endpoints.config IS
'Protocol-specific configuration as JSONB.
HTTP: {url, method, timeout, headers, ssl_verify}
Kafka: {brokers, topic, key, partition, compression}
MQTT: {host, port, topic, qos, username, password}
Redis: {host, port, stream_key, maxlen}
AMQP: {host, port, exchange, routing_key, vhost}';

COMMENT ON COLUMN ulak.endpoints.retry_policy IS
'Retry policy: {"max_retries": 10, "backoff": "exponential|linear|fixed"}';

COMMENT ON COLUMN ulak.endpoints.enabled IS
'Whether endpoint is active. Disabled endpoints are skipped by workers.';

COMMENT ON COLUMN ulak.endpoints.circuit_failure_count IS
'Consecutive dispatch failure count for circuit breaker. Reset to 0 on successful dispatch.';

COMMENT ON COLUMN ulak.endpoints.circuit_state IS
'Circuit breaker state: closed (normal), open (blocking), half_open (testing recovery).';

-- Create queue table
-- All columns match the C Message struct in src/core/entities.h
CREATE TABLE ulak.queue (
    id bigserial PRIMARY KEY,
    endpoint_id bigint NOT NULL REFERENCES ulak.endpoints(id) ON DELETE RESTRICT,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'expired')),
    retry_count int NOT NULL DEFAULT 0,
    next_retry_at timestamptz DEFAULT NOW(),
    last_error text,
    processing_started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    -- Message prioritization, scheduling, and tracing
    priority smallint NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 10),
    scheduled_at timestamptz,
    idempotency_key text,
    correlation_id uuid,
    expires_at timestamptz,
    -- Payload hash for idempotency conflict detection
    payload_hash text,
    -- Message ordering (messages with same key processed serially)
    ordering_key text,
    -- Per-message protocol options
    headers jsonb,
    metadata jsonb,
    response jsonb,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);

COMMENT ON TABLE ulak.queue IS 'Transactional ulak message queue. Messages are inserted atomically with business transactions and delivered asynchronously by background workers.';
COMMENT ON COLUMN ulak.queue.id IS 'Auto-incrementing message ID, used for modulo worker partitioning and ordering.';
COMMENT ON COLUMN ulak.queue.endpoint_id IS 'FK to endpoints table. Determines target protocol, URL/broker, and dispatch configuration.';
COMMENT ON COLUMN ulak.queue.payload IS 'Message body as JSONB. Passed directly to the protocol dispatcher for delivery.';
COMMENT ON COLUMN ulak.queue.status IS 'Lifecycle state: pending → processing → completed/failed/expired. Workers use partial indexes on this column.';
COMMENT ON COLUMN ulak.queue.retry_count IS 'Number of delivery attempts so far. Compared against endpoint retry_policy max_retries.';
COMMENT ON COLUMN ulak.queue.next_retry_at IS 'Earliest time this message can be retried. Set by worker on transient failure using backoff policy. NULL means immediately eligible.';
COMMENT ON COLUMN ulak.queue.last_error IS 'Most recent dispatch error. Prefixed with [RETRYABLE] or [PERMANENT] for error classification.';
COMMENT ON COLUMN ulak.queue.processing_started_at IS 'Timestamp when worker claimed this message. Used to detect stuck messages (stale processing recovery).';
COMMENT ON COLUMN ulak.queue.completed_at IS 'Timestamp when message was successfully delivered to the target endpoint.';
COMMENT ON COLUMN ulak.queue.failed_at IS 'Timestamp when message permanently failed (max retries exceeded or [PERMANENT] error). Moved to DLQ.';
COMMENT ON COLUMN ulak.queue.priority IS 'Dispatch priority 0-10 (higher = first). Workers fetch ORDER BY priority DESC. Default 0. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.scheduled_at IS 'Deferred delivery: message not dispatched until this time. NULL means dispatch immediately. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.idempotency_key IS 'Application-provided dedup key. Partial unique index prevents duplicate pending/processing messages. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.correlation_id IS 'UUID for distributed tracing. Passed through to dispatch result and response capture. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.expires_at IS 'Message TTL: automatically marked as expired by mark_expired_messages() after this time. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.payload_hash IS 'MD5 hash of payload, computed by send_with_options() for idempotency conflict detection.';
COMMENT ON COLUMN ulak.queue.ordering_key IS 'FIFO ordering guarantee: messages with the same key are delivered strictly in order. Set via send_with_options().';
COMMENT ON COLUMN ulak.queue.headers IS 'Per-message protocol headers (JSONB). Merged with endpoint-level headers. Used by HTTP (request headers) and Kafka (record headers).';
COMMENT ON COLUMN ulak.queue.metadata IS 'Application-defined metadata (JSONB). Not sent to target — stored for auditing, filtering, or operational queries.';
COMMENT ON COLUMN ulak.queue.response IS 'Dispatch response captured when ulak.capture_response GUC is enabled. Contains status, headers, body (protocol-specific).';
COMMENT ON COLUMN ulak.queue.created_at IS 'Message creation timestamp. Used for archive partitioning and ordering within same priority level.';
COMMENT ON COLUMN ulak.queue.updated_at IS 'Last modification timestamp. Updated on every status change by the worker.';

-- Storage tuning for queue table:
-- fillfactor=70 leaves 30% free space per page for in-place HOT updates on status changes.
-- Aggressive autovacuum prevents bloat from frequent status UPDATEs.
ALTER TABLE ulak.queue SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.02
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Per-status partial indexes replace the composite idx_queue_endpoint_status(endpoint_id, status).
-- Removing status from index KEYs increases HOT update likelihood for status transitions
-- and reduces index size (each partial index contains only matching rows).
CREATE INDEX idx_queue_endpoint_pending ON ulak.queue(endpoint_id)
WHERE status = 'pending';

CREATE INDEX idx_queue_endpoint_processing ON ulak.queue(endpoint_id)
WHERE status = 'processing';

CREATE INDEX idx_queue_endpoint_terminal ON ulak.queue(endpoint_id)
WHERE status IN ('completed', 'failed');

CREATE INDEX idx_queue_next_retry ON ulak.queue(next_retry_at)
WHERE next_retry_at IS NOT NULL;

CREATE INDEX idx_queue_created_at ON ulak.queue(created_at);

CREATE INDEX idx_queue_processing ON ulak.queue(processing_started_at)
WHERE status = 'processing';

-- Optimized composite index matching worker fetch ORDER BY (priority DESC, endpoint_id, created_at ASC)
CREATE INDEX idx_queue_worker_fetch ON ulak.queue(priority DESC, endpoint_id, created_at ASC)
WHERE status = 'pending';

-- Index for cleanup queries
CREATE INDEX idx_queue_cleanup ON ulak.queue(created_at)
WHERE status IN ('completed', 'failed', 'expired');

-- Enabled endpoints index
CREATE INDEX idx_queue_endpoints_enabled ON ulak.endpoints(enabled)
WHERE enabled = true;

-- Partial unique index for idempotency deduplication
-- Only enforces uniqueness for active (pending/processing) messages with a key set
CREATE UNIQUE INDEX idx_queue_idempotency_key ON ulak.queue(idempotency_key)
WHERE idempotency_key IS NOT NULL AND status IN ('pending', 'processing');

-- Ordering key indexes: worker fetch checks NOT EXISTS on processing messages
CREATE INDEX idx_queue_ordering_processing ON ulak.queue(ordering_key)
WHERE ordering_key IS NOT NULL AND status = 'processing';

CREATE INDEX idx_queue_ordering_pending ON ulak.queue(ordering_key, created_at ASC)
WHERE ordering_key IS NOT NULL AND status = 'pending';

-- ============================================================================
-- DEAD LETTER QUEUE TABLE
-- ============================================================================

CREATE TABLE ulak.dlq (
    id bigserial PRIMARY KEY,
    original_message_id bigint NOT NULL,
    endpoint_id bigint NOT NULL REFERENCES ulak.endpoints(id) ON DELETE RESTRICT,
    endpoint_name text,
    protocol text,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'failed',
    retry_count int NOT NULL DEFAULT 0,
    last_error text,
    priority smallint NOT NULL DEFAULT 0,
    scheduled_at timestamptz,
    idempotency_key text,
    correlation_id uuid,
    expires_at timestamptz,
    payload_hash text,
    ordering_key text,
    headers jsonb,
    metadata jsonb,
    response jsonb,
    original_created_at timestamptz NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT NOW(),
    archived_at timestamptz DEFAULT NOW()
) WITH (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

-- Dead Letter Queue indexes
CREATE INDEX idx_dlq_endpoint_id ON ulak.dlq(endpoint_id);
CREATE INDEX idx_dlq_archived_at ON ulak.dlq(archived_at);
-- DB-006: Unique index to prevent duplicate DLQ entries for same message
CREATE UNIQUE INDEX idx_dlq_original_message_unique ON ulak.dlq(original_message_id);
CREATE INDEX idx_dlq_original_created_at ON ulak.dlq(original_created_at);
CREATE INDEX idx_dlq_failed_at ON ulak.dlq(failed_at);
-- Composite index for redrive_endpoint() which filters on endpoint_id + status
CREATE INDEX idx_dlq_endpoint_failed_created ON ulak.dlq(endpoint_id, original_created_at)
WHERE status = 'failed';

-- ============================================================================
-- QUEUE ARCHIVE TABLE (Partitioned by created_at)
-- ============================================================================

-- Archive table for completed/failed/expired messages (monthly partitioned)
CREATE TABLE ulak.archive (
    id bigint NOT NULL,
    endpoint_id bigint NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'expired')),
    retry_count int NOT NULL DEFAULT 0,
    next_retry_at timestamptz,
    last_error text,
    processing_started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    -- Additional fields
    priority smallint NOT NULL DEFAULT 0,
    scheduled_at timestamptz,
    idempotency_key text,
    correlation_id uuid,
    expires_at timestamptz,
    payload_hash text,
    ordering_key text,
    headers jsonb,
    metadata jsonb,
    response jsonb,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    archived_at timestamptz DEFAULT NOW()
) PARTITION BY RANGE (created_at);

-- Primary key (includes partition key)
ALTER TABLE ulak.archive
    ADD CONSTRAINT archive_pkey PRIMARY KEY (id, created_at);

-- Index for UI queries by endpoint
CREATE INDEX idx_archive_endpoint_created
    ON ulak.archive(endpoint_id, created_at DESC);

-- Index for status-based queries
CREATE INDEX idx_archive_status_created
    ON ulak.archive(status, created_at DESC);

-- Index for archived_at queries
CREATE INDEX idx_archive_archived_at
    ON ulak.archive(archived_at);

-- BRIN index for time-range scans on append-only archive (~48x smaller than B-tree)
-- Benchmarked with 500K rows across 10 monthly partitions:
--   B-tree: 11.5MB, BRIN: 240KB — nearly identical range query performance
--   pages_per_range=64: best balance for 1month/3month range queries
CREATE INDEX idx_archive_created_brin
    ON ulak.archive USING BRIN (created_at)
    WITH (pages_per_range = 64, autosummarize = on);

-- Default partition (safety net for inserts without matching partition)
CREATE TABLE IF NOT EXISTS ulak.archive_default
    PARTITION OF ulak.archive DEFAULT;

-- Create initial monthly partitions (current month + 3 months ahead)
DO $$
DECLARE
    v_start date;
    v_end date;
    v_name text;
BEGIN
    FOR i IN 0..3 LOOP
        v_start := date_trunc('month', CURRENT_DATE)::date + (i || ' months')::interval;
        v_end := v_start + '1 month'::interval;
        v_name := 'archive_' || to_char(v_start, 'YYYY_MM');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ulak' AND c.relname = v_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE ulak.%I PARTITION OF ulak.archive
                 FOR VALUES FROM (%L) TO (%L)',
                v_name, v_start, v_end
            );
        END IF;
    END LOOP;
END
$$;

-- ============================================================================
-- EVENT LOG TABLE
-- ============================================================================

CREATE TABLE ulak.event_log (
    id bigserial PRIMARY KEY,
    event_type text NOT NULL,
    entity_type text NOT NULL,
    entity_id bigint NOT NULL,
    old_value jsonb,
    new_value jsonb,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_log_created ON ulak.event_log(created_at);
CREATE INDEX idx_event_log_entity ON ulak.event_log(entity_type, entity_id, created_at DESC);
CREATE INDEX idx_event_log_event_type ON ulak.event_log(event_type, created_at DESC);

-- ============================================================================
-- EVENT TYPES (Fan-out / Pub-Sub)
-- ============================================================================

CREATE TABLE ulak.event_types (
    id bigserial PRIMARY KEY,
    name text UNIQUE NOT NULL,
    description text,
    schema jsonb,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW()
);

COMMENT ON TABLE ulak.event_types IS
'Event type definitions for pub/sub fan-out.
Each event_type can have multiple subscriptions to different endpoints.
Use publish() to send an event to all subscribed endpoints.';

-- ============================================================================
-- SUBSCRIPTIONS (Fan-out / Pub-Sub)
-- ============================================================================

CREATE TABLE ulak.subscriptions (
    id bigserial PRIMARY KEY,
    event_type_id bigint NOT NULL REFERENCES ulak.event_types(id) ON DELETE CASCADE,
    endpoint_id bigint NOT NULL REFERENCES ulak.endpoints(id) ON DELETE CASCADE,
    filter jsonb,
    enabled boolean NOT NULL DEFAULT true,
    description text,
    created_at timestamptz DEFAULT NOW(),
    updated_at timestamptz DEFAULT NOW(),
    UNIQUE (event_type_id, endpoint_id)
);

CREATE INDEX idx_subscriptions_event_type ON ulak.subscriptions(event_type_id) WHERE enabled = true;
CREATE INDEX idx_subscriptions_endpoint ON ulak.subscriptions(endpoint_id);

COMMENT ON TABLE ulak.subscriptions IS
'Maps event types to endpoints for fan-out delivery.
When publish() is called, messages are created for all active subscriptions.
Filter uses JSONB containment (@>) for payload matching.';

-- ============================================================================
-- ROLE-BASED ACCESS CONTROL (RBAC)
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ulak_admin') THEN
        CREATE ROLE ulak_admin NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ulak_application') THEN
        CREATE ROLE ulak_application NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ulak_monitor') THEN
        CREATE ROLE ulak_monitor NOLOGIN;
    END IF;
END
$$;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

REVOKE ALL ON SCHEMA ulak FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ulak FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA ulak FROM PUBLIC;
-- NOTE: REVOKE on functions is done AFTER function creation (see below)
-- because PostgreSQL defaults to GRANT EXECUTE TO PUBLIC for new functions.

GRANT USAGE ON SCHEMA ulak TO ulak_admin, ulak_application, ulak_monitor;

-- Admin role: Full access
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.endpoints TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.queue TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.dlq TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.archive TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.endpoints_id_seq TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.queue_id_seq TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.dlq_id_seq TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.event_log TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.event_log_id_seq TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.event_types TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.event_types_id_seq TO ulak_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ulak.subscriptions TO ulak_admin;
GRANT USAGE ON SEQUENCE ulak.subscriptions_id_seq TO ulak_admin;

-- Application role: Send messages and read queue
GRANT SELECT ON ulak.endpoints TO ulak_application;
GRANT SELECT ON ulak.queue TO ulak_application;
-- NOTE: No INSERT on ulak.queue — applications must use the send()/publish()
-- SECURITY DEFINER API which enforces endpoint validation and backpressure.
GRANT SELECT ON ulak.dlq TO ulak_application;
GRANT SELECT ON ulak.archive TO ulak_application;
GRANT SELECT ON ulak.event_log TO ulak_application;
GRANT SELECT ON ulak.event_types TO ulak_application;
GRANT SELECT ON ulak.subscriptions TO ulak_application;

-- Monitor role: Read-only access
GRANT SELECT ON ulak.endpoints TO ulak_monitor;
GRANT SELECT ON ulak.queue TO ulak_monitor;
GRANT SELECT ON ulak.dlq TO ulak_monitor;
GRANT SELECT ON ulak.archive TO ulak_monitor;
GRANT SELECT ON ulak.event_log TO ulak_monitor;
GRANT SELECT ON ulak.event_types TO ulak_monitor;
GRANT SELECT ON ulak.subscriptions TO ulak_monitor;

-- ============================================================================
-- TRIGGER FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_endpoints_updated_at
    BEFORE UPDATE ON ulak.endpoints
    FOR EACH ROW EXECUTE FUNCTION ulak.update_updated_at();

CREATE TRIGGER update_queue_updated_at
    BEFORE UPDATE ON ulak.queue
    FOR EACH ROW EXECUTE FUNCTION ulak.update_updated_at();

CREATE TRIGGER update_event_types_updated_at
    BEFORE UPDATE ON ulak.event_types
    FOR EACH ROW EXECUTE FUNCTION ulak.update_updated_at();

CREATE TRIGGER update_subscriptions_updated_at
    BEFORE UPDATE ON ulak.subscriptions
    FOR EACH ROW EXECUTE FUNCTION ulak.update_updated_at();

-- Notify trigger for new messages
-- Uses current_setting to allow suppression during batch operations
CREATE OR REPLACE FUNCTION ulak.notify_new_message()
RETURNS TRIGGER AS $$
BEGIN
    -- Skip NOTIFY if suppressed (e.g., during bulk COPY or send_batch)
    IF current_setting('ulak.suppress_notify', true) = 'on' THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('ulak_new_msg', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_new_message_trigger
    AFTER INSERT ON ulak.queue
    FOR EACH STATEMENT EXECUTE FUNCTION ulak.notify_new_message();

-- ============================================================================
-- PAYLOAD IMMUTABILITY (TRD-01: Security Hardening)
-- ============================================================================

-- Prevent modification of payload and headers after message creation.
-- Status, retry_count, last_error etc. can still be updated by workers.
-- This ensures message integrity for audit/compliance.
CREATE OR REPLACE FUNCTION ulak.prevent_payload_modification()
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
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enforce_payload_immutability
    BEFORE UPDATE ON ulak.queue
    FOR EACH ROW EXECUTE FUNCTION ulak.prevent_payload_modification();

-- ============================================================================
-- SQL FUNCTIONS (called by worker via SPI)
-- ============================================================================

-- NOTE: update_circuit_breaker and mark_expired_messages are defined below
-- with full implementations.

-- ============================================================================
-- C FUNCTIONS (from shared library)
-- ============================================================================

-- Main send function
CREATE OR REPLACE FUNCTION ulak.send(
    endpoint_name text,
    payload jsonb
) RETURNS boolean
LANGUAGE c
VOLATILE STRICT
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS 'ulak', 'ulak_send';

COMMENT ON FUNCTION ulak.send(text, jsonb) IS
'Send a message to the specified endpoint for asynchronous delivery.
Example: SELECT ulak.send(''webhook'', ''{"event": "user.created"}'');';

-- Endpoint management functions
-- Note: C implementation (ulak_create_endpoint) accepts 3 args: name, protocol, config
CREATE OR REPLACE FUNCTION ulak.create_endpoint(
    name text,
    protocol text,
    config jsonb
) RETURNS bigint
LANGUAGE c
VOLATILE
AS 'ulak', 'ulak_create_endpoint';

CREATE OR REPLACE FUNCTION ulak.drop_endpoint(
    endpoint_name text
) RETURNS boolean
LANGUAGE c
VOLATILE STRICT
AS 'ulak', 'ulak_drop_endpoint';

-- Note: C implementation (ulak_alter_endpoint) accepts 2 args: name, new_config
CREATE OR REPLACE FUNCTION ulak.alter_endpoint(
    endpoint_name text,
    new_config jsonb
) RETURNS boolean
LANGUAGE c
VOLATILE
AS 'ulak', 'ulak_alter_endpoint';

CREATE OR REPLACE FUNCTION ulak.validate_endpoint_config(
    protocol text,
    config jsonb
) RETURNS boolean
LANGUAGE c
STABLE STRICT
AS 'ulak', 'validate_endpoint_config_sql';

-- NOTE: enable_endpoint and disable_endpoint are defined below
-- with full implementations including event logging.

-- ============================================================================
-- MONITORING FUNCTIONS (C implementations)
-- ============================================================================

-- Worker Status Function (returns status of all active workers)
CREATE OR REPLACE FUNCTION ulak.get_worker_status()
RETURNS TABLE (
    pid INTEGER,
    state TEXT,
    started_at TIMESTAMPTZ,
    messages_processed BIGINT,
    last_activity TIMESTAMPTZ,
    error_count INTEGER,
    last_error TEXT
)
LANGUAGE c STABLE
AS 'ulak', 'ulak_worker_status';

COMMENT ON FUNCTION ulak.get_worker_status() IS
'Returns one row per configured worker slot from shared memory.
Shows PID/state plus per-worker counters for registered ulak workers.';


-- Health Check Function (returns overall system health)
CREATE OR REPLACE FUNCTION ulak.health_check()
RETURNS TABLE (
    component TEXT,
    status TEXT,
    details TEXT,
    checked_at TIMESTAMPTZ
)
LANGUAGE c STABLE
AS 'ulak', 'ulak_health_check';

COMMENT ON FUNCTION ulak.health_check() IS
'Performs a health check of all ulak components.
Returns status (healthy, degraded, unhealthy) for shared_memory and workers.';

-- Shared Memory Metrics (C implementation — internal, called by metrics())
CREATE OR REPLACE FUNCTION ulak._shmem_metrics()
RETURNS TABLE (
    metric_name text,
    metric_value double precision,
    labels jsonb,
    metric_type text
)
LANGUAGE c STABLE
AS 'ulak', 'ulak_shmem_metrics';

COMMENT ON FUNCTION ulak._shmem_metrics() IS
'Internal: returns per-worker and global counters from shared memory.
Called by ulak.metrics() to combine with SQL-derived metrics.';

-- Unified Metrics Function (PL/pgSQL wrapper)
CREATE OR REPLACE FUNCTION ulak.metrics()
RETURNS TABLE (
    metric_name text,
    metric_value double precision,
    labels jsonb,
    metric_type text
)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    -- Queue depth by status (global)
    RETURN QUERY
    SELECT 'queue_depth'::text,
           count(*)::double precision,
           jsonb_build_object('status', q.status),
           'gauge'::text
    FROM ulak.queue q
    GROUP BY q.status;

    -- Queue depth by endpoint and status
    RETURN QUERY
    SELECT 'queue_depth_by_endpoint'::text,
           count(*)::double precision,
           jsonb_build_object('endpoint', e.name, 'status', q.status),
           'gauge'::text
    FROM ulak.queue q
    JOIN ulak.endpoints e ON e.id = q.endpoint_id
    GROUP BY e.name, q.status;

    -- Oldest message age (pending and processing)
    RETURN QUERY
    SELECT 'oldest_message_age_seconds'::text,
           EXTRACT(EPOCH FROM (now() - min(q.created_at)))::double precision,
           jsonb_build_object('status', q.status),
           'gauge'::text
    FROM ulak.queue q
    WHERE q.status IN ('pending', 'processing')
    GROUP BY q.status;

    -- DLQ depth by endpoint
    RETURN QUERY
    SELECT 'dlq_depth'::text,
           count(*)::double precision,
           jsonb_build_object('endpoint', d.endpoint_name, 'status', d.status),
           'gauge'::text
    FROM ulak.dlq d
    GROUP BY d.endpoint_name, d.status;

    -- Endpoint circuit breaker state (0=closed, 1=half_open, 2=open)
    RETURN QUERY
    SELECT 'endpoint_circuit_state'::text,
           CASE e.circuit_state
               WHEN 'closed' THEN 0
               WHEN 'half_open' THEN 1
               WHEN 'open' THEN 2
           END::double precision,
           jsonb_build_object('endpoint', e.name),
           'gauge'::text
    FROM ulak.endpoints e;

    -- Shared memory metrics (worker counters, spawns, errors)
    RETURN QUERY
    SELECT * FROM ulak._shmem_metrics();
END;
$$;

COMMENT ON FUNCTION ulak.metrics() IS
'Unified metrics surface returning all key metrics as key-value rows.
Each row has metric_name, metric_value, labels (JSONB), and metric_type (gauge/counter).
Designed for Prometheus/Grafana integration via sql_exporter or similar.';

-- ============================================================================
-- DLQ HELPER FUNCTIONS
-- ============================================================================

-- Archive a single failed message to DLQ and remove from queue
-- Called by worker when max retries exceeded
CREATE OR REPLACE FUNCTION ulak.archive_single_to_dlq(
    p_message_id bigint
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_archived boolean := false;
BEGIN
    -- Insert into DLQ from queue with full endpoint info via JOIN
    INSERT INTO ulak.dlq (
        original_message_id, endpoint_id, endpoint_name, protocol,
        payload, retry_count, last_error,
        priority, scheduled_at, idempotency_key, correlation_id, expires_at,
        payload_hash, ordering_key, headers, metadata, response,
        original_created_at, failed_at
    )
    SELECT
        q.id, q.endpoint_id, e.name, e.protocol,
        q.payload, q.retry_count, q.last_error,
        q.priority, q.scheduled_at, q.idempotency_key, q.correlation_id, q.expires_at,
        q.payload_hash, q.ordering_key, q.headers, q.metadata, q.response,
        q.created_at, COALESCE(q.failed_at, NOW())
    FROM ulak.queue q
    JOIN ulak.endpoints e ON e.id = q.endpoint_id
    WHERE q.id = p_message_id
    ON CONFLICT (original_message_id) DO NOTHING;

    -- Check if insert succeeded
    IF FOUND THEN
        -- Delete from queue only if DLQ insert was successful
        DELETE FROM ulak.queue WHERE id = p_message_id;
        v_archived := true;
        RAISE LOG '[ulak] Message % archived to DLQ', p_message_id;
    ELSE
        RAISE WARNING '[ulak] Message % already in DLQ or not found', p_message_id;
    END IF;

    RETURN v_archived;
END;
$$;

COMMENT ON FUNCTION ulak.archive_single_to_dlq(bigint) IS
'Archives a failed message from queue to dead letter queue.
Removes the message from queue after successful archival.
Returns true if archived, false if message not found or already in DLQ.';

-- ============================================================================
-- ENHANCED SEND FUNCTION WITH OPTIONS
-- ============================================================================

-- Extended send function with priority, scheduling, idempotency, correlation, and TTL
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

COMMENT ON FUNCTION ulak.send_with_options(text, jsonb, smallint, timestamptz, text, uuid, timestamptz, text) IS
'Extended send function with additional options:
- priority: 0-10 (higher = processed first)
- scheduled_at: delay delivery until this time
- idempotency_key: unique key for deduplication
- correlation_id: UUID for distributed tracing
- expires_at: TTL - message skipped if expired';

-- ============================================================================
-- HIGH-THROUGHPUT BATCH SEND
-- ============================================================================

-- Bulk send multiple messages in a single INSERT (unnest pattern)
-- Achieves 10-15x higher throughput than individual send() calls
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

    -- Suppress trigger NOTIFY during bulk insert (we do our own at the end)
    PERFORM set_config('ulak.suppress_notify', 'on', true);

    -- Bulk insert all payloads in a single INSERT using unnest
    WITH inserted AS (
        INSERT INTO ulak.queue (endpoint_id, payload, status, next_retry_at, created_at, updated_at)
        SELECT v_endpoint_id, p, 'pending', NOW(), NOW(), NOW()
        FROM unnest(p_payloads) AS p
        RETURNING id
    )
    SELECT array_agg(id) INTO v_ids FROM inserted;

    -- Re-enable trigger NOTIFY and send single notification for batch
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    PERFORM pg_notify('ulak_new_msg', '');

    RETURN v_ids;
END;
$$;

COMMENT ON FUNCTION ulak.send_batch(text, jsonb[]) IS
'High-throughput batch send: inserts multiple messages in a single SQL statement.
Uses unnest pattern for 10-15x higher throughput than individual send() calls.
Single NOTIFY is sent for the entire batch.
Example: SELECT ulak.send_batch(''webhook'', ARRAY[''{...}''::jsonb, ''{...}''::jsonb])';

-- Batch send with priority support
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
    PERFORM pg_notify('ulak_new_msg', '');

    RETURN v_ids;
END;
$$;

COMMENT ON FUNCTION ulak.send_batch_with_priority(text, jsonb[], smallint) IS
'Batch send with priority support. All messages in the batch get the same priority.';

-- ============================================================================
-- ENDPOINT MANAGEMENT FUNCTIONS
-- ============================================================================

-- Enable an endpoint (resume message processing)
CREATE OR REPLACE FUNCTION ulak.enable_endpoint(
    p_endpoint_name text
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_endpoint_id bigint;
    v_was_enabled boolean;
BEGIN
    SELECT id, enabled INTO v_endpoint_id, v_was_enabled
    FROM ulak.endpoints WHERE name = p_endpoint_name;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist', p_endpoint_name;
    END IF;

    UPDATE ulak.endpoints
    SET enabled = true, updated_at = NOW()
    WHERE id = v_endpoint_id;

    -- Log state change event
    IF NOT v_was_enabled THEN
        INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
        VALUES ('endpoint.enabled', 'endpoint', v_endpoint_id,
                jsonb_build_object('enabled', false),
                jsonb_build_object('enabled', true));
    END IF;

    RETURN true;
END;
$$;

COMMENT ON FUNCTION ulak.enable_endpoint(text) IS
'Enable an endpoint to resume message processing.';

-- Disable an endpoint (pause message processing)
CREATE OR REPLACE FUNCTION ulak.disable_endpoint(
    p_endpoint_name text
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_endpoint_id bigint;
    v_was_enabled boolean;
BEGIN
    SELECT id, enabled INTO v_endpoint_id, v_was_enabled
    FROM ulak.endpoints WHERE name = p_endpoint_name;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist', p_endpoint_name;
    END IF;

    UPDATE ulak.endpoints
    SET enabled = false, updated_at = NOW()
    WHERE id = v_endpoint_id;

    -- Log state change event
    IF v_was_enabled THEN
        INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
        VALUES ('endpoint.disabled', 'endpoint', v_endpoint_id,
                jsonb_build_object('enabled', true),
                jsonb_build_object('enabled', false));
    END IF;

    RETURN true;
END;
$$;

COMMENT ON FUNCTION ulak.disable_endpoint(text) IS
'Disable an endpoint to pause message processing.
Pending messages will wait until endpoint is re-enabled.';

-- Reset circuit breaker for an endpoint
CREATE OR REPLACE FUNCTION ulak.reset_circuit_breaker(
    p_endpoint_name text
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_endpoint_id bigint;
    v_old_state text;
BEGIN
    SELECT id, circuit_state INTO v_endpoint_id, v_old_state
    FROM ulak.endpoints WHERE name = p_endpoint_name;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint ''%'' does not exist', p_endpoint_name;
    END IF;

    UPDATE ulak.endpoints
    SET circuit_state = 'closed',
        circuit_failure_count = 0,
        circuit_opened_at = NULL,
        circuit_half_open_at = NULL,
        updated_at = NOW()
    WHERE id = v_endpoint_id;

    -- Log circuit breaker reset event
    INSERT INTO ulak.event_log (event_type, entity_type, entity_id, old_value, new_value)
    VALUES ('circuit.reset', 'endpoint', v_endpoint_id,
            jsonb_build_object('state', v_old_state),
            jsonb_build_object('state', 'closed'));

    RETURN true;
END;
$$;

COMMENT ON FUNCTION ulak.reset_circuit_breaker(text) IS
'Reset circuit breaker state for an endpoint to closed state.
Clears failure count and allows message processing to resume normally.';

-- Get endpoint health information
CREATE OR REPLACE FUNCTION ulak.get_endpoint_health(
    p_endpoint_name text DEFAULT NULL
) RETURNS TABLE (
    endpoint_name text,
    protocol text,
    enabled boolean,
    circuit_state text,
    circuit_failure_count integer,
    last_success_at timestamptz,
    last_failure_at timestamptz,
    pending_messages bigint,
    oldest_pending_message timestamptz
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.name::text,
        e.protocol::text,
        e.enabled,
        e.circuit_state::text,
        e.circuit_failure_count,
        e.last_success_at,
        e.last_failure_at,
        COUNT(q.id)::bigint AS pending_messages,
        MIN(q.created_at) AS oldest_pending_message
    FROM ulak.endpoints e
    LEFT JOIN ulak.queue q ON e.id = q.endpoint_id AND q.status = 'pending'
    WHERE (p_endpoint_name IS NULL OR e.name = p_endpoint_name)
    GROUP BY e.id, e.name, e.protocol, e.enabled, e.circuit_state,
             e.circuit_failure_count, e.last_success_at, e.last_failure_at
    ORDER BY e.name;
END;
$$;

COMMENT ON FUNCTION ulak.get_endpoint_health(text) IS
'Get health information for endpoints including circuit breaker state,
failure counts, and pending message statistics.
If endpoint_name is NULL, returns health for all endpoints.';

-- ============================================================================
-- INTERNAL CIRCUIT BREAKER UPDATE FUNCTION
-- ============================================================================

-- Update circuit breaker state based on dispatch result (called by worker)
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
    v_threshold := current_setting('ulak.circuit_breaker_threshold', true)::int;
    IF v_threshold IS NULL THEN v_threshold := 5; END IF;

    v_cooldown := current_setting('ulak.circuit_breaker_cooldown', true)::int;
    IF v_cooldown IS NULL THEN v_cooldown := 60; END IF;

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

COMMENT ON FUNCTION ulak.update_circuit_breaker(bigint, boolean) IS
'Internal function to update circuit breaker state after message dispatch.
Called by the background worker after each dispatch attempt.';

-- ============================================================================
-- EXPIRED MESSAGE HANDLING
-- ============================================================================

-- Mark expired messages (called periodically by worker)
CREATE OR REPLACE FUNCTION ulak.mark_expired_messages()
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_count integer;
BEGIN
    UPDATE ulak.queue
    SET
        status = 'expired',
        updated_at = NOW()
    WHERE status = 'pending'
      AND expires_at IS NOT NULL
      AND expires_at < NOW();

    GET DIAGNOSTICS v_count = ROW_COUNT;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Marked % messages as expired', v_count;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.mark_expired_messages() IS
'Mark messages with expired TTL as expired status.
Called periodically by the background worker.';

-- ============================================================================
-- ARCHIVE PARTITION MAINTENANCE
-- ============================================================================

-- Maintain archive partitions (pre-create monthly partitions ahead of time)
CREATE OR REPLACE FUNCTION ulak.maintain_archive_partitions(
    p_months_ahead integer DEFAULT 3
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, ulak
AS $$
DECLARE
    v_start date;
    v_end date;
    v_partition_name text;
    v_created integer := 0;
BEGIN
    FOR i IN 0..p_months_ahead LOOP
        v_start := date_trunc('month', CURRENT_DATE)::date + (i || ' months')::interval;
        v_end := v_start + '1 month'::interval;
        v_partition_name := 'archive_' || to_char(v_start, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'ulak' AND c.relname = v_partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE ulak.%I PARTITION OF ulak.archive
                 FOR VALUES FROM (%L) TO (%L)',
                v_partition_name, v_start, v_end
            );
            v_created := v_created + 1;
        END IF;
    END LOOP;

    RETURN v_created;
END;
$$;

COMMENT ON FUNCTION ulak.maintain_archive_partitions(integer) IS
'Pre-create monthly archive partitions ahead of time.
Should be called periodically from maintenance tasks or pg_cron.
Returns the number of partitions created.';

-- ============================================================================
-- EVENT LOG CLEANUP
-- ============================================================================

-- Cleanup old event log entries based on retention GUC
CREATE OR REPLACE FUNCTION ulak.cleanup_event_log()
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_retention_days integer;
    v_count integer;
BEGIN
    v_retention_days := current_setting('ulak.event_log_retention_days', true)::int;
    IF v_retention_days IS NULL THEN v_retention_days := 30; END IF;

    DELETE FROM ulak.event_log
    WHERE created_at < NOW() - (v_retention_days || ' days')::interval;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Cleaned up % event log entries older than % days', v_count, v_retention_days;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.cleanup_event_log() IS
'Delete event log entries older than ulak.event_log_retention_days.
Returns the number of entries deleted.';

-- ============================================================================
-- DLQ CLEANUP
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak.cleanup_dlq()
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_retention_days integer;
    v_count integer;
BEGIN
    v_retention_days := current_setting('ulak.dlq_retention_days', true)::int;
    IF v_retention_days IS NULL OR v_retention_days <= 0 THEN v_retention_days := 30; END IF;

    DELETE FROM ulak.dlq
    WHERE archived_at < NOW() - (v_retention_days || ' days')::interval;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Cleaned up % DLQ messages older than % days', v_count, v_retention_days;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.cleanup_dlq() IS
'Delete DLQ messages older than ulak.dlq_retention_days (default 30).
Returns the number of entries deleted. Called automatically by worker maintenance.';

-- ============================================================================
-- QUEUE BACKPRESSURE
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak._check_backpressure(
    p_projected_additional bigint DEFAULT 0
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_max_size integer;
    v_threshold_offset bigint;
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

    v_threshold_offset := v_max_size - GREATEST(v_projected, 1);

    PERFORM 1
    FROM ulak.queue
    WHERE status IN ('pending', 'processing')
    OFFSET v_threshold_offset
    LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION '[ulak] Queue backpressure: pending/processing queue at current workload would exceed limit % with % projected additional messages. Increase ulak.max_queue_size or wait for messages to be processed.',
            v_max_size, v_projected
        USING ERRCODE = '53400'; -- configuration_limit_exceeded
    END IF;
END;
$$;

COMMENT ON FUNCTION ulak._check_backpressure(bigint) IS
'Internal: Check queue size against ulak.max_queue_size limit, including optional projected additional rows.
Uses a threshold probe instead of COUNT(*) to detect when the current queue or projected total would exceed the limit.
Raises exception with SQLSTATE 53400 if the current queue or the projected total would exceed the limit.
Set max_queue_size=0 to disable.';

-- ============================================================================
-- HIGH-THROUGHPUT MODE
-- ============================================================================

-- Enable high-throughput mode for the current session
-- Disables synchronous_commit for ~2-3x faster writes at the cost of
-- up to 3× WAL segment duration (default ~600ms) of data loss on crash.
-- Safe for ulak pattern because messages are idempotent.
CREATE OR REPLACE FUNCTION ulak.enable_fast_mode()
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    SET LOCAL synchronous_commit = off;
END;
$$;

COMMENT ON FUNCTION ulak.enable_fast_mode() IS
'Enable high-throughput mode by disabling synchronous_commit.
Provides ~2-3x faster writes. Safe for ulak pattern (idempotent messages).
Effect is transaction-scoped.';

-- ============================================================================
-- PUB/SUB: EVENT TYPE MANAGEMENT
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak.create_event_type(
    p_name text,
    p_description text DEFAULT NULL,
    p_schema jsonb DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    v_id bigint;
BEGIN
    INSERT INTO ulak.event_types (name, description, schema)
    VALUES (p_name, p_description, p_schema)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.drop_event_type(
    p_name text
) RETURNS boolean
LANGUAGE plpgsql VOLATILE STRICT AS $$
DECLARE
    v_id bigint;
BEGIN
    DELETE FROM ulak.event_types WHERE name = p_name RETURNING id INTO v_id;
    IF v_id IS NULL THEN
        RAISE EXCEPTION 'Event type ''%'' does not exist', p_name;
    END IF;
    RETURN true;
END;
$$;

-- ============================================================================
-- PUB/SUB: SUBSCRIPTION MANAGEMENT
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak.subscribe(
    p_event_type text,
    p_endpoint_name text,
    p_filter jsonb DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql VOLATILE AS $$
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

    INSERT INTO ulak.subscriptions (event_type_id, endpoint_id, filter)
    VALUES (v_event_type_id, v_endpoint_id, p_filter)
    RETURNING id INTO v_sub_id;

    RETURN v_sub_id;
END;
$$;

CREATE OR REPLACE FUNCTION ulak.unsubscribe(
    p_subscription_id bigint
) RETURNS boolean
LANGUAGE plpgsql VOLATILE STRICT AS $$
DECLARE
    v_deleted boolean;
BEGIN
    DELETE FROM ulak.subscriptions WHERE id = p_subscription_id
    RETURNING true INTO v_deleted;
    IF v_deleted IS NULL THEN
        RAISE EXCEPTION 'Subscription ID % does not exist', p_subscription_id;
    END IF;
    RETURN true;
END;
$$;

-- ============================================================================
-- PUB/SUB: PUBLISH (FAN-OUT)
-- ============================================================================

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

    -- Re-enable NOTIFY and send single notification for entire fan-out
    PERFORM set_config('ulak.suppress_notify', 'off', true);
    IF v_count > 0 THEN
        PERFORM pg_notify('ulak_new_msg', '');
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
        PERFORM pg_notify('ulak_new_msg', '');
    END IF;

    RETURN v_total;
END;
$$;

-- ============================================================================
-- ARCHIVE COMPLETED MESSAGES
-- ============================================================================

-- Move completed messages from queue to archive table
-- Called periodically by the background worker to prevent queue table bloat
CREATE OR REPLACE FUNCTION ulak.archive_completed_messages(
    p_older_than_seconds integer DEFAULT 3600,
    p_batch_size integer DEFAULT 1000
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_count integer := 0;
    v_batch integer;
    v_cutoff timestamptz;
BEGIN
    v_cutoff := NOW() - (p_older_than_seconds || ' seconds')::interval;

    -- Move completed messages in batches to avoid long locks
    LOOP
        WITH moved AS (
            DELETE FROM ulak.queue
            WHERE id IN (
                SELECT id FROM ulak.queue
                WHERE (
                    status = 'completed'
                    AND completed_at IS NOT NULL
                    AND completed_at < v_cutoff
                ) OR (
                    status = 'expired'
                    AND updated_at IS NOT NULL
                    AND updated_at < v_cutoff
                )
                LIMIT p_batch_size
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, endpoint_id, payload, status, retry_count,
                      next_retry_at, last_error, processing_started_at,
                      completed_at, failed_at, priority, scheduled_at,
                      idempotency_key, correlation_id, expires_at,
                      payload_hash, ordering_key, headers, metadata, response,
                      created_at, updated_at
        )
        INSERT INTO ulak.archive (
            id, endpoint_id, payload, status, retry_count,
            next_retry_at, last_error, processing_started_at,
            completed_at, failed_at, priority, scheduled_at,
            idempotency_key, correlation_id, expires_at,
            payload_hash, ordering_key, headers, metadata, response,
            created_at, updated_at, archived_at
        )
        SELECT
            id, endpoint_id, payload, status, retry_count,
            next_retry_at, last_error, processing_started_at,
            completed_at, failed_at, priority, scheduled_at,
            idempotency_key, correlation_id, expires_at,
            payload_hash, ordering_key, headers, metadata, response,
            created_at, updated_at, NOW()
        FROM moved;

        GET DIAGNOSTICS v_batch = ROW_COUNT;
        v_count := v_count + v_batch;

        -- Exit when no more rows to archive
        EXIT WHEN v_batch = 0;
    END LOOP;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Archived % completed/expired messages from queue', v_count;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.archive_completed_messages(integer, integer) IS
'Move completed and expired messages from queue to archive table.
Called periodically by the background worker to prevent queue table bloat.
Parameters:
- p_older_than_seconds: archive completed messages by completed_at and expired messages by updated_at once older than N seconds (default 3600)
- p_batch_size: max messages to move per batch iteration (default 1000)
Returns the total number of messages archived.';

-- ============================================================================
-- DROP OLD ARCHIVE PARTITIONS
-- ============================================================================

-- Drop archive partitions older than retention period
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
                EXECUTE format('DROP TABLE IF EXISTS ulak.%I', v_partition.relname);
                v_dropped := v_dropped + 1;
                RAISE LOG '[ulak] Dropped old archive partition: %', v_partition.relname;
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
-- MESSAGE REPLAY (Archive → Queue)
-- ============================================================================

-- Replay a single message from archive back to queue
CREATE OR REPLACE FUNCTION ulak.replay_message(
    p_archive_message_id bigint
) RETURNS bigint
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_new_id bigint;
BEGIN
    INSERT INTO ulak.queue (
        endpoint_id, payload, status, priority,
        scheduled_at, idempotency_key, correlation_id, expires_at,
        payload_hash, ordering_key, headers, metadata
    )
    SELECT
        endpoint_id, payload, 'pending', priority,
        NULL, NULL, correlation_id, NULL,
        payload_hash, ordering_key, headers, metadata
    FROM ulak.archive
    WHERE id = p_archive_message_id
    RETURNING id INTO v_new_id;

    IF v_new_id IS NULL THEN
        RAISE EXCEPTION 'Message % not found in archive', p_archive_message_id;
    END IF;

    -- Log the replay event
    INSERT INTO ulak.event_log (event_type, entity_type, entity_id, metadata)
    VALUES ('replay', 'message', v_new_id,
            jsonb_build_object('original_id', p_archive_message_id, 'source', 'archive'));

    RAISE LOG '[ulak] Replayed archive message % as new message %',
               p_archive_message_id, v_new_id;

    RETURN v_new_id;
END;
$$;

COMMENT ON FUNCTION ulak.replay_message(bigint) IS
'Replay a single message from archive back to queue as a new pending message.
Idempotency key and expires_at are cleared to avoid conflicts.
Returns the new message ID.';

-- Replay messages from archive by endpoint and time range
CREATE OR REPLACE FUNCTION ulak.replay_range(
    p_endpoint_id bigint,
    p_from_ts timestamptz,
    p_to_ts timestamptz,
    p_status text DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_count integer := 0;
    v_batch integer;
BEGIN
    WITH replayed AS (
        INSERT INTO ulak.queue (
            endpoint_id, payload, status, priority,
            scheduled_at, idempotency_key, correlation_id, expires_at,
            payload_hash, ordering_key, headers, metadata
        )
        SELECT
            a.endpoint_id, a.payload, 'pending', a.priority,
            NULL, NULL, a.correlation_id, NULL,
            a.payload_hash, a.ordering_key, a.headers, a.metadata
        FROM ulak.archive a
        WHERE a.endpoint_id = p_endpoint_id
          AND a.created_at >= p_from_ts
          AND a.created_at <= p_to_ts
          AND (p_status IS NULL OR a.status = p_status)
        RETURNING id
    )
    SELECT count(*) INTO v_count FROM replayed;

    IF v_count > 0 THEN
        -- Log the bulk replay event
        INSERT INTO ulak.event_log (event_type, entity_type, entity_id, metadata)
        VALUES ('replay_range', 'endpoint', p_endpoint_id,
                jsonb_build_object(
                    'count', v_count,
                    'from', p_from_ts,
                    'to', p_to_ts,
                    'status_filter', p_status
                ));

        RAISE LOG '[ulak] Replayed % messages for endpoint % (% to %)',
                   v_count, p_endpoint_id, p_from_ts, p_to_ts;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.replay_range(bigint, timestamptz, timestamptz, text) IS
'Replay archived messages for an endpoint within a time range.
Optional status filter (completed, failed, expired).
Returns the number of messages replayed.';

-- ============================================================================
-- DLQ REDRIVE (DLQ → Queue)
-- ============================================================================

-- Redrive a single message from DLQ back to queue
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
BEGIN
    -- Get DLQ message info
    SELECT original_message_id, endpoint_name
    INTO v_original_id, v_endpoint_name
    FROM ulak.dlq WHERE id = p_dlq_id;

    IF v_original_id IS NULL THEN
        RAISE EXCEPTION 'DLQ message % not found', p_dlq_id;
    END IF;

    -- Insert back into queue as pending with reset retry count
    INSERT INTO ulak.queue (
        endpoint_id, payload, status, retry_count, priority,
        scheduled_at, idempotency_key, correlation_id, expires_at,
        payload_hash, headers, metadata
    )
    SELECT
        endpoint_id, payload, 'pending', 0, priority,
        NULL, NULL, correlation_id, NULL,
        payload_hash, headers, metadata
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
Retry count is reset to 0. DLQ entry is marked as redriven (not deleted).
Returns the new message ID.';

-- Redrive all DLQ messages for a specific endpoint
CREATE OR REPLACE FUNCTION ulak.redrive_endpoint(
    p_endpoint_name text
) RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_count integer := 0;
    v_endpoint_id bigint;
    rec RECORD;
BEGIN
    -- Resolve endpoint
    SELECT id INTO v_endpoint_id
    FROM ulak.endpoints WHERE name = p_endpoint_name;

    IF v_endpoint_id IS NULL THEN
        RAISE EXCEPTION 'Endpoint "%" not found', p_endpoint_name;
    END IF;

    -- Redrive each DLQ message individually (preserves per-message logging)
    FOR rec IN
        SELECT id FROM ulak.dlq
        WHERE endpoint_id = v_endpoint_id AND status = 'failed'
        ORDER BY original_created_at ASC
    LOOP
        PERFORM ulak.redrive_message(rec.id);
        v_count := v_count + 1;
    END LOOP;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Redrove % DLQ messages for endpoint "%"',
                   v_count, p_endpoint_name;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.redrive_endpoint(text) IS
'Redrive all failed DLQ messages for a specific endpoint back to queue.
Messages are redriven in original creation order.
Returns the number of messages redriven.';

-- Redrive all failed DLQ messages across all endpoints
CREATE OR REPLACE FUNCTION ulak.redrive_all()
RETURNS integer
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_count integer := 0;
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT id FROM ulak.dlq
        WHERE status = 'failed'
        ORDER BY original_created_at ASC
    LOOP
        PERFORM ulak.redrive_message(rec.id);
        v_count := v_count + 1;
    END LOOP;

    IF v_count > 0 THEN
        RAISE LOG '[ulak] Redrove % DLQ messages across all endpoints', v_count;
    END IF;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ulak.redrive_all() IS
'Redrive all failed DLQ messages across all endpoints.
Returns the total number of messages redriven.';

-- ============================================================================
-- DLQ SUMMARY VIEW
-- ============================================================================

CREATE OR REPLACE FUNCTION ulak.dlq_summary()
RETURNS TABLE (
    endpoint_name text,
    failed_count bigint,
    redriven_count bigint,
    oldest_failed_at timestamptz,
    newest_failed_at timestamptz
)
LANGUAGE sql STABLE
AS $$
    SELECT
        d.endpoint_name,
        count(*) FILTER (WHERE d.status = 'failed') AS failed_count,
        count(*) FILTER (WHERE d.status = 'redriven') AS redriven_count,
        min(d.failed_at) FILTER (WHERE d.status = 'failed') AS oldest_failed_at,
        max(d.failed_at) FILTER (WHERE d.status = 'failed') AS newest_failed_at
    FROM ulak.dlq d
    GROUP BY d.endpoint_name
    ORDER BY failed_count DESC;
$$;

COMMENT ON FUNCTION ulak.dlq_summary() IS
'Summary of DLQ messages grouped by endpoint.
Shows failed and redriven counts with time range.';

-- Revoke default PUBLIC execute on all functions.
-- This MUST come AFTER all CREATE FUNCTION statements because PostgreSQL
-- automatically grants EXECUTE TO PUBLIC on new functions.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ulak FROM PUBLIC;

-- ============================================================================
-- FUNCTION PERMISSIONS (consolidated)
-- NOTE: ulak follows a single-tenant model. The application role has SELECT
-- access to queue, DLQ, archive, and event tables across all endpoints.
-- Multi-tenant isolation, if needed, should be implemented via Row-Level
-- Security (RLS) policies at the deployment level.
-- ============================================================================

-- Admin functions: endpoint management
GRANT EXECUTE ON FUNCTION ulak.create_endpoint(text, text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.drop_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.alter_endpoint(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.validate_endpoint_config(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.enable_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.disable_endpoint(text) TO ulak_admin;

-- Admin functions: circuit breaker and maintenance
GRANT EXECUTE ON FUNCTION ulak.update_circuit_breaker(bigint, boolean) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.reset_circuit_breaker(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.mark_expired_messages() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.maintain_archive_partitions(integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_event_log() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_dlq() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak._check_backpressure(bigint) TO ulak_admin;
-- NOTE: _check_backpressure is an internal function called by SECURITY DEFINER
-- send/publish APIs. No direct GRANT to ulak_application needed.
GRANT EXECUTE ON FUNCTION ulak.archive_single_to_dlq(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.archive_completed_messages(integer, integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.cleanup_old_archive_partitions(integer) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.replay_message(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.replay_range(bigint, timestamptz, timestamptz, text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_message(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_endpoint(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.redrive_all() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.dlq_summary() TO ulak_admin;

-- Admin functions: pub/sub management
GRANT EXECUTE ON FUNCTION ulak.create_event_type(text, text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.drop_event_type(text) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.subscribe(text, text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.unsubscribe(bigint) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.publish(text, jsonb) TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.publish_batch(jsonb) TO ulak_admin;

-- Admin functions: monitoring
GRANT EXECUTE ON FUNCTION ulak.get_worker_status() TO ulak_admin;
GRANT EXECUTE ON FUNCTION ulak.health_check() TO ulak_admin;

-- Application functions
GRANT EXECUTE ON FUNCTION ulak.send(text, jsonb) TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.send_with_options(text, jsonb, smallint, timestamptz, text, uuid, timestamptz, text) TO ulak_application;
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
GRANT EXECUTE ON FUNCTION ulak._shmem_metrics() TO ulak_application;
GRANT EXECUTE ON FUNCTION ulak.metrics() TO ulak_application;
