-- 15_production_hardening.sql: DLQ cleanup, backpressure function, autovacuum (TRD-06)
-- Note: Backpressure edge cases (at limit, above limit) tested via e2e with ALTER SYSTEM.
-- GUC max_queue_size is PGC_SIGHUP so cannot be SET in regression tests.

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint('ph_ep', 'http',
    '{"url": "http://localhost:9999/ph"}'::jsonb) IS NOT NULL AS ep_created;

CREATE TEMP TABLE ph_ids AS
SELECT id AS endpoint_id FROM ulak.endpoints WHERE name = 'ph_ep';

-- ============================================================================
-- CLEANUP_DLQ: EMPTY TABLE RETURNS 0
-- ============================================================================

SELECT ulak.cleanup_dlq() AS dlq_cleanup_empty;

-- ============================================================================
-- CLEANUP_DLQ: RETENTION BOUNDARY (default 30 days)
-- ============================================================================

-- 3 recent entries (within retention)
INSERT INTO ulak.dlq (
    original_message_id, endpoint_id, endpoint_name, protocol,
    payload, retry_count, original_created_at, failed_at, archived_at
)
SELECT
    1000 + s, (SELECT endpoint_id FROM ph_ids), 'ph_ep', 'http',
    format('{"dlq_recent": %s}', s)::jsonb, 3,
    NOW() - interval '1 day', NOW() - interval '1 day',
    NOW() - (s || ' days')::interval
FROM generate_series(1, 3) AS s;

-- 2 old entries (beyond 30d retention)
INSERT INTO ulak.dlq (
    original_message_id, endpoint_id, endpoint_name, protocol,
    payload, retry_count, original_created_at, failed_at, archived_at
)
SELECT
    2000 + s, (SELECT endpoint_id FROM ph_ids), 'ph_ep', 'http',
    format('{"dlq_old": %s}', s)::jsonb, 5,
    NOW() - interval '90 days', NOW() - interval '90 days',
    NOW() - ((30 + s) || ' days')::interval
FROM generate_series(1, 2) AS s;

SELECT count(*) AS dlq_before FROM ulak.dlq
WHERE endpoint_id = (SELECT endpoint_id FROM ph_ids);

SELECT ulak.cleanup_dlq() AS dlq_cleaned;

SELECT count(*) AS dlq_after FROM ulak.dlq
WHERE endpoint_id = (SELECT endpoint_id FROM ph_ids);

DELETE FROM ulak.dlq WHERE endpoint_id = (SELECT endpoint_id FROM ph_ids);

-- ============================================================================
-- _CHECK_BACKPRESSURE: FUNCTION EXISTS AND WORKS (default limit 1M, queue empty)
-- ============================================================================

-- With default max_queue_size=1000000 and empty queue, should pass
SELECT ulak._check_backpressure();

-- Large projected insert count should be rejected even when queue is currently empty
DO $$
BEGIN
    PERFORM ulak._check_backpressure(1000001);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'projected backpressure: rejected';
END $$;

-- Verify function exists with correct signature
SELECT proname, pronargs FROM pg_proc
WHERE proname = '_check_backpressure'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'ulak');

-- ============================================================================
-- DLQ AUTOVACUUM SETTINGS
-- ============================================================================

SELECT
    (reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0.05']) AS dlq_vacuum_sf,
    (reloptions @> ARRAY['autovacuum_analyze_scale_factor=0.05']) AS dlq_analyze_sf
FROM pg_class WHERE oid = 'ulak.dlq'::regclass;

-- ============================================================================
-- QUEUE AUTOVACUUM SETTINGS
-- ============================================================================

SELECT
    (reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0.01']) AS queue_vacuum_sf,
    (reloptions @> ARRAY['autovacuum_vacuum_cost_delay=2']) AS queue_cost_delay
FROM pg_class WHERE oid = 'ulak.queue'::regclass;

-- ============================================================================
-- GUC DEFAULTS VERIFICATION
-- ============================================================================

SELECT current_setting('ulak.dlq_retention_days') AS dlq_retention;
SELECT current_setting('ulak.archive_retention_months') AS archive_retention;
SELECT current_setting('ulak.max_queue_size') AS max_queue;

-- ============================================================================
-- MONITORING SURFACE
-- ============================================================================

SELECT count(*) = current_setting('ulak.workers')::int AS worker_row_count
FROM ulak.get_worker_status();

SELECT bool_and(state IN ('running', 'stopped')) AS worker_states_valid
FROM ulak.get_worker_status();

SELECT count(*) = 2 AS health_component_count
FROM ulak.health_check();

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM ph_ids);
DELETE FROM ulak.dlq WHERE endpoint_id = (SELECT endpoint_id FROM ph_ids);
DROP TABLE ph_ids;
SELECT ulak.drop_endpoint('ph_ep');
