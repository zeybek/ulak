-- 29_queue_health.sql: queue_health() unified health snapshot
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint(
    'health_test_ep',
    'http',
    '{"url": "http://localhost:9999/health", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- ============================================================================
-- EMPTY QUEUE HEALTH
-- ============================================================================

SELECT total_pending = 0 AS empty_pending,
       total_processing = 0 AS empty_processing,
       oldest_pending_age_seconds IS NULL AS no_pending_age,
       oldest_processing_age_seconds IS NULL AS no_processing_age,
       endpoints_with_open_circuit = 0 AS no_open_circuits,
       dlq_depth = 0 AS empty_dlq,
       archive_default_rows = 0 AS empty_archive_default
FROM ulak.queue_health();

-- ============================================================================
-- WITH PENDING MESSAGES
-- ============================================================================

SELECT ulak.send('health_test_ep', '{"health": 1}'::jsonb);
SELECT ulak.send('health_test_ep', '{"health": 2}'::jsonb);

SELECT total_pending = 2 AS two_pending,
       total_processing = 0 AS zero_processing
FROM ulak.queue_health();

-- ============================================================================
-- WITH PROCESSING MESSAGES
-- ============================================================================

UPDATE ulak.queue SET status = 'processing', processing_started_at = NOW()
WHERE id = (
    SELECT id FROM ulak.queue
    WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'health_test_ep')
      AND status = 'pending'
    ORDER BY id LIMIT 1
);

SELECT total_pending = 1 AS one_pending,
       total_processing = 1 AS one_processing,
       oldest_processing_age_seconds IS NOT NULL AS has_processing_age
FROM ulak.queue_health();

-- ============================================================================
-- WITH OPEN CIRCUIT BREAKER
-- ============================================================================

UPDATE ulak.endpoints SET circuit_state = 'open' WHERE name = 'health_test_ep';

SELECT endpoints_with_open_circuit = 1 AS one_open_circuit
FROM ulak.queue_health();

-- Reset circuit
UPDATE ulak.endpoints SET circuit_state = 'closed' WHERE name = 'health_test_ep';

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'health_test_ep');
SELECT ulak.drop_endpoint('health_test_ep');
