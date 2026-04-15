-- ============================================================================
-- Test: Unified metrics() function
-- ============================================================================

-- 1. _shmem_metrics() returns rows without error
SELECT count(*) > 0 AS has_shmem_metrics FROM ulak._shmem_metrics();

-- 2. Verify _shmem_metrics column structure
SELECT metric_name, metric_type
FROM ulak._shmem_metrics()
WHERE labels = '{}'::jsonb
ORDER BY metric_name;

-- 3. metrics() returns rows without error
SELECT count(*) > 0 AS has_metrics FROM ulak.metrics();

-- 4. Verify expected metric names from shmem
SELECT DISTINCT metric_name
FROM ulak.metrics()
WHERE metric_name IN ('messages_processed_total', 'errors_total',
                       'spawns_total', 'spawn_failures_total', 'restarts_total')
ORDER BY metric_name;

-- 5. Create endpoint and queue messages to verify SQL-sourced metrics
-- Use direct INSERT to avoid non-deterministic endpoint ID in elog(INFO) output
INSERT INTO ulak.endpoints (name, protocol, config, enabled)
VALUES ('metrics_test_ep', 'http',
        '{"url": "http://localhost:9999/test"}'::jsonb, true);

INSERT INTO ulak.queue (endpoint_id, payload, status)
SELECT e.id, '{"test": true}'::jsonb, 'pending'
FROM ulak.endpoints e WHERE e.name = 'metrics_test_ep';

INSERT INTO ulak.queue (endpoint_id, payload, status)
SELECT e.id, '{"test": true}'::jsonb, 'pending'
FROM ulak.endpoints e WHERE e.name = 'metrics_test_ep';

-- 6. Verify queue_depth metric reflects inserted messages
SELECT metric_name, metric_value, labels->>'status' AS status
FROM ulak.metrics()
WHERE metric_name = 'queue_depth' AND labels->>'status' = 'pending';

-- 7. Verify queue_depth_by_endpoint metric
SELECT metric_name, metric_value, labels->>'endpoint' AS endpoint
FROM ulak.metrics()
WHERE metric_name = 'queue_depth_by_endpoint'
  AND labels->>'endpoint' = 'metrics_test_ep'
  AND labels->>'status' = 'pending';

-- 8. Verify endpoint_circuit_state metric
SELECT metric_name, metric_value, labels->>'endpoint' AS endpoint
FROM ulak.metrics()
WHERE metric_name = 'endpoint_circuit_state'
  AND labels->>'endpoint' = 'metrics_test_ep';

-- 9. Verify metric_type values are valid
SELECT DISTINCT metric_type FROM ulak.metrics() ORDER BY metric_type;

-- 10. Verify all labels are valid JSONB
SELECT count(*) AS invalid_labels
FROM ulak.metrics()
WHERE labels IS NULL;

-- Cleanup
DELETE FROM ulak.queue
WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'metrics_test_ep');
DELETE FROM ulak.endpoints WHERE name = 'metrics_test_ep';
