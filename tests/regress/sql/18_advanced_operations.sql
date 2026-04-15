-- 18_advanced_operations.sql: FK restrict, batch ops, ordering, payload immutability

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint('adv_ep', 'http',
    '{"url": "http://localhost:9999/adv"}'::jsonb) IS NOT NULL AS ep_created;

CREATE TEMP TABLE adv_ids AS
SELECT id AS endpoint_id FROM ulak.endpoints WHERE name = 'adv_ep';

SET ulak.max_queue_size = 0;

-- ============================================================================
-- FK RESTRICT: ENDPOINT DROP WITH PENDING MESSAGES
-- ============================================================================

SELECT ulak.send('adv_ep', '{"fk": 1}'::jsonb);
SELECT ulak.send('adv_ep', '{"fk": 2}'::jsonb);

DO $$
BEGIN
    PERFORM ulak.drop_endpoint('adv_ep');
    RAISE NOTICE 'ERROR: should have been blocked';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'FK RESTRICT: drop blocked with pending messages';
END $$;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- SEND_BATCH: EMPTY ARRAY
-- ============================================================================

SELECT ulak.send_batch('adv_ep', ARRAY[]::jsonb[]) AS empty_batch;

SELECT count(*) AS after_empty_batch FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- SEND_BATCH: VALID PAYLOADS
-- ============================================================================

SELECT array_length(
    ulak.send_batch('adv_ep', ARRAY[
        '{"batch": 1}'::jsonb, '{"batch": 2}'::jsonb, '{"batch": 3}'::jsonb
    ]), 1
) AS batch_count;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- SEND_BATCH_WITH_PRIORITY
-- ============================================================================

SELECT array_length(
    ulak.send_batch_with_priority('adv_ep',
        ARRAY['{"prio": 1}'::jsonb, '{"prio": 2}'::jsonb], 7::smallint
    ), 1
) AS prio_batch_count;

SELECT DISTINCT priority AS batch_priority FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload->>'prio' IS NOT NULL;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- SEND_BATCH: NONEXISTENT ENDPOINT
-- ============================================================================

DO $$
BEGIN
    PERFORM ulak.send_batch('nonexistent_ep_xyz', ARRAY['{"x":1}'::jsonb]);
    RAISE NOTICE 'ERROR: should have raised';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'send_batch nonexistent: rejected';
END $$;

-- ============================================================================
-- SEND_BATCH: DISABLED ENDPOINT
-- ============================================================================

SELECT ulak.disable_endpoint('adv_ep');

DO $$
BEGIN
    PERFORM ulak.send_batch('adv_ep', ARRAY['{"disabled": 1}'::jsonb]);
    RAISE NOTICE 'ERROR: should have raised';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'send_batch disabled: rejected';
END $$;

SELECT ulak.enable_endpoint('adv_ep');

-- ============================================================================
-- QUEUE ORDERING: PRIORITY DESC, CREATED_AT ASC
-- ============================================================================

INSERT INTO ulak.queue (endpoint_id, payload, status, priority, created_at, updated_at)
VALUES
    ((SELECT endpoint_id FROM adv_ids), '{"ord": "low_old"}'::jsonb, 'pending', 1, NOW()-interval '5 min', NOW()),
    ((SELECT endpoint_id FROM adv_ids), '{"ord": "high_new"}'::jsonb, 'pending', 5, NOW()-interval '1 min', NOW()),
    ((SELECT endpoint_id FROM adv_ids), '{"ord": "high_old"}'::jsonb, 'pending', 5, NOW()-interval '3 min', NOW()),
    ((SELECT endpoint_id FROM adv_ids), '{"ord": "med"}'::jsonb, 'pending', 3, NOW()-interval '2 min', NOW()),
    ((SELECT endpoint_id FROM adv_ids), '{"ord": "low_new"}'::jsonb, 'pending', 1, NOW()-interval '1 min', NOW());

SELECT payload->>'ord' AS msg, priority
FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload ? 'ord'
ORDER BY priority DESC, created_at ASC;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- PAYLOAD IMMUTABILITY TRIGGER
-- ============================================================================

INSERT INTO ulak.queue (endpoint_id, payload, status)
SELECT endpoint_id, '{"immutable": "original"}'::jsonb, 'pending' FROM adv_ids;

-- Payload modification blocked
DO $$
BEGIN
    UPDATE ulak.queue SET payload = '{"immutable": "modified"}'::jsonb
    WHERE payload @> '{"immutable": "original"}'::jsonb;
    RAISE NOTICE 'ERROR: should have blocked';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'payload immutability enforced';
END $$;

-- Headers modification blocked
DO $$
BEGIN
    UPDATE ulak.queue SET headers = '{"X-New": "val"}'::jsonb
    WHERE payload @> '{"immutable": "original"}'::jsonb;
    RAISE NOTICE 'ERROR: should have blocked';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'headers immutability enforced';
END $$;

-- Status update still works
UPDATE ulak.queue SET status = 'processing', processing_started_at = NOW()
WHERE payload @> '{"immutable": "original"}'::jsonb;

SELECT status = 'processing' AS status_updated FROM ulak.queue
WHERE payload @> '{"immutable": "original"}'::jsonb;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- SKIP LOCKED SIMULATION
-- ============================================================================

INSERT INTO ulak.queue (endpoint_id, payload, status)
SELECT (SELECT endpoint_id FROM adv_ids), format('{"sl": %s}', s)::jsonb, 'pending'
FROM generate_series(1, 5) AS s;

DO $$
DECLARE v_id bigint; v_count int;
BEGIN
    SELECT id INTO v_id FROM ulak.queue
    WHERE status = 'pending' AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'adv_ep')
    ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;

    UPDATE ulak.queue SET status = 'processing' WHERE id = v_id;

    SELECT count(*) INTO v_count FROM ulak.queue
    WHERE status = 'pending' AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'adv_ep');

    RAISE NOTICE 'remaining pending: %', v_count;
END $$;

SELECT status, count(*) AS cnt FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload->>'sl' IS NOT NULL
GROUP BY status ORDER BY status;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);

-- ============================================================================
-- ARCHIVE_COMPLETED_MESSAGES
-- ============================================================================

INSERT INTO ulak.queue (endpoint_id, payload, status, completed_at, created_at, updated_at)
SELECT (SELECT endpoint_id FROM adv_ids), format('{"arch": %s}', s)::jsonb, 'completed',
       NOW()-interval '2 hours', NOW()-interval '3 hours', NOW()-interval '2 hours'
FROM generate_series(1, 5) AS s;

-- Recent completed (should NOT be archived)
INSERT INTO ulak.queue (endpoint_id, payload, status, completed_at, created_at, updated_at)
SELECT (SELECT endpoint_id FROM adv_ids), '{"arch": "recent"}'::jsonb, 'completed',
       NOW()-interval '5 min', NOW()-interval '10 min', NOW()-interval '5 min';

-- Old expired (should be archived based on updated_at)
INSERT INTO ulak.queue (endpoint_id, payload, status, expires_at, created_at, updated_at)
SELECT (SELECT endpoint_id FROM adv_ids), '{"arch": "expired_old"}'::jsonb, 'expired',
       NOW()-interval '3 hours', NOW()-interval '4 hours', NOW()-interval '2 hours';

-- Recent expired (should NOT be archived yet)
INSERT INTO ulak.queue (endpoint_id, payload, status, expires_at, created_at, updated_at)
SELECT (SELECT endpoint_id FROM adv_ids), '{"arch": "expired_recent"}'::jsonb, 'expired',
       NOW()-interval '20 min', NOW()-interval '30 min', NOW()-interval '5 min';

SELECT ulak.archive_completed_messages(3600, 100) AS archived;

SELECT count(*) AS recent_in_queue FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload->>'arch' = 'recent';

SELECT count(*) AS recent_expired_in_queue FROM ulak.queue
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload->>'arch' = 'expired_recent';

SELECT count(*) AS old_in_archive FROM ulak.archive
WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids) AND payload->>'arch' IS NOT NULL AND payload->>'arch' != 'recent';

-- ============================================================================
-- PRIORITY VALIDATION
-- ============================================================================

DO $$
BEGIN
    PERFORM ulak.send_with_options('adv_ep', '{"p": 1}'::jsonb, p_priority => 11::smallint);
    RAISE NOTICE 'ERROR: should reject > 10';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'priority > 10: rejected';
END $$;

DO $$
BEGIN
    PERFORM ulak.send_with_options('adv_ep', '{"p": 1}'::jsonb, p_priority => (-1)::smallint);
    RAISE NOTICE 'ERROR: should reject < 0';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'priority < 0: rejected';
END $$;

-- ============================================================================
-- DISABLED ENDPOINT: send_with_options
-- ============================================================================

SELECT ulak.disable_endpoint('adv_ep');

DO $$
BEGIN
    PERFORM ulak.send_with_options('adv_ep', '{"dis": 1}'::jsonb);
    RAISE NOTICE 'ERROR: should reject disabled';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'send_with_options disabled: rejected';
END $$;

SELECT ulak.enable_endpoint('adv_ep');

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);
DELETE FROM ulak.dlq WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);
DELETE FROM ulak.archive WHERE endpoint_id = (SELECT endpoint_id FROM adv_ids);
DELETE FROM ulak.event_log;
DROP TABLE adv_ids;
SELECT ulak.drop_endpoint('adv_ep');
