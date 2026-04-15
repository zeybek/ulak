-- 13_ordering_key.sql: Message ordering via ordering_key
-- pg_regress test for ulak

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint('ord_ep', 'http',
    '{"url": "http://localhost:9999/ord"}'::jsonb) IS NOT NULL AS ep_created;

CREATE TEMP TABLE ord_ep AS
SELECT id FROM ulak.endpoints WHERE name = 'ord_ep';

-- ============================================================================
-- SEND WITH ORDERING KEY
-- ============================================================================

SELECT ulak.send_with_options(
    'ord_ep', '{"seq": 1}'::jsonb,
    p_ordering_key => 'order-123'
) IS NOT NULL AS msg1;

SELECT ulak.send_with_options(
    'ord_ep', '{"seq": 2}'::jsonb,
    p_ordering_key => 'order-123'
) IS NOT NULL AS msg2;

SELECT ulak.send_with_options(
    'ord_ep', '{"seq": 3}'::jsonb,
    p_ordering_key => 'order-456'
) IS NOT NULL AS msg3;

-- NULL ordering_key (no ordering constraint)
SELECT ulak.send_with_options(
    'ord_ep', '{"seq": 4}'::jsonb
) IS NOT NULL AS msg4_nokey;

-- Verify ordering_key stored correctly
SELECT ordering_key, payload->>'seq' AS seq
FROM ulak.queue
WHERE endpoint_id = (SELECT id FROM ord_ep)
ORDER BY id;

-- ============================================================================
-- ORDERING ENFORCEMENT: processing blocks same key
-- ============================================================================

-- Simulate: set first order-123 message to 'processing'
UPDATE ulak.queue
SET status = 'processing', processing_started_at = NOW()
WHERE id = (
    SELECT id FROM ulak.queue
    WHERE ordering_key = 'order-123' AND status = 'pending'
    ORDER BY created_at ASC LIMIT 1
);

-- Query like worker does: should NOT return order-123 (already processing)
-- Should return: order-456 (different key) + NULL key message
SELECT ordering_key, payload->>'seq' AS seq
FROM ulak.queue q
WHERE q.status = 'pending'
  AND (q.ordering_key IS NULL
       OR NOT EXISTS (
           SELECT 1 FROM ulak.queue q2
           WHERE q2.ordering_key = q.ordering_key
             AND q2.status = 'processing'
             AND q2.id != q.id
       ))
ORDER BY q.created_at;

-- ============================================================================
-- DIFFERENT KEYS ARE INDEPENDENT
-- ============================================================================

-- order-456 and NULL key should both be fetchable
SELECT count(*) AS fetchable_count
FROM ulak.queue q
WHERE q.status = 'pending'
  AND q.endpoint_id = (SELECT id FROM ord_ep)
  AND (q.ordering_key IS NULL
       OR NOT EXISTS (
           SELECT 1 FROM ulak.queue q2
           WHERE q2.ordering_key = q.ordering_key
             AND q2.status = 'processing'
             AND q2.id != q.id
       ));

-- ============================================================================
-- COMPLETED STATUS DOES NOT BLOCK
-- ============================================================================

-- Mark the processing message as completed
UPDATE ulak.queue
SET status = 'completed', completed_at = NOW()
WHERE ordering_key = 'order-123' AND status = 'processing';

-- Now the second order-123 message should be fetchable
SELECT ordering_key, payload->>'seq' AS seq
FROM ulak.queue q
WHERE q.status = 'pending'
  AND q.endpoint_id = (SELECT id FROM ord_ep)
  AND (q.ordering_key IS NULL
       OR NOT EXISTS (
           SELECT 1 FROM ulak.queue q2
           WHERE q2.ordering_key = q.ordering_key
             AND q2.status = 'processing'
             AND q2.id != q.id
       ))
ORDER BY q.ordering_key NULLS LAST, q.created_at;

-- ============================================================================
-- FAILED STATUS DOES NOT BLOCK
-- ============================================================================

-- Set second order-123 to failed
UPDATE ulak.queue
SET status = 'failed', failed_at = NOW()
WHERE ordering_key = 'order-123' AND status = 'pending';

-- No more order-123 pending, so nothing to block
-- Remaining: order-456 + NULL
SELECT ordering_key, payload->>'seq' AS seq
FROM ulak.queue q
WHERE q.status = 'pending'
  AND q.endpoint_id = (SELECT id FROM ord_ep)
ORDER BY q.created_at;

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ord_ep);
DROP TABLE ord_ep;
SELECT ulak.drop_endpoint('ord_ep');
