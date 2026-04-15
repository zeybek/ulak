-- 11_event_types.sql: Event types and subscriptions CRUD
-- pg_regress test for ulak pub/sub

-- ============================================================================
-- EVENT TYPE: CREATE
-- ============================================================================

-- Create with name only
SELECT ulak.create_event_type('order.created') IS NOT NULL AS created;

-- Create with description
SELECT ulak.create_event_type('order.shipped', 'Order was shipped') IS NOT NULL AS created;

-- Create with schema
SELECT ulak.create_event_type('payment.completed', 'Payment done', '{"type": "object"}'::jsonb) IS NOT NULL AS created;

-- Verify all exist
SELECT name, description, schema IS NOT NULL AS has_schema
FROM ulak.event_types ORDER BY name;

-- ============================================================================
-- EVENT TYPE: DUPLICATE REJECTED
-- ============================================================================

DO $$ BEGIN
    PERFORM ulak.create_event_type('order.created');
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'duplicate event type rejected';
END $$;

-- ============================================================================
-- EVENT TYPE: DROP
-- ============================================================================

SELECT ulak.drop_event_type('payment.completed');

-- Verify dropped
SELECT name FROM ulak.event_types ORDER BY name;

-- Drop non-existent raises error
DO $$ BEGIN
    PERFORM ulak.drop_event_type('nonexistent.event');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'drop nonexistent rejected';
END $$;

-- ============================================================================
-- SUBSCRIPTION: SETUP ENDPOINTS
-- ============================================================================

SELECT ulak.create_endpoint('sub_ep_a', 'http',
    '{"url": "http://localhost:9999/a"}'::jsonb) IS NOT NULL AS ep_a;
SELECT ulak.create_endpoint('sub_ep_b', 'http',
    '{"url": "http://localhost:9999/b"}'::jsonb) IS NOT NULL AS ep_b;
SELECT ulak.create_endpoint('sub_ep_c', 'http',
    '{"url": "http://localhost:9999/c"}'::jsonb) IS NOT NULL AS ep_c;

-- ============================================================================
-- SUBSCRIPTION: CREATE
-- ============================================================================

SELECT ulak.subscribe('order.created', 'sub_ep_a') IS NOT NULL AS sub_a;
SELECT ulak.subscribe('order.created', 'sub_ep_b') IS NOT NULL AS sub_b;
SELECT ulak.subscribe('order.shipped', 'sub_ep_c') IS NOT NULL AS sub_c;

-- Verify subscriptions
SELECT et.name AS event_type, e.name AS endpoint, s.enabled
FROM ulak.subscriptions s
JOIN ulak.event_types et ON et.id = s.event_type_id
JOIN ulak.endpoints e ON e.id = s.endpoint_id
ORDER BY et.name, e.name;

-- ============================================================================
-- SUBSCRIPTION: DUPLICATE REJECTED
-- ============================================================================

DO $$ BEGIN
    PERFORM ulak.subscribe('order.created', 'sub_ep_a');
EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'duplicate subscription rejected';
END $$;

-- ============================================================================
-- SUBSCRIPTION: INVALID REFERENCES
-- ============================================================================

-- Non-existent event type
DO $$ BEGIN
    PERFORM ulak.subscribe('nonexistent.event', 'sub_ep_a');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'subscribe to nonexistent event type rejected';
END $$;

-- Non-existent endpoint
DO $$ BEGIN
    PERFORM ulak.subscribe('order.created', 'nonexistent_ep');
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'subscribe to nonexistent endpoint rejected';
END $$;

-- ============================================================================
-- SUBSCRIPTION: WITH FILTER
-- ============================================================================

SELECT ulak.subscribe('order.created', 'sub_ep_c', '{"vip": true}'::jsonb) IS NOT NULL AS filtered_sub;

SELECT et.name AS event_type, e.name AS endpoint, s.filter IS NOT NULL AS has_filter
FROM ulak.subscriptions s
JOIN ulak.event_types et ON et.id = s.event_type_id
JOIN ulak.endpoints e ON e.id = s.endpoint_id
ORDER BY et.name, e.name;

-- ============================================================================
-- UNSUBSCRIBE
-- ============================================================================

-- Get the filtered subscription ID
CREATE TEMP TABLE unsub_test AS
SELECT s.id FROM ulak.subscriptions s
JOIN ulak.endpoints e ON e.id = s.endpoint_id
JOIN ulak.event_types et ON et.id = s.event_type_id
WHERE e.name = 'sub_ep_c' AND et.name = 'order.created';

SELECT ulak.unsubscribe((SELECT id FROM unsub_test));

DROP TABLE unsub_test;

-- Non-existent subscription raises error
DO $$ BEGIN
    PERFORM ulak.unsubscribe(999999);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'unsubscribe nonexistent rejected';
END $$;

-- ============================================================================
-- CASCADE DELETE: DROP EVENT TYPE REMOVES SUBSCRIPTIONS
-- ============================================================================

SELECT count(*) AS subs_before FROM ulak.subscriptions;

SELECT ulak.drop_event_type('order.shipped');

SELECT count(*) AS subs_after FROM ulak.subscriptions;

-- ============================================================================
-- CASCADE DELETE: DROP ENDPOINT REMOVES SUBSCRIPTIONS
-- ============================================================================

-- sub_ep_a has a subscription to order.created
SELECT count(*) AS subs_before_ep_drop FROM ulak.subscriptions;

SELECT ulak.drop_endpoint('sub_ep_a');

SELECT count(*) AS subs_after_ep_drop FROM ulak.subscriptions;

-- ============================================================================
-- CLEANUP
-- ============================================================================

DELETE FROM ulak.queue;
DELETE FROM ulak.subscriptions;
DELETE FROM ulak.event_types;
SELECT ulak.drop_endpoint('sub_ep_b');
SELECT ulak.drop_endpoint('sub_ep_c');
