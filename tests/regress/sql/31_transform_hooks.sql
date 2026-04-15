-- 31_transform_hooks.sql: Payload transformation hooks on subscriptions
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint(
    'transform_test_ep',
    'http',
    '{"url": "http://localhost:9999/transform", "method": "POST"}'::jsonb
) IS NOT NULL AS ep_created;

-- Create a transform function in public schema
CREATE OR REPLACE FUNCTION public.test_transform(p_payload jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    RETURN p_payload || jsonb_build_object('transformed', true, 'source', 'test_transform');
END;
$$;

-- ============================================================================
-- SUBSCRIBE WITH TRANSFORM
-- ============================================================================

SELECT ulak.create_event_type('transform.test', 'Transform test event') IS NOT NULL AS event_type_created;

-- Subscribe with transform function
SELECT ulak.subscribe(
    'transform.test',
    'transform_test_ep',
    p_transform_fn => 'public.test_transform'
) IS NOT NULL AS subscribed_with_transform;

-- Verify transform_fn stored
SELECT transform_fn = 'public.test_transform' AS transform_fn_stored
FROM ulak.subscriptions s
JOIN ulak.event_types et ON s.event_type_id = et.id
WHERE et.name = 'transform.test';

-- ============================================================================
-- PUBLISH WITH TRANSFORM
-- ============================================================================

SELECT ulak.publish('transform.test', '{"order_id": 42}'::jsonb) = 1 AS published;

-- Verify payload was transformed
SELECT
    (payload->>'order_id')::int = 42 AS original_field_preserved,
    payload->>'transformed' = 'true' AS transform_applied,
    payload->>'source' = 'test_transform' AS transform_source_added
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'transform_test_ep'
ORDER BY q.created_at DESC LIMIT 1;

-- ============================================================================
-- SUBSCRIBE WITHOUT TRANSFORM (NULL)
-- ============================================================================

SELECT ulak.create_endpoint(
    'no_transform_ep',
    'http',
    '{"url": "http://localhost:9999/notransform", "method": "POST"}'::jsonb
) IS NOT NULL AS ep2_created;

SELECT ulak.subscribe(
    'transform.test',
    'no_transform_ep'
) IS NOT NULL AS subscribed_no_transform;

-- Publish again — both subscribers get message
SELECT ulak.publish('transform.test', '{"order_id": 99}'::jsonb) = 2 AS published_to_both;

-- transform_test_ep should have transformed payload
SELECT payload ? 'transformed' AS transformed_ep_has_flag
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'transform_test_ep'
ORDER BY q.created_at DESC LIMIT 1;

-- no_transform_ep should have original payload (no transform_fn)
SELECT NOT (payload ? 'transformed') AS plain_ep_no_flag
FROM ulak.queue q
JOIN ulak.endpoints e ON q.endpoint_id = e.id
WHERE e.name = 'no_transform_ep'
ORDER BY q.created_at DESC LIMIT 1;

-- ============================================================================
-- SUBSCRIBE WITH NONEXISTENT TRANSFORM FUNCTION
-- ============================================================================

DO $$
BEGIN
    PERFORM ulak.subscribe(
        'transform.test',
        'no_transform_ep',
        p_transform_fn => 'nonexistent_schema.nonexistent_func'
    );
    RAISE EXCEPTION 'Should have raised transform function not found';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%does not exist%' THEN
        RAISE NOTICE 'nonexistent_transform_error: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id IN (
    SELECT id FROM ulak.endpoints WHERE name IN ('transform_test_ep', 'no_transform_ep')
);
DELETE FROM ulak.subscriptions WHERE event_type_id = (SELECT id FROM ulak.event_types WHERE name = 'transform.test');
SELECT ulak.drop_event_type('transform.test');
SELECT ulak.drop_endpoint('transform_test_ep');
SELECT ulak.drop_endpoint('no_transform_ep');
DROP FUNCTION IF EXISTS public.test_transform(jsonb);
