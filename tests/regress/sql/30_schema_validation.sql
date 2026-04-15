-- 30_schema_validation.sql: validate_payload_schema + publish with schema
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- VALIDATE_PAYLOAD_SCHEMA: TYPE CHECKING
-- ============================================================================

-- Valid object
SELECT ulak.validate_payload_schema(
    '{"name": "test"}'::jsonb,
    '{"type": "object"}'::jsonb
) AS object_valid;

-- Invalid: expected object, got array
DO $$
BEGIN
    PERFORM ulak.validate_payload_schema(
        '[1, 2, 3]'::jsonb,
        '{"type": "object"}'::jsonb
    );
    RAISE EXCEPTION 'Should have raised type mismatch';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%expected object%' THEN
        RAISE NOTICE 'type_mismatch_detected: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- Valid array
SELECT ulak.validate_payload_schema(
    '[1, 2, 3]'::jsonb,
    '{"type": "array"}'::jsonb
) AS array_valid;

-- Valid string
SELECT ulak.validate_payload_schema(
    '"hello"'::jsonb,
    '{"type": "string"}'::jsonb
) AS string_valid;

-- Valid number
SELECT ulak.validate_payload_schema(
    '42'::jsonb,
    '{"type": "number"}'::jsonb
) AS number_valid;

-- ============================================================================
-- VALIDATE_PAYLOAD_SCHEMA: REQUIRED FIELDS
-- ============================================================================

-- All required fields present
SELECT ulak.validate_payload_schema(
    '{"name": "test", "age": 25}'::jsonb,
    '{"type": "object", "required": ["name", "age"]}'::jsonb
) AS required_present;

-- Missing required field
DO $$
BEGIN
    PERFORM ulak.validate_payload_schema(
        '{"name": "test"}'::jsonb,
        '{"type": "object", "required": ["name", "age"]}'::jsonb
    );
    RAISE EXCEPTION 'Should have raised missing field';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%required field%age%missing%' THEN
        RAISE NOTICE 'missing_required_detected: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- VALIDATE_PAYLOAD_SCHEMA: PROPERTY TYPES
-- ============================================================================

-- Correct property types
SELECT ulak.validate_payload_schema(
    '{"name": "test", "count": 5, "active": true}'::jsonb,
    '{"type": "object", "properties": {"name": {"type": "string"}, "count": {"type": "number"}, "active": {"type": "boolean"}}}'::jsonb
) AS properties_valid;

-- Wrong property type
DO $$
BEGIN
    PERFORM ulak.validate_payload_schema(
        '{"name": 123}'::jsonb,
        '{"type": "object", "properties": {"name": {"type": "string"}}}'::jsonb
    );
    RAISE EXCEPTION 'Should have raised property type mismatch';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%field%name%expected string%' THEN
        RAISE NOTICE 'property_type_mismatch: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- NULL schema passes anything
SELECT ulak.validate_payload_schema('{"anything": true}'::jsonb, NULL) AS null_schema_passes;

-- ============================================================================
-- PUBLISH WITH SCHEMA VALIDATION
-- ============================================================================

SELECT ulak.create_endpoint(
    'schema_test_ep',
    'http',
    '{"url": "http://localhost:9999/schema", "method": "POST"}'::jsonb
) IS NOT NULL AS ep_created;

-- Create event type WITH schema
SELECT ulak.create_event_type(
    'schema.test',
    'Test event with schema',
    '{"type": "object", "required": ["order_id", "total"], "properties": {"order_id": {"type": "number"}, "total": {"type": "number"}}}'::jsonb
) IS NOT NULL AS event_type_created;

SELECT ulak.subscribe('schema.test', 'schema_test_ep') IS NOT NULL AS subscribed;

-- Valid payload passes schema
SELECT ulak.publish('schema.test', '{"order_id": 1, "total": 99.99}'::jsonb) = 1 AS valid_published;

-- Invalid payload fails schema (missing required field)
DO $$
BEGIN
    PERFORM ulak.publish('schema.test', '{"order_id": 1}'::jsonb);
    RAISE EXCEPTION 'Should have raised schema validation error';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%required field%total%missing%' THEN
        RAISE NOTICE 'publish_schema_validation: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- Invalid payload fails schema (wrong type)
DO $$
BEGIN
    PERFORM ulak.publish('schema.test', '{"order_id": "not_a_number", "total": 99.99}'::jsonb);
    RAISE EXCEPTION 'Should have raised schema validation error';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%expected number%' THEN
        RAISE NOTICE 'publish_type_validation: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- Event type without schema should pass any payload
SELECT ulak.create_event_type('no_schema.test', 'No schema event') IS NOT NULL AS no_schema_created;
SELECT ulak.subscribe('no_schema.test', 'schema_test_ep') IS NOT NULL AS no_schema_subscribed;
SELECT ulak.publish('no_schema.test', '{"anything": "goes"}'::jsonb) = 1 AS no_schema_published;

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'schema_test_ep');
SELECT ulak.unsubscribe((SELECT id FROM ulak.subscriptions WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'schema_test_ep') AND event_type_id = (SELECT id FROM ulak.event_types WHERE name = 'schema.test')));
SELECT ulak.unsubscribe((SELECT id FROM ulak.subscriptions WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'schema_test_ep') AND event_type_id = (SELECT id FROM ulak.event_types WHERE name = 'no_schema.test')));
SELECT ulak.drop_event_type('schema.test');
SELECT ulak.drop_event_type('no_schema.test');
SELECT ulak.drop_endpoint('schema_test_ep');
