-- 27_send_options_v2.sql: send_with_options enhancements — on_conflict, debounce
-- pg_regress test for ulak
SET client_min_messages = warning;

-- ============================================================================
-- SETUP
-- ============================================================================

SELECT ulak.create_endpoint(
    'sov2_test_ep',
    'http',
    '{"url": "http://localhost:9999/sov2", "method": "POST"}'::jsonb
) IS NOT NULL AS created;

-- ============================================================================
-- ON_CONFLICT = 'raise' (default, existing behavior)
-- ============================================================================

-- First send with idempotency key
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "raise_1"}'::jsonb,
    p_idempotency_key => 'raise-test-1'
) IS NOT NULL AS first_send;

-- Same key, same payload hash → should return existing ID silently
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "raise_1"}'::jsonb,
    p_idempotency_key => 'raise-test-1'
) IS NOT NULL AS duplicate_same_payload;

-- Same key, different payload → should raise exception
DO $$
BEGIN
    PERFORM ulak.send_with_options(
        'sov2_test_ep', '{"test": "raise_DIFFERENT"}'::jsonb,
        p_idempotency_key => 'raise-test-1'
    );
    RAISE EXCEPTION 'Should have raised idempotency conflict';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%conflict%' THEN
        RAISE NOTICE 'raise_conflict_detected: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- ON_CONFLICT = 'skip'
-- ============================================================================

-- Send with new key
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "skip_1"}'::jsonb,
    p_idempotency_key => 'skip-test-1'
) IS NOT NULL AS skip_first_send;

-- Same key, different payload, on_conflict='skip' → return existing ID, no error
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "skip_DIFFERENT"}'::jsonb,
    p_idempotency_key => 'skip-test-1',
    p_on_conflict => 'skip'
) IS NOT NULL AS skip_returns_existing;

-- Payload should be unchanged (original)
SELECT payload->>'test' = 'skip_1' AS payload_unchanged
FROM ulak.queue WHERE idempotency_key = 'skip-test-1' AND status = 'pending';

-- ============================================================================
-- ON_CONFLICT = 'replace'
-- ============================================================================

-- Send with new key
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "replace_1"}'::jsonb,
    p_idempotency_key => 'replace-test-1'
) IS NOT NULL AS replace_first_send;

-- Same key, different payload, on_conflict='replace' → update payload
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "replace_UPDATED"}'::jsonb,
    p_idempotency_key => 'replace-test-1',
    p_on_conflict => 'replace'
) IS NOT NULL AS replace_returns_id;

-- Payload should be updated
SELECT payload->>'test' = 'replace_UPDATED' AS payload_replaced
FROM ulak.queue WHERE idempotency_key = 'replace-test-1' AND status = 'pending';

-- ============================================================================
-- ON_CONFLICT VALIDATION
-- ============================================================================

-- Invalid on_conflict value should raise
DO $$
BEGIN
    PERFORM ulak.send_with_options(
        'sov2_test_ep', '{"test": "invalid"}'::jsonb,
        p_on_conflict => 'invalid_value'
    );
    RAISE EXCEPTION 'Should have raised on_conflict validation error';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%on_conflict must be%' THEN
        RAISE NOTICE 'on_conflict_validation: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- DEBOUNCE: within window
-- ============================================================================

-- First send with debounce
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "debounce_1"}'::jsonb,
    p_idempotency_key => 'debounce-test-1',
    p_debounce_seconds => 60
) AS debounce_first_id;

-- Second send within window → should return same ID (skip)
SELECT ulak.send_with_options(
    'sov2_test_ep', '{"test": "debounce_2"}'::jsonb,
    p_idempotency_key => 'debounce-test-1',
    p_debounce_seconds => 60
) AS debounce_second_id;

-- Should still have original payload (window not expired)
SELECT payload->>'test' = 'debounce_1' AS debounce_payload_unchanged
FROM ulak.queue WHERE idempotency_key = 'debounce-test-1' AND status = 'pending';

-- Only one message should exist
SELECT count(*) = 1 AS debounce_single_message
FROM ulak.queue WHERE idempotency_key = 'debounce-test-1' AND status IN ('pending', 'processing');

-- ============================================================================
-- DEBOUNCE: requires idempotency_key
-- ============================================================================

DO $$
BEGIN
    PERFORM ulak.send_with_options(
        'sov2_test_ep', '{"test": "no_key"}'::jsonb,
        p_debounce_seconds => 60
    );
    RAISE EXCEPTION 'Should have raised debounce requires idempotency_key error';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%requires idempotency_key%' THEN
        RAISE NOTICE 'debounce_requires_key: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- PAYLOAD IMMUTABILITY: direct update still blocked
-- ============================================================================

DO $$
DECLARE
    v_id bigint;
BEGIN
    SELECT id INTO v_id FROM ulak.queue
    WHERE idempotency_key = 'replace-test-1' AND status = 'pending';

    UPDATE ulak.queue SET payload = '{"hacked": true}'::jsonb WHERE id = v_id;
    RAISE EXCEPTION 'Should have raised immutability error';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%payload modification not allowed%' THEN
        RAISE NOTICE 'immutability_enforced: true';
    ELSE
        RAISE;
    END IF;
END $$;

-- ============================================================================
-- CLEANUP
-- ============================================================================
RESET client_min_messages;

DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'sov2_test_ep');
SELECT ulak.drop_endpoint('sov2_test_ep');
