-- 01_schema.sql: Schema verification — tables, indexes, functions, triggers, roles
-- pg_regress test for ulak

-- ============================================================================
-- TABLES
-- ============================================================================

-- Verify endpoints table exists with expected columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'ulak' AND table_name = 'endpoints'
ORDER BY ordinal_position;

-- Verify queue table exists with expected columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'ulak' AND table_name = 'queue'
ORDER BY ordinal_position;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Verify indexes exist on queue table
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'ulak' AND tablename = 'queue'
ORDER BY indexname;

-- Verify indexes exist on endpoints table
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'ulak' AND tablename = 'endpoints'
ORDER BY indexname;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Verify all expected functions exist
SELECT p.proname, pg_get_function_arguments(p.oid) AS args
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'ulak'
ORDER BY p.proname;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Verify triggers on endpoints table
SELECT tgname
FROM pg_trigger
WHERE tgrelid = 'ulak.endpoints'::regclass
  AND NOT tgisinternal
ORDER BY tgname;

-- Verify triggers on queue table
SELECT tgname
FROM pg_trigger
WHERE tgrelid = 'ulak.queue'::regclass
  AND NOT tgisinternal
ORDER BY tgname;

-- ============================================================================
-- ROLES
-- ============================================================================

-- Verify RBAC roles exist
SELECT rolname
FROM pg_roles
WHERE rolname LIKE 'ulak_%'
ORDER BY rolname;
