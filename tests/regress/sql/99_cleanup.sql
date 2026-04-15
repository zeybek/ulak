-- 99_cleanup.sql: Extension drop and test role cleanup
-- pg_regress test for ulak

-- Clean up any remaining queue entries first (FK constraints)
DELETE FROM ulak.queue;

-- Clean up DLQ entries
DELETE FROM ulak.dlq;

-- Clean up event log entries
DELETE FROM ulak.event_log;

-- Clean up archive entries (includes all partitions)
DELETE FROM ulak.archive;

-- Clean up any remaining endpoints
DELETE FROM ulak.endpoints;

-- Reset SSRF test override
ALTER SYSTEM RESET ulak.http_allow_internal_urls;
SELECT pg_reload_conf();

-- Drop the extension (CASCADE to drop schema and all objects)
DROP EXTENSION ulak CASCADE;

-- Verify extension is removed
SELECT EXISTS(
    SELECT 1 FROM pg_extension WHERE extname = 'ulak'
) AS extension_still_exists;

-- Verify schema is removed
SELECT EXISTS(
    SELECT 1 FROM pg_namespace WHERE nspname = 'ulak'
) AS schema_still_exists;

-- Clean up test role grants (roles themselves survive extension drop)
-- The roles are created by the extension SQL, but DO $$ checks prevent duplicates
-- We can safely drop them if they exist
DO $$
BEGIN
    -- Revoke role memberships from current user
    BEGIN
        EXECUTE format('REVOKE ulak_admin FROM %I', current_user);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    BEGIN
        EXECUTE format('REVOKE ulak_application FROM %I', current_user);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    BEGIN
        EXECUTE format('REVOKE ulak_monitor FROM %I', current_user);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
END
$$;

-- Try to drop the roles (may fail if privileges remain — version-dependent error format)
DO $$
BEGIN
    BEGIN DROP ROLE IF EXISTS ulak_admin; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN DROP ROLE IF EXISTS ulak_application; EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN DROP ROLE IF EXISTS ulak_monitor; EXCEPTION WHEN OTHERS THEN NULL; END;
END
$$;

-- Verify roles status
SELECT count(*)::int AS role_count
FROM pg_roles
WHERE rolname LIKE 'ulak_%';
