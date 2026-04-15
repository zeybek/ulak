-- ulak Complete Uninstallation Script
--
-- This script completely removes ulak from your PostgreSQL cluster,
-- including the extension, schema, and cluster-level roles.
--
-- IMPORTANT:
-- - Run this script as a superuser
-- - If ulak is installed in multiple databases, run DROP EXTENSION
--   in each database first, then drop roles once at the end
-- - Back up any data you need before running this script
--
-- Usage:
--   psql -d your_database -f sql/uninstall_ulak.sql
--
-- Or step by step:
--   1. DROP EXTENSION ulak CASCADE;  -- in each database
--   2. DROP ROLE IF EXISTS ulak_admin;
--   3. DROP ROLE IF EXISTS ulak_application;
--   4. DROP ROLE IF EXISTS ulak_monitor;

\echo '=== ulak Uninstallation Script ==='
\echo ''

-- Step 1: Drop the extension (this removes schema, tables, functions, etc.)
\echo 'Step 1: Dropping extension...'
DROP EXTENSION IF EXISTS ulak CASCADE;

-- Step 2: Drop cluster-level roles
-- Note: Roles are cluster-wide. If ulak is used in other databases,
-- you may want to skip this step or run it only after removing the extension
-- from ALL databases.
\echo 'Step 2: Dropping roles...'

-- Check if roles are still used elsewhere before dropping
DO $$
DECLARE
    role_in_use BOOLEAN;
BEGIN
    -- Check ulak_admin
    SELECT EXISTS (
        SELECT 1 FROM pg_roles r
        JOIN pg_auth_members m ON r.oid = m.member
        WHERE r.rolname = 'ulak_admin'
    ) INTO role_in_use;

    IF role_in_use THEN
        RAISE NOTICE 'ulak_admin has member roles - dropping anyway';
    END IF;
END $$;

DROP ROLE IF EXISTS ulak_admin;
DROP ROLE IF EXISTS ulak_application;
DROP ROLE IF EXISTS ulak_monitor;

\echo ''
\echo '=== Uninstallation Complete ==='
\echo ''
\echo 'Remember to also:'
\echo '  1. Remove ulak from shared_preload_libraries in postgresql.conf'
\echo '  2. Remove any ulak.* GUC settings from postgresql.conf'
\echo '  3. Restart PostgreSQL for changes to take effect'
\echo ''
