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

-- Name the members of each role: dropping a role removes their membership
DO $$
DECLARE
    v_role text;
    v_members text;
BEGIN
    FOREACH v_role IN ARRAY ARRAY['ulak_admin', 'ulak_application', 'ulak_monitor'] LOOP
        SELECT string_agg(quote_ident(m.rolname), ', ' ORDER BY m.rolname) INTO v_members
        FROM pg_auth_members am
        JOIN pg_roles r ON r.oid = am.roleid
        JOIN pg_roles m ON m.oid = am.member
        WHERE r.rolname = v_role;

        IF v_members IS NOT NULL THEN
            RAISE NOTICE 'role % has members (%); dropping it removes their membership', v_role, v_members;
        END IF;
    END LOOP;
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
