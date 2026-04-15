-- 09_rbac.sql: Role-based access control — admin, application, monitor permissions
-- pg_regress test for ulak

-- ============================================================================
-- ADMIN ROLE PERMISSIONS
-- ============================================================================

SET ROLE ulak_admin;

-- Admin can read endpoints
SELECT count(*) >= 0 AS admin_can_read_endpoints FROM ulak.endpoints;

-- Admin can insert endpoints directly
INSERT INTO ulak.endpoints (name, protocol, config)
VALUES ('rbac_admin_ep', 'http', '{"url": "http://localhost/admin"}'::jsonb);

-- Admin can update endpoints
UPDATE ulak.endpoints SET enabled = false WHERE name = 'rbac_admin_ep';

-- Admin can read queue
SELECT count(*) >= 0 AS admin_can_read_queue FROM ulak.queue;

-- Admin can insert into queue
INSERT INTO ulak.queue (endpoint_id, payload)
SELECT id, '{"rbac":"admin_msg"}'::jsonb FROM ulak.endpoints WHERE name = 'rbac_admin_ep';

-- Admin can delete from queue
DELETE FROM ulak.queue WHERE payload @> '{"rbac":"admin_msg"}'::jsonb;

-- Admin can delete endpoints
DELETE FROM ulak.endpoints WHERE name = 'rbac_admin_ep';

-- Archive maintenance functions are executable as admin via SECURITY DEFINER
SELECT proname, prosecdef
FROM pg_proc
WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'ulak')
  AND proname IN ('maintain_archive_partitions', 'cleanup_old_archive_partitions')
ORDER BY proname;

RESET ROLE;

-- ============================================================================
-- APPLICATION ROLE PERMISSIONS
-- ============================================================================

SET ROLE ulak_application;

-- Application can read endpoints
SELECT count(*) >= 0 AS app_can_read_endpoints FROM ulak.endpoints;

-- Application can read queue
SELECT count(*) >= 0 AS app_can_read_queue FROM ulak.queue;

-- Application CANNOT insert directly into queue (must use send() API)
INSERT INTO ulak.queue (endpoint_id, payload)
SELECT id, '{"rbac":"app_msg"}'::jsonb FROM ulak.endpoints WHERE name = 'test_http_endpoint';

-- Application CANNOT insert into endpoints (should fail)
INSERT INTO ulak.endpoints (name, protocol, config)
VALUES ('rbac_app_ep', 'http', '{"url": "http://localhost/app"}'::jsonb);

-- Application CANNOT update endpoints (should fail)
UPDATE ulak.endpoints SET enabled = false WHERE name = 'test_http_endpoint';

-- Application CANNOT delete from endpoints (should fail)
DELETE FROM ulak.endpoints WHERE name = 'test_http_endpoint';

-- Application CANNOT update queue (should fail)
UPDATE ulak.queue SET status = 'completed' WHERE payload @> '{"rbac":"app_msg"}'::jsonb;

-- Application CANNOT delete from queue (should fail)
DELETE FROM ulak.queue WHERE payload @> '{"rbac":"app_msg"}'::jsonb;

RESET ROLE;

-- ============================================================================
-- MONITOR ROLE PERMISSIONS
-- ============================================================================

SET ROLE ulak_monitor;

-- Monitor can read endpoints
SELECT count(*) >= 0 AS monitor_can_read_endpoints FROM ulak.endpoints;

-- Monitor can read queue
SELECT count(*) >= 0 AS monitor_can_read_queue FROM ulak.queue;

-- Monitor CANNOT insert into endpoints (should fail)
INSERT INTO ulak.endpoints (name, protocol, config)
VALUES ('rbac_mon_ep', 'http', '{"url": "http://localhost/mon"}'::jsonb);

-- Monitor CANNOT insert into queue (should fail)
INSERT INTO ulak.queue (endpoint_id, payload)
SELECT id, '{"rbac":"mon_msg"}'::jsonb FROM ulak.endpoints WHERE name = 'test_http_endpoint';

-- Monitor CANNOT update queue (should fail)
UPDATE ulak.queue SET status = 'failed' WHERE 1=0;

-- Monitor CANNOT delete from queue (should fail)
DELETE FROM ulak.queue WHERE 1=0;

RESET ROLE;

-- ============================================================================
-- NEGATIVE RBAC TESTS: Unprivileged role cannot call internal functions
-- ============================================================================
-- Create a temporary unprivileged role
DO $$
BEGIN
    BEGIN
        CREATE ROLE ulak_test_noperm LOGIN;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END
$$;
GRANT USAGE ON SCHEMA ulak TO ulak_test_noperm;

SET ROLE ulak_test_noperm;

-- Internal admin functions should be denied
SELECT ulak.health_check();
SELECT ulak.mark_expired_messages();

RESET ROLE;

-- Clean up
REVOKE ALL ON SCHEMA ulak FROM ulak_test_noperm;
DROP ROLE IF EXISTS ulak_test_noperm;

-- ============================================================================
-- CLEANUP (as superuser)
-- ============================================================================

-- No cleanup needed — application role can no longer INSERT directly into queue.
