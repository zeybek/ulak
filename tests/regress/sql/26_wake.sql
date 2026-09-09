-- ============================================================================
-- Test: worker wake-up path (ulak._wake_workers) and its GUCs
-- ============================================================================

-- 1. Defaults: in-database workers are woken through shared memory, no NOTIFY
SHOW ulak.wake_notify;
SHOW ulak.notify_throttle_ms;

-- 2. Callable with an unknown row (wake all), an unkeyed row and a keyed row
SELECT ulak._wake_workers(NULL, NULL);
SELECT ulak._wake_workers(42, NULL);
SELECT ulak._wake_workers(42, 'order-1');

-- 3. The queue trigger and the batch/publish APIs route through it
SELECT pg_get_functiondef('ulak.notify_new_message'::regproc) LIKE '%ulak._wake_workers(NULL, NULL)%' AS trigger_uses_wake;
SELECT count(*) AS functions_calling_pg_notify
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'ulak' AND p.prosrc LIKE '%pg_notify%';

-- 4. send() and send_with_options() still enqueue (the wake happens at commit)
INSERT INTO ulak.endpoints (name, protocol, config, enabled)
VALUES ('wake_ep', 'http', '{"url": "https://example.com/wake"}'::jsonb, false);
BEGIN;
UPDATE ulak.endpoints SET enabled = true WHERE name = 'wake_ep';
SELECT ulak.send('wake_ep', '{"n": 1}'::jsonb);
SELECT ulak.send_with_options('wake_ep', '{"n": 2}'::jsonb, 0::smallint, NULL, NULL, NULL, NULL, 'key-1') > 0 AS keyed_send;
SELECT count(*) AS queued FROM ulak.queue q JOIN ulak.endpoints e ON e.id = q.endpoint_id WHERE e.name = 'wake_ep';
ROLLBACK;
DELETE FROM ulak.endpoints WHERE name = 'wake_ep';
