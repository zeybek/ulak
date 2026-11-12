# Test: a missing synchronous standby neither hangs the worker nor its shutdown
#
# With synchronous_standby_names set and no standby connected, a commit at
# synchronous_commit = on waits in SyncRepWaitForLSN() for a confirmation
# that never comes. Only ProcDiePending, a query cancel or postmaster death
# end that wait, and ulak's SIGTERM handler sets none of them. The worker runs
# its session at synchronous_commit = local (which still flushes WAL), so its
# own commits (TTL expiry here; also stale recovery, archiving, heartbeat)
# never wait there and it stays stoppable.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;
use Time::HiRes qw(time);

# This script's own sessions must not wait for the standby either.
$ENV{PGOPTIONS} = '-c synchronous_commit=local';

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 1);    # max_wal_senders > 0, or sync rep is off
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 200
ulak.worker_restart_delay = 1
synchronous_standby_names = 'nonexistent'
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

my $worker_q = "SELECT pid FROM pg_stat_activity WHERE backend_type = 'ulak worker'";
$node->poll_query_until('postgres', "SELECT count(*) = 1 FROM ($worker_q) w")
    or BAIL_OUT('Worker did not start');

# 1. The setup really blocks a synchronous commit.
my ($probe_out, $probe_err, $probe_timed_out);
$node->psql('postgres', 'SET synchronous_commit = on; CREATE TABLE sync_probe (x int);',
    stdout => \$probe_out, stderr => \$probe_err,
    timeout => 3, timed_out => \$probe_timed_out);
ok($probe_timed_out, 'a commit at synchronous_commit = on waits for the missing standby');

# 2. Worker 0's TTL expiry commits at its session level and becomes visible.
#    The endpoint is disabled, so only mark_expired_messages() touches the row.
$node->safe_psql('postgres', q{
    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('sync_ep', 'http', '{"url": "http://127.0.0.1:1/x"}'::jsonb, false);
    INSERT INTO ulak.queue (endpoint_id, payload, expires_at)
    SELECT id, '{"n": 1}'::jsonb, now() FROM ulak.endpoints WHERE name = 'sync_ep';
});
ok($node->poll_query_until('postgres',
        "SELECT status = 'expired' FROM ulak.queue WHERE payload->>'n' = '1'"),
    'the worker commit is not held back by the missing standby');

# 3. A SIGHUP that sets synchronous_commit in the file does not undo it.
$node->append_conf('postgresql.conf', "synchronous_commit = on\n");
$node->reload;
sleep 2;
$node->safe_psql('postgres', q{
    INSERT INTO ulak.queue (endpoint_id, payload, expires_at)
    SELECT id, '{"n": 2}'::jsonb, now() FROM ulak.endpoints WHERE name = 'sync_ep';
});
ok($node->poll_query_until('postgres',
        "SELECT status = 'expired' FROM ulak.queue WHERE payload->>'n' = '2'"),
    'still not held back after a reload with synchronous_commit = on in the file');

# 4. pg_terminate_backend() stops and restarts the worker promptly.
my $pid1 = $node->safe_psql('postgres', $worker_q);
my $t0 = time();
$node->safe_psql('postgres', "SELECT pg_terminate_backend($pid1)");
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 1 FROM ($worker_q) w WHERE pid <> $pid1"),
    'a terminated worker is restarted');
ok(time() - $t0 < 15, sprintf('within %.1fs', time() - $t0));

# 5. A fast shutdown completes (the probe backend from step 1 is
#    terminated by the postmaster; the worker must exit on its SIGTERM).
$t0 = time();
my $rc = system('pg_ctl', '-D', $node->data_dir, '-m', 'fast', '-t', '30', 'stop');
my $elapsed = time() - $t0;
ok($rc == 0 && $elapsed < 15,
    sprintf('a fast shutdown completes (%.1fs, pg_ctl exit %d)', $elapsed, $rc >> 8));
$node->_update_pid(0) if $rc == 0;
