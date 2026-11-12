# Test: a terminated or cancelled worker comes back and is still woken at commit
#
# The postmaster restarts a background worker that exits with code 1, never
# one that exits with 0. pg_cancel_backend() while the worker waits on its
# latch ends it through the bgworker error handler, past the loop epilogue,
# so the registry slot must be freed by an exit callback for the restarted
# worker to register its latch.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;
use Time::HiRes qw(time);

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 60000
ulak.worker_restart_delay = 1
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 1
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

my $worker_q = "SELECT pid FROM pg_stat_activity WHERE backend_type = 'ulak worker'";
$node->poll_query_until('postgres', "SELECT count(*) = 1 FROM ($worker_q) w")
    or BAIL_OUT('Worker did not start');
my $pid1 = $node->safe_psql('postgres', $worker_q);

# 1. Terminate: exit code 1 makes the postmaster restart the worker.
$node->safe_psql('postgres', "SELECT pg_terminate_backend($pid1)");
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 1 FROM ($worker_q) w WHERE pid <> $pid1"),
    'a terminated worker is restarted');
my $pid2 = $node->safe_psql('postgres', $worker_q);

# 2. Cancel while the worker sleeps on its latch (the poll interval is 60 s).
sleep 2;
$node->safe_psql('postgres', "SELECT pg_cancel_backend($pid2)");
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 1 FROM ($worker_q) w WHERE pid NOT IN ($pid1, $pid2)"),
    'a cancelled worker is restarted');
my $pid3 = $node->safe_psql('postgres', $worker_q);

# 3. The registry slot belongs to the new process, not to a dead one.
$node->poll_query_until('postgres',
    "SELECT count(*) = 1 FROM ulak.get_worker_status() WHERE pid = $pid3")
    or diag('worker status: ' . $node->safe_psql('postgres',
        'SELECT pid, state FROM ulak.get_worker_status()'));
is($node->safe_psql('postgres', 'SELECT pid FROM ulak.get_worker_status()'),
    $pid3, 'the registry slot holds the restarted worker PID');

my $log = slurp_file($node->logfile);
unlike($log, qr/already occupied/, 'no restart found its slot still occupied');

# 4. A commit wakes the restarted worker at once instead of after 60 s.
$node->safe_psql('postgres', q{
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('restart_ep', 'http', '{"url": "http://127.0.0.1:1/restart"}'::jsonb)
});
sleep 2;
my $t0 = time();
$node->safe_psql('postgres', q{SELECT ulak.send('restart_ep', '{"n": 1}'::jsonb)});
$node->poll_query_until('postgres',
    "SELECT retry_count > 0 FROM ulak.queue WHERE payload->>'n' = '1'")
    or BAIL_OUT('message was never attempted');
my $elapsed = time() - $t0;
ok($elapsed < 10,
    sprintf('the restarted worker was woken %.1fs after commit (poll interval is 60 s)',
        $elapsed));

# 5. Exit code 1 on SIGTERM must not keep a shutdown from completing
#    (the postmaster starts no background workers once it is stopping).
$node->stop('fast');
pass('a fast shutdown completes with the worker exiting with code 1');
