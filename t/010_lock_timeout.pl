# Test: a worker waiting on someone else's lock still stops, and loses nothing
#
# ulak's SIGTERM handler only sets a flag and the latch, which ends a latch
# wait but not a heavyweight-lock wait, such as a result transaction queued
# behind an open admin transaction (here: the row lock an UPDATE of
# ulak.endpoints takes, which update_circuit_breaker() needs as well). The
# worker runs with lock_timeout = 10s: the wait ends in an ERROR, the claimed
# row is released, and the worker exits on its pending SIGTERM.

use strict;
use warnings;
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 5;
use Time::HiRes qw(time);

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 200
ulak.worker_restart_delay = 1
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 1
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

my $worker_q = "SELECT pid FROM pg_stat_activity WHERE backend_type = 'ulak worker'";
$node->poll_query_until('postgres', "SELECT count(*) = 1 FROM ($worker_q) w")
    or BAIL_OUT('Worker did not start');

# Connection refused: every attempt fails fast and updates the circuit breaker.
$node->safe_psql('postgres', q{
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('lock_ep', 'http', '{"url": "http://127.0.0.1:1/lock"}'::jsonb)
});

# An open transaction holds the endpoint row the way an UPDATE of it does.
# FOR NO KEY UPDATE, not FOR UPDATE: the latter also blocks the FOR KEY SHARE
# lock of the queue's foreign key check, so send() itself would wait.
my ($hout, $herr) = ('', '');
my $holder = IPC::Run::start(
    [ 'psql', '-X', '-q', '-d', $node->connstr('postgres') . ' application_name=lock_holder',
      '-c', "BEGIN; SELECT 1 FROM ulak.endpoints WHERE name = 'lock_ep' FOR NO KEY UPDATE; "
          . "SELECT pg_sleep(300); COMMIT;" ],
    '>', \$hout, '2>', \$herr);
$node->poll_query_until('postgres',
    "SELECT count(*) = 1 FROM pg_stat_activity
     WHERE application_name = 'lock_holder' AND wait_event = 'PgSleep'")
    or BAIL_OUT('lock holder did not start');

my $id = $node->safe_psql('postgres',
    q{SELECT ulak.send_with_options('lock_ep', '{"n": 1}'::jsonb)});
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 1 FROM pg_stat_activity
         WHERE backend_type = 'ulak worker' AND wait_event_type = 'Lock'"),
    'the worker waits for the endpoint row held by the open transaction');

# Terminate it while it waits.
my $pid1 = $node->safe_psql('postgres', $worker_q);
my $t0 = time();
$node->safe_psql('postgres', "SELECT pg_terminate_backend($pid1)");
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 1 FROM ($worker_q) w WHERE pid <> $pid1"),
    'the waiting worker exits and is restarted');
my $elapsed = time() - $t0;
ok($elapsed < 20, sprintf('within lock_timeout plus restart (%.1fs)', $elapsed));
like(slurp_file($node->logfile), qr/canceling statement due to lock timeout/,
    'the wait ended with a lock timeout');

# Release the lock: the message must still be there and get its retry.
$node->safe_psql('postgres',
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'lock_holder'");
$holder->finish;
ok($node->poll_query_until('postgres',
        "SELECT status = 'pending' AND retry_count >= 1 AND last_error LIKE '[RETRYABLE]%'
         FROM ulak.queue WHERE id = $id"),
    'the message is not lost: it is retried once the lock is gone');

$node->stop;
