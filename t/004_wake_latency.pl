# Test: senders wake the delivery workers at commit through shared memory
#
# With ulak.poll_interval at its maximum (60 s) a message can only be
# attempted quickly if the committing backend woke the worker itself
# (src/wake.c). Two messages land on two different workers (id-based
# partitioning), so both latches must be set. Also checks that a message can
# be sent inside a prepared transaction, which NOTIFY used to forbid.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 5;
use Time::HiRes qw(time);

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 2
ulak.poll_interval = 60000
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 1
max_prepared_transactions = 2
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

$node->poll_query_until('postgres',
    "SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'")
    or BAIL_OUT('Workers did not start');

is($node->safe_psql('postgres', 'SHOW ulak.wake_notify'), 'off',
    'NOTIFY is off by default: workers are woken through shared memory');

# Nothing to do: both workers go to sleep for the full 60 s poll interval.
sleep 3;

# Unreachable target: the first attempt fails fast with a connection error,
# which is enough to prove the worker woke up (retry_count becomes 1).
$node->safe_psql('postgres', q{
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('wake_ep', 'http', '{"url": "http://127.0.0.1:1/wake"}'::jsonb)
});

my $t0 = time();
$node->safe_psql('postgres', q{
    SELECT ulak.send('wake_ep', '{"n": 1}'::jsonb);
    SELECT ulak.send('wake_ep', '{"n": 2}'::jsonb);
});
$node->poll_query_until('postgres',
    "SELECT count(*) = 2 FROM ulak.queue WHERE retry_count > 0 OR status <> 'pending'")
    or BAIL_OUT('Messages were never attempted');
my $elapsed = time() - $t0;
ok($elapsed < 10,
    sprintf('both partitions attempted delivery %.1fs after commit (poll interval is 60 s)', $elapsed));

my $partitions = $node->safe_psql('postgres',
    "SELECT count(DISTINCT id % 2) FROM ulak.queue");
is($partitions, '2', 'the two messages belong to two different partitions');

# A prepared transaction: NOTIFY would have refused PREPARE TRANSACTION.
$node->safe_psql('postgres', q{
    BEGIN;
    SELECT ulak.send('wake_ep', '{"n": 3}'::jsonb);
    PREPARE TRANSACTION 'ulak_wake';
});
is($node->safe_psql('postgres', "SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'ulak_wake'"),
    '1', 'a transaction that queued a message can be prepared');
$node->safe_psql('postgres', "COMMIT PREPARED 'ulak_wake'");
is($node->safe_psql('postgres', "SELECT count(*) FROM ulak.queue WHERE payload->>'n' = '3'"),
    '1', 'the prepared message is in the queue after COMMIT PREPARED');

$node->stop;
