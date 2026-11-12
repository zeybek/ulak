# Test: senders wake the delivery workers at commit through shared memory
#
# With ulak.poll_interval at its maximum (60 s) a message can only be
# attempted quickly if the committing backend woke the worker itself
# (src/wake.c). Two messages land on two different workers (id-based
# partitioning), so both latches must be set. Also checks that a message can
# be sent inside a prepared transaction (where NOTIFY is not allowed), and that
# a send() whose INSERT fails inside a savepoint does not leave the trigger
# wake-up switched off for the rest of the transaction.

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

# send() switches the statement trigger's wake-up off around its own INSERT.
# When that INSERT raises and a savepoint catches it, the switch must not stay
# off, or a later plain INSERT in the same transaction wakes nobody.
$node->safe_psql('postgres', q{
    CREATE FUNCTION public.fail_marked() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF NEW.payload->>'fail' = 'yes' THEN
            RAISE EXCEPTION 'injected insert failure';
        END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER fail_marked BEFORE INSERT ON ulak.queue
        FOR EACH ROW EXECUTE FUNCTION public.fail_marked();
});
sleep 3;
$t0 = time();
$node->safe_psql('postgres', q{
    DO $$
    BEGIN
        BEGIN
            PERFORM ulak.send('wake_ep', '{"fail": "yes"}'::jsonb);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        INSERT INTO ulak.queue (endpoint_id, payload)
        SELECT id, '{"n": 4}'::jsonb FROM ulak.endpoints WHERE name = 'wake_ep';
    END $$;
});
$node->poll_query_until('postgres',
    "SELECT retry_count > 0 OR status <> 'pending' FROM ulak.queue WHERE payload->>'n' = '4'")
    or BAIL_OUT('the row inserted after the failed send was never attempted');
$elapsed = time() - $t0;
ok($elapsed < 10,
    sprintf('the INSERT after a failed send() still woke a worker (%.1fs, poll interval 60 s)',
        $elapsed));
$node->safe_psql('postgres', 'DROP TRIGGER fail_marked ON ulak.queue');

$node->stop;
