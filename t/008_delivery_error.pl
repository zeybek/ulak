# Test: an ERROR raised during delivery keeps the outcomes already known
#
# An ERROR raised while delivering an endpoint group is caught for that group:
# outcomes recorded so far are written, the rows the error concerns are
# charged a retry, and untouched rows go back uncharged.
#
# The error is injected into the heartbeat, which runs between two
# synchronous deliveries once a quarter of ulak.stale_recovery_timeout has
# passed: a listener that never answers makes the first delivery take
# longer than that, and a trigger refuses the heartbeat UPDATE.

use strict;
use warnings;
use IO::Socket::INET;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;

my $listener = IO::Socket::INET->new(
    LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', Listen => 5, ReuseAddr => 1)
    or BAIL_OUT("cannot open a local listener: $!");
my $port = $listener->sockport;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 200
ulak.capture_response = on
ulak.stale_recovery_timeout = 60
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 5
ulak.http_timeout = 17
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');
$node->poll_query_until('postgres',
    "SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'ulak worker'")
    or BAIL_OUT('Worker did not start');

# Refuse the heartbeat (processing -> processing); claims and results pass.
$node->safe_psql('postgres', q{
    CREATE FUNCTION public.fail_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF OLD.status = 'processing' AND NEW.status = 'processing' THEN
            RAISE EXCEPTION 'injected heartbeat failure';
        END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER fail_heartbeat BEFORE UPDATE ON ulak.queue
        FOR EACH ROW EXECUTE FUNCTION public.fail_heartbeat();
});
$node->safe_psql('postgres', qq{
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('hang_ep', 'http', '{"url": "http://127.0.0.1:$port/hang"}'::jsonb)
});
my ($id1, $id2) = split /\n/, $node->safe_psql('postgres', q{
    SELECT ulak.send_with_options('hang_ep', jsonb_build_object('n', g))
    FROM generate_series(1, 2) g ORDER BY 1
});

# The first delivery times out after 17 s; the heartbeat before the second fails.
$node->poll_query_until('postgres',
    "SELECT retry_count = 1 FROM ulak.queue WHERE id = $id1")
    or BAIL_OUT('the first row never had its outcome written');
pass('the row attempted before the error has its failure written (retry_count 1)');
like($node->safe_psql('postgres', "SELECT last_error FROM ulak.queue WHERE id = $id1"),
    qr/RETRYABLE/, 'with the delivery error it actually got');
is($node->safe_psql('postgres', "SELECT retry_count FROM ulak.queue WHERE id = $id2"),
    '0', 'the row not yet attempted is not charged a retry');
like(slurp_file($node->logfile),
    qr/Delivery to endpoint \d+ raised an error, 0 message\(s\) charged a retry: injected heartbeat failure/,
    'the error is caught at the endpoint group, not in the worker loop');

$node->safe_psql('postgres', 'DROP TRIGGER fail_heartbeat ON ulak.queue');
close($listener);
$node->stop;
