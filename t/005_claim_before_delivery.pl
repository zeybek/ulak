# Test: the claim commits before delivery starts
#
# A listener that accepts the TCP connection but never answers keeps the
# HTTP dispatcher busy until ulak.http_timeout. While it hangs, the row must
# be visible as 'processing' from another session and no worker may hold an
# open transaction, xid, snapshot or lock (the old design held all of them for
# the whole delivery). When the listener goes away the row is retried.

use strict;
use warnings;
use IO::Socket::INET;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

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
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 5
ulak.http_timeout = 20
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');
$node->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'")
    or BAIL_OUT('Worker did not start');

$node->safe_psql('postgres', qq{
    INSERT INTO ulak.endpoints (name, protocol, config)
    VALUES ('hang_ep', 'http', '{"url": "http://127.0.0.1:$port/hang"}'::jsonb)
});
my $id = $node->safe_psql('postgres',
    q{SELECT ulak.send_with_options('hang_ep', '{"n": 1}'::jsonb)});

# The worker connects (kernel backlog accepts it) and then waits for a response.
$node->poll_query_until('postgres',
    "SELECT status = 'processing' FROM ulak.queue WHERE id = $id")
    or BAIL_OUT('row never became processing');
pass('row is visible as processing from another session while delivery is in flight');

sleep 2;
is($node->safe_psql('postgres',
        "SELECT status FROM ulak.queue WHERE id = $id"),
    'processing', 'still processing two seconds into the hung delivery');

my $worker_state = $node->safe_psql('postgres', q{
    SELECT count(*) FILTER (WHERE xact_start IS NOT NULL) || ' ' ||
           count(*) FILTER (WHERE backend_xid IS NOT NULL) || ' ' ||
           count(*) FILTER (WHERE backend_xmin IS NOT NULL)
    FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'});
is($worker_state, '0 0 0', 'no worker holds a transaction, xid or snapshot during delivery');

my $locks = $node->safe_psql('postgres', q{
    SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE a.backend_type LIKE '%ulak%' AND l.locktype NOT IN ('relation', 'virtualxid')});
is($locks, '0', 'no worker holds row or transaction locks during delivery');

# Drop the listener: the pending connection is reset and the delivery fails fast.
close($listener);
$node->poll_query_until('postgres',
    "SELECT retry_count = 1 AND status = 'pending' FROM ulak.queue WHERE id = $id")
    or BAIL_OUT('row was not retried after the listener went away');
pass('row went back to pending with one retry recorded');
like($node->safe_psql('postgres', "SELECT last_error FROM ulak.queue WHERE id = $id"),
    qr/RETRYABLE/, 'the failure is classified as retryable');

$node->stop;
