# Test: delivery outcomes that depend on the worker's claim and result logic
#
# Runs two workers against a small HTTP responder and checks:
#  - an ordering key whose hashtext() is INT32_MIN neither breaks the claim
#    query (abs() of an int4 INT32_MIN raises "integer out of range") nor
#    stops other rows;
#  - per-message headers and metadata survive HTTP batch mode, which has no
#    produce_ex;
#  - Retry-After on a 429 sets next_retry_at with capture_response off, in
#    batch mode (error-string marker) and in the dispatch_ex path;
#  - a rate limit below one per second still delivers, and the scalar
#    "rate_limit": N form is enforced;
#  - while the circuit is half_open with a probe out, rows are deferred
#    instead of delivered, and a probe whose deadline passed is retaken;
#  - a maintenance task that keeps failing does not stop worker 0 from
#    delivering its partition.

use strict;
use warnings;
use IO::Socket::INET;
use POSIX ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 14;
use Time::HiRes qw(time);

# --- HTTP responder: /429 answers 429 + Retry-After: 120, anything else 200 ---
my $server = IO::Socket::INET->new(
    LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp', Listen => 64, ReuseAddr => 1)
    or BAIL_OUT("cannot open a local listener: $!");
my $port = $server->sockport;
my $reqlog = PostgreSQL::Test::Utils::tempdir() . '/requests.log';

my $responder = fork();
BAIL_OUT("fork failed: $!") unless defined $responder;
if ($responder == 0) {
    while (my $c = $server->accept) {
        my ($req, $len, $body) = ('', 0, '');
        local $/ = "\r\n";
        while (defined(my $line = <$c>)) {
            $req .= $line;
            $len = $1 if $line =~ /^Content-Length:\s*(\d+)/i;
            last if $line eq "\r\n";
        }
        read($c, $body, $len) if $len;
        if (open(my $fh, '>>', $reqlog)) {
            print $fh $req, $body, "\n----\n";
            close $fh;
        }
        my ($path) = $req =~ m{^\S+ (\S+)};
        if (defined $path && $path =~ m{/429}) {
            print $c "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\n"
              . "Content-Length: 0\r\nConnection: close\r\n\r\n";
        } else {
            print $c "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        }
        close $c;
    }
    POSIX::_exit(0);    # no END blocks: they belong to the parent's cluster
}
close $server;

sub requests { return -e $reqlog ? slurp_file($reqlog) : ''; }

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 2
ulak.poll_interval = 200
ulak.http_allow_internal_urls = on
ulak.http_connect_timeout = 2
ulak.http_timeout = 5
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');
$node->poll_query_until('postgres',
    "SELECT count(*) = 2 FROM pg_stat_activity WHERE backend_type = 'ulak worker'")
    or BAIL_OUT('Workers did not start');

my $base = "http://127.0.0.1:$port";
$node->safe_psql('postgres', qq{
    INSERT INTO ulak.endpoints (name, protocol, config) VALUES
      ('ok_ep',  'http', '{"url": "$base/ok"}'),
      ('ra_ep',  'http', '{"url": "$base/429"}'),
      ('rl_ep',  'http', '{"url": "$base/rl", "rate_limit": {"limit": 30, "interval": "minute"}}'),
      ('rls_ep', 'http', '{"url": "$base/rls", "rate_limit": 0.5}'),
      ('cb_ep',  'http', '{"url": "$base/cb"}');
});

# 1. INT32_MIN ordering key, two workers
is($node->safe_psql('postgres', "SELECT hashtext('order-7000652638004')"),
    '-2147483648', 'the test key hashes to INT32_MIN');
$node->safe_psql('postgres', q{
    SELECT ulak.send_with_options('ok_ep', '{"case": "int32min"}'::jsonb,
                                  p_ordering_key => 'order-7000652638004');
    SELECT ulak.send('ok_ep', '{"case": "plain"}'::jsonb);
});
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 2 FROM ulak.queue WHERE payload->>'case' IN ('int32min', 'plain')
         AND status = 'completed'"),
    'the INT32_MIN-key row and an unkeyed row are delivered');
unlike(slurp_file($node->logfile), qr/integer out of range/,
    'no claim query failed with integer out of range');

# 2. Per-message headers + metadata in HTTP batch mode (capture_response off)
$node->safe_psql('postgres', q{
    INSERT INTO ulak.queue (endpoint_id, payload, headers, metadata)
    SELECT id, '{"case": "headers"}', '{"X-Ulak-Test": "hdr-123"}', '{"url_suffix": "/suffix"}'
    FROM ulak.endpoints WHERE name = 'ok_ep'
});
$node->poll_query_until('postgres',
    "SELECT status = 'completed' FROM ulak.queue WHERE payload->>'case' = 'headers'")
    or BAIL_OUT('headers row was not delivered');
my ($hreq) = grep { /"case": "headers"/ } split /\n----\n/, requests();
$hreq //= '';
like($hreq, qr/^X-Ulak-Test: hdr-123\r$/m, 'the per-message header reached the server');
like($hreq, qr{^POST /ok/suffix }, 'the per-message url_suffix was applied');

# 3. Retry-After honoured with capture_response off
$node->safe_psql('postgres', q{
    SELECT ulak.send('ra_ep', '{"case": "ra_batch"}'::jsonb);
    INSERT INTO ulak.queue (endpoint_id, payload, headers)
    SELECT id, '{"case": "ra_sync"}', '{"X-Ulak-Test": "ra"}'
    FROM ulak.endpoints WHERE name = 'ra_ep';
});
$node->poll_query_until('postgres',
    "SELECT count(*) = 2 FROM ulak.queue WHERE payload->>'case' LIKE 'ra_%' AND retry_count = 1")
    or BAIL_OUT('429 rows were not attempted');
is($node->safe_psql('postgres',
        "SELECT next_retry_at > now() + interval '60 seconds' FROM ulak.queue
         WHERE payload->>'case' = 'ra_batch'"),
    't', 'batch mode: Retry-After 120 sets next_retry_at (policy delay would be 10 s)');
is($node->safe_psql('postgres',
        "SELECT next_retry_at > now() + interval '60 seconds' FROM ulak.queue
         WHERE payload->>'case' = 'ra_sync'"),
    't', 'dispatch_ex path: Retry-After 120 sets next_retry_at without capture_response');

# 4. 30 per minute (0.5 tokens per second, no burst) still delivers
$node->safe_psql('postgres', q{SELECT ulak.send('rl_ep', '{"case": "rl"}'::jsonb)});
ok($node->poll_query_until('postgres',
        "SELECT status = 'completed' FROM ulak.queue WHERE payload->>'case' = 'rl'"),
    'a rate limit of 30 per minute delivers');

# 5. "rate_limit": 0.5 is enforced: one token up front, then one every 2 s
$node->safe_psql('postgres', q{
    SELECT ulak.send('rls_ep', jsonb_build_object('case', 'rls', 'n', g))
    FROM generate_series(1, 3) g
});
$node->poll_query_until('postgres',
    "SELECT count(*) >= 1 FROM ulak.queue WHERE payload->>'case' = 'rls' AND status = 'completed'")
    or BAIL_OUT('scalar rate-limited rows were never delivered');
my $t_first = time();
cmp_ok($node->safe_psql('postgres',
        "SELECT count(*) FROM ulak.queue WHERE payload->>'case' = 'rls' AND status = 'completed'"),
    '<', 3, 'the scalar rate limit holds the other rows back');
$node->poll_query_until('postgres',
    "SELECT count(*) = 3 FROM ulak.queue WHERE payload->>'case' = 'rls' AND status = 'completed'")
    or BAIL_OUT('scalar rate-limited rows did not all complete');
cmp_ok(time() - $t_first, '>=', 2.5, 'two more tokens at 0.5 per second take several seconds');

# 6. half_open with a probe out: defer; deadline passed: one probe, then closed
$node->safe_psql('postgres', q{
    UPDATE ulak.endpoints SET circuit_state = 'half_open',
           circuit_half_open_at = now() + interval '1 hour'
    WHERE name = 'cb_ep';
    SELECT ulak.send('cb_ep', jsonb_build_object('case', 'cb', 'n', g))
    FROM generate_series(1, 2) g;
});
sleep 3;
is($node->safe_psql('postgres',
        "SELECT count(*) FILTER (WHERE status = 'completed') || ' ' ||
                count(*) FILTER (WHERE last_error LIKE 'Circuit breaker half_open%')
         FROM ulak.queue WHERE payload->>'case' = 'cb'"),
    '0 2', 'rows are deferred while the half_open probe is out');
$node->safe_psql('postgres', q{
    UPDATE ulak.endpoints SET circuit_half_open_at = now() - interval '1 second'
    WHERE name = 'cb_ep'
});
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 2 FROM ulak.queue WHERE payload->>'case' = 'cb' AND status = 'completed'
         AND (SELECT circuit_state FROM ulak.endpoints WHERE name = 'cb_ep') = 'closed'"),
    'a probe whose deadline passed is retaken, closes the circuit and the rest follows');

# 7. Worker 0 keeps delivering while a maintenance task fails every time
$node->safe_psql('postgres', q{
    CREATE OR REPLACE FUNCTION ulak.mark_expired_messages() RETURNS integer
    LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected maintenance failure'; END $$;
});
sleep 3;
like(slurp_file($node->logfile),
    qr/Maintenance task mark_expired_messages failed \(non-fatal\): injected maintenance failure/,
    'the failing maintenance task is reported and contained');
$node->safe_psql('postgres', q{
    SELECT ulak.send('ok_ep', jsonb_build_object('case', 'maint', 'n', g))
    FROM generate_series(1, 4) g
});
ok($node->poll_query_until('postgres',
        "SELECT count(*) = 4 FROM ulak.queue WHERE payload->>'case' = 'maint'
         AND status = 'completed'"),
    'both partitions, worker 0 included, deliver while the task keeps failing');

$node->stop;
kill 'TERM', $responder;
waitpid($responder, 0);
