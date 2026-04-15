# Test: ulak worker startup and registration
#
# Verifies that the extension loads via shared_preload_libraries,
# background workers start, and monitoring functions return data.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 5;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 500
});
$node->start;

# 1. Extension creates without error
$node->safe_psql('postgres', 'CREATE EXTENSION ulak');
pass('Extension created successfully');

# 2. Poll until worker appears in pg_stat_activity
$node->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'")
    or BAIL_OUT('Worker did not appear in pg_stat_activity');
my $worker_count = $node->safe_psql('postgres',
    "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'");
cmp_ok($worker_count, '>=', 1, 'Worker visible in pg_stat_activity');

# 3. get_worker_status() returns at least one row
my $status_rows = $node->safe_psql('postgres',
    "SELECT count(*) FROM ulak.get_worker_status()");
cmp_ok($status_rows, '>=', 1, 'get_worker_status() returns rows');

# 4. health_check() returns healthy components
my $health = $node->safe_psql('postgres',
    "SELECT string_agg(component || ':' || status, ',') FROM ulak.health_check()");
like($health, qr/shared_memory:healthy/, 'Shared memory component is healthy');

# 5. Server log contains the exact initialization message from _PG_init
my $log = $node->logfile;
open my $fh, '<', $log or die "Cannot open log: $!";
my $log_content = do { local $/; <$fh> };
close $fh;
like($log_content, qr/\[ulak\] extension initialized successfully/,
     'Extension initialization log message found');

eval { $node->stop; };
