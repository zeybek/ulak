# Test: SIGHUP configuration reload
#
# Verifies that GUC parameter changes take effect after pg_reload_conf()
# without requiring a full server restart.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 4;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 500
ulak.batch_size = 50
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

# 1. Wait for worker to start
$node->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'")
    or BAIL_OUT('Worker did not start');
pass('Worker started');

# 2. Verify initial GUC value
my $initial = $node->safe_psql('postgres', "SHOW ulak.batch_size");
is($initial, '50', 'Initial batch_size is 50');

# 3. Change GUC and reload, poll until effective
$node->append_conf('postgresql.conf', "ulak.batch_size = 200\n");
$node->reload;
$node->poll_query_until('postgres',
    "SELECT current_setting('ulak.batch_size') = '200'")
    or BAIL_OUT('batch_size did not update after reload');

my $updated = $node->safe_psql('postgres', "SHOW ulak.batch_size");
is($updated, '200', 'batch_size updated to 200 after reload');

# 4. Worker is still running after reload
my $worker_count = $node->safe_psql('postgres',
    "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'");
cmp_ok($worker_count, '>=', 1, 'Worker still running after SIGHUP reload');

eval { $node->stop; };
