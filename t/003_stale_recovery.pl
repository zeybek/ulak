# Test: Stale processing message recovery after restart
#
# Verifies that messages stuck in 'processing' state are recovered
# back to 'pending' after a server restart (simulating worker crash), and
# that a stuck message whose retries are used up goes to the DLQ instead of
# being delivered again forever (a payload that crashes the worker would
# otherwise loop through recovery indefinitely).

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf('postgresql.conf', qq{
shared_preload_libraries = 'ulak'
ulak.database = 'postgres'
ulak.workers = 1
ulak.poll_interval = 200
ulak.stale_recovery_timeout = 5
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION ulak');

# Wait for worker to start
$node->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'")
    or BAIL_OUT('Worker did not start');

# 1. Create an endpoint (direct INSERT to avoid non-deterministic output)
$node->safe_psql('postgres', q{
    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('tap_test_ep', 'http',
            '{"url": "http://localhost:19999/test"}'::jsonb, true)
});
pass('Test endpoint created');

# 2. Simulate a stale processing message (as if worker crashed mid-dispatch)
$node->safe_psql('postgres', q{
    INSERT INTO ulak.queue (endpoint_id, payload, status, processing_started_at)
    VALUES (
        (SELECT id FROM ulak.endpoints WHERE name = 'tap_test_ep'),
        '{"test": "stale_recovery"}'::jsonb,
        'processing',
        NOW() - interval '1 hour'
    )
});

# A second stuck message on its last retry (default retry_policy: max_retries 10)
my $exhausted_id = $node->safe_psql('postgres', q{
    INSERT INTO ulak.queue (endpoint_id, payload, status, retry_count, processing_started_at)
    VALUES (
        (SELECT id FROM ulak.endpoints WHERE name = 'tap_test_ep'),
        '{"test": "stale_exhausted"}'::jsonb,
        'processing',
        9,
        NOW() - interval '1 hour'
    )
    RETURNING id
});

my $processing = $node->safe_psql('postgres',
    "SELECT count(*) FROM ulak.queue WHERE status = 'processing'");
is($processing, '2', 'Messages are in processing state');

# 3. Restart server (simulates crash recovery scenario)
$node->stop('fast');
$node->start;

# Poll until stale recovery completes (message returns to pending)
$node->poll_query_until('postgres',
    "SELECT count(*) > 0 FROM ulak.queue
     WHERE status = 'pending' AND payload::text LIKE '%stale_recovery%'")
    or BAIL_OUT('Stale recovery did not complete');

# 4. Verify stale message was recovered to pending
my $recovered = $node->safe_psql('postgres',
    "SELECT count(*) FROM ulak.queue WHERE status = 'pending'
     AND payload::text LIKE '%stale_recovery%'");
is($recovered, '1', 'Stale processing message recovered to pending');

# 4b. The exhausted one is in the DLQ with its retry charged, not in the queue
$node->poll_query_until('postgres',
    "SELECT count(*) = 1 FROM ulak.dlq WHERE original_message_id = $exhausted_id")
    or diag('exhausted stale message did not reach the DLQ');
is($node->safe_psql('postgres',
        "SELECT retry_count || ' ' || (last_error LIKE '%max retries reached')
         FROM ulak.dlq WHERE original_message_id = $exhausted_id"),
    '10 true', 'Stale message on its last retry moved to the DLQ with retry_count 10');
is($node->safe_psql('postgres', "SELECT count(*) FROM ulak.queue WHERE id = $exhausted_id"),
    '0', 'and removed from the queue');

# 5. Worker is running after restart
my $worker_count = $node->safe_psql('postgres',
    "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE '%ulak%'");
cmp_ok($worker_count, '>=', 1, 'Worker running after restart');

# Cleanup - use eval to prevent teardown errors from failing the test
eval { $node->stop; };
