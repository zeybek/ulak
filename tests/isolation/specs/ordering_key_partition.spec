# Test: ordering_key hash-partitioning across multiple workers
#
# All messages with the same ordering_key must route to the same worker.
# When two workers (total_workers=2) run the partitioned fetch query
# concurrently, exactly one of them sees the rows; the other is empty.
# This guards against the multi-worker FIFO race in v0.0.2 where id-based
# modulo partitioning routed siblings of the same key to different workers.
#
# 'order-7000652638004' hashes to INT32_MIN. abs() of that int4 raises
# "integer out of range", so a claim query that took abs() before widening
# to bigint failed for every worker while such a row was pending. With the
# cast first it lands in partition 2147483648 % 2 = 0.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled)
    VALUES ('iso_partition_ep', 'http',
            jsonb_build_object('url', 'http://localhost:9999/webhook'), false);

    -- 5 messages all sharing ordering_key 'partition-X'
    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('seq', g), 'partition-X'
    FROM ulak.endpoints e, generate_series(1, 5) g
    WHERE e.name = 'iso_partition_ep';

    INSERT INTO ulak.queue (endpoint_id, payload, ordering_key)
    SELECT e.id, jsonb_build_object('seq', 6), 'order-7000652638004'
    FROM ulak.endpoints e
    WHERE e.name = 'iso_partition_ep';
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_partition_ep';
}

# Worker 0: total_workers=2, worker_id=0
session "w0"
setup { BEGIN; }
step "w0_hash" { SELECT hashtext('order-7000652638004') AS int32_min_hash; }
step "w0_fetch"
{
    -- Lock then count via CTE (FOR UPDATE not allowed with aggregates).
    WITH locked AS (
        SELECT q.id
        FROM ulak.queue q
        WHERE q.status = 'pending'
          AND ((CASE WHEN q.ordering_key IS NULL THEN q.id
                     ELSE abs(hashtext(q.ordering_key)::bigint)
                END) % 2) = 0
        FOR UPDATE OF q SKIP LOCKED
    )
    SELECT count(*) AS fetched FROM locked;
}
step "w0_commit" { COMMIT; }

# Worker 1: total_workers=2, worker_id=1
session "w1"
setup { BEGIN; }
step "w1_fetch"
{
    WITH locked AS (
        SELECT q.id
        FROM ulak.queue q
        WHERE q.status = 'pending'
          AND ((CASE WHEN q.ordering_key IS NULL THEN q.id
                     ELSE abs(hashtext(q.ordering_key)::bigint)
                END) % 2) = 1
        FOR UPDATE OF q SKIP LOCKED
    )
    SELECT count(*) AS fetched FROM locked;
}
step "w1_commit" { COMMIT; }

# Worker 0 claims all 6 rows (both keys hash to partition 0), worker 1 sees 0,
# and neither query fails on the INT32_MIN key.
permutation "w0_hash" "w0_fetch" "w1_fetch" "w0_commit" "w1_commit"
