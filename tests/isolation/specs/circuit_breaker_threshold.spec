# Test: Concurrent failures crossing circuit breaker threshold
#
# Two workers fail concurrently when count is at threshold-1.
# Advisory lock serializes: first crosses threshold and opens circuit,
# second increments further while circuit stays open.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    -- Seed failure count at 9 (default threshold is 10)
    INSERT INTO ulak.endpoints (name, protocol, config, enabled,
        circuit_state, circuit_failure_count)
    VALUES ('iso_cb_thresh', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'),
        false, 'closed', 9);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_cb_thresh';
}

session "w1"
step "w1_fail"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_thresh'),
        false
    );
}
step "w1_check"
{
    SELECT circuit_state, circuit_failure_count,
           (circuit_half_open_at IS NOT NULL) AS has_half_open_at
    FROM ulak.endpoints WHERE name = 'iso_cb_thresh';
}

session "w2"
step "w2_fail"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_thresh'),
        false
    );
}
step "w2_check"
{
    SELECT circuit_state, circuit_failure_count,
           (circuit_half_open_at IS NOT NULL) AS has_half_open_at
    FROM ulak.endpoints WHERE name = 'iso_cb_thresh';
}

# Both fail → count goes 9→10→11, circuit opens at 10
permutation "w1_fail" "w2_fail" "w1_check"
permutation "w1_fail" "w2_fail" "w2_check"
