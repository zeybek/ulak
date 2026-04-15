# Test: Circuit breaker advisory lock prevents race conditions
#
# Two workers concurrently call update_circuit_breaker() for the same
# endpoint. The advisory lock must serialize updates — no lost updates.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled, circuit_state, circuit_failure_count)
    VALUES ('iso_cb_ep', 'http', jsonb_build_object('url', 'http://localhost:9999/webhook'), false, 'closed', 0);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_cb_ep';
}

# Worker 1: reports a failure
session "w1"
step "w1_fail"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_ep'),
        false
    );
}
step "w1_check"
{
    SELECT circuit_state, circuit_failure_count
    FROM ulak.endpoints WHERE name = 'iso_cb_ep';
}

# Worker 2: also reports a failure concurrently
session "w2"
step "w2_fail"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_ep'),
        false
    );
}
step "w2_check"
{
    SELECT circuit_state, circuit_failure_count
    FROM ulak.endpoints WHERE name = 'iso_cb_ep';
}

# Both fail concurrently — count must be exactly 2 (not 1 from lost update)
permutation "w1_fail" "w2_fail" "w1_check"
permutation "w1_fail" "w2_fail" "w2_check"
