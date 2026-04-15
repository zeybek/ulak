# Test: Circuit breaker half_open recovery race
#
# One worker reports success (closes circuit), another reports failure.
# Advisory lock serializes: last writer wins. Both permutations end closed
# because success unconditionally resets to closed.

setup
{
    CREATE EXTENSION IF NOT EXISTS ulak;
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints;

    INSERT INTO ulak.endpoints (name, protocol, config, enabled,
        circuit_state, circuit_failure_count)
    VALUES ('iso_cb_recover', 'http',
        jsonb_build_object('url', 'http://localhost:9999/webhook'),
        false, 'half_open', 5);
}

teardown
{
    DELETE FROM ulak.queue;
    DELETE FROM ulak.endpoints WHERE name = 'iso_cb_recover';
}

session "w1"
step "w1_success"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_recover'),
        true
    );
}

session "w2"
step "w2_fail"
{
    SELECT ulak.update_circuit_breaker(
        (SELECT id FROM ulak.endpoints WHERE name = 'iso_cb_recover'),
        false
    );
}

session "checker"
step "check_state"
{
    SELECT circuit_state, circuit_failure_count
    FROM ulak.endpoints WHERE name = 'iso_cb_recover';
}

# Success first, then failure: closed→fail increments to 1
permutation "w1_success" "w2_fail" "check_state"
# Failure first, then success: half_open→open, then open→closed(0)
permutation "w2_fail" "w1_success" "check_state"
