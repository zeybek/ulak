#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak MQTT E2E Test Suite
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

MQTT_BROKER="mosquitto"
MQTT_PORT=1883

# ─── MQTT helpers ───

mqtt_sub_count() {
  local topic="$1" timeout="${2:-5}"
  # Subscribe, collect messages until timeout, count lines
  docker exec ulak-postgres-1 timeout "$timeout" \
    mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "$topic" -C 1 -W "$timeout" 2>/dev/null | wc -l | tr -d ' '
}

mqtt_sub_payload() {
  local topic="$1" timeout="${2:-5}"
  docker exec ulak-postgres-1 timeout "$timeout" \
    mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "$topic" -C 1 -W "$timeout" 2>/dev/null
}

mqtt_ready() {
  docker exec ulak-mosquitto-1 mosquitto_pub -h localhost -p 1883 -t "health/check" -m "ping" 2>/dev/null
}

# ─── Pre-flight ───

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak MQTT E2E Test Suite                            ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not running${NC}"
  exit 1
fi

# Check mosquitto_sub available
if ! docker exec ulak-postgres-1 which mosquitto_sub > /dev/null 2>&1; then
  echo "Installing mosquitto-clients..."
  docker exec ulak-postgres-1 bash -c "apt-get update -qq && apt-get install -y -qq mosquitto-clients > /dev/null 2>&1"
fi

echo -e "${GREEN}Pre-flight OK${NC} — mqtt:1883, pg:ready"

# Setup GUCs
psql_quiet "ALTER SYSTEM SET ulak.database = '$DB';"
psql_quiet "ALTER SYSTEM SET ulak.poll_interval = 100;"
docker restart ulak-postgres-1 > /dev/null 2>&1
sleep 4

# Clean slate
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'mt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# ═══════════════════════════════════════════════════
# TEST 1: Single Message Publish (QoS 0)
# ═══════════════════════════════════════════════════
header "TEST 1: Single Message Publish (QoS 0)"

create_endpoint "mt-single" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/single\", \"qos\": 0}" "mqtt"

# Start subscriber in background BEFORE sending
docker exec ulak-postgres-1 timeout 15 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/single" -C 1 -W 15 > /tmp/mqtt-sub-1.txt 2>/dev/null &
SUB_PID=$!
sleep 1

send_msg "mt-single" "{\"test\": \"mqtt-qos0\"}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'mt-single';")
assert_eq "Message completed" "1" "$COMPLETED"

# Wait for subscriber
wait $SUB_PID 2>/dev/null
RECEIVED=$(cat /tmp/mqtt-sub-1.txt 2>/dev/null)
assert_contains "Payload received via MQTT" "mqtt-qos0" "$RECEIVED"

# ═══════════════════════════════════════════════════
# TEST 2: QoS 1 (At Least Once)
# ═══════════════════════════════════════════════════
header "TEST 2: QoS 1 Delivery"

psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-qos1" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/qos1\", \"qos\": 1}" "mqtt"

docker exec ulak-postgres-1 timeout 15 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/qos1" -q 1 -C 1 -W 15 > /tmp/mqtt-sub-2.txt 2>/dev/null &
SUB_PID=$!
sleep 1

send_msg "mt-qos1" "{\"test\": \"mqtt-qos1\"}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'mt-qos1';")
assert_eq "QoS 1 message completed" "1" "$COMPLETED"

wait $SUB_PID 2>/dev/null
RECEIVED=$(cat /tmp/mqtt-sub-2.txt 2>/dev/null)
assert_contains "QoS 1 payload received" "mqtt-qos1" "$RECEIVED"

# ═══════════════════════════════════════════════════
# TEST 3: Batch Publish (50 messages)
# ═══════════════════════════════════════════════════
header "TEST 3: Batch Publish (50 messages)"

psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-batch" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/batch\"}" "mqtt"

# Subscribe for multiple messages
docker exec ulak-postgres-1 timeout 20 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/batch" -C 50 -W 20 > /tmp/mqtt-sub-3.txt 2>/dev/null &
SUB_PID=$!
sleep 1

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 50) g);
  PERFORM ulak.send_batch('mt-batch', a);
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'mt-batch';")
assert_eq "50 messages completed" "50" "$COMPLETED"

wait $SUB_PID 2>/dev/null
RECEIVED_COUNT=$(wc -l < /tmp/mqtt-sub-3.txt 2>/dev/null | tr -d ' ')
assert_eq "50 messages received via MQTT" "50" "$RECEIVED_COUNT"

# ═══════════════════════════════════════════════════
# TEST 4: Multiple Topics
# ═══════════════════════════════════════════════════
header "TEST 4: Multiple Topics (3 × 20 messages)"

psql_quiet "DELETE FROM ulak.queue;"

for i in 1 2 3; do
  create_endpoint "mt-topic-$i" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/topic$i\"}" "mqtt"
done

psql_quiet "
DO \$\$
BEGIN
  FOR ep IN 1..3 LOOP
    PERFORM ulak.send_batch('mt-topic-' || ep,
      ARRAY(SELECT jsonb_build_object('ep', ep, 'i', g) FROM generate_series(1, 20) g));
  END LOOP;
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE 'mt-topic-%';")
assert_eq "60 messages completed" "60" "$COMPLETED"

# ═══════════════════════════════════════════════════
# TEST 5: Throughput (500 messages)
# ═══════════════════════════════════════════════════
header "TEST 5: Throughput (500 messages)"

psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-throughput" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/throughput\"}" "mqtt"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 500) g);
  PERFORM ulak.send_batch('mt-throughput', a);
END; \$\$;"

wait_queue_drain 30

THROUGHPUT=$(psql_exec "
SELECT round(count(*)::numeric / GREATEST(extract(epoch from max(q.completed_at)-min(q.created_at)), 0.001))
FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'mt-throughput';")

TOTAL=$((TOTAL + 1))
if [ "$THROUGHPUT" -ge 50 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} MQTT throughput: ${THROUGHPUT} msg/s (>=50)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} MQTT throughput too low: ${THROUGHPUT} msg/s"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 6: Error Classification
# ═══════════════════════════════════════════════════
header "TEST 6: Error Classification"

psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"

create_endpoint "mt-err" "{\"broker\": \"nonexistent-host\", \"topic\": \"mt/err\"}" "mqtt"
send_msg "mt-err" "{\"should_fail\": true}"

sleep 10

STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-err') LIMIT 1;")
ERROR=$(psql_exec "SELECT left(last_error, 12) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-err') LIMIT 1;")

assert_eq "Failed message pending (retryable)" "pending" "$STATUS"
assert_contains "Error has RETRYABLE prefix" "RETRYABLE" "$ERROR"

# ═══════════════════════════════════════════════════
# TEST 7: Retained Messages
# ═══════════════════════════════════════════════════
header "TEST 7: Retained Messages"

psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-retain" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/retained\", \"retain\": true}" "mqtt"

send_msg "mt-retain" "{\"retained\": true}"
wait_queue_drain 15

# Subscribe AFTER publish — retained message should still be received
RETAINED=$(docker exec ulak-postgres-1 timeout 5 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/retained" -C 1 -W 5 2>/dev/null)
assert_contains "Retained message received" "retained" "$RETAINED"

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'mt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

print_summary
exit $?
