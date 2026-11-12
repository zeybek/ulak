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

# ── QoS 1 on the synchronous path (capture_response forces dispatch_ex):
#    the worker must wait for the broker's PUBACK, not just flush its buffer,
#    and the acknowledged mid is captured on the row. ──
section "QoS 1 synchronous dispatch waits for PUBACK (capture_response=on)..."
psql_quiet "ALTER SYSTEM SET ulak.capture_response = on;"
psql_quiet "SELECT pg_reload_conf();"
sleep 1
psql_quiet "DELETE FROM ulak.queue;"

docker exec ulak-postgres-1 timeout 15 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/qos1" -q 1 -C 1 -W 15 > /tmp/mqtt-sub-2b.txt 2>/dev/null &
SUB_PID=$!
sleep 1

send_msg "mt-qos1" "{\"test\": \"mqtt-qos1-sync\"}"
wait_queue_drain 15

SYNC_COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'mt-qos1';")
assert_eq "QoS 1 sync message completed" "1" "$SYNC_COMPLETED"
SYNC_MID=$(psql_exec "SELECT coalesce((response->>'mqtt_mid')::int > 0, false) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'mt-qos1' LIMIT 1;")
assert_eq "Acknowledged mid captured in response" "t" "$SYNC_MID"

wait $SUB_PID 2>/dev/null
RECEIVED=$(cat /tmp/mqtt-sub-2b.txt 2>/dev/null)
assert_contains "QoS 1 sync payload received" "mqtt-qos1-sync" "$RECEIVED"

psql_quiet "ALTER SYSTEM SET ulak.capture_response = off;"
psql_quiet "SELECT pg_reload_conf();"
sleep 1

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
# TEST 8: Connect to an unreachable broker is bounded
# ═══════════════════════════════════════════════════
header "TEST 8: Connect Bounded by ulak.mqtt_timeout"

# A blocking mosquitto_connect() waits for the kernel's TCP connect timeout
# (minutes) against a blackholed address; the connect runs in
# mosquitto_loop() under ulak.mqtt_timeout (5 s by default).
psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-blackhole" "{\"broker\": \"192.0.2.1\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/blackhole\", \"qos\": 1}" "mqtt"
send_msg "mt-blackhole" "{\"blackhole\": true}"
BH_ERR=""
for _ in $(seq 1 25); do
  BH_ERR=$(psql_exec "SELECT last_error FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-blackhole') AND retry_count >= 1;")
  [ -n "$BH_ERR" ] && break
  sleep 1
done
assert_contains "Unreachable broker: attempt ends within 25 s" "timed out after" "$BH_ERR"
assert_contains "Unreachable broker: retryable" "RETRYABLE" "$BH_ERR"
psql_quiet "SELECT ulak.disable_endpoint('mt-blackhole');"

# ═══════════════════════════════════════════════════
# TEST 9: Configured client_id with several workers
# ═══════════════════════════════════════════════════
header "TEST 9: client_id Per Worker"

# With one shared client_id, each connect would make the broker close another
# worker's connection (session takeover). With ulak.workers > 1 each worker
# connects as <client_id>_<worker number>.
MQTT_WORKERS=$(psql_exec "SELECT current_setting('ulak.workers');")
if [ "$MQTT_WORKERS" -gt 1 ] 2>/dev/null; then
  psql_quiet "DELETE FROM ulak.queue;"
  CID_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  create_endpoint "mt-cid" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/cid\", \"qos\": 1, \"client_id\": \"mt-fixed\"}" "mqtt"
  CID_ROWS=$((MQTT_WORKERS * 10))
  psql_quiet "SELECT ulak.send_batch('mt-cid', ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, ${CID_ROWS}) g));"
  wait_queue_drain 30
  CID_OK=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'mt-cid' AND q.status = 'completed' AND q.retry_count = 0;")
  assert_eq "All ${CID_ROWS} rows delivered on the first attempt" "$CID_ROWS" "$CID_OK"
  CID_TAKEOVER=$(docker logs --since "$CID_SINCE" ulak-mosquitto-1 2>&1 | grep -c "already connected, closing old connection" || true)
  assert_eq "No session takeover in the broker log" "0" "$CID_TAKEOVER"
  CID_IDS=$(docker logs --since "$CID_SINCE" ulak-mosquitto-1 2>&1 | grep -o "as mt-fixed_[0-9]*" | sort -u | wc -l | tr -d ' ')
  assert_gt "Workers connected under distinct ids (mt-fixed_N)" "0" "$CID_IDS"
else
  echo -e "  ${YELLOW}⊘${NC} skipped: ulak.workers = ${MQTT_WORKERS}, needs more than 1"
  SKIP=$((SKIP + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 10: Default QoS is 1 (at-least-once)
# ═══════════════════════════════════════════════════
header "TEST 10: Default QoS"

# QoS 0 marks the row delivered once the PUBLISH is written, with no broker
# acknowledgment, so ulak.mqtt_default_qos is 1. A QoS 2 subscriber prints
# the QoS the message was published with (%q).
psql_quiet "DELETE FROM ulak.queue;"
create_endpoint "mt-default-qos" "{\"broker\": \"${MQTT_BROKER}\", \"port\": ${MQTT_PORT}, \"topic\": \"mt/default-qos\"}" "mqtt"
docker exec ulak-postgres-1 timeout 15 mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "mt/default-qos" -q 2 -F '%q' -C 1 -W 15 > /tmp/mqtt-sub-10.txt 2>/dev/null &
SUB_PID=$!
sleep 1
send_msg "mt-default-qos" "{\"test\": \"default-qos\"}"
wait_queue_drain 15
wait $SUB_PID 2>/dev/null || true
assert_eq "Endpoint without qos publishes at QoS 1" "1" "$(tr -d '[:space:]' < /tmp/mqtt-sub-10.txt)"
rm -f /tmp/mqtt-sub-10.txt

# ═══════════════════════════════════════════════════
# TEST 11: CONNACK refusals (server unavailable vs not authorized)
# ═══════════════════════════════════════════════════
header "TEST 11: CONNACK Refusal Classification"

# Every refused CONNACK surfaces from mosquitto_loop() as MOSQ_ERR_CONN_REFUSED.
# "Server unavailable" (return code 3) stays retryable; only refusals that will
# not clear by themselves (bad credentials, not authorized, ...) are permanent.
# A fake broker on the Docker host answers every CONNECT with one code.
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"
FAKE_MQTT="$SCRIPT_DIR/fixtures/fake-mqtt-broker.py"
python3 "$FAKE_MQTT" 1885 3 > /dev/null 2>&1 &
FAKE3_PID=$!
python3 "$FAKE_MQTT" 1886 5 > /dev/null 2>&1 &
FAKE5_PID=$!
sleep 1

create_endpoint "mt-connack3" "{\"broker\": \"${WEBHOOK_HOST}\", \"port\": 1885, \"topic\": \"mt/connack\", \"qos\": 1}" "mqtt"
create_endpoint "mt-connack5" "{\"broker\": \"${WEBHOOK_HOST}\", \"port\": 1886, \"topic\": \"mt/connack\", \"qos\": 1}" "mqtt"
send_msg "mt-connack3" "{\"connack\": 3}"
send_msg "mt-connack5" "{\"connack\": 5}"

C3_ERR=""
for _ in $(seq 1 20); do
  C3_ERR=$(psql_exec "SELECT last_error FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-connack3') AND retry_count >= 1;")
  [ -n "$C3_ERR" ] && break
  sleep 1
done
C5_ERR=""
for _ in $(seq 1 20); do
  C5_ERR=$(psql_exec "SELECT d.last_error FROM ulak.dlq d WHERE d.endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-connack5');")
  [ -n "$C5_ERR" ] && break
  sleep 1
done
echo "  rc 3: ${C3_ERR}"
echo "  rc 5: ${C5_ERR}"
assert_eq "CONNACK 3 (server unavailable): [RETRYABLE]" "[RETRYABLE]" "${C3_ERR:0:11}"
assert_eq "CONNACK 3: row kept for retry" "pending" "$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-connack3');")"
assert_eq "CONNACK 3: nothing in the DLQ" "0" "$(psql_exec "SELECT count(*) FROM ulak.dlq WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'mt-connack3');")"
assert_eq "CONNACK 5 (not authorized): [PERMANENT], dead-lettered" "[PERMANENT]" "${C5_ERR:0:11}"
psql_quiet "SELECT ulak.disable_endpoint('mt-connack3');"
psql_quiet "SELECT ulak.disable_endpoint('mt-connack5');"
kill "$FAKE3_PID" "$FAKE5_PID" 2>/dev/null || true

# ═══════════════════════════════════════════════════
# TEST 12: MQTT 5 reports publishes the broker refuses
# ═══════════════════════════════════════════════════
header "TEST 12: MQTT 5 Publish Refusals"

# A second Mosquitto on the compose network. Port 1883: anonymous clients may
# publish to mt/acl/ok only. Port 1884: anonymous connections are refused.
# MQTT 3.1.1 acknowledgments carry no reason code and Mosquitto acknowledges a
# publish its ACL refused, so only "protocol_version": "5" can report it.
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"
ACL_NET=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' ulak-postgres-1 | awk '{print $1}')
docker rm -f ulak-mqtt-acl > /dev/null 2>&1 || true
docker run -d --rm --name ulak-mqtt-acl --network "$ACL_NET" eclipse-mosquitto:2 sh -c '
  printf "topic readwrite mt/acl/ok\n" > /tmp/acl
  printf "per_listener_settings true\nlistener 1883\nallow_anonymous true\nacl_file /tmp/acl\nlistener 1884\nallow_anonymous false\n" > /tmp/m.conf
  exec mosquitto -c /tmp/m.conf' > /dev/null
for _ in $(seq 1 20); do
  docker exec ulak-postgres-1 mosquitto_pub -h ulak-mqtt-acl -p 1883 -t mt/acl/ok -m ping > /dev/null 2>&1 && break
  sleep 1
done

ACL_EP() { echo "{\"broker\": \"ulak-mqtt-acl\", \"port\": ${2:-1883}, \"topic\": \"$1\", \"qos\": ${3:-1}${4:+, \"protocol_version\": \"$4\"}}"; }
create_endpoint "mt-v5-denied"     "$(ACL_EP mt/acl/denied 1883 1 5)" "mqtt"
create_endpoint "mt-v5-denied-q2"  "$(ACL_EP mt/acl/denied 1883 2 5)" "mqtt"
create_endpoint "mt-v5-ok"         "$(ACL_EP mt/acl/ok 1883 1 5)" "mqtt"
create_endpoint "mt-v311-denied"   "$(ACL_EP mt/acl/denied 1883 1)" "mqtt"
create_endpoint "mt-v5-refused"    "$(ACL_EP mt/acl/ok 1884 1 5)" "mqtt"

docker exec ulak-postgres-1 timeout 15 mosquitto_sub -h ulak-mqtt-acl -p 1883 -V mqttv5 -t mt/acl/ok -q 1 -C 1 -W 15 > /tmp/mqtt-sub-12.txt 2>/dev/null &
SUB_PID=$!
sleep 1
for ep in mt-v5-denied mt-v5-denied-q2 mt-v5-ok mt-v311-denied mt-v5-refused; do
  send_msg "$ep" "{\"test\": \"$ep\"}"
done

acl_dlq_error() {
  psql_exec "SELECT d.last_error FROM ulak.dlq d JOIN ulak.endpoints e ON e.id = d.endpoint_id WHERE e.name = '$1' LIMIT 1;"
}
for _ in $(seq 1 30); do
  [ "$(psql_exec "SELECT count(*) FROM ulak.dlq d JOIN ulak.endpoints e ON e.id = d.endpoint_id WHERE e.name IN ('mt-v5-denied', 'mt-v5-denied-q2', 'mt-v5-refused');")" = "3" ] &&
    [ "$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON e.id = q.endpoint_id WHERE e.name IN ('mt-v5-ok', 'mt-v311-denied') AND q.status = 'completed';")" = "2" ] && break
  sleep 1
done
wait $SUB_PID 2>/dev/null || true

V5_Q1_ERR=$(acl_dlq_error mt-v5-denied)
V5_Q2_ERR=$(acl_dlq_error mt-v5-denied-q2)
V5_CONN_ERR=$(acl_dlq_error mt-v5-refused)
echo "  QoS 1 refused:     ${V5_Q1_ERR}"
echo "  QoS 2 refused:     ${V5_Q2_ERR}"
echo "  connect refused:   ${V5_CONN_ERR}"
assert_eq "MQTT 5, QoS 1: refused publish dead-lettered as [PERMANENT]" "[PERMANENT]" "${V5_Q1_ERR:0:11}"
assert_contains "MQTT 5, QoS 1: error names the refusal" "MQTT broker refused the publish" "$V5_Q1_ERR"
assert_contains "MQTT 5, QoS 1: reason code 135 (not authorized)" "reason code 135" "$V5_Q1_ERR"
assert_eq "MQTT 5, QoS 2: refused publish dead-lettered as [PERMANENT] (PUBREC)" "[PERMANENT]" "${V5_Q2_ERR:0:11}"
assert_contains "MQTT 5, QoS 2: error names the refusal" "MQTT broker refused the publish" "$V5_Q2_ERR"
assert_eq "MQTT 5: refused CONNACK is [PERMANENT]" "[PERMANENT]" "${V5_CONN_ERR:0:11}"
assert_contains "MQTT 5: refused CONNACK reported with its reason" "MQTT connection refused" "$V5_CONN_ERR"
assert_eq "MQTT 5: allowed topic completed" "completed" "$(psql_exec "SELECT q.status FROM ulak.queue q JOIN ulak.endpoints e ON e.id = q.endpoint_id WHERE e.name = 'mt-v5-ok';")"
assert_eq "MQTT 5: allowed topic delivered" "1" "$(grep -c 'mt-v5-ok' /tmp/mqtt-sub-12.txt 2>/dev/null || echo 0)"
# The known 3.1.1 limitation: the refused publish is acknowledged and completes
assert_eq "MQTT 3.1.1: refused publish still completes (no reason code)" "completed" "$(psql_exec "SELECT q.status FROM ulak.queue q JOIN ulak.endpoints e ON e.id = q.endpoint_id WHERE e.name = 'mt-v311-denied';")"
rm -f /tmp/mqtt-sub-12.txt

section "Synchronous path (capture_response = on)"
CAPTURE_AUTO=$(psql_exec "SELECT coalesce(sourcefile LIKE '%postgresql.auto.conf', false) FROM pg_settings WHERE name = 'ulak.capture_response';")
CAPTURE_SAVED=$(psql_exec "SHOW ulak.capture_response;")
docker exec ulak-postgres-1 psql -U postgres -d "$DB" -q -c "ALTER SYSTEM SET ulak.capture_response = on;" -c "SELECT pg_reload_conf();" > /dev/null 2>&1
sleep 1
create_endpoint "mt-v5-denied-sync" "$(ACL_EP mt/acl/denied 1883 1 5)" "mqtt"
send_msg "mt-v5-denied-sync" "{\"test\": \"sync\"}"
V5_SYNC_ERR=""
for _ in $(seq 1 20); do
  V5_SYNC_ERR=$(acl_dlq_error mt-v5-denied-sync)
  [ -n "$V5_SYNC_ERR" ] && break
  sleep 1
done
echo "  sync refused:      ${V5_SYNC_ERR}"
assert_eq "MQTT 5, sync dispatch: refused publish dead-lettered as [PERMANENT]" "[PERMANENT]" "${V5_SYNC_ERR:0:11}"
assert_contains "MQTT 5, sync dispatch: error names the refusal" "MQTT broker refused the publish" "$V5_SYNC_ERR"
if [ "$CAPTURE_AUTO" = "t" ]; then
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -q -c "ALTER SYSTEM SET ulak.capture_response = ${CAPTURE_SAVED};" -c "SELECT pg_reload_conf();" > /dev/null 2>&1
else
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -q -c "ALTER SYSTEM RESET ulak.capture_response;" -c "SELECT pg_reload_conf();" > /dev/null 2>&1
fi
for ep in mt-v5-denied mt-v5-denied-q2 mt-v5-ok mt-v311-denied mt-v5-refused mt-v5-denied-sync; do
  psql_quiet "SELECT ulak.disable_endpoint('$ep');"
done
docker rm -f ulak-mqtt-acl > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'mt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

print_summary
exit $?
