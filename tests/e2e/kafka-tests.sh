#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak Kafka E2E Test Suite
# Tests: produce/consume, batch, keys, headers,
#        error handling, circuit breaker
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

KAFKA_BROKER="kafka:9092"

# ─── Kafka helpers ───

kcat_consume() {
  local topic="$1" timeout="${2:-5}"
  docker exec ulak-postgres-1 timeout "$timeout" kcat -b "$KAFKA_BROKER" -t "$topic" -C -e -o beginning -J 2>/dev/null
}

kcat_count() {
  local topic="$1"
  docker exec ulak-postgres-1 timeout 5 kcat -b "$KAFKA_BROKER" -t "$topic" -C -e -o beginning 2>/dev/null | wc -l | tr -d ' '
}

kafka_create_topic() {
  local topic="$1" partitions="${2:-3}"
  # Delete + recreate for clean state
  docker exec ulak-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
  sleep 1
  docker exec ulak-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic "$topic" \
    --partitions "$partitions" --replication-factor 1 2>/dev/null || true
}

kafka_delete_topic() {
  local topic="$1"
  docker exec ulak-kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
}

kafka_ready() {
  docker exec ulak-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 > /dev/null 2>&1
}

# ─── Pre-flight ───

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak Kafka E2E Test Suite                           ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

if ! kafka_ready; then
  echo -e "${RED}ERROR: Kafka broker not running${NC}"
  exit 1
fi

if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not running${NC}"
  exit 1
fi

echo -e "${GREEN}Pre-flight OK${NC} — kafka:9092 ready, pg:ready"

# Setup extension + GUCs
psql_quiet "ALTER SYSTEM SET ulak.database = '$DB';"
psql_quiet "ALTER SYSTEM SET ulak.poll_interval = 100;"
psql_quiet "ALTER SYSTEM SET ulak.kafka_flush_timeout = 1000;"
psql_quiet "ALTER SYSTEM SET ulak.batch_size = 200;"
psql_quiet "ALTER SYSTEM SET ulak.kafka_batch_capacity = 200;"
docker restart ulak-postgres-1 > /dev/null 2>&1
sleep 4

# Clean slate
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'kt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# ═══════════════════════════════════════════════════
# TEST 1: Single Message Produce + Consume
# ═══════════════════════════════════════════════════
header "TEST 1: Single Message Produce + Consume"

kafka_create_topic "kt-single"
create_endpoint "kt-single" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-single\"}" "kafka"
send_msg "kt-single" "{\"test\": \"single\", \"ts\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'kt-single';")
assert_eq "Message completed in queue" "1" "$COMPLETED"

KAFKA_MSG=$(kcat_count "kt-single")
assert_eq "Message in Kafka topic" "1" "$KAFKA_MSG"

# Verify payload integrity
PAYLOAD=$(docker exec ulak-postgres-1 timeout 5 kcat -b "$KAFKA_BROKER" -t kt-single -C -e -o beginning 2>/dev/null | head -1)
assert_contains "Payload contains test field" "single" "$PAYLOAD"

# ═══════════════════════════════════════════════════
# TEST 2: Batch Produce (100 messages)
# ═══════════════════════════════════════════════════
header "TEST 2: Batch Produce (100 messages)"

psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-batch"
create_endpoint "kt-batch" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-batch\"}" "kafka"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g, 'batch', true) FROM generate_series(1, 100) g);
  PERFORM ulak.send_batch('kt-batch', a);
END; \$\$;"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'kt-batch';")
assert_eq "100 messages completed" "100" "$COMPLETED"

sleep 2
KAFKA_MSG=$(kcat_count "kt-batch")
assert_eq "100 messages in Kafka" "100" "$KAFKA_MSG"

# ═══════════════════════════════════════════════════
# TEST 3: Key-Based Partitioning
# ═══════════════════════════════════════════════════
header "TEST 3: Key-Based Partitioning"

psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-keys" 3
create_endpoint "kt-keys" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-keys\", \"key\": \"same-key\"}" "kafka"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 50) g);
  PERFORM ulak.send_batch('kt-keys', a);
END; \$\$;"

wait_queue_drain 15
sleep 2

# All messages with same key should go to same partition
PARTITIONS=$(docker exec ulak-postgres-1 timeout 5 kcat -b "$KAFKA_BROKER" -t kt-keys -C -e -o beginning -f '%p\n' 2>/dev/null | sort -u | wc -l | tr -d ' ')
assert_eq "All messages on same partition (same key)" "1" "$PARTITIONS"

# ═══════════════════════════════════════════════════
# TEST 4: Kafka Headers
# ═══════════════════════════════════════════════════
header "TEST 4: Kafka Headers"

psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-headers"
create_endpoint "kt-headers" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-headers\", \"headers\": {\"ce_type\": \"order.created\", \"x-source\": \"ulak\"}}" "kafka"

send_msg "kt-headers" "{\"order_id\": 42}"
wait_queue_drain 15
sleep 2

# Check headers using kcat format string (most reliable)
RAW_HEADERS=$(docker exec ulak-postgres-1 timeout 5 kcat -b "$KAFKA_BROKER" -t kt-headers -C -e -o beginning -f '%h\n' 2>/dev/null | head -1)
if echo "$RAW_HEADERS" | grep -q "ce_type"; then
  TOTAL=$((TOTAL + 1)); PASS=$((PASS + 1))
  echo -e "  ${GREEN}✓${NC} Kafka headers present: $RAW_HEADERS"
else
  TOTAL=$((TOTAL + 1)); FAIL=$((FAIL + 1))
  echo -e "  ${RED}✗${NC} Kafka headers missing (got: $RAW_HEADERS)"
fi

KAFKA_MSG=$(kcat_count "kt-headers")
assert_eq "Message delivered to Kafka" "1" "$KAFKA_MSG"

# ── Per-message headers (ulak.queue.headers is a flat object; overrides static keys) ──
section "Per-message headers override static headers..."
psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-msg-headers"
create_endpoint "kt-msg-headers" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-msg-headers\", \"headers\": {\"x-source\": \"static\"}}" "kafka"

# ulak.queue.headers is immutable after insert (audit trigger), so set it on the
# row itself the way an admin/ETL path would (send() has no headers parameter).
psql_quiet "INSERT INTO ulak.queue (endpoint_id, payload, headers) SELECT id, '{\"order_id\": 43}'::jsonb, '{\"x-trace\": \"trace-43\", \"x-source\": \"per-message\"}'::jsonb FROM ulak.endpoints WHERE name = 'kt-msg-headers';"

wait_queue_drain 20
sleep 2

MSG_HEADERS=$(docker exec ulak-postgres-1 timeout 5 kcat -b "$KAFKA_BROKER" -t kt-msg-headers -C -e -o beginning -f '%h\n' 2>/dev/null | head -1)
assert_contains "Per-message header forwarded" "x-trace=trace-43" "$MSG_HEADERS"
assert_contains "Per-message header overrides static" "x-source=per-message" "$MSG_HEADERS"

# ═══════════════════════════════════════════════════
# TEST 5: Throughput (1K messages)
# ═══════════════════════════════════════════════════
header "TEST 5: Throughput (1K messages)"

psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-throughput"
create_endpoint "kt-throughput" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-throughput\"}" "kafka"

RESULT=$(psql_exec "
DO \$\$ DECLARE a jsonb[]; t0 timestamptz;
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 1000) g);
  t0 := clock_timestamp();
  PERFORM ulak.send_batch('kt-throughput', a);
  RAISE NOTICE 'enqueue=%ms', round(extract(epoch from (clock_timestamp()-t0))*1000);
END; \$\$;")

wait_queue_drain 15

THROUGHPUT=$(psql_exec "
SELECT round(count(*)::numeric / GREATEST(extract(epoch from max(q.completed_at)-min(q.created_at)), 0.001))
FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'kt-throughput';")

TOTAL=$((TOTAL + 1))
if [ "$THROUGHPUT" -gt 100 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} Kafka throughput: ${THROUGHPUT} msg/s (>100)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} Kafka throughput too low: ${THROUGHPUT} msg/s"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 6: Idempotent Producer (no duplicates)
# ═══════════════════════════════════════════════════
header "TEST 6: Idempotent Producer"

psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-idemp"
create_endpoint "kt-idemp" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-idemp\"}" "kafka"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 500) g);
  PERFORM ulak.send_batch('kt-idemp', a);
END; \$\$;"

wait_queue_drain 30
sleep 3

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'kt-idemp';")
KAFKA_MSG=$(kcat_count "kt-idemp")

assert_eq "500 completed in queue" "500" "$COMPLETED"
assert_eq "500 in Kafka (no duplicates)" "500" "$KAFKA_MSG"

# ═══════════════════════════════════════════════════
# TEST 7: Multiple Topics (Fan-out)
# ═══════════════════════════════════════════════════
header "TEST 7: Multiple Topics (3 endpoints × 100 messages)"

psql_quiet "DELETE FROM ulak.queue;"

for i in 1 2 3; do
  kafka_create_topic "kt-fan-$i"
  create_endpoint "kt-fan-$i" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-fan-$i\"}" "kafka"
done

psql_quiet "
DO \$\$
BEGIN
  FOR ep IN 1..3 LOOP
    PERFORM ulak.send_batch('kt-fan-' || ep,
      ARRAY(SELECT jsonb_build_object('ep', ep, 'i', g) FROM generate_series(1, 100) g));
  END LOOP;
END; \$\$;"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE 'kt-fan-%';")
assert_eq "300 messages completed" "300" "$COMPLETED"

TOTAL_KAFKA=0
for i in 1 2 3; do
  C=$(kcat_count "kt-fan-$i")
  TOTAL_KAFKA=$((TOTAL_KAFKA + C))
done
assert_eq "300 messages across 3 Kafka topics" "300" "$TOTAL_KAFKA"

# ═══════════════════════════════════════════════════
# TEST 8: Error Classification (DLQ)
# ═══════════════════════════════════════════════════
header "TEST 8: Error Classification"

psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"

psql_quiet "ALTER SYSTEM SET ulak.capture_response = off;"
psql_quiet "SELECT pg_reload_conf();"
sleep 1

# Create endpoint pointing to non-existent broker — will timeout (retryable)
create_endpoint "kt-err" "{\"broker\": \"nonexistent:9092\", \"topic\": \"kt-err\"}" "kafka"

send_msg "kt-err" "{\"should_fail\": true}"

# The row is claimed ('processing') at once and stays so until librdkafka gives
# up after ulak.kafka_delivery_timeout (30 s by default); wait for the retry to
# be recorded instead of sampling at a fixed moment.
ERROR=""
for _ in $(seq 1 50); do
  ERROR=$(psql_exec "SELECT left(last_error, 12) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'kt-err') LIMIT 1;")
  [ -n "$ERROR" ] && break
  sleep 1
done

# Should be back in pending with a retry scheduled (retryable error)
STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'kt-err') LIMIT 1;")
echo "  last_error: $(psql_exec "SELECT left(last_error, 120) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'kt-err') LIMIT 1;")"

assert_eq "Failed message still pending (retryable)" "pending" "$STATUS"
assert_contains "Error has RETRYABLE prefix" "RETRYABLE" "$ERROR"

# ═══════════════════════════════════════════════════
# TEST 8b: Error prefix on the synchronous path
# ═══════════════════════════════════════════════════
header "TEST 8b: Error Prefix (capture_response)"

# The worker reads [PERMANENT]/[RETRYABLE] only at the start of last_error.
# A prefix anywhere else is not seen, and a permanent failure would be retried.
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "ALTER SYSTEM SET ulak.capture_response = on;"
psql_quiet "ALTER SYSTEM SET ulak.kafka_delivery_timeout = 5000;"
psql_quiet "SELECT pg_reload_conf();"
sleep 2
# message.timeout.ms below the 5 s wait: librdkafka's own failure report
# arrives first, which is the path under test
create_endpoint "kt-err-sync" "{\"broker\": \"nonexistent:9092\", \"topic\": \"kt-err\", \"options\": {\"message.timeout.ms\": \"2000\"}}" "kafka"
send_msg "kt-err-sync" "{\"should_fail\": true}"
SYNC_ERR=""
for _ in $(seq 1 30); do
  SYNC_ERR=$(psql_exec "SELECT last_error FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'kt-err-sync') AND retry_count >= 1;")
  [ -n "$SYNC_ERR" ] && break
  sleep 1
done
echo "  last_error: ${SYNC_ERR:0:120}"
assert_eq "Sync path: classification prefix at the start" "[RETRYABLE]" "${SYNC_ERR:0:11}"
assert_contains "Sync path: the failure came from the delivery report" "delivery failed" "$SYNC_ERR"
psql_quiet "SELECT ulak.disable_endpoint('kt-err-sync');"
psql_quiet "ALTER SYSTEM RESET ulak.capture_response;"
psql_quiet "ALTER SYSTEM RESET ulak.kafka_delivery_timeout;"
psql_quiet "SELECT pg_reload_conf();"

# ═══════════════════════════════════════════════════
# TEST 9: acks=1 and non-string options
# ═══════════════════════════════════════════════════
header "TEST 9: acks=1 + Numeric Option Values"

# enable.idempotence is on by default and librdkafka refuses it with acks=1,
# so acks below "all" turns idempotence off. Options given as JSON numbers
# ("linger.ms": 10) reach librdkafka too.
psql_quiet "DELETE FROM ulak.queue;"
kafka_create_topic "kt-acks1"
create_endpoint "kt-acks1" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-acks1\", \"options\": {\"acks\": \"1\", \"linger.ms\": 10}}" "kafka"
ACKS1_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
send_msg "kt-acks1" "{\"test\": \"acks1\"}"
wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'kt-acks1';")
assert_eq "acks=1 endpoint delivers" "1" "$COMPLETED"
assert_eq "acks=1 message in Kafka" "1" "$(kcat_count kt-acks1)"
IGNORED=$(docker logs --since "$ACKS1_SINCE" ulak-postgres-1 2>&1 | grep -c "Ignoring Kafka option\|Failed to set Kafka option" || true)
assert_eq "No option dropped or rejected" "0" "$IGNORED"

# ═══════════════════════════════════════════════════
# TEST 10: Late delivery report during synchronous dispatch
# ═══════════════════════════════════════════════════
header "TEST 10: Late Delivery Report (capture_response)"

# With capture_response on, each message is produced and waited for alone,
# with its own opaque, so the late report of a message that timed out cannot
# complete the next one: every captured offset must be the offset of that
# row's own payload.
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "ALTER SYSTEM SET ulak.capture_response = on;"
psql_quiet "ALTER SYSTEM SET ulak.kafka_delivery_timeout = 5000;"
psql_quiet "SELECT pg_reload_conf();"
sleep 2
kafka_create_topic "kt-late" 1
create_endpoint "kt-late" "{\"broker\": \"${KAFKA_BROKER}\", \"topic\": \"kt-late\"}" "kafka"
send_msg "kt-late" "{\"m\": \"warm\"}"
wait_queue_drain 20

docker pause ulak-kafka-1 > /dev/null
send_msg "kt-late" "{\"m\": \"A\"}"
A_RETRIED="f"
for _ in $(seq 1 40); do
  A_RETRIED=$(psql_exec "SELECT (retry_count >= 1) FROM ulak.queue WHERE payload->>'m' = 'A';")
  [ "$A_RETRIED" = "t" ] && break
  sleep 0.5
done
docker unpause ulak-kafka-1 > /dev/null
assert_eq "Message produced while the broker is frozen times out as retryable" "t" "$A_RETRIED"

send_msg "kt-late" "{\"m\": \"B\"}"
psql_quiet "UPDATE ulak.queue SET next_retry_at = now() WHERE payload->>'m' = 'A';"
DONE=0
for _ in $(seq 1 60); do
  DONE=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE payload->>'m' IN ('A', 'B') AND status = 'completed';")
  [ "$DONE" = "2" ] && break
  sleep 1
done
assert_eq "A (after retry) and B completed" "2" "$DONE"
sleep 2

ROWS=$(psql_exec "SELECT payload->>'m' || '=' || coalesce(response->>'kafka_offset', 'none') FROM ulak.queue WHERE payload->>'m' IN ('warm', 'A', 'B') ORDER BY id;")
MISMATCH=$(kcat_consume "kt-late" 8 | ROWS="$ROWS" python3 -c '
import json, os, sys
offsets = {}
for line in sys.stdin:
    try:
        rec = json.loads(line)
        offsets.setdefault(json.loads(rec["payload"])["m"], set()).add(str(rec["offset"]))
    except Exception:
        pass
bad = [row for row in os.environ["ROWS"].split()
       if row.split("=")[1] not in offsets.get(row.split("=")[0], set())]
print(",".join(bad) if bad else "none")
')
assert_eq "Every captured kafka_offset belongs to that row's own message" "none" "$MISMATCH"

psql_quiet "ALTER SYSTEM RESET ulak.capture_response;"
psql_quiet "ALTER SYSTEM RESET ulak.kafka_delivery_timeout;"
psql_quiet "SELECT pg_reload_conf();"

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

# Clean queue + endpoints
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'kt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# Print summary
print_summary
exit $?
