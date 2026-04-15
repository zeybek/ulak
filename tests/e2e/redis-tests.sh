#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak Redis Streams E2E Test Suite
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

REDIS_HOST="redis"

# ─── Redis helpers ───

redis_cmd() {
  docker exec ulak-postgres-1 redis-cli -h "$REDIS_HOST" "$@" 2>/dev/null
}

redis_xlen() {
  redis_cmd XLEN "$1" | tr -d ' '
}

redis_xrange() {
  redis_cmd XRANGE "$1" - +
}

redis_del() {
  redis_cmd DEL "$1" > /dev/null
}

redis_ready() {
  redis_cmd PING | grep -q PONG
}

# ─── Pre-flight ───

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak Redis Streams E2E Test Suite                   ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

if ! redis_ready; then
  echo -e "${RED}ERROR: Redis not running${NC}"
  exit 1
fi

if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not running${NC}"
  exit 1
fi

echo -e "${GREEN}Pre-flight OK${NC} — redis:6379 ready, pg:ready"

# Setup GUCs
psql_quiet "ALTER SYSTEM SET ulak.database = '$DB';"
psql_quiet "ALTER SYSTEM SET ulak.poll_interval = 100;"
docker restart ulak-postgres-1 > /dev/null 2>&1
sleep 4

# Clean slate
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'rt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# ═══════════════════════════════════════════════════
# TEST 1: Single Message Produce + Consume
# ═══════════════════════════════════════════════════
header "TEST 1: Single Message → Redis Stream"

redis_del "rt-single"
create_endpoint "rt-single" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-single\"}" "redis"
send_msg "rt-single" "{\"test\": \"single\", \"ts\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'rt-single';")
assert_eq "Message completed" "1" "$COMPLETED"

XLEN=$(redis_xlen "rt-single")
assert_eq "Message in Redis stream" "1" "$XLEN"

# Verify payload in stream
PAYLOAD=$(redis_cmd XRANGE rt-single - + 2>/dev/null | grep -A1 "payload" | tail -1)
assert_contains "Payload intact" "single" "$PAYLOAD"

# ═══════════════════════════════════════════════════
# TEST 2: Batch Produce (100 messages)
# ═══════════════════════════════════════════════════
header "TEST 2: Batch Produce (100 messages)"

psql_quiet "DELETE FROM ulak.queue;"
redis_del "rt-batch"
create_endpoint "rt-batch" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-batch\"}" "redis"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 100) g);
  PERFORM ulak.send_batch('rt-batch', a);
END; \$\$;"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'rt-batch';")
assert_eq "100 messages completed" "100" "$COMPLETED"

sleep 1
XLEN=$(redis_xlen "rt-batch")
assert_eq "100 messages in Redis" "100" "$XLEN"

# ═══════════════════════════════════════════════════
# TEST 3: Stream ID Capture
# ═══════════════════════════════════════════════════
header "TEST 3: Stream ID Capture"

psql_quiet "DELETE FROM ulak.queue;"
redis_del "rt-streamid"
create_endpoint "rt-streamid" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-streamid\"}" "redis"

send_msg "rt-streamid" "{\"capture\": true}"
wait_queue_drain 15

# Stream should have exactly 1 entry
XLEN=$(redis_xlen "rt-streamid")
assert_eq "Stream has 1 entry" "1" "$XLEN"

# Get the stream ID from Redis
STREAM_ID=$(redis_cmd XRANGE rt-streamid - + 2>/dev/null | head -1)
TOTAL=$((TOTAL + 1))
if echo "$STREAM_ID" | grep -qE '^[0-9]+-[0-9]+$'; then
  echo -e "  ${GREEN}✓${NC} Valid stream ID: $STREAM_ID"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} Invalid stream ID: $STREAM_ID"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 4: MAXLEN Trimming
# ═══════════════════════════════════════════════════
header "TEST 4: MAXLEN Trimming"

psql_quiet "DELETE FROM ulak.queue;"
redis_del "rt-maxlen"
create_endpoint "rt-maxlen" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-maxlen\", \"maxlen\": 50, \"maxlen_approximate\": false}" "redis"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 100) g);
  PERFORM ulak.send_batch('rt-maxlen', a);
END; \$\$;"

wait_queue_drain 15
sleep 1

XLEN=$(redis_xlen "rt-maxlen")
TOTAL=$((TOTAL + 1))
if [ "$XLEN" -le 55 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} MAXLEN trimming works (stream length: $XLEN, limit: 50)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} MAXLEN not enforced (stream length: $XLEN, expected <= 55)"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 5: Multiple Streams (Fan-out)
# ═══════════════════════════════════════════════════
header "TEST 5: Multiple Streams (3 endpoints × 50 messages)"

psql_quiet "DELETE FROM ulak.queue;"

for i in 1 2 3; do
  redis_del "rt-fan-$i"
  create_endpoint "rt-fan-$i" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-fan-$i\"}" "redis"
done

psql_quiet "
DO \$\$
BEGIN
  FOR ep IN 1..3 LOOP
    PERFORM ulak.send_batch('rt-fan-' || ep,
      ARRAY(SELECT jsonb_build_object('ep', ep, 'i', g) FROM generate_series(1, 50) g));
  END LOOP;
END; \$\$;"

wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE 'rt-fan-%';")
assert_eq "150 messages completed" "150" "$COMPLETED"

TOTAL_REDIS=0
for i in 1 2 3; do
  C=$(redis_xlen "rt-fan-$i")
  TOTAL_REDIS=$((TOTAL_REDIS + C))
done
assert_eq "150 across 3 streams" "150" "$TOTAL_REDIS"

# ═══════════════════════════════════════════════════
# TEST 6: Throughput (1K messages)
# ═══════════════════════════════════════════════════
header "TEST 6: Throughput (1K messages)"

psql_quiet "DELETE FROM ulak.queue;"
redis_del "rt-throughput"
create_endpoint "rt-throughput" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-throughput\"}" "redis"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 1000) g);
  PERFORM ulak.send_batch('rt-throughput', a);
END; \$\$;"

wait_queue_drain 15

THROUGHPUT=$(psql_exec "
SELECT round(count(*)::numeric / GREATEST(extract(epoch from max(q.completed_at)-min(q.created_at)), 0.001))
FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'rt-throughput';")

TOTAL=$((TOTAL + 1))
if [ "$THROUGHPUT" -gt 100 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} Redis throughput: ${THROUGHPUT} msg/s (>100)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} Redis throughput too low: ${THROUGHPUT} msg/s"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 7: Error Classification (connection failure)
# ═══════════════════════════════════════════════════
header "TEST 7: Error Classification"

psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"

create_endpoint "rt-err" "{\"host\": \"nonexistent-host\", \"stream_key\": \"rt-err\"}" "redis"
send_msg "rt-err" "{\"should_fail\": true}"

sleep 10

STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'rt-err') LIMIT 1;")
ERROR=$(psql_exec "SELECT left(last_error, 12) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'rt-err') LIMIT 1;")

assert_eq "Failed message pending (retryable)" "pending" "$STATUS"
assert_contains "Error has RETRYABLE prefix" "RETRYABLE" "$ERROR"

# ═══════════════════════════════════════════════════
# TEST 8: Field Format Verification
# ═══════════════════════════════════════════════════
header "TEST 8: Field Format (payload + ts)"

psql_quiet "DELETE FROM ulak.queue;"
redis_del "rt-fields"
create_endpoint "rt-fields" "{\"host\": \"${REDIS_HOST}\", \"stream_key\": \"rt-fields\"}" "redis"

send_msg "rt-fields" "{\"order_id\": 42, \"amount\": 99.99}"
wait_queue_drain 15

# Check that stream entry has 'payload' and 'ts' fields
FIELDS=$(redis_cmd XRANGE rt-fields - + 2>/dev/null)
assert_contains "Has payload field" "payload" "$FIELDS"
assert_contains "Has ts field" "ts" "$FIELDS"
assert_contains "Payload has order_id" "order_id" "$FIELDS"

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'rt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

print_summary
exit $?
