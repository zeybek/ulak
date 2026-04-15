#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak AMQP E2E Test Suite
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

AMQP_HOST="rabbitmq"
AMQP_PORT=5672
AMQP_USER="guest"
AMQP_PASS="guest"

# ─── RabbitMQ helpers ───

rabbit_cmd() {
  docker exec ulak-rabbitmq-1 rabbitmqctl "$@" 2>/dev/null
}

rabbit_declare_queue() {
  local queue="$1"
  # Declare queue via rabbitmqadmin (management plugin)
  docker exec ulak-rabbitmq-1 rabbitmqadmin declare queue name="$queue" durable=false 2>/dev/null || \
    rabbit_cmd eval "rabbit_amqqueue:declare(rabbit_misc:r(<<\"/\">>, queue, <<\"$queue\">>), false, false, [], none, <<\"guest\">>)." 2>/dev/null || true
}

rabbit_queue_messages() {
  local queue="$1"
  rabbit_cmd list_queues name messages 2>/dev/null | grep "^${queue}" | awk '{print $2}' | tr -d ' '
}

rabbit_purge_queue() {
  local queue="$1"
  rabbit_cmd purge_queue "$queue" 2>/dev/null || true
}

rabbit_delete_queue() {
  local queue="$1"
  rabbit_cmd delete_queue "$queue" 2>/dev/null || true
}

rabbit_ready() {
  rabbit_cmd ping > /dev/null 2>&1
}

# ─── Pre-flight ───

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak AMQP E2E Test Suite                            ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

if ! rabbit_ready; then
  echo -e "${RED}ERROR: RabbitMQ not running${NC}"
  exit 1
fi

if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not running${NC}"
  exit 1
fi

echo -e "${GREEN}Pre-flight OK${NC} — rabbitmq:5672 ready, pg:ready"

# Setup GUCs
psql_quiet "ALTER SYSTEM SET ulak.database = '$DB';"
psql_quiet "ALTER SYSTEM SET ulak.poll_interval = 100;"
psql_quiet "ALTER SYSTEM SET ulak.amqp_delivery_timeout = 5000;"
docker restart ulak-postgres-1 > /dev/null 2>&1
sleep 4

# Clean slate
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'at-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# ═══════════════════════════════════════════════════
# TEST 1: Single Message Publish
# ═══════════════════════════════════════════════════
header "TEST 1: Single Message Publish"

rabbit_delete_queue "at-single"
rabbit_declare_queue "at-single"

create_endpoint "at-single" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-single\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"

send_msg "at-single" "{\"test\": \"amqp-single\"}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'at-single';")
assert_eq "Message completed" "1" "$COMPLETED"

QMSG=$(rabbit_queue_messages "at-single")
assert_eq "Message in RabbitMQ queue" "1" "$QMSG"

# ═══════════════════════════════════════════════════
# TEST 2: Batch Publish (100 messages)
# ═══════════════════════════════════════════════════
header "TEST 2: Batch Publish (100 messages)"

psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-batch"
rabbit_declare_queue "at-batch"

create_endpoint "at-batch" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-batch\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 100) g);
  PERFORM ulak.send_batch('at-batch', a);
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'at-batch';")
assert_eq "100 messages completed" "100" "$COMPLETED"

sleep 1
QMSG=$(rabbit_queue_messages "at-batch")
assert_eq "100 in RabbitMQ" "100" "$QMSG"

# ═══════════════════════════════════════════════════
# TEST 3: Multiple Routing Keys (Fan-out)
# ═══════════════════════════════════════════════════
header "TEST 3: Multiple Routing Keys (3 × 50 messages)"

psql_quiet "DELETE FROM ulak.queue;"

for i in 1 2 3; do
  rabbit_delete_queue "at-fan-$i"
  rabbit_declare_queue "at-fan-$i"
  create_endpoint "at-fan-$i" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-fan-$i\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
done

psql_quiet "
DO \$\$
BEGIN
  FOR ep IN 1..3 LOOP
    PERFORM ulak.send_batch('at-fan-' || ep,
      ARRAY(SELECT jsonb_build_object('ep', ep, 'i', g) FROM generate_series(1, 50) g));
  END LOOP;
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE 'at-fan-%';")
assert_eq "150 messages completed" "150" "$COMPLETED"

TOTAL_RMQ=0
for i in 1 2 3; do
  C=$(rabbit_queue_messages "at-fan-$i")
  TOTAL_RMQ=$((TOTAL_RMQ + ${C:-0}))
done
assert_eq "150 across 3 RabbitMQ queues" "150" "$TOTAL_RMQ"

# ═══════════════════════════════════════════════════
# TEST 4: Persistent Messages
# ═══════════════════════════════════════════════════
header "TEST 4: Persistent Messages"

psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-persist"
rabbit_declare_queue "at-persist"

create_endpoint "at-persist" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-persist\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\", \"persistent\": true}" "amqp"

send_msg "at-persist" "{\"persistent\": true}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'at-persist';")
assert_eq "Persistent message completed" "1" "$COMPLETED"

QMSG=$(rabbit_queue_messages "at-persist")
assert_eq "Persistent message in queue" "1" "$QMSG"

# ═══════════════════════════════════════════════════
# TEST 5: Throughput (500 messages)
# ═══════════════════════════════════════════════════
header "TEST 5: Throughput (500 messages)"

psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-throughput"
rabbit_declare_queue "at-throughput"

create_endpoint "at-throughput" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-throughput\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 500) g);
  PERFORM ulak.send_batch('at-throughput', a);
END; \$\$;"

wait_queue_drain 30

THROUGHPUT=$(psql_exec "
SELECT round(count(*)::numeric / GREATEST(extract(epoch from max(q.completed_at)-min(q.created_at)), 0.001))
FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'at-throughput';")

TOTAL=$((TOTAL + 1))
if [ "${THROUGHPUT:-0}" -ge 100 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} AMQP throughput: ${THROUGHPUT} msg/s (>=100)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} AMQP throughput too low: ${THROUGHPUT} msg/s"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 6: Error Classification
# ═══════════════════════════════════════════════════
header "TEST 6: Error Classification"

psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"

create_endpoint "at-err" "{\"host\": \"nonexistent-host\", \"exchange\": \"e\", \"routing_key\": \"r\", \"username\": \"guest\", \"password\": \"guest\"}" "amqp"
send_msg "at-err" "{\"should_fail\": true}"

sleep 20

STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-err') LIMIT 1;")
ERROR=$(psql_exec "SELECT left(last_error, 12) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-err') LIMIT 1;")

assert_eq "Failed message pending (retryable)" "pending" "$STATUS"
assert_contains "Error has RETRYABLE prefix" "RETRYABLE" "$ERROR"

# ═══════════════════════════════════════════════════
# TEST 7: Publisher Confirms (batch verification)
# ═══════════════════════════════════════════════════
header "TEST 7: Publisher Confirms (200 messages batch)"

psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-confirm"
rabbit_declare_queue "at-confirm"

create_endpoint "at-confirm" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-confirm\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 200) g);
  PERFORM ulak.send_batch('at-confirm', a);
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'at-confirm';")
assert_eq "200 confirmed messages" "200" "$COMPLETED"

QMSG=$(rabbit_queue_messages "at-confirm")
assert_eq "200 in RabbitMQ queue" "200" "$QMSG"

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'at-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

for q in at-single at-batch at-fan-1 at-fan-2 at-fan-3 at-persist at-throughput at-confirm; do
  rabbit_delete_queue "$q"
done

print_summary
exit $?
