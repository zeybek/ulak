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
  # durable=true: RabbitMQ 4 refuses transient non-exclusive queues by default
  # (deprecated feature transient_nonexcl_queues); ulak itself declares no queues.
  docker exec ulak-rabbitmq-1 rabbitmqadmin declare queue name="$queue" durable=true 2>/dev/null || \
    rabbit_cmd eval "rabbit_amqqueue:declare(rabbit_misc:r(<<\"/\">>, queue, <<\"$queue\">>), true, false, [], none, <<\"guest\">>)." 2>/dev/null || true
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

# The row is claimed ('processing') at once; wait for the retry to be
# recorded instead of sampling at a fixed moment.
ERROR=""
for _ in $(seq 1 45); do
  ERROR=$(psql_exec "SELECT left(last_error, 12) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-err') LIMIT 1;")
  [ -n "$ERROR" ] && break
  sleep 1
done

STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-err') LIMIT 1;")

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
# TEST 8: mandatory publish that no queue takes
# ═══════════════════════════════════════════════════
header "TEST 8: mandatory + Unroutable"

# RabbitMQ answers with basic.return and then basic.ack; the row must fail
# (retryable) instead of being completed by the ack.
psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-unroutable"
create_endpoint "at-unroutable" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-unroutable\", \"mandatory\": true, \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
send_msg "at-unroutable" "{\"test\": \"unroutable\"}"
UR_ERR=""
for _ in $(seq 1 20); do
  UR_ERR=$(psql_exec "SELECT last_error FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-unroutable') AND retry_count >= 1;")
  [ -n "$UR_ERR" ] && break
  sleep 1
done
UR_STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-unroutable');")
assert_eq "Unroutable mandatory message is not completed" "pending" "$UR_STATUS"
assert_contains "Error names the broker's return" "returned by broker: 312" "$UR_ERR"

# Once a queue exists for the routing key, the retry delivers it
rabbit_declare_queue "at-unroutable"
psql_quiet "UPDATE ulak.queue SET next_retry_at = now() WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-unroutable');"
UR_DONE=""
for _ in $(seq 1 20); do
  UR_DONE=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-unroutable');")
  [ "$UR_DONE" = "completed" ] && break
  sleep 1
done
assert_eq "Delivered after the queue is declared" "completed" "$UR_DONE"
assert_eq "One message in the queue" "1" "$(rabbit_queue_messages at-unroutable)"

# ═══════════════════════════════════════════════════
# TEST 9: message_id is the queue row id
# ═══════════════════════════════════════════════════
header "TEST 9: message_id = msg_<row id>"

# Delivery tags restart at 1 on every connection, so they cannot be the id.
psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-msgid"
rabbit_declare_queue "at-msgid"
create_endpoint "at-msgid" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-msgid\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
send_msg "at-msgid" "{\"test\": \"msgid\"}"
wait_queue_drain 15
ROW_ID=$(psql_exec "SELECT q.id FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'at-msgid';")
if docker exec ulak-rabbitmq-1 sh -c 'command -v rabbitmqadmin' > /dev/null 2>&1; then
  # rabbitmqadmin 2.x (RabbitMQ 4 images) prints a table row with
  # 'message_id: "<id>"'; 1.x takes key=value arguments and has -f raw_json
  GOT_ID=$(docker exec ulak-rabbitmq-1 rabbitmqadmin --non-interactive get messages --queue at-msgid --ack-mode ack_requeue_true 2>/dev/null \
    | grep -o 'message_id: "[^"]*"' | head -1 | sed 's/message_id: "\(.*\)"/\1/')
  if [ -z "$GOT_ID" ]; then
    GOT_ID=$(docker exec ulak-rabbitmq-1 rabbitmqadmin get queue=at-msgid ackmode=ack_requeue_true -f raw_json 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['properties'].get('message_id','MISSING'))" 2>/dev/null)
  fi
  assert_eq "message_id carries the row id" "msg_${ROW_ID}" "$GOT_ID"
else
  echo -e "  ${YELLOW}⊘${NC} skipped: rabbitmqadmin not available in the broker image"
  SKIP=$((SKIP + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 10: First publish after the broker dropped an idle connection
# ═══════════════════════════════════════════════════
header "TEST 10: Idle Past the Heartbeat"

# librabbitmq only handles heartbeats inside library calls, so an idle worker
# sends none and the broker closes the connection after two missed intervals.
psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-idle"
rabbit_declare_queue "at-idle"
create_endpoint "at-idle" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-idle\", \"heartbeat\": 2, \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
# One ordering_key, so both rows land on the same worker and the second one
# finds that worker's idle connection (with ulak.workers > 1 they would
# otherwise likely go to different workers and connections).
psql_quiet "SELECT ulak.send_with_options('at-idle', '{\"idle\": 1}'::jsonb, p_ordering_key := 'at-idle');"
wait_queue_drain 15
sleep 12
psql_quiet "SELECT ulak.send_with_options('at-idle', '{\"idle\": 2}'::jsonb, p_ordering_key := 'at-idle');"
IDLE_DONE=""
for _ in $(seq 1 30); do
  IDLE_DONE=$(psql_exec "SELECT status || ':' || retry_count FROM ulak.queue WHERE payload->>'idle' = '2';")
  case "$IDLE_DONE" in completed:*) break ;; esac
  sleep 1
done
assert_eq "Publish after idle: delivered on the first attempt" "completed:0" "$IDLE_DONE"
echo "  last_error of the second message: $(psql_exec "SELECT coalesce(last_error, '-') FROM ulak.queue WHERE payload->>'idle' = '2';")"

# ═══════════════════════════════════════════════════
# TEST 11: Broker that stops answering after login
# ═══════════════════════════════════════════════════
header "TEST 11: Synchronous RPC Bounded (channel.open)"

# librabbitmq waits for synchronous replies (channel.open-ok, confirm.select-ok)
# forever unless amqp_set_rpc_timeout is set. A proxy on the Docker host
# relays to RabbitMQ until the client sends channel.open and then drops
# everything the broker says. The row must fail [RETRYABLE] within about
# ulak.amqp_connection_timeout, and the same worker must go on: a row of
# another endpoint in the same partition is delivered afterwards.
psql_quiet "DELETE FROM ulak.queue;"
rabbit_delete_queue "at-after-stall"
rabbit_declare_queue "at-after-stall"
STALL_PORT=5673
python3 "$SCRIPT_DIR/fixtures/amqp-stall-proxy.py" "$STALL_PORT" localhost "$AMQP_PORT" > /dev/null 2>&1 &
STALL_PID=$!
sleep 1
CONN_TIMEOUT=$(psql_exec "SELECT current_setting('ulak.amqp_connection_timeout');")
WORKERS=$(psql_exec "SELECT current_setting('ulak.workers');")
# Two ordering keys in the same partition, so one worker handles both rows
KEY_B=$(psql_exec "SELECT k FROM (SELECT 'at-b-' || g AS k FROM generate_series(1, 1000) g) s WHERE abs(hashtext(k)::bigint) % ${WORKERS} = abs(hashtext('at-a')::bigint) % ${WORKERS} LIMIT 1;")

create_endpoint "at-stall" "{\"host\": \"${WEBHOOK_HOST}\", \"port\": ${STALL_PORT}, \"exchange\": \"\", \"routing_key\": \"at-after-stall\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
create_endpoint "at-after-stall" "{\"host\": \"${AMQP_HOST}\", \"port\": ${AMQP_PORT}, \"exchange\": \"\", \"routing_key\": \"at-after-stall\", \"username\": \"${AMQP_USER}\", \"password\": \"${AMQP_PASS}\"}" "amqp"
STALL_START=$(date +%s)
psql_quiet "SELECT ulak.send_with_options('at-stall', '{\"stall\": \"a\"}'::jsonb, p_ordering_key := 'at-a');"
sleep 2
psql_quiet "SELECT ulak.send_with_options('at-after-stall', '{\"stall\": \"b\"}'::jsonb, p_ordering_key := '${KEY_B}');"

STALL_ERR=""
STALL_SECS=""
LIMIT=$((CONN_TIMEOUT + 15))
for _ in $(seq 1 $LIMIT); do
  STALL_ERR=$(psql_exec "SELECT last_error FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-stall') AND retry_count >= 1;")
  if [ -n "$STALL_ERR" ]; then STALL_SECS=$(( $(date +%s) - STALL_START )); break; fi
  sleep 1
done
echo "  stalled row after ${STALL_SECS:-?} s (connection timeout ${CONN_TIMEOUT} s): ${STALL_ERR:0:110}"
assert_eq "Stalled channel.open: [RETRYABLE]" "[RETRYABLE]" "${STALL_ERR:0:11}"
assert_contains "Stalled channel.open: failed in channel open" "channel open failed" "$STALL_ERR"
B_DONE=""
for _ in $(seq 1 $LIMIT); do
  B_DONE=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'at-after-stall');")
  [ "$B_DONE" = "completed" ] && break
  sleep 1
done
assert_eq "Same worker delivers the next row afterwards" "completed" "$B_DONE"
psql_quiet "SELECT ulak.disable_endpoint('at-stall');"
kill "$STALL_PID" 2>/dev/null || true

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'at-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

for q in at-single at-batch at-fan-1 at-fan-2 at-fan-3 at-persist at-throughput at-confirm at-unroutable at-msgid at-idle at-after-stall; do
  rabbit_delete_queue "$q"
done

print_summary
exit $?
