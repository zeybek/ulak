#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak NATS E2E Test Suite
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

NATS_HOST="nats"
NATS_PORT=4222
NATS_MON_PORT=8222

# ─── NATS helpers ───

nats_ready() {
  curl -sf "http://localhost:${NATS_MON_PORT}/healthz" > /dev/null 2>&1
}

# Create a JetStream stream via NATS API (from postgres container using nats C client isn't possible,
# so we use the monitoring API. Actually NATS doesn't expose stream creation via HTTP monitoring.
# We'll create streams by publishing to $JS.API.STREAM.CREATE.<name> from psql via the extension.)
nats_install_cli() {
  docker exec ulak-postgres-1 bash -c "
    if command -v nats > /dev/null 2>&1; then return 0; fi
    apt-get update -qq > /dev/null 2>&1
    apt-get install -y -qq wget unzip > /dev/null 2>&1
    cd /tmp && \
    wget -q 'https://github.com/nats-io/natscli/releases/download/v0.2.2/nats-0.2.2-linux-arm64.zip' -O nats.zip && \
    unzip -o nats.zip -d /tmp/nats-extract > /dev/null && \
    cp /tmp/nats-extract/nats-0.2.2-linux-arm64/nats /usr/local/bin/nats && \
    chmod +x /usr/local/bin/nats && \
    rm -rf nats.zip /tmp/nats-extract
  " 2>/dev/null
}

nats_create_stream() {
  local stream="$1"
  local subjects="$2"
  docker exec ulak-postgres-1 nats -s "nats://${NATS_HOST}:${NATS_PORT}" stream add "${stream}" \
    --subjects="${subjects}" \
    --storage=memory \
    --retention=limits \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=1h \
    --discard=old \
    --replicas=1 \
    --defaults 2>/dev/null || true
}

nats_stream_msgs() {
  local stream="$1"
  docker exec ulak-postgres-1 bash -c "
    nats -s nats://${NATS_HOST}:${NATS_PORT} stream info ${stream} --json 2>/dev/null
  " 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',{}).get('messages',0))" 2>/dev/null || echo "0"
}

nats_purge_stream() {
  local stream="$1"
  docker exec ulak-postgres-1 bash -c "
    nats -s nats://${NATS_HOST}:${NATS_PORT} stream purge ${stream} -f 2>/dev/null
  " 2>/dev/null || true
  sleep 1
}

nats_delete_stream() {
  local stream="$1"
  docker exec ulak-postgres-1 bash -c "
    nats -s nats://${NATS_HOST}:${NATS_PORT} stream rm ${stream} -f 2>/dev/null
  " 2>/dev/null || true
}

# ─── Pre-flight ───

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak NATS E2E Test Suite                            ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

if ! nats_ready; then
  echo -e "${RED}ERROR: NATS not running${NC}"
  exit 1
fi

if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not running${NC}"
  exit 1
fi

echo -e "${GREEN}Pre-flight OK${NC} — nats:4222 ready, pg:ready"

# Ensure nats CLI is available in postgres container
nats_install_cli

# Setup GUCs
psql_quiet "ALTER SYSTEM SET ulak.database = '$DB';"
psql_quiet "ALTER SYSTEM SET ulak.poll_interval = 100;"
psql_quiet "ALTER SYSTEM SET ulak.nats_flush_timeout = 10000;"
docker restart ulak-postgres-1 > /dev/null 2>&1
sleep 4

# Ensure extension exists
psql_quiet "CREATE EXTENSION IF NOT EXISTS ulak;"

# Clean slate
psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'nt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

# Create JetStream streams for tests
nats_create_stream "NTTEST" "nt.>"

# ═══════════════════════════════════════════════════
# TEST 1: JetStream Basic Publish
# ═══════════════════════════════════════════════════
header "TEST 1: JetStream Basic Publish"

nats_purge_stream "NTTEST"

create_endpoint "nt-js-single" "{\"url\": \"nats://${NATS_HOST}:${NATS_PORT}\", \"subject\": \"nt.single\", \"jetstream\": true}" "nats"

send_msg "nt-js-single" "{\"test\": \"jetstream-single\"}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'nt-js-single';")
assert_eq "JetStream message completed" "1" "$COMPLETED"

MSGS=$(nats_stream_msgs "NTTEST")
assert_eq "Message in NATS stream" "1" "$MSGS"

# ═══════════════════════════════════════════════════
# TEST 2: JetStream Batch (100 messages)
# ═══════════════════════════════════════════════════
header "TEST 2: JetStream Batch (100 messages)"

psql_quiet "DELETE FROM ulak.queue;"
nats_purge_stream "NTTEST"

create_endpoint "nt-js-batch" "{\"url\": \"nats://${NATS_HOST}:${NATS_PORT}\", \"subject\": \"nt.batch\", \"jetstream\": true}" "nats"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 100) g);
  PERFORM ulak.send_batch('nt-js-batch', a);
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'nt-js-batch';")
assert_eq "100 messages completed" "100" "$COMPLETED"

MSGS=$(nats_stream_msgs "NTTEST")
assert_eq "100 in NATS stream" "100" "$MSGS"

# ═══════════════════════════════════════════════════
# TEST 3: Core NATS (fire-and-forget)
# ═══════════════════════════════════════════════════
header "TEST 3: Core NATS (fire-and-forget)"

psql_quiet "DELETE FROM ulak.queue;"

create_endpoint "nt-core" "{\"url\": \"nats://${NATS_HOST}:${NATS_PORT}\", \"subject\": \"nt.core.test\", \"jetstream\": false}" "nats"

send_msg "nt-core" "{\"test\": \"core-nats\"}"
wait_queue_drain 15

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'nt-core';")
assert_eq "Core NATS message completed" "1" "$COMPLETED"

# ═══════════════════════════════════════════════════
# TEST 4: Multiple Subjects (3 endpoints × 50 msgs)
# ═══════════════════════════════════════════════════
header "TEST 4: Multiple Subjects (3 × 50 messages)"

psql_quiet "DELETE FROM ulak.queue;"
nats_purge_stream "NTTEST"

for i in 1 2 3; do
  create_endpoint "nt-multi-$i" "{\"url\": \"nats://${NATS_HOST}:${NATS_PORT}\", \"subject\": \"nt.multi.$i\", \"jetstream\": true}" "nats"
done

psql_quiet "
DO \$\$
BEGIN
  FOR ep IN 1..3 LOOP
    PERFORM ulak.send_batch('nt-multi-' || ep,
      ARRAY(SELECT jsonb_build_object('ep', ep, 'i', g) FROM generate_series(1, 50) g));
  END LOOP;
END; \$\$;"

wait_queue_drain 20

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE 'nt-multi-%';")
assert_eq "150 messages completed" "150" "$COMPLETED"

MSGS=$(nats_stream_msgs "NTTEST")
assert_eq "150 in NATS stream" "150" "$MSGS"

# ═══════════════════════════════════════════════════
# TEST 5: Throughput (1K messages)
# ═══════════════════════════════════════════════════
header "TEST 5: Throughput (1K messages)"

psql_quiet "DELETE FROM ulak.queue;"
nats_purge_stream "NTTEST"

create_endpoint "nt-throughput" "{\"url\": \"nats://${NATS_HOST}:${NATS_PORT}\", \"subject\": \"nt.throughput\", \"jetstream\": true}" "nats"

psql_quiet "
DO \$\$ DECLARE a jsonb[];
BEGIN
  a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 1000) g);
  PERFORM ulak.send_batch('nt-throughput', a);
END; \$\$;"

wait_queue_drain 30

THROUGHPUT=$(psql_exec "
SELECT round(count(*)::numeric / GREATEST(extract(epoch from max(q.completed_at)-min(q.created_at)), 0.001))
FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name = 'nt-throughput';")

TOTAL=$((TOTAL + 1))
if [ "${THROUGHPUT:-0}" -ge 100 ] 2>/dev/null; then
  echo -e "  ${GREEN}✓${NC} NATS throughput: ${THROUGHPUT} msg/s (>=100)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} NATS throughput too low: ${THROUGHPUT} msg/s"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 6: Error Classification
# ═══════════════════════════════════════════════════
header "TEST 6: Error Classification"

psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DELETE FROM ulak.dlq;"

create_endpoint "nt-err" "{\"url\": \"nats://nonexistent-host:4222\", \"subject\": \"test.err\", \"jetstream\": false}" "nats"
send_msg "nt-err" "{\"should_fail\": true}"

sleep 15

STATUS=$(psql_exec "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'nt-err') LIMIT 1;")
ERROR=$(psql_exec "SELECT left(coalesce(last_error, ''), 50) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'nt-err') LIMIT 1;")

# When dispatcher_create fails (connection refused), the worker defers the message
# without setting last_error. Message stays pending — this is correct retryable behavior.
assert_eq "Failed message still pending" "pending" "$STATUS"

# Verify connection error is logged (worker log shows WARNING)
NATS_ERRS=$(docker logs ulak-postgres-1 2>&1 | grep -c "NATS connection failed to nats://nonexistent-host")
assert_gt "Connection error logged" "0" "$NATS_ERRS"

# ═══════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════

psql_quiet "DELETE FROM ulak.dlq;"
psql_quiet "DELETE FROM ulak.queue;"
psql_quiet "DO \$\$ DECLARE r RECORD; BEGIN FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'nt-%' LOOP BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; END; \$\$;"

nats_delete_stream "NTTEST"

print_summary
exit $?
