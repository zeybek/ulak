#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBHOOK_SERVER="$SCRIPT_DIR/fixtures/webhook-server.js"

# ─────────────────────────────────────────────────
# ulak Stress Test Suite
# Measures: enqueue rate, dispatch rate, e2e latency,
#           worker scaling, batch size optimization
# ─────────────────────────────────────────────────

WEBHOOK_PORT=9876
WEBHOOK_HOST="host.docker.internal"
WEBHOOK_URL="http://${WEBHOOK_HOST}:${WEBHOOK_PORT}"
DB="ulak_test"
PSQL="docker exec ulak-postgres-1 psql -U postgres -d $DB -q -t -A"
PSQL_VERBOSE="docker exec ulak-postgres-1 psql -U postgres -d $DB"

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Configuration ───
STRESS_MESSAGES=${1:-10000}  # Total messages per test (override with arg)
WARMUP=500                   # Warmup messages before measuring

header() {
  echo ""
  echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${CYAN}║  $1${NC}"
  echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
}

section() {
  echo ""
  echo -e "${BOLD}━━━ $1 ━━━${NC}"
}

result() {
  local label="$1" value="$2" unit="$3"
  printf "  %-40s ${GREEN}%s${NC} %s\n" "$label" "$value" "$unit"
}

warn() {
  echo -e "  ${YELLOW}⚠ $1${NC}"
}

# ─── Pre-flight ───
header "ulak Stress Test — ${STRESS_MESSAGES} messages"

# Check webhook server
if ! curl -sf http://localhost:${WEBHOOK_PORT}/health > /dev/null 2>&1; then
  echo -e "${RED}ERROR: Webhook server not running on port ${WEBHOOK_PORT}${NC}"
  echo "  Start it: node $WEBHOOK_SERVER"
  exit 1
fi

# Check PostgreSQL
if ! $PSQL -c "SELECT 1;" > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not accessible${NC}"
  exit 1
fi

echo -e "${GREEN}Pre-flight OK${NC}"

# Show current config
section "Current Configuration"
CONFIG=$($PSQL -c "
SELECT format('workers=%s  poll_interval=%sms  batch_size=%s  http_max_conn_per_host=%s  http_max_total=%s  http_batch_capacity=%s',
  current_setting('ulak.workers'),
  current_setting('ulak.poll_interval'),
  current_setting('ulak.batch_size'),
  current_setting('ulak.http_max_connections_per_host'),
  current_setting('ulak.http_max_total_connections'),
  current_setting('ulak.http_batch_capacity')
);")
echo "  $CONFIG"

# ─── Helpers ───

cleanup_queue() {
  $PSQL -c "DELETE FROM ulak.queue;" 2>/dev/null
  $PSQL -c "DELETE FROM ulak.dlq;" 2>/dev/null
  curl -s -X POST http://localhost:${WEBHOOK_PORT}/api/clear > /dev/null 2>&1
}

drop_stress_endpoints() {
  # Must delete queue/dlq entries first (FK constraint), then drop endpoints
  $PSQL -c "
    DELETE FROM ulak.dlq WHERE endpoint_id IN (SELECT id FROM ulak.endpoints WHERE name LIKE 'stress-%');
    DELETE FROM ulak.queue WHERE endpoint_id IN (SELECT id FROM ulak.endpoints WHERE name LIKE 'stress-%');
  " 2>/dev/null
  $PSQL -c "
    DO \$\$
    DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT name FROM ulak.endpoints WHERE name LIKE 'stress-%' LOOP
        PERFORM ulak.drop_endpoint(r.name);
      END LOOP;
    END;
    \$\$;
  " 2>/dev/null
}

wait_all_delivered() {
  local max_wait=${1:-60}
  local elapsed=0
  while [ $elapsed -lt $max_wait ]; do
    local pending
    pending=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")
    if [ "$pending" = "0" ]; then
      return 0
    fi
    sleep 0.5
    elapsed=$((elapsed + 1))
  done
  return 1
}

get_webhook_count() {
  local tag="$1"
  curl -s "http://localhost:${WEBHOOK_PORT}/api/received/${tag}" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0"
}

set_guc() {
  local name="$1" value="$2"
  $PSQL -c "ALTER SYSTEM SET ulak.${name} = ${value};" 2>/dev/null
  $PSQL -c "SELECT pg_reload_conf();" > /dev/null 2>&1
  sleep 0.5
}

# ═══════════════════════════════════════════════════
# TEST 1: Enqueue Throughput
# ═══════════════════════════════════════════════════
header "TEST 1: Enqueue Throughput (SQL INSERT performance)"

cleanup_queue
drop_stress_endpoints

# Create endpoint (won't actually deliver — we only measure enqueue)
$PSQL -c "SELECT ulak.create_endpoint('stress-enqueue', 'http', '{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}'::jsonb);" > /dev/null

section "1a. Individual send() — ${STRESS_MESSAGES} messages"
ENQUEUE_SINGLE=$($PSQL -c "
DO \$\$
DECLARE
  start_ts timestamptz;
  end_ts timestamptz;
  elapsed_ms numeric;
BEGIN
  start_ts := clock_timestamp();
  FOR i IN 1..${STRESS_MESSAGES} LOOP
    PERFORM ulak.send('stress-enqueue', jsonb_build_object('i', i, 'ts', extract(epoch from clock_timestamp())));
  END LOOP;
  end_ts := clock_timestamp();
  elapsed_ms := extract(epoch from (end_ts - start_ts)) * 1000;
  RAISE NOTICE 'elapsed_ms=%', elapsed_ms;
END;
\$\$;
" 2>&1 | grep "elapsed_ms=" | sed 's/.*elapsed_ms=//')

if [ -n "$ENQUEUE_SINGLE" ]; then
  ENQUEUE_SINGLE_RATE=$(python3 -c "print(f'{${STRESS_MESSAGES} / (${ENQUEUE_SINGLE} / 1000):.0f}')")
  result "send() rate" "$ENQUEUE_SINGLE_RATE" "msg/s"
  result "send() total time" "${ENQUEUE_SINGLE}" "ms"
fi

cleanup_queue

section "1b. send_batch() — ${STRESS_MESSAGES} messages (batches of 500)"
ENQUEUE_BATCH=$($PSQL -c "
DO \$\$
DECLARE
  start_ts timestamptz;
  end_ts timestamptz;
  elapsed_ms numeric;
  batch_arr jsonb[];
  batch_num int := 0;
BEGIN
  start_ts := clock_timestamp();
  FOR chunk_start IN 1..${STRESS_MESSAGES} BY 500 LOOP
    batch_arr := ARRAY(
      SELECT jsonb_build_object('i', g, 'batch', batch_num)
      FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${STRESS_MESSAGES})) g
    );
    PERFORM ulak.send_batch('stress-enqueue', batch_arr);
    batch_num := batch_num + 1;
  END LOOP;
  end_ts := clock_timestamp();
  elapsed_ms := extract(epoch from (end_ts - start_ts)) * 1000;
  RAISE NOTICE 'elapsed_ms=%', elapsed_ms;
END;
\$\$;
" 2>&1 | grep "elapsed_ms=" | sed 's/.*elapsed_ms=//')

if [ -n "$ENQUEUE_BATCH" ]; then
  ENQUEUE_BATCH_RATE=$(python3 -c "print(f'{${STRESS_MESSAGES} / (${ENQUEUE_BATCH} / 1000):.0f}')")
  result "send_batch() rate" "$ENQUEUE_BATCH_RATE" "msg/s"
  result "send_batch() total time" "${ENQUEUE_BATCH}" "ms"
  SPEEDUP=$(python3 -c "print(f'{${ENQUEUE_SINGLE} / ${ENQUEUE_BATCH}:.1f}x')" 2>/dev/null || echo "N/A")
  result "Speedup vs send()" "$SPEEDUP" ""
fi

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 2: Dispatch Throughput (single endpoint)
# ═══════════════════════════════════════════════════
header "TEST 2: Dispatch Throughput — Single Endpoint"

drop_stress_endpoints
$PSQL -c "SELECT ulak.create_endpoint('stress-dispatch', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-dispatch\", \"method\": \"POST\"}'::jsonb);" > /dev/null

section "Enqueuing ${STRESS_MESSAGES} messages..."

$PSQL -c "
DO \$\$
DECLARE batch_arr jsonb[];
BEGIN
  FOR chunk_start IN 1..${STRESS_MESSAGES} BY 500 LOOP
    batch_arr := ARRAY(
      SELECT jsonb_build_object('i', g)
      FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${STRESS_MESSAGES})) g
    );
    PERFORM ulak.send_batch('stress-dispatch', batch_arr);
  END LOOP;
END;
\$\$;
" > /dev/null

QUEUE_DEPTH=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status = 'pending';")
result "Queue depth" "$QUEUE_DEPTH" "messages"

section "Waiting for dispatch..."
DISPATCH_START=$(date +%s%N)

if wait_all_delivered 120; then
  DISPATCH_END=$(date +%s%N)
  DISPATCH_MS=$(( (DISPATCH_END - DISPATCH_START) / 1000000 ))
  DELIVERED=$(get_webhook_count "ok/stress-dispatch")
  DISPATCH_RATE=$(python3 -c "print(f'{${DELIVERED} / (${DISPATCH_MS} / 1000):.0f}')")

  result "Messages delivered" "$DELIVERED" ""
  result "Dispatch time" "$DISPATCH_MS" "ms"
  result "Dispatch rate" "$DISPATCH_RATE" "msg/s"
else
  DELIVERED=$(get_webhook_count "ok/stress-dispatch")
  PENDING=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")
  warn "Timeout after 120s — delivered: ${DELIVERED}, pending: ${PENDING}"
fi

# E2E latency
section "E2E Latency (completed messages)"
$PSQL -c "
SELECT
  'p50=' || percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))),
  'p95=' || percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))),
  'p99=' || percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))),
  'max=' || max(extract(epoch from (completed_at - created_at)))
FROM ulak.queue
WHERE status = 'completed' AND completed_at IS NOT NULL;
" 2>/dev/null | while IFS='|' read -r p50 p95 p99 mx; do
  p50=$(echo "$p50" | xargs)
  p95=$(echo "$p95" | xargs)
  p99=$(echo "$p99" | xargs)
  mx=$(echo "$mx" | xargs)
  result "p50 latency" "$(python3 -c "print(f'{float(\"${p50}\".split(\"=\")[1]):.3f}')" 2>/dev/null || echo "$p50")" "s"
  result "p95 latency" "$(python3 -c "print(f'{float(\"${p95}\".split(\"=\")[1]):.3f}')" 2>/dev/null || echo "$p95")" "s"
  result "p99 latency" "$(python3 -c "print(f'{float(\"${p99}\".split(\"=\")[1]):.3f}')" 2>/dev/null || echo "$p99")" "s"
  result "max latency" "$(python3 -c "print(f'{float(\"${mx}\".split(\"=\")[1]):.3f}')" 2>/dev/null || echo "$mx")" "s"
done

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 3: Multi-Endpoint Fan-Out
# ═══════════════════════════════════════════════════
header "TEST 3: Multi-Endpoint Fan-Out (10 endpoints × ${STRESS_MESSAGES} messages)"

drop_stress_endpoints
for i in $(seq 1 10); do
  $PSQL -c "SELECT ulak.create_endpoint('stress-fan-${i}', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-fan-${i}\", \"method\": \"POST\"}'::jsonb);" > /dev/null
done

TOTAL_MSGS=$((STRESS_MESSAGES))
PER_EP=$((TOTAL_MSGS / 10))

section "Enqueuing ${PER_EP} messages × 10 endpoints = ${TOTAL_MSGS} total..."

$PSQL -c "
DO \$\$
DECLARE batch_arr jsonb[];
BEGIN
  FOR ep IN 1..10 LOOP
    FOR chunk_start IN 1..${PER_EP} BY 500 LOOP
      batch_arr := ARRAY(
        SELECT jsonb_build_object('ep', ep, 'i', g)
        FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${PER_EP})) g
      );
      PERFORM ulak.send_batch('stress-fan-' || ep, batch_arr);
    END LOOP;
  END LOOP;
END;
\$\$;
" > /dev/null

section "Waiting for dispatch..."
FAN_START=$(date +%s%N)

if wait_all_delivered 180; then
  FAN_END=$(date +%s%N)
  FAN_MS=$(( (FAN_END - FAN_START) / 1000000 ))

  FAN_TOTAL=0
  for i in $(seq 1 10); do
    c=$(get_webhook_count "ok/stress-fan-${i}")
    FAN_TOTAL=$((FAN_TOTAL + c))
  done

  FAN_RATE=$(python3 -c "print(f'{${FAN_TOTAL} / (${FAN_MS} / 1000):.0f}')")
  result "Total delivered" "$FAN_TOTAL" "messages"
  result "Fan-out time" "$FAN_MS" "ms"
  result "Fan-out rate" "$FAN_RATE" "msg/s"
else
  FAN_TOTAL=0
  for i in $(seq 1 10); do
    c=$(get_webhook_count "ok/stress-fan-${i}")
    FAN_TOTAL=$((FAN_TOTAL + c))
  done
  PENDING=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")
  warn "Timeout after 180s — delivered: ${FAN_TOTAL}, pending: ${PENDING}"
fi

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 4: Batch Size Comparison
# ═══════════════════════════════════════════════════
header "TEST 4: Batch Size Optimization"

BATCH_TEST_MSGS=$((STRESS_MESSAGES / 2))

for BS in 50 100 200 500; do
  section "batch_size=${BS}"

  drop_stress_endpoints
  cleanup_queue
  set_guc "batch_size" "$BS"
  set_guc "http_batch_capacity" "$BS"

  $PSQL -c "SELECT ulak.create_endpoint('stress-bs', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-bs-${BS}\", \"method\": \"POST\"}'::jsonb);" > /dev/null

  $PSQL -c "
  DO \$\$
  DECLARE batch_arr jsonb[];
  BEGIN
    FOR chunk_start IN 1..${BATCH_TEST_MSGS} BY 500 LOOP
      batch_arr := ARRAY(
        SELECT jsonb_build_object('i', g)
        FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${BATCH_TEST_MSGS})) g
      );
      PERFORM ulak.send_batch('stress-bs', batch_arr);
    END LOOP;
  END;
  \$\$;
  " > /dev/null

  BS_START=$(date +%s%N)
  if wait_all_delivered 120; then
    BS_END=$(date +%s%N)
    BS_MS=$(( (BS_END - BS_START) / 1000000 ))
    BS_DELIVERED=$(get_webhook_count "ok/stress-bs-${BS}")
    BS_RATE=$(python3 -c "print(f'{${BS_DELIVERED} / (${BS_MS} / 1000):.0f}')")
    result "batch_size=${BS}" "${BS_RATE}" "msg/s (${BS_DELIVERED} in ${BS_MS}ms)"
  else
    BS_DELIVERED=$(get_webhook_count "ok/stress-bs-${BS}")
    warn "Timeout — delivered ${BS_DELIVERED}/${BATCH_TEST_MSGS}"
  fi
done

# Reset batch_size
set_guc "batch_size" "200"
set_guc "http_batch_capacity" "200"

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 5: Connection Pool Scaling
# ═══════════════════════════════════════════════════
header "TEST 5: Connection Pool — max_connections_per_host"

CONN_TEST_MSGS=$((STRESS_MESSAGES / 2))

for MAXCONN in 5 10 25 50; do
  section "max_connections_per_host=${MAXCONN}"

  drop_stress_endpoints
  cleanup_queue
  set_guc "http_max_connections_per_host" "$MAXCONN"
  set_guc "http_max_total_connections" "$((MAXCONN * 2))"

  $PSQL -c "SELECT ulak.create_endpoint('stress-conn', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-conn-${MAXCONN}\", \"method\": \"POST\"}'::jsonb);" > /dev/null

  $PSQL -c "
  DO \$\$
  DECLARE batch_arr jsonb[];
  BEGIN
    FOR chunk_start IN 1..${CONN_TEST_MSGS} BY 500 LOOP
      batch_arr := ARRAY(
        SELECT jsonb_build_object('i', g)
        FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${CONN_TEST_MSGS})) g
      );
      PERFORM ulak.send_batch('stress-conn', batch_arr);
    END LOOP;
  END;
  \$\$;
  " > /dev/null

  CONN_START=$(date +%s%N)
  if wait_all_delivered 120; then
    CONN_END=$(date +%s%N)
    CONN_MS=$(( (CONN_END - CONN_START) / 1000000 ))
    CONN_DELIVERED=$(get_webhook_count "ok/stress-conn-${MAXCONN}")
    CONN_RATE=$(python3 -c "print(f'{${CONN_DELIVERED} / (${CONN_MS} / 1000):.0f}')")
    result "max_conn=${MAXCONN}" "${CONN_RATE}" "msg/s (${CONN_DELIVERED} in ${CONN_MS}ms)"
  else
    CONN_DELIVERED=$(get_webhook_count "ok/stress-conn-${MAXCONN}")
    warn "Timeout — delivered ${CONN_DELIVERED}/${CONN_TEST_MSGS}"
  fi
done

# Reset connection settings
set_guc "http_max_connections_per_host" "10"
set_guc "http_max_total_connections" "25"

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 6: Mixed Workload (success + failure + slow)
# ═══════════════════════════════════════════════════
header "TEST 6: Mixed Workload — 70% success, 20% failure, 10% slow"

drop_stress_endpoints
MIXED_MSGS=$((STRESS_MESSAGES / 2))
MIXED_OK=$((MIXED_MSGS * 70 / 100))
MIXED_FAIL=$((MIXED_MSGS * 20 / 100))
MIXED_SLOW=$((MIXED_MSGS - MIXED_OK - MIXED_FAIL))

$PSQL -c "SELECT ulak.create_endpoint('stress-mixed-ok', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-mixed\", \"method\": \"POST\"}'::jsonb);" > /dev/null
$PSQL -c "SELECT ulak.create_endpoint('stress-mixed-fail', 'http', '{\"url\": \"${WEBHOOK_URL}/fail/stress-mixed\", \"method\": \"POST\"}'::jsonb);" > /dev/null
$PSQL -c "SELECT ulak.create_endpoint('stress-mixed-slow', 'http', '{\"url\": \"${WEBHOOK_URL}/slow/stress-mixed\", \"method\": \"POST\"}'::jsonb);" > /dev/null

section "Enqueuing ${MIXED_MSGS} mixed messages..."

$PSQL -c "
DO \$\$
DECLARE batch_arr jsonb[];
BEGIN
  -- OK messages (70%)
  FOR chunk_start IN 1..${MIXED_OK} BY 500 LOOP
    batch_arr := ARRAY(
      SELECT jsonb_build_object('type', 'ok', 'i', g)
      FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${MIXED_OK})) g
    );
    PERFORM ulak.send_batch('stress-mixed-ok', batch_arr);
  END LOOP;

  -- Fail messages (20%)
  FOR chunk_start IN 1..${MIXED_FAIL} BY 500 LOOP
    batch_arr := ARRAY(
      SELECT jsonb_build_object('type', 'fail', 'i', g)
      FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${MIXED_FAIL})) g
    );
    PERFORM ulak.send_batch('stress-mixed-fail', batch_arr);
  END LOOP;

  -- Slow messages (10%)
  FOR chunk_start IN 1..${MIXED_SLOW} BY 500 LOOP
    batch_arr := ARRAY(
      SELECT jsonb_build_object('type', 'slow', 'i', g)
      FROM generate_series(chunk_start, LEAST(chunk_start + 499, ${MIXED_SLOW})) g
    );
    PERFORM ulak.send_batch('stress-mixed-slow', batch_arr);
  END LOOP;
END;
\$\$;
" > /dev/null

section "Waiting for processing (30s snapshot)..."
sleep 30

MIXED_COMPLETED=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
MIXED_FAILED=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status = 'failed';")
MIXED_PENDING=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")

result "Completed" "$MIXED_COMPLETED" "messages"
result "Failed (retrying)" "$MIXED_FAILED" "messages"
result "Still pending" "$MIXED_PENDING" "messages"
result "Success rate" "$(python3 -c "t=${MIXED_COMPLETED}+${MIXED_FAILED}+${MIXED_PENDING}; print(f'{${MIXED_COMPLETED}/t*100:.1f}%' if t>0 else 'N/A')")" ""

# Check circuit breaker states
section "Circuit Breaker States"
$PSQL -c "SELECT name, circuit_state, circuit_failure_count FROM ulak.endpoints WHERE name LIKE 'stress-mixed%';" 2>/dev/null | while IFS='|' read -r name state count; do
  name=$(echo "$name" | xargs)
  state=$(echo "$state" | xargs)
  count=$(echo "$count" | xargs)
  if [ -n "$name" ]; then
    result "$name" "$state (failures: $count)" ""
  fi
done

cleanup_queue

# ═══════════════════════════════════════════════════
# TEST 7: Sustained Load (continuous feed)
# ═══════════════════════════════════════════════════
header "TEST 7: Sustained Load — 60 second continuous feed"

drop_stress_endpoints
$PSQL -c "SELECT ulak.create_endpoint('stress-sustained', 'http', '{\"url\": \"${WEBHOOK_URL}/ok/stress-sustained\", \"method\": \"POST\"}'::jsonb);" > /dev/null

section "Feeding messages for 60 seconds..."
SUSTAINED_TOTAL=0
SUSTAINED_START=$(date +%s)
SUSTAINED_END=$((SUSTAINED_START + 60))

while [ $(date +%s) -lt $SUSTAINED_END ]; do
  $PSQL -c "
  DO \$\$
  DECLARE batch_arr jsonb[];
  BEGIN
    batch_arr := ARRAY(
      SELECT jsonb_build_object('i', g, 'ts', extract(epoch from clock_timestamp()))
      FROM generate_series(1, 200) g
    );
    PERFORM ulak.send_batch('stress-sustained', batch_arr);
  END;
  \$\$;
  " > /dev/null 2>&1
  SUSTAINED_TOTAL=$((SUSTAINED_TOTAL + 200))
  sleep 0.1
done

section "Waiting for drain..."
wait_all_delivered 60

SUSTAINED_DELIVERED=$(get_webhook_count "ok/stress-sustained")
SUSTAINED_COMPLETED=$($PSQL -c "SELECT count(*) FROM ulak.queue WHERE status = 'completed' AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'stress-sustained');")

result "Total enqueued" "$SUSTAINED_TOTAL" "messages"
result "Total delivered" "$SUSTAINED_DELIVERED" "messages"
result "Enqueue rate" "$(python3 -c "print(f'{${SUSTAINED_TOTAL}/60:.0f}')")" "msg/s (target)"
result "Effective dispatch" "$(python3 -c "print(f'{${SUSTAINED_DELIVERED}/60:.0f}')" 2>/dev/null || echo "N/A")" "msg/s"

# Final latency stats
$PSQL -c "
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))) as p50,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))) as p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch from (completed_at - created_at))) as p99
FROM ulak.queue
WHERE status = 'completed' AND completed_at IS NOT NULL
  AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'stress-sustained');
" 2>/dev/null | while IFS='|' read -r p50 p95 p99; do
  p50=$(echo "$p50" | xargs)
  p95=$(echo "$p95" | xargs)
  p99=$(echo "$p99" | xargs)
  if [ -n "$p50" ]; then
    result "Sustained p50 latency" "$(python3 -c "print(f'{float(\"$p50\"):.3f}')" 2>/dev/null || echo "$p50")" "s"
    result "Sustained p95 latency" "$(python3 -c "print(f'{float(\"$p95\"):.3f}')" 2>/dev/null || echo "$p95")" "s"
    result "Sustained p99 latency" "$(python3 -c "print(f'{float(\"$p99\"):.3f}')" 2>/dev/null || echo "$p99")" "s"
  fi
done

cleanup_queue

# ═══════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════
header "STRESS TEST COMPLETE"
echo ""
echo -e "  ${BOLD}Environment:${NC}"
echo "  PostgreSQL: $(docker exec ulak-postgres-1 psql -U postgres -t -A -c 'SELECT version();' 2>/dev/null | head -1 | cut -d' ' -f1-2)"
echo "  Workers: $($PSQL -c "SELECT current_setting('ulak.workers');")"
echo "  Docker CPU: $(docker inspect ulak-postgres-1 --format '{{.HostConfig.NanoCpus}}' 2>/dev/null || echo 'unlimited')"
echo ""
echo -e "  ${YELLOW}Note: Results depend on Docker resources, network, and webhook server capacity.${NC}"
echo -e "  ${YELLOW}For production benchmarks, test on bare metal with dedicated resources.${NC}"

# Final cleanup
drop_stress_endpoints
cleanup_queue
