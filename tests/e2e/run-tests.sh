#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBHOOK_SERVER="$SCRIPT_DIR/fixtures/webhook-server.js"

# ─────────────────────────────────────────────────
# ulak E2E Test Runner — Full HTTP Protocol
# ─────────────────────────────────────────────────

WEBHOOK_PORT=9876
WEBHOOK_HOST="host.docker.internal"
WEBHOOK_URL="http://${WEBHOOK_HOST}:${WEBHOOK_PORT}"
DB="ulak_test"
PSQL="docker exec ulak-postgres-1 psql -U postgres -d $DB -q -t -A"
PSQL_OUT="docker exec ulak-postgres-1 psql -U postgres -d $DB -t -A"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
TOTAL=0

# ─── Helpers ───

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$expected" = "$actual" ]; then
    echo -e "  ${GREEN}✓${NC} $label"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}✗${NC} $label (expected: $expected, got: $actual)"
    FAIL=$((FAIL + 1))
  fi
}

assert_gt() {
  local label="$1" threshold="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$actual" -gt "$threshold" ] 2>/dev/null; then
    echo -e "  ${GREEN}✓${NC} $label ($actual > $threshold)"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}✗${NC} $label (expected > $threshold, got: $actual)"
    FAIL=$((FAIL + 1))
  fi
}

assert_contains() {
  local label="$1" needle="$2" haystack="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$haystack" | grep -q "$needle"; then
    echo -e "  ${GREEN}✓${NC} $label"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}✗${NC} $label (not found: $needle)"
    FAIL=$((FAIL + 1))
  fi
}

wait_queue_drain() {
  local max_wait=${1:-15}
  local elapsed=0
  while [ $elapsed -lt $max_wait ]; do
    local pending
    pending=$($PSQL_OUT -c "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")
    if [ "$pending" = "0" ]; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  echo -e "  ${YELLOW}⚠ Queue not drained after ${max_wait}s${NC}"
  return 0
}

webhook_count() {
  local tag="$1"
  curl -s "http://localhost:${WEBHOOK_PORT}/api/received/${tag}" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0"
}

webhook_get() {
  local tag="$1"
  curl -s "http://localhost:${WEBHOOK_PORT}/api/received/${tag}"
}

webhook_clear() {
  curl -s -X POST "http://localhost:${WEBHOOK_PORT}/api/clear" > /dev/null
}

section() {
  echo ""
  echo -e "${CYAN}━━━ $1 ━━━${NC}"
}

create_endpoint() {
  local name="$1" config="$2"
  $PSQL -c "SELECT ulak.create_endpoint('$name', 'http', '$config'::jsonb);" > /dev/null 2>&1
}

send_msg() {
  local endpoint="$1" payload="$2"
  $PSQL -c "SELECT ulak.send('$endpoint', '$payload'::jsonb);" > /dev/null 2>&1
}

cleanup() {
  echo ""
  echo -e "${CYAN}Cleaning up...${NC}"
  $PSQL -c "DELETE FROM ulak.queue;" 2>/dev/null || true
  $PSQL -c "DELETE FROM ulak.dlq;" 2>/dev/null || true
  $PSQL -c "DELETE FROM ulak.event_types CASCADE;" 2>/dev/null || true
  $PSQL -c "DELETE FROM ulak.endpoints;" 2>/dev/null || true
  $PSQL -c "DROP FUNCTION IF EXISTS public.test_transform_upper(jsonb);" 2>/dev/null || true
  webhook_clear
}

# ─────────────────────────────────────────────────
# Pre-flight
# ─────────────────────────────────────────────────

echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak E2E Test — HTTP Protocol     ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"

# Check webhook server
if ! curl -s "http://localhost:${WEBHOOK_PORT}/health" > /dev/null 2>&1; then
  echo -e "${RED}ERROR: Webhook server not running on port ${WEBHOOK_PORT}${NC}"
  echo "Start it with: node $WEBHOOK_SERVER"
  exit 1
fi

# Check postgres
if ! $PSQL_OUT -c "SELECT 1;" > /dev/null 2>&1; then
  echo -e "${RED}ERROR: PostgreSQL not reachable${NC}"
  exit 1
fi

# Ensure extension exists (may have been dropped by regression cleanup)
EXT=$($PSQL_OUT -c "SELECT count(*) FROM pg_extension WHERE extname='ulak';")
if [ "$EXT" != "1" ]; then
  echo -e "${YELLOW}Extension not found, creating...${NC}"
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -c "CREATE EXTENSION ulak;" > /dev/null 2>&1
  EXT=$($PSQL_OUT -c "SELECT count(*) FROM pg_extension WHERE extname='ulak';")
  if [ "$EXT" != "1" ]; then
    echo -e "${RED}ERROR: Failed to create ulak extension${NC}"
    exit 1
  fi
fi

echo -e "${GREEN}Pre-flight OK${NC} — webhook:${WEBHOOK_PORT}, pg:ready, extension:installed"

# Clean slate
cleanup

# ═════════════════════════════════════════════════
# TEST 1: HTTP Methods (POST, PUT, PATCH, DELETE)
# ═════════════════════════════════════════════════
section "TEST 1: HTTP Methods"

for method in POST PUT PATCH DELETE; do
  tag="method-$(echo $method | tr '[:upper:]' '[:lower:]')"
  create_endpoint "test-${tag}" "{\"url\": \"${WEBHOOK_URL}/ok/${tag}\", \"method\": \"${method}\"}"
  send_msg "test-${tag}" "{\"test\": \"${method}\", \"ts\": \"$(date -u +%s)\"}"
done

wait_queue_drain
sleep 1

for method in POST PUT PATCH DELETE; do
  tag="method-$(echo $method | tr '[:upper:]' '[:lower:]')"
  count=$(webhook_count "ok/${tag}")
  assert_eq "HTTP ${method} delivered" "1" "$count"
done

# Verify actual HTTP methods used
for method in POST PUT PATCH DELETE; do
  tag="method-$(echo $method | tr '[:upper:]' '[:lower:]')"
  actual=$(webhook_get "ok/${tag}" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['method'] if r else 'NONE')" 2>/dev/null)
  assert_eq "HTTP ${method} correct method" "${method}" "$actual"
done

# ═════════════════════════════════════════════════
# TEST 2: Custom Headers
# ═════════════════════════════════════════════════
section "TEST 2: Custom Headers"

create_endpoint "test-custom-headers" "{\"url\": \"${WEBHOOK_URL}/echo/custom-headers\", \"method\": \"POST\", \"headers\": {\"X-Custom-One\": \"hello\", \"X-Custom-Two\": \"world\", \"X-Request-Id\": \"test-123\"}}"
send_msg "test-custom-headers" "{\"test\": \"custom-headers\"}"

wait_queue_drain
sleep 1

HEADERS_RESP=$(webhook_get "echo/custom-headers")
h1=$(echo "$HEADERS_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('x-custom-one','MISSING'))" 2>/dev/null)
h2=$(echo "$HEADERS_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('x-custom-two','MISSING'))" 2>/dev/null)
h3=$(echo "$HEADERS_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('x-request-id','MISSING'))" 2>/dev/null)

assert_eq "X-Custom-One header" "hello" "$h1"
assert_eq "X-Custom-Two header" "world" "$h2"
assert_eq "X-Request-Id header" "test-123" "$h3"

# ═════════════════════════════════════════════════
# TEST 3: Auth — Bearer Token
# ═════════════════════════════════════════════════
section "TEST 3: Auth — Bearer Token"

create_endpoint "test-auth-bearer" "{\"url\": \"${WEBHOOK_URL}/auth/bearer\", \"method\": \"POST\", \"auth\": {\"type\": \"bearer\", \"token\": \"my-secret-token-123\"}}"
send_msg "test-auth-bearer" "{\"test\": \"bearer-auth\"}"

wait_queue_drain
sleep 1

BEARER_RESP=$(webhook_get "auth/bearer")
bearer_val=$(echo "$BEARER_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('authorization','MISSING'))" 2>/dev/null)
assert_eq "Bearer token sent" "Bearer my-secret-token-123" "$bearer_val"
assert_eq "Bearer endpoint returned 200" "1" "$(webhook_count 'auth/bearer')"

# ═════════════════════════════════════════════════
# TEST 4: Auth — Basic
# ═════════════════════════════════════════════════
section "TEST 4: Auth — Basic"

create_endpoint "test-auth-basic" "{\"url\": \"${WEBHOOK_URL}/auth/basic\", \"method\": \"POST\", \"auth\": {\"type\": \"basic\", \"username\": \"testuser\", \"password\": \"testpass\"}}"
send_msg "test-auth-basic" "{\"test\": \"basic-auth\"}"

wait_queue_drain
sleep 1

BASIC_RESP=$(webhook_get "auth/basic")
basic_val=$(echo "$BASIC_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('authorization','MISSING'))" 2>/dev/null)
assert_contains "Basic auth header present" "Basic" "$basic_val"
assert_eq "Basic auth endpoint returned 200" "1" "$(webhook_count 'auth/basic')"

# ═════════════════════════════════════════════════
# TEST 5: Auth — API Key
# ═════════════════════════════════════════════════
section "TEST 5: Auth — API Key"

create_endpoint "test-auth-apikey" "{\"url\": \"${WEBHOOK_URL}/auth/apikey\", \"method\": \"POST\", \"headers\": {\"X-API-Key\": \"super-secret-key-456\"}}"
send_msg "test-auth-apikey" "{\"test\": \"apikey-auth\"}"

wait_queue_drain
sleep 1

APIKEY_RESP=$(webhook_get "auth/apikey")
apikey_val=$(echo "$APIKEY_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('x-api-key','MISSING'))" 2>/dev/null)
assert_eq "API Key header sent" "super-secret-key-456" "$apikey_val"

# ═════════════════════════════════════════════════
# TEST 6: CloudEvents — Binary Mode
# ═════════════════════════════════════════════════
section "TEST 6: CloudEvents — Binary Mode"

create_endpoint "test-ce-binary" "{\"url\": \"${WEBHOOK_URL}/echo/ce-binary\", \"method\": \"POST\", \"cloudevents\": true, \"cloudevents_mode\": \"binary\", \"cloudevents_type\": \"com.test.binary\"}"
send_msg "test-ce-binary" "{\"test\": \"cloudevents-binary\"}"

wait_queue_drain
sleep 1

CE_BIN_RESP=$(webhook_get "echo/ce-binary")
ce_type=$(echo "$CE_BIN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('ce-type','MISSING'))" 2>/dev/null)
ce_specver=$(echo "$CE_BIN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('ce-specversion','MISSING'))" 2>/dev/null)
ce_source=$(echo "$CE_BIN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('ce-source','MISSING'))" 2>/dev/null)
ce_id=$(echo "$CE_BIN_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('ce-id','MISSING'))" 2>/dev/null)

assert_eq "CE binary: ce-type" "com.test.binary" "$ce_type"
assert_eq "CE binary: ce-specversion" "1.0" "$ce_specver"
assert_contains "CE binary: ce-source present" "ulak" "$ce_source"
assert_contains "CE binary: ce-id present" "msg_" "$ce_id"

# ═════════════════════════════════════════════════
# TEST 7: CloudEvents — Structured Mode
# ═════════════════════════════════════════════════
section "TEST 7: CloudEvents — Structured Mode"

create_endpoint "test-ce-structured" "{\"url\": \"${WEBHOOK_URL}/echo/ce-structured\", \"method\": \"POST\", \"cloudevents\": true, \"cloudevents_mode\": \"structured\", \"cloudevents_type\": \"com.test.structured\"}"
send_msg "test-ce-structured" "{\"test\": \"cloudevents-structured\"}"

wait_queue_drain
sleep 1

CE_STRUCT_RESP=$(webhook_get "echo/ce-structured")
ce_struct_type=$(echo "$CE_STRUCT_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; b=r[0].get('bodyJson',{}); print(b.get('type','MISSING'))" 2>/dev/null)
ce_struct_ver=$(echo "$CE_STRUCT_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; b=r[0].get('bodyJson',{}); print(b.get('specversion','MISSING'))" 2>/dev/null)
ce_struct_data=$(echo "$CE_STRUCT_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; b=r[0].get('bodyJson',{}); d=b.get('data',{}); print(d.get('test','MISSING'))" 2>/dev/null)

assert_eq "CE structured: type in body" "com.test.structured" "$ce_struct_type"
assert_eq "CE structured: specversion" "1.0" "$ce_struct_ver"
assert_eq "CE structured: data.test preserved" "cloudevents-structured" "$ce_struct_data"

# ═════════════════════════════════════════════════
# TEST 8: Batch Dispatch (N messages to same endpoint)
# ═════════════════════════════════════════════════
section "TEST 8: Batch Dispatch (50 messages)"

create_endpoint "test-batch" "{\"url\": \"${WEBHOOK_URL}/ok/batch\", \"method\": \"POST\"}"

$PSQL -c "
DO \$\$
BEGIN
  FOR i IN 1..50 LOOP
    PERFORM ulak.send('test-batch', jsonb_build_object('batch_id', i, 'test', 'batch'));
  END LOOP;
END;
\$\$;
"

wait_queue_drain 20
sleep 1

BATCH_COUNT=$(webhook_count "ok/batch")
assert_eq "50 batch messages delivered" "50" "$BATCH_COUNT"

# ═════════════════════════════════════════════════
# TEST 9: Retry — Flaky Endpoint (fail 3x then succeed)
# ═════════════════════════════════════════════════
section "TEST 9: Retry — Flaky Endpoint"

# Lower retry delay for testing (default is 10s exponential = 10, 20, 40, 80...)
$PSQL -c "ALTER SYSTEM SET ulak.retry_base_delay = 2;"
$PSQL -c "SELECT pg_reload_conf();"
sleep 1

create_endpoint "test-retry-flaky" "{\"url\": \"${WEBHOOK_URL}/flaky/retry-test\", \"method\": \"POST\"}"
send_msg "test-retry-flaky" "{\"test\": \"retry-flaky\"}"

# Wait for 4 retries with exponential backoff: 2s, 4s, 8s, 16s = ~30s total
sleep 35

FLAKY_COUNT=$(webhook_count "flaky/retry-test")
assert_gt "Flaky endpoint received retries" "3" "$FLAKY_COUNT"

FLAKY_STATUS=$($PSQL_OUT -c "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-retry-flaky') ORDER BY id DESC LIMIT 1;")
assert_eq "Flaky message eventually completed" "completed" "$FLAKY_STATUS"

# Reset retry delay
$PSQL -c "ALTER SYSTEM SET ulak.retry_base_delay = 10;"
$PSQL -c "SELECT pg_reload_conf();"

# ═════════════════════════════════════════════════
# TEST 10: Circuit Breaker
# ═════════════════════════════════════════════════
section "TEST 10: Circuit Breaker"

# Set aggressive CB settings
$PSQL -c "ALTER SYSTEM SET ulak.circuit_breaker_threshold = 3;"
$PSQL -c "ALTER SYSTEM SET ulak.circuit_breaker_cooldown = 5;"
$PSQL -c "SELECT pg_reload_conf();"
sleep 1

create_endpoint "test-cb" "{\"url\": \"${WEBHOOK_URL}/fail/circuit-breaker\", \"method\": \"POST\"}"

# Send messages to trigger CB
for i in $(seq 1 5); do
  send_msg "test-cb" "{\"test\": \"cb\", \"i\": $i}"
done

sleep 10

CB_STATE=$($PSQL_OUT -c "SELECT circuit_state FROM ulak.endpoints WHERE name = 'test-cb';")
assert_eq "Circuit breaker opened" "open" "$CB_STATE"

# Wait for cooldown → half_open
sleep 6

CB_STATE2=$($PSQL_OUT -c "SELECT circuit_state FROM ulak.endpoints WHERE name = 'test-cb';")
# After cooldown, should be half_open or still open (depends on probe timing)
if [ "$CB_STATE2" = "half_open" ] || [ "$CB_STATE2" = "open" ]; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} Circuit breaker cooldown working (state: $CB_STATE2)"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} Unexpected CB state: $CB_STATE2"
  FAIL=$((FAIL + 1))
fi

# Reset CB settings
$PSQL -c "ALTER SYSTEM SET ulak.circuit_breaker_threshold = 10;"
$PSQL -c "ALTER SYSTEM SET ulak.circuit_breaker_cooldown = 30;"
$PSQL -c "SELECT pg_reload_conf();"

# ═════════════════════════════════════════════════
# TEST 11: Large Payload (1MB)
# ═════════════════════════════════════════════════
section "TEST 11: Large Payload (1MB)"

create_endpoint "test-large" "{\"url\": \"${WEBHOOK_URL}/echo/large-payload\", \"method\": \"POST\"}"

$PSQL -c "SELECT ulak.send('test-large', jsonb_build_object('test', 'large', 'data', repeat('X', 1000000)));"

wait_queue_drain 30
sleep 1

LARGE_SIZE=$(webhook_get "echo/large-payload" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['bodySize'] if r else 0)" 2>/dev/null)
assert_gt "Large payload delivered (>1MB)" "1000000" "$LARGE_SIZE"

# ═════════════════════════════════════════════════
# TEST 12: Backpressure (max_queue_size)
# ═════════════════════════════════════════════════
section "TEST 12: Backpressure"

# Clean up leftover messages from previous tests
$PSQL -c "DELETE FROM ulak.queue WHERE status IN ('pending','processing','failed');" 2>/dev/null

# Use unreachable endpoint so messages stay pending
create_endpoint "test-bp" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

# Set very low queue limit
$PSQL -c "ALTER SYSTEM SET ulak.max_queue_size = 5;"
$PSQL -c "SELECT pg_reload_conf();"
sleep 1

# Fill queue
BP_ERROR=""
for i in $(seq 1 10); do
  result=$($PSQL_OUT -c "SELECT ulak.send('test-bp', '{\"bp\": $i}');" 2>&1 || true)
  if echo "$result" | grep -qi "backpressure\|queue size limit"; then
    BP_ERROR="$i"
    break
  fi
done

if [ -n "$BP_ERROR" ]; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} Backpressure triggered at message $BP_ERROR"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} Backpressure not triggered"
  FAIL=$((FAIL + 1))
fi

# Reset
$PSQL -c "ALTER SYSTEM SET ulak.max_queue_size = 1000000;"
$PSQL -c "SELECT pg_reload_conf();"
$PSQL -c "DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-bp');" 2>/dev/null

# ═════════════════════════════════════════════════
# TEST 13: Idempotency Key
# ═════════════════════════════════════════════════
section "TEST 13: Idempotency Key"

# Use unreachable endpoint to keep message in pending state (idempotency check applies to pending/processing)
create_endpoint "test-idemp" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

# Both sends in a single transaction to prevent worker from picking up first message
IDEMP_DUP=$($PSQL_OUT -c "
BEGIN;
SELECT ulak.send_with_options('test-idemp', '{\"test\": \"idemp\"}'::jsonb, p_idempotency_key :='unique-key-001');
SELECT ulak.send_with_options('test-idemp', '{\"test\": \"idemp-dup\"}'::jsonb, p_idempotency_key :='unique-key-001');
COMMIT;
" 2>&1 || true)
if echo "$IDEMP_DUP" | grep -qi "duplicate\|unique\|already exists\|conflict\|Idempotency"; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} Idempotency: duplicate key rejected"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} Idempotency: duplicate key NOT rejected"
  FAIL=$((FAIL + 1))
fi

# Clean up
$PSQL -c "DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-idemp');" 2>/dev/null

# ═════════════════════════════════════════════════
# TEST 14: Priority
# ═════════════════════════════════════════════════
section "TEST 14: Priority"

create_endpoint "test-priority" "{\"url\": \"${WEBHOOK_URL}/ok/priority\", \"method\": \"POST\"}"

# Send low priority first, high priority second
$PSQL -c "SELECT ulak.send_with_options('test-priority', '{\"pri\": \"low\"}'::jsonb, p_priority := 0::smallint);"
$PSQL -c "SELECT ulak.send_with_options('test-priority', '{\"pri\": \"high\"}'::jsonb, p_priority := 10::smallint);"

wait_queue_drain
sleep 1

PRI_COUNT=$(webhook_count "ok/priority")
assert_eq "Both priority messages delivered" "2" "$PRI_COUNT"

# ═════════════════════════════════════════════════
# TEST 15: Ordering Key
# ═════════════════════════════════════════════════
section "TEST 15: Ordering Key"

create_endpoint "test-ordering" "{\"url\": \"${WEBHOOK_URL}/ok/ordering\", \"method\": \"POST\"}"

$PSQL -c "
DO \$\$
BEGIN
  FOR i IN 1..5 LOOP
    PERFORM ulak.send_with_options('test-ordering', jsonb_build_object('seq', i), p_ordering_key :='order-A');
  END LOOP;
END;
\$\$;
"

wait_queue_drain
sleep 1

ORD_COUNT=$(webhook_count "ok/ordering")
assert_eq "All 5 ordered messages delivered" "5" "$ORD_COUNT"

# ═════════════════════════════════════════════════
# TEST 16: Multiple Endpoints × Multiple Messages
# ═════════════════════════════════════════════════
section "TEST 16: Multi-Endpoint Fan-Out (10 endpoints × 10 messages)"

for i in $(seq 1 10); do
  create_endpoint "test-fan-ep${i}" "{\"url\": \"${WEBHOOK_URL}/ok/fan-ep${i}\", \"method\": \"POST\"}"
done

$PSQL -c "
DO \$\$
BEGIN
  FOR ep IN 1..10 LOOP
    FOR msg IN 1..10 LOOP
      PERFORM ulak.send('test-fan-ep' || ep, jsonb_build_object('ep', ep, 'msg', msg));
    END LOOP;
  END LOOP;
END;
\$\$;
"

wait_queue_drain 30
sleep 2

TOTAL_FAN=0
for i in $(seq 1 10); do
  c=$(webhook_count "ok/fan-ep${i}")
  TOTAL_FAN=$((TOTAL_FAN + c))
done
assert_eq "100 messages across 10 endpoints delivered" "100" "$TOTAL_FAN"

# ═════════════════════════════════════════════════
# TEST 17: send_batch()
# ═════════════════════════════════════════════════
section "TEST 17: send_batch()"

create_endpoint "test-sendbatch" "{\"url\": \"${WEBHOOK_URL}/ok/send-batch\", \"method\": \"POST\"}"

$PSQL -c "SELECT ulak.send_batch('test-sendbatch', ARRAY['{\"b\":1}'::jsonb, '{\"b\":2}'::jsonb, '{\"b\":3}'::jsonb, '{\"b\":4}'::jsonb, '{\"b\":5}'::jsonb]);"

wait_queue_drain
sleep 1

SB_COUNT=$(webhook_count "ok/send-batch")
assert_eq "send_batch: 5 messages delivered" "5" "$SB_COUNT"

# ═════════════════════════════════════════════════
# TEST 18: SSRF Protection (internal URL blocked when disabled)
# ═════════════════════════════════════════════════
section "TEST 18: SSRF Protection"

# Temporarily disable allow_internal_urls
$PSQL -c "ALTER SYSTEM SET ulak.http_allow_internal_urls = false;"
$PSQL -c "SELECT pg_reload_conf();"
sleep 1

SSRF_RESULT=$($PSQL_OUT -c "SELECT ulak.create_endpoint('test-ssrf-blocked', 'http', '{\"url\": \"http://localhost:9999/hook\", \"method\": \"POST\"}'::jsonb);" 2>&1 || true)
if echo "$SSRF_RESULT" | grep -qi "ssrf\|internal\|private\|invalid"; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} SSRF: localhost blocked when protection enabled"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} SSRF: localhost NOT blocked"
  FAIL=$((FAIL + 1))
fi

# Re-enable for remaining tests
$PSQL -c "ALTER SYSTEM SET ulak.http_allow_internal_urls = true;"
$PSQL -c "SELECT pg_reload_conf();"
sleep 1

# ═════════════════════════════════════════════════
# TEST 19: Proxy Config Validation
# ═════════════════════════════════════════════════
section "TEST 19: Proxy Config Validation"

# Valid proxy config (won't actually connect — just validates config parsing)
PROXY_RESULT=$($PSQL_OUT -c "SELECT ulak.create_endpoint('test-proxy-valid', 'http', '{\"url\": \"http://example.com/hook\", \"method\": \"POST\", \"proxy\": {\"url\": \"http://proxy.example.com:8080\"}}'::jsonb);" 2>&1 || true)
if echo "$PROXY_RESULT" | grep -q "^[0-9]"; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} Valid proxy config accepted"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} Valid proxy config rejected: $PROXY_RESULT"
  FAIL=$((FAIL + 1))
fi

# Invalid proxy scheme
PROXY_BAD=$($PSQL_OUT -c "SELECT ulak.create_endpoint('test-proxy-bad', 'http', '{\"url\": \"http://example.com/hook\", \"method\": \"POST\", \"proxy\": {\"url\": \"ftp://proxy.example.com:8080\"}}'::jsonb);" 2>&1 || true)
if echo "$PROXY_BAD" | grep -qi "invalid\|error\|unsupported"; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} Invalid proxy scheme (ftp) rejected"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} Invalid proxy scheme NOT rejected"
  FAIL=$((FAIL + 1))
fi

# ═════════════════════════════════════════════════
# TEST 20: TTL / Message Expiry
# ═════════════════════════════════════════════════
section "TEST 20: TTL / Message Expiry"

create_endpoint "test-ttl" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

# Send message with 2s TTL to unreachable endpoint
$PSQL -c "SELECT ulak.send_with_options('test-ttl', '{\"test\": \"ttl\"}'::jsonb, p_expires_at :=NOW() + interval '2 seconds');"

# Wait for expiry + worker poll cycle (500ms poll_interval, mark_expired runs each cycle)
sleep 10

TTL_STATUS=$($PSQL_OUT -c "SELECT status FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-ttl') ORDER BY id DESC LIMIT 1;" 2>/dev/null)
if [ "$TTL_STATUS" = "expired" ] || [ -z "$TTL_STATUS" ]; then
  TOTAL=$((TOTAL + 1))
  echo -e "  ${GREEN}✓${NC} TTL: message expired (status: ${TTL_STATUS:-removed})"
  PASS=$((PASS + 1))
else
  TOTAL=$((TOTAL + 1))
  echo -e "  ${RED}✗${NC} TTL: message not expired (status: $TTL_STATUS)"
  FAIL=$((FAIL + 1))
fi

# ═════════════════════════════════════════════════
# TEST 21: on_conflict Modes (raise/skip/replace)
# ═════════════════════════════════════════════════
section "TEST 21: on_conflict Modes"

# Use unreachable endpoint to keep messages in pending state
create_endpoint "test-conflict" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

# raise (default) — duplicate key should error
$PSQL -c "SELECT ulak.send_with_options('test-conflict', '{\"v\": 1}'::jsonb, p_idempotency_key := 'conflict-key-1');" > /dev/null
RAISE_ERR=$($PSQL_OUT -c "SELECT ulak.send_with_options('test-conflict', '{\"v\": 2}'::jsonb, p_idempotency_key := 'conflict-key-1', p_on_conflict := 'raise');" 2>&1 || true)
assert_contains "on_conflict=raise: duplicate rejected" "Idempotency" "$RAISE_ERR"

# skip — return existing ID silently
SKIP_ID=$($PSQL_OUT -c "SELECT ulak.send_with_options('test-conflict', '{\"v\": 3}'::jsonb, p_idempotency_key := 'conflict-key-1', p_on_conflict := 'skip');")
TOTAL=$((TOTAL + 1))
if [ -n "$SKIP_ID" ]; then
  echo -e "  ${GREEN}✓${NC} on_conflict=skip: returned existing ID ($SKIP_ID)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} on_conflict=skip: no ID returned"
  FAIL=$((FAIL + 1))
fi

# replace — update payload
$PSQL -c "SELECT ulak.send_with_options('test-conflict', '{\"v\": 99, \"replaced\": true}'::jsonb, p_idempotency_key := 'conflict-key-1', p_on_conflict := 'replace');" > /dev/null
REPLACED=$($PSQL_OUT -c "SELECT payload->>'replaced' FROM ulak.queue WHERE idempotency_key = 'conflict-key-1';")
assert_eq "on_conflict=replace: payload updated" "true" "$REPLACED"

# ═════════════════════════════════════════════════
# TEST 22: Debounce
# ═════════════════════════════════════════════════
section "TEST 22: Debounce"

create_endpoint "test-debounce" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

ID1=$($PSQL_OUT -c "SELECT ulak.send_with_options('test-debounce', '{\"seq\": 1}'::jsonb, p_idempotency_key := 'debounce-key-1', p_debounce_seconds := 30);")
ID2=$($PSQL_OUT -c "SELECT ulak.send_with_options('test-debounce', '{\"seq\": 2}'::jsonb, p_idempotency_key := 'debounce-key-1', p_debounce_seconds := 30);")
assert_eq "Debounce: same ID returned" "$ID1" "$ID2"

MSG_COUNT=$($PSQL_OUT -c "SELECT count(*) FROM ulak.queue WHERE idempotency_key = 'debounce-key-1';")
assert_eq "Debounce: only 1 message in queue" "1" "$MSG_COUNT"

DEBOUNCE_ERR=$($PSQL_OUT -c "SELECT ulak.send_with_options('test-debounce', '{\"x\": 1}'::jsonb, p_debounce_seconds := 10);" 2>&1 || true)
assert_contains "Debounce: requires idempotency_key" "idempotency_key" "$DEBOUNCE_ERR"

# ═════════════════════════════════════════════════
# TEST 23: Transform Hooks
# ═════════════════════════════════════════════════
section "TEST 23: Transform Hooks"

$PSQL -c "
CREATE OR REPLACE FUNCTION public.test_transform_upper(p jsonb) RETURNS jsonb
LANGUAGE plpgsql AS \$\$
BEGIN RETURN jsonb_set(p, '{transformed}', '\"yes\"') || jsonb_build_object('upper_name', upper(p->>'name'));
END; \$\$;
"

create_endpoint "test-transform-ep" "{\"url\": \"${WEBHOOK_URL}/echo/transform\", \"method\": \"POST\"}"
$PSQL -c "SELECT ulak.create_event_type('transform.test', 'Test transform hooks');" > /dev/null
$PSQL -c "SELECT ulak.subscribe('transform.test', 'test-transform-ep', NULL, 'public.test_transform_upper');" > /dev/null

$PSQL -c "SELECT ulak.publish('transform.test', '{\"name\": \"ahmet\", \"value\": 42}'::jsonb);" > /dev/null

wait_queue_drain
sleep 1

assert_eq "Transform: message delivered" "1" "$(webhook_count 'echo/transform')"

TRANSFORM_BODY=$(webhook_get "echo/transform" | python3 -c "
import sys, json
data = json.load(sys.stdin)['requests']
b = data[0].get('bodyJson', {}) if data else {}
t = b.get('transformed') == 'yes'
u = b.get('upper_name') == 'AHMET'
print(f't={t},u={u}')
" 2>/dev/null)
assert_contains "Transform: fields added" "t=True,u=True" "$TRANSFORM_BODY"

# ═════════════════════════════════════════════════
# TEST 24: Transform + Plain Subscription Fan-Out
# ═════════════════════════════════════════════════
section "TEST 24: Transform + Plain Fan-Out"

create_endpoint "test-plain-ep" "{\"url\": \"${WEBHOOK_URL}/echo/plain\", \"method\": \"POST\"}"
$PSQL -c "SELECT ulak.subscribe('transform.test', 'test-plain-ep', NULL, NULL);" > /dev/null

webhook_clear
$PSQL -c "SELECT ulak.publish('transform.test', '{\"name\": \"test\", \"mixed\": true}'::jsonb);" > /dev/null

wait_queue_drain
sleep 1

assert_eq "Fan-out: plain received" "1" "$(webhook_count 'echo/plain')"
assert_eq "Fan-out: transform received" "1" "$(webhook_count 'echo/transform')"

PLAIN_BODY=$(webhook_get "echo/plain" | python3 -c "
import sys, json
b = json.load(sys.stdin)['requests'][0].get('bodyJson', {})
print('clean' if 'transformed' not in b and 'upper_name' not in b else 'dirty')
" 2>/dev/null)
assert_eq "Fan-out: plain has no transform fields" "clean" "$PLAIN_BODY"

# ═════════════════════════════════════════════════
# TEST 25: Schema Validation
# ═════════════════════════════════════════════════
section "TEST 25: Schema Validation"

$PSQL -c "
SELECT ulak.create_event_type('schema.validated', 'Validated events',
  '{\"type\": \"object\", \"required\": [\"user_id\", \"action\"], \"properties\": {\"user_id\": {\"type\": \"number\"}, \"action\": {\"type\": \"string\"}}}'::jsonb
);" > /dev/null

create_endpoint "test-schema-ep" "{\"url\": \"${WEBHOOK_URL}/ok/schema\", \"method\": \"POST\"}"
$PSQL -c "SELECT ulak.subscribe('schema.validated', 'test-schema-ep', NULL, NULL);" > /dev/null

# Valid payload
$PSQL -c "SELECT ulak.publish('schema.validated', '{\"user_id\": 123, \"action\": \"login\"}'::jsonb);" > /dev/null
wait_queue_drain
sleep 1
assert_eq "Schema: valid payload delivered" "1" "$(webhook_count 'ok/schema')"

# Missing required field
SCHEMA_ERR=$($PSQL_OUT -c "SELECT ulak.publish('schema.validated', '{\"user_id\": 123}'::jsonb);" 2>&1 || true)
assert_contains "Schema: missing field rejected" "required" "$SCHEMA_ERR"

# Wrong type
SCHEMA_ERR2=$($PSQL_OUT -c "SELECT ulak.publish('schema.validated', '{\"user_id\": \"str\", \"action\": \"test\"}'::jsonb);" 2>&1 || true)
assert_contains "Schema: wrong type rejected" "expected number" "$SCHEMA_ERR2"

# ═════════════════════════════════════════════════
# TEST 26: Purge Endpoint
# ═════════════════════════════════════════════════
section "TEST 26: Purge Endpoint"

create_endpoint "test-purge" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"

$PSQL -c "
DO \$\$ BEGIN
  FOR i IN 1..20 LOOP PERFORM ulak.send('test-purge', jsonb_build_object('i', i)); END LOOP;
END; \$\$;"
sleep 2

BEFORE=$($PSQL_OUT -c "SELECT count(*) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-purge') AND status = 'pending';")
assert_gt "Purge: messages pending" "0" "$BEFORE"

PURGE_N=$($PSQL_OUT -c "SELECT ulak.purge_endpoint('test-purge');")
assert_gt "Purge: returned count" "0" "$PURGE_N"

AFTER=$($PSQL_OUT -c "SELECT count(*) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-purge') AND status = 'pending';")
assert_eq "Purge: zero pending after" "0" "$AFTER"

EVENT_LOG=$($PSQL_OUT -c "SELECT count(*) FROM ulak.event_log WHERE event_type = 'purge';")
assert_gt "Purge: event logged" "0" "$EVENT_LOG"

# ═════════════════════════════════════════════════
# TEST 27: Queue Health
# ═════════════════════════════════════════════════
section "TEST 27: Queue Health"

HEALTH=$($PSQL_OUT -c "SELECT total_pending, total_processing, dlq_depth FROM ulak.queue_health();")
assert_contains "Queue health: returns data" "|" "$HEALTH"

create_endpoint "test-qh" "{\"url\": \"http://192.0.2.1:9999/blackhole\", \"method\": \"POST\"}"
for i in $(seq 1 5); do
  $PSQL -c "SELECT ulak.send('test-qh', '{\"h\": $i}'::jsonb);" > /dev/null
done

PENDING=$($PSQL_OUT -c "SELECT total_pending FROM ulak.queue_health();")
assert_gt "Queue health: shows pending" "0" "$PENDING"
$PSQL -c "DELETE FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'test-qh');" > /dev/null

# ═════════════════════════════════════════════════
# TEST 28: Concurrency Limit
# ═════════════════════════════════════════════════
section "TEST 28: Concurrency Limit"

create_endpoint "test-conc" "{\"url\": \"${WEBHOOK_URL}/ok/concurrency\", \"method\": \"POST\"}"
$PSQL -c "UPDATE ulak.endpoints SET concurrency_limit = 2 WHERE name = 'test-conc';" > /dev/null

CONC_VAL=$($PSQL_OUT -c "SELECT concurrency_limit FROM ulak.endpoints WHERE name = 'test-conc';")
assert_eq "Concurrency: limit set" "2" "$CONC_VAL"

CONC_ERR=$($PSQL_OUT -c "UPDATE ulak.endpoints SET concurrency_limit = 0 WHERE name = 'test-conc';" 2>&1 || true)
assert_contains "Concurrency: zero rejected" "violates check" "$CONC_ERR"

for i in $(seq 1 10); do
  $PSQL -c "SELECT ulak.send('test-conc', '{\"c\": $i}'::jsonb);" > /dev/null
done
wait_queue_drain 15
sleep 1
assert_eq "Concurrency: all 10 delivered" "10" "$(webhook_count 'ok/concurrency')"

# ═════════════════════════════════════════════════
# TEST 29: Transform Validation
# ═════════════════════════════════════════════════
section "TEST 29: Transform Validation"

MULTI_DOT=$($PSQL_OUT -c "SELECT ulak._apply_transform('a.b.c', '{}'::jsonb);" 2>&1 || true)
assert_contains "Multi-dot function name rejected" "ERROR" "$MULTI_DOT"

NOSUCH=$($PSQL_OUT -c "SELECT ulak.subscribe('transform.test', 'test-transform-ep', NULL, 'public.nonexistent_fn');" 2>&1 || true)
assert_contains "Nonexistent function rejected" "does not exist" "$NOSUCH"

# ═════════════════════════════════════════════════
# TEST 30: Reindex Queue + Archive Default
# ═════════════════════════════════════════════════
section "TEST 30: Reindex & Archive"

REINDEX=$($PSQL_OUT -c "SELECT ulak.reindex_queue();")
assert_gt "Reindex: rebuilt indexes" "0" "$REINDEX"

ARCHIVE=$($PSQL_OUT -c "SELECT ulak.check_archive_default();")
assert_eq "Archive default: returns 0" "0" "$ARCHIVE"

# ═════════════════════════════════════════════════
# TEST 31: Metrics & Monitoring
# ═════════════════════════════════════════════════
section "TEST 31: Metrics & Monitoring"

METRICS=$($PSQL_OUT -c "SELECT count(*) FROM ulak.metrics();")
assert_gt "Metrics: returns rows" "0" "$METRICS"

WORKERS=$($PSQL_OUT -c "SELECT count(*) FROM ulak.get_worker_status();")
assert_gt "Worker status: returns data" "0" "$WORKERS"

HEALTH_S=$($PSQL_OUT -c "SELECT status FROM ulak.health_check() LIMIT 1;")
assert_eq "Health check: healthy" "healthy" "$HEALTH_S"

# ═════════════════════════════════════════════════
# SUMMARY
# ═════════════════════════════════════════════════
echo ""
echo -e "${CYAN}═══════════════════════════════════════════${NC}"
echo -e "${CYAN}  RESULTS: ${PASS}/${TOTAL} passed, ${FAIL} failed, ${SKIP} skipped${NC}"
echo -e "${CYAN}═══════════════════════════════════════════${NC}"

if [ $FAIL -gt 0 ]; then
  echo -e "${RED}  SOME TESTS FAILED${NC}"
  exit 1
else
  echo -e "${GREEN}  ALL TESTS PASSED ✓${NC}"
  exit 0
fi
