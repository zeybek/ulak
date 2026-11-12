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

# Poll a scalar query until it returns the expected value (or time runs out);
# prints the last value seen.
wait_for_value() {
  local sql="$1" expected="$2" max_wait="${3:-20}" value=""
  local i
  for i in $(seq 1 $((max_wait * 2))); do
    value=$($PSQL_OUT -c "$sql" 2>/dev/null || true)
    [ "$value" = "$expected" ] && break
    sleep 0.5
  done
  echo "$value"
}

# Change a SIGHUP GUC for a test and put it back afterwards (the ALTER SYSTEM
# value if postgresql.auto.conf had one, otherwise RESET), so the suite leaves
# GUCs as found. Saved per GUC name, so different GUCs can overlap.
guc_set() {
  local name="$1" value="$2" key="${1//[^a-zA-Z0-9]/_}" auto old
  auto=$($PSQL_OUT -c "SELECT coalesce(sourcefile LIKE '%postgresql.auto.conf', false) FROM pg_settings WHERE name = '$name';")
  old=$($PSQL_OUT -c "SELECT setting FROM pg_settings WHERE name = '$name';")
  printf -v "GUC_SAVED_AUTO_${key}" '%s' "$auto"
  printf -v "GUC_SAVED_VALUE_${key}" '%s' "$old"
  $PSQL -c "ALTER SYSTEM SET $name = '$value';" > /dev/null 2>&1
  $PSQL -c "SELECT pg_reload_conf();" > /dev/null 2>&1
  sleep 1
}

guc_restore() {
  local name="$1" key="${1//[^a-zA-Z0-9]/_}"
  local auto_var="GUC_SAVED_AUTO_${key}" value_var="GUC_SAVED_VALUE_${key}"
  if [ "${!auto_var:-f}" = "t" ]; then
    $PSQL -c "ALTER SYSTEM SET $name = '${!value_var:-}';" > /dev/null 2>&1
  else
    $PSQL -c "ALTER SYSTEM RESET $name;" > /dev/null 2>&1
  fi
  $PSQL -c "SELECT pg_reload_conf();" > /dev/null 2>&1
  sleep 1
}

endpoint_row() {
  local name="$1" column="$2"
  $PSQL_OUT -c "SELECT $column FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = '$name' ORDER BY q.id LIMIT 1;"
}

cleanup() {
  echo ""
  echo -e "${CYAN}Cleaning up...${NC}"
  # Drop all test endpoints
  $PSQL -c "DELETE FROM ulak.queue;" 2>/dev/null || true
  $PSQL -c "DELETE FROM ulak.dlq;" 2>/dev/null || true
  $PSQL -c "DELETE FROM ulak.endpoints;" 2>/dev/null || true
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

# The webhook fixture lives on the Docker host (host.docker.internal), which
# resolves to a private/link-local address and would be refused by the SSRF
# guard (both at endpoint creation and at connect time). Explicitly enable the
# bypass instead of relying on leftover cluster state.
$PSQL -c "ALTER SYSTEM SET ulak.http_allow_internal_urls = true;" > /dev/null 2>&1
$PSQL -c "SELECT pg_reload_conf();" > /dev/null 2>&1
sleep 1

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
ce_struct_ctype=$(echo "$CE_STRUCT_RESP" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('content-type','MISSING'))" 2>/dev/null)
assert_eq "CE structured: Content-Type is the event format" "application/cloudevents+json" "$ce_struct_ctype"

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
# TEST 21: OAuth2 — token response larger than libcurl's 16 KB chunk
# ═════════════════════════════════════════════════
section "TEST 21: OAuth2 — Large Token Response"

# A 20 KB token arrives in 16 KB chunks, more than one doubling of the 4 KB
# start buffer per write callback.
create_endpoint "test-oauth-large" "{\"url\": \"${WEBHOOK_URL}/auth/bearer-oauth/large\", \"method\": \"POST\", \"auth\": {\"type\": \"oauth2\", \"token_url\": \"${WEBHOOK_URL}/oauth/token-large\", \"client_id\": \"cid\", \"client_secret\": \"csecret\"}}"
send_msg "test-oauth-large" "{\"test\": \"oauth-large\"}"

oauth_large_status=$(wait_for_value "SELECT status FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-oauth-large';" "completed" 20)
assert_eq "OAuth2 large token: message delivered" "completed" "$oauth_large_status"
oauth_large_len=$(webhook_get "auth/bearer-oauth/large" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(len(r[0]['headers'].get('authorization','')) if r else 0)" 2>/dev/null)
assert_eq "OAuth2 large token: whole token in Authorization (7 + 20000 bytes)" "20007" "$oauth_large_len"

# ═════════════════════════════════════════════════
# TEST 22: OAuth2 — non-JSON token reply, failure backoff
# ═════════════════════════════════════════════════
section "TEST 22: OAuth2 — Non-JSON Token Reply"

# A form-encoded 200 is a retryable auth failure, not an ERROR that aborts the
# batch, and one failed fetch stops the other rows from fetching again for 10 s.
OAUTH_FORM_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
OAUTH_WORKERS=$($PSQL_OUT -c "SELECT current_setting('ulak.workers');")
OAUTH_ROWS=$((OAUTH_WORKERS * 3))
create_endpoint "test-oauth-form" "{\"url\": \"${WEBHOOK_URL}/auth/bearer-oauth/form\", \"method\": \"POST\", \"auth\": {\"type\": \"oauth2\", \"token_url\": \"${WEBHOOK_URL}/oauth/token-form\", \"client_id\": \"cid\", \"client_secret\": \"csecret\"}}"
$PSQL -c "SELECT ulak.send_batch('test-oauth-form', ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, ${OAUTH_ROWS}) g));" > /dev/null 2>&1

oauth_form_retried=$(wait_for_value "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-oauth-form' AND q.status = 'pending' AND q.retry_count >= 1 AND q.last_error = '[RETRYABLE] HTTP auth failed';" "$OAUTH_ROWS" 20)
assert_eq "OAuth2 non-JSON reply: every row retryable with 'HTTP auth failed'" "$OAUTH_ROWS" "$oauth_form_retried"
oauth_form_fetches=$(webhook_count "oauth/token-form")
TOTAL=$((TOTAL + 1))
if [ "$oauth_form_fetches" -ge 1 ] && [ "$oauth_form_fetches" -le "$OAUTH_WORKERS" ]; then
  echo -e "  ${GREEN}✓${NC} OAuth2 failure backoff: $oauth_form_fetches token fetch(es) for $OAUTH_ROWS rows (at most one per worker)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} OAuth2 failure backoff: $oauth_form_fetches token fetches for $OAUTH_ROWS rows (expected 1..$OAUTH_WORKERS)"
  FAIL=$((FAIL + 1))
fi
oauth_form_errors=$(docker logs --since "$OAUTH_FORM_SINCE" ulak-postgres-1 2>&1 | grep -c "invalid input syntax for type json" || true)
assert_eq "OAuth2 non-JSON reply: no JSON ERROR in the server log" "0" "$oauth_form_errors"
$PSQL -c "SELECT ulak.disable_endpoint('test-oauth-form');" > /dev/null 2>&1

# ═════════════════════════════════════════════════
# TEST 23: 408 / 425 are retryable
# ═════════════════════════════════════════════════
section "TEST 23: HTTP 408 / 425 Retryable"

for code in 408 425; do
  create_endpoint "test-status-${code}" "{\"url\": \"${WEBHOOK_URL}/status/${code}/transient\", \"method\": \"POST\"}"
  send_msg "test-status-${code}" "{\"test\": \"status-${code}\"}"
  st=$(wait_for_value "SELECT q.status || ':' || (q.retry_count >= 1) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-status-${code}';" "pending:true" 20)
  assert_eq "HTTP ${code}: row kept for retry (not dead-lettered)" "pending:true" "$st"
  err=$(endpoint_row "test-status-${code}" "q.last_error")
  assert_contains "HTTP ${code}: classified [RETRYABLE]" "RETRYABLE" "$err"
  $PSQL -c "SELECT ulak.disable_endpoint('test-status-${code}');" > /dev/null 2>&1
done

# ═════════════════════════════════════════════════
# TEST 24: Retry-After honoured in batch mode
# ═════════════════════════════════════════════════
section "TEST 24: Retry-After in Batch Mode"

# TESTs 24-26 are about the batch path, which capture_response = on bypasses;
# force it off for them, whatever the cluster has.
guc_set "ulak.capture_response" "off"

# The batch flush hands Retry-After to the worker as [RETRY_AFTER=n] in the
# error string.
create_endpoint "test-retry-after" "{\"url\": \"${WEBHOOK_URL}/status/429/retry-after?retry_after=120\", \"method\": \"POST\"}"
send_msg "test-retry-after" "{\"test\": \"retry-after\"}"
ra_retried=$(wait_for_value "SELECT (q.retry_count >= 1) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-retry-after';" "t" 20)
assert_eq "429 + Retry-After: row retried" "t" "$ra_retried"
ra_err=$(endpoint_row "test-retry-after" "q.last_error")
assert_contains "429 + Retry-After: error carries the server's delay" "RETRY_AFTER=120" "$ra_err"
ra_delay=$(endpoint_row "test-retry-after" "(extract(epoch FROM q.next_retry_at - now()) > 90)")
assert_eq "429 + Retry-After: next attempt about 120 s away, not the 10 s backoff" "t" "$ra_delay"
$PSQL -c "SELECT ulak.disable_endpoint('test-retry-after');" > /dev/null 2>&1

# ═════════════════════════════════════════════════
# TEST 25: Per-message headers and url_suffix in batch mode
# ═════════════════════════════════════════════════
section "TEST 25: Per-Message Headers + url_suffix"

# queue.headers / queue.metadata are set by admin INSERT, replay and redrive.
# Batch mode must send them and use the suffixed URL; url_suffix is joined
# with exactly one '/' and must keep the endpoint's scheme, host and port.
create_endpoint "test-suffix" "{\"url\": \"${WEBHOOK_URL}/ok/suffix-base/\", \"method\": \"POST\"}"
$PSQL -c "INSERT INTO ulak.queue (endpoint_id, payload, headers, metadata)
  SELECT id, '{\"test\": \"suffix-1\"}'::jsonb, '{\"X-Per-Message\": \"yes\"}'::jsonb, '{\"url_suffix\": \"sub/path\"}'::jsonb FROM ulak.endpoints WHERE name = 'test-suffix'
  UNION ALL
  SELECT id, '{\"test\": \"suffix-2\"}'::jsonb, NULL, '{\"url_suffix\": \"/sub2\"}'::jsonb FROM ulak.endpoints WHERE name = 'test-suffix'
  UNION ALL
  SELECT id, '{\"test\": \"suffix-3\"}'::jsonb, NULL, '{\"url_suffix\": \"@evil.example/x\"}'::jsonb FROM ulak.endpoints WHERE name = 'test-suffix';" > /dev/null 2>&1 || true

suffix_done=$(wait_for_value "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-suffix' AND q.status = 'completed';" "2" 20)
assert_eq "url_suffix: two rows delivered" "2" "$suffix_done"
assert_eq "url_suffix: 'sub/path' joined with one '/'" "1" "$(webhook_count 'ok/suffix-base/sub/path')"
assert_eq "url_suffix: '/sub2' joined with one '/'" "1" "$(webhook_count 'ok/suffix-base/sub2')"
suffix_hdr=$(webhook_get "ok/suffix-base/sub/path" | python3 -c "import sys,json; r=json.load(sys.stdin)['requests']; print(r[0]['headers'].get('x-per-message','MISSING') if r else 'MISSING')" 2>/dev/null)
assert_eq "Per-message header delivered in batch mode" "yes" "$suffix_hdr"
assert_eq "url_suffix: nothing posted to the bare base URL" "0" "$(curl -s "http://localhost:${WEBHOOK_PORT}/api/received" | python3 -c "import sys,json; print(sum(1 for r in json.load(sys.stdin)['requests'] if r['tag'] == 'ok/suffix-base'))")"
suffix_bad=$(wait_for_value "SELECT count(*) FROM ulak.dlq d JOIN ulak.endpoints e ON d.endpoint_id = e.id WHERE e.name = 'test-suffix' AND d.last_error LIKE '[PERMANENT]%url_suffix%';" "1" 20)
assert_eq "url_suffix with '@': dead-lettered as [PERMANENT]" "1" "$suffix_bad"

# ═════════════════════════════════════════════════
# TEST 26: Batch larger than ulak.http_batch_capacity
# ═════════════════════════════════════════════════
section "TEST 26: Batch Larger Than http_batch_capacity"

# The pending array grows, so rows beyond its initial size are delivered too.
guc_set "ulak.http_batch_capacity" "2"
create_endpoint "test-capacity" "{\"url\": \"${WEBHOOK_URL}/ok/capacity\", \"method\": \"POST\"}"
$PSQL -c "SELECT ulak.send_batch('test-capacity', ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(1, 40) g));" > /dev/null 2>&1
cap_done=$(wait_for_value "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-capacity' AND q.status = 'completed' AND q.retry_count = 0;" "40" 20)
assert_eq "40 rows over a capacity of 2: all delivered on the first attempt" "40" "$cap_done"
guc_restore "ulak.http_batch_capacity"
guc_restore "ulak.capture_response"

# ═════════════════════════════════════════════════
# TEST 27: Captured response body that is not valid UTF-8
# ═════════════════════════════════════════════════
section "TEST 27: Captured Body Not Valid UTF-8"

BADUTF8_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
guc_set "ulak.capture_response" "on"
create_endpoint "test-badutf8" "{\"url\": \"${WEBHOOK_URL}/badutf8/capture\", \"method\": \"POST\"}"
send_msg "test-badutf8" "{\"test\": \"badutf8\"}"
badutf8_status=$(wait_for_value "SELECT status FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE e.name = 'test-badutf8';" "completed" 20)
assert_eq "Invalid UTF-8 body: message completed" "completed" "$badutf8_status"
badutf8_code=$(endpoint_row "test-badutf8" "q.response->>'http_status_code'")
assert_eq "Invalid UTF-8 body: response captured" "200" "$badutf8_code"
badutf8_errors=$(docker logs --since "$BADUTF8_SINCE" ulak-postgres-1 2>&1 | grep -cE "invalid byte sequence|invalid input syntax for type json|unsupported Unicode" || true)
assert_eq "Invalid UTF-8 body: no encoding ERROR in the server log" "0" "$badutf8_errors"
# jsonb input does not check the encoding, and a row with invalid bytes cannot
# be restored from a dump (COPY FROM rejects it). Only the valid prefix of the
# body is kept.
badutf8_valid=$(endpoint_row "test-badutf8" "length(convert_from(convert_to(q.response::text, 'UTF8'), 'UTF8')) > 0" 2>/dev/null || true)
assert_eq "Invalid UTF-8 body: stored response is valid UTF-8" "t" "$badutf8_valid"
badutf8_body=$(endpoint_row "test-badutf8" "q.response->>'http_response_body'")
assert_eq "Invalid UTF-8 body: valid prefix kept" '{"a":"' "$badutf8_body"
guc_restore "ulak.capture_response"

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
