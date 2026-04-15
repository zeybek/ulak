#!/usr/bin/env bash
set -uo pipefail

# ─────────────────────────────────────────────────
# ulak Advanced Test Suite
# 8 tests: crash recovery, memory, ordering,
#          resilience, DLQ, DDL, signatures, edge cases
# Each test gets a fresh Docker environment
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/test-helpers.sh"

# Pre-flight: webhook server must be running
if ! webhook_health; then
  echo -e "${RED}ERROR: Webhook server not running on port ${WEBHOOK_PORT}${NC}"
  echo "  Start it: node $WEBHOOK_SERVER"
  exit 1
fi

echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║  ulak Advanced Test Suite — 8 Tests                  ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"

# ═══════════════════════════════════════════════════
# TEST 1: Crash Recovery
# ═══════════════════════════════════════════════════
header "TEST 1: Crash Recovery"

fresh_env "ulak.workers=1" "ulak.poll_interval=100" "ulak.stale_recovery_timeout=10"
webhook_clear

create_endpoint "crash-test" "{\"url\": \"${WEBHOOK_URL}/ok/crash-recovery\", \"method\": \"POST\"}"

section "Enqueuing 100 messages..."
send_batch "crash-test" 100

# Let worker start processing
sleep 2

# Kill the worker
WORKER_PID=$(psql_exec "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE '%ulak%' AND state != 'idle' LIMIT 1;")
if [ -z "$WORKER_PID" ]; then
  WORKER_PID=$(psql_exec "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE '%ulak%' LIMIT 1;")
fi

section "Killing worker PID=$WORKER_PID..."
psql_exec "SELECT pg_terminate_backend(${WORKER_PID});" > /dev/null

sleep 1

# Check for stuck processing messages
PROCESSING=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'processing';")
COMPLETED_BEFORE=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
echo "  Status after kill: processing=$PROCESSING, completed=$COMPLETED_BEFORE"

section "Waiting for stale recovery + re-dispatch (20s)..."
sleep 20

# Worker should have restarted and recovered stale messages
wait_queue_drain 30

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
DELIVERED=$(webhook_count "ok/crash-recovery")

assert_eq "All 100 messages completed" "100" "$COMPLETED"
assert_gt "Webhook received >= 100 (at-least-once)" "99" "$DELIVERED"
# Note: pg_terminate_backend causes expected FATAL log — check for PANIC only
PANICS=$(docker logs ulak-postgres-1 2>&1 | grep -c "PANIC" || true)
TOTAL=$((TOTAL + 1))
if [ "$PANICS" = "0" ]; then
  echo -e "  ${GREEN}✓${NC} No PANIC in PostgreSQL logs (FATAL from terminate is expected)"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} Found $PANICS PANIC entries in logs"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 2: Memory Leak Detection
# ═══════════════════════════════════════════════════
header "TEST 2: Memory Leak Detection (3 minute sustained load)"

fresh_env "ulak.workers=2" "ulak.poll_interval=100" "ulak.batch_size=200"
webhook_clear

create_endpoint "mem-test" "{\"url\": \"${WEBHOOK_URL}/ok/memory-test\", \"method\": \"POST\"}"

# Warmup phase — let PG shared buffers, connection pools, and caches stabilize
section "Warmup phase (2000 messages)..."
send_batch "mem-test" 2000
wait_queue_drain 30

# Get worker PIDs AFTER warmup
WORKER_PIDS=$(docker exec ulak-postgres-1 bash -c "ps aux | grep 'ulak' | grep -v grep | awk '{print \$2}'" 2>/dev/null)
FIRST_PID=$(echo "$WORKER_PIDS" | head -1)

if [ -n "$FIRST_PID" ]; then
  # Baseline memory AFTER warmup (PG caches populated, shared buffers warm)
  MEM_START=$(docker exec ulak-postgres-1 bash -c "cat /proc/${FIRST_PID}/status 2>/dev/null | grep VmRSS | awk '{print \$2}'" 2>/dev/null)
  echo "  Post-warmup baseline RSS: ${MEM_START} kB (PID $FIRST_PID)"
fi

section "Sustained load for 3 minutes..."
TOTAL_SENT=0
for round in $(seq 1 36); do  # 36 rounds × 5s = 180s = 3 min
  send_batch "mem-test" 500
  TOTAL_SENT=$((TOTAL_SENT + 500))
  sleep 5

  # Sample memory every 30s
  if [ $((round % 6)) -eq 0 ] && [ -n "$FIRST_PID" ]; then
    MEM_NOW=$(docker exec ulak-postgres-1 bash -c "cat /proc/${FIRST_PID}/status 2>/dev/null | grep VmRSS | awk '{print \$2}'" 2>/dev/null)
    ELAPSED=$((round * 5))
    echo "  ${ELAPSED}s: RSS=${MEM_NOW} kB, sent=${TOTAL_SENT}"
  fi
done

section "Waiting for drain..."
wait_queue_drain 60

if [ -n "$FIRST_PID" ]; then
  MEM_END=$(docker exec ulak-postgres-1 bash -c "cat /proc/${FIRST_PID}/status 2>/dev/null | grep VmRSS | awk '{print \$2}'" 2>/dev/null)
  echo "  Final RSS: ${MEM_END} kB"

  if [ -n "$MEM_START" ] && [ -n "$MEM_END" ] && [ "$MEM_START" -gt 0 ] 2>/dev/null; then
    GROWTH=$(python3 -c "print(f'{(${MEM_END} - ${MEM_START}) / ${MEM_START} * 100:.1f}')")
    echo "  Memory growth: ${GROWTH}%"

    TOTAL=$((TOTAL + 1))
    # Allow up to 50% growth (conservative for Docker + PG internals)
    if python3 -c "exit(0 if ${MEM_END} < ${MEM_START} * 1.5 else 1)" 2>/dev/null; then
      echo -e "  ${GREEN}✓${NC} Memory growth within bounds (<50%)"
      PASS=$((PASS + 1))
    else
      echo -e "  ${RED}✗${NC} Memory growth exceeds 50%: ${MEM_START}kB → ${MEM_END}kB"
      FAIL=$((FAIL + 1))
    fi
  fi
fi

DELIVERED=$(webhook_count "ok/memory-test")
assert_gt "Messages delivered" "0" "$DELIVERED"
echo "  Total sent: $TOTAL_SENT, delivered: $DELIVERED"

# Verify worker didn't crash (same PID)
if [ -n "$FIRST_PID" ]; then
  STILL_ALIVE=$(docker exec ulak-postgres-1 bash -c "kill -0 ${FIRST_PID} 2>/dev/null && echo yes || echo no")
  assert_eq "Worker survived (PID $FIRST_PID)" "yes" "$STILL_ALIVE"
fi

# ═══════════════════════════════════════════════════
# TEST 3: Ordering Key Correctness
# ═══════════════════════════════════════════════════
header "TEST 3: Ordering Key Correctness"

fresh_env "ulak.workers=4" "ulak.poll_interval=100" "ulak.batch_size=200"
webhook_clear

create_endpoint "order-test" "{\"url\": \"${WEBHOOK_URL}/echo/ordering\", \"method\": \"POST\"}"

section "Sending 500 messages (10 keys × 50 each)..."
psql_quiet "
DO \$\$
BEGIN
  FOR k IN 1..10 LOOP
    FOR s IN 1..50 LOOP
      PERFORM ulak.send_with_options(
        'order-test',
        jsonb_build_object('key', 'order-' || k, 'seq', s),
        p_ordering_key := 'order-' || k
      );
    END LOOP;
  END LOOP;
END;
\$\$;"

section "Waiting for delivery..."
wait_queue_drain 60

DELIVERED=$(webhook_count "echo/ordering")
assert_eq "All 500 messages delivered" "500" "$DELIVERED"

section "Verifying order per key..."
ORDER_OK=$(webhook_get "echo/ordering" | python3 -c "
import sys, json
data = json.load(sys.stdin)['requests']
# Group by ordering key
groups = {}
for r in data:
  body = r.get('bodyJson', {})
  key = body.get('key', '')
  seq = body.get('seq', 0)
  if key not in groups:
    groups[key] = []
  groups[key].append(seq)

# Check each group is strictly increasing
all_ok = True
violations = 0
for key, seqs in sorted(groups.items()):
  for i in range(1, len(seqs)):
    if seqs[i] <= seqs[i-1]:
      violations += 1
      all_ok = False

print(f'{len(groups)} keys, {violations} violations, {\"OK\" if all_ok else \"FAIL\"}')
" 2>/dev/null)

assert_contains "Ordering correct" "OK" "$ORDER_OK"
echo "  $ORDER_OK"

# ═══════════════════════════════════════════════════
# TEST 4: Connection Resilience
# ═══════════════════════════════════════════════════
header "TEST 4: Connection Resilience (SKIPPED — webhook kill crashes OrbStack on macOS)"
echo -e "  ${YELLOW}⚠ Skipped: lsof/kill on macOS OrbStack crashes Docker daemon${NC}"
echo -e "  ${YELLOW}  Run this test on Linux or bare-metal PostgreSQL${NC}"
TOTAL=$((TOTAL + 4))
SKIP=$((SKIP + 4))
if false; then
# --- SKIP START ---

fresh_env "ulak.workers=2" "ulak.poll_interval=100" \
  "ulak.retry_base_delay=2" "ulak.circuit_breaker_threshold=50"
webhook_clear

create_endpoint "resilience" "{\"url\": \"${WEBHOOK_URL}/ok/resilience\", \"method\": \"POST\"}"

section "Phase 1: Baseline — 50 messages with server UP..."
send_batch "resilience" 50
wait_queue_drain 15
PHASE1=$(webhook_count "ok/resilience")
assert_eq "Phase 1: 50 delivered" "50" "$PHASE1"

section "Phase 2: Stop webhook server, send 50 more..."
# Kill ALL processes on webhook port
for pid in $(lsof -ti :${WEBHOOK_PORT} 2>/dev/null); do
  kill -9 "$pid" 2>/dev/null
done
sleep 2

# Verify server is actually down
if webhook_health; then
  echo -e "  ${YELLOW}Warning: webhook server still running, trying harder...${NC}"
  for pid in $(lsof -ti :${WEBHOOK_PORT} 2>/dev/null); do
    kill -9 "$pid" 2>/dev/null
  done
  sleep 2
fi

send_batch "resilience" 50
sleep 15  # Give workers time to attempt delivery and fail

# Messages should be stuck (pending with retry delay or failed)
STUCK=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing','failed') AND endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'resilience');")
assert_gt "Messages stuck while server down" "0" "$STUCK"

section "Phase 3: Restart webhook server..."
# Wait for port to be free
for i in $(seq 1 10); do
  lsof -ti :${WEBHOOK_PORT} > /dev/null 2>&1 || break
  sleep 1
done
node "$WEBHOOK_SERVER" > /dev/null 2>&1 &
WEBHOOK_NEW_PID=$!
sleep 3

# Verify webhook server is back
if webhook_health; then
  echo "  Webhook server restarted (PID $WEBHOOK_NEW_PID)"
else
  echo -e "  ${RED}Webhook server failed to restart${NC}"
fi

section "Waiting for recovery (60s)..."
wait_queue_drain 60

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
DELIVERED=$(webhook_count "ok/resilience")

assert_eq "All 100 completed" "100" "$COMPLETED"
# Phase 2 messages delivered after server restart
assert_gt "Webhook received messages after recovery" "40" "$DELIVERED"
# --- SKIP END ---
fi

# ═══════════════════════════════════════════════════
# TEST 5: DLQ + Redrive
# ═══════════════════════════════════════════════════
header "TEST 5: DLQ + Redrive"

fresh_env "ulak.workers=1" "ulak.poll_interval=100" \
  "ulak.default_max_retries=3" "ulak.retry_base_delay=1" \
  "ulak.circuit_breaker_threshold=1000" "ulak.batch_size=1"
webhook_clear

create_endpoint "dlq-source" "{\"url\": \"${WEBHOOK_URL}/permanent/dlq-test\", \"method\": \"POST\"}"

section "Sending 5 messages to permanent-fail endpoint..."
for i in $(seq 1 5); do
  send_msg "dlq-source" "{\"dlq_test\": $i}"
done

section "Waiting for permanent fail → DLQ (30s)..."
# Permanent errors (HTTP 400) are detected immediately — [PERMANENT] prefix
# But worker needs to process the batch first. Wait generously.
for i in $(seq 1 60); do
  DLQ_NOW=$(psql_exec "SELECT count(*) FROM ulak.dlq;" 2>/dev/null)
  [ "$DLQ_NOW" = "5" ] && break
  sleep 0.5
done

# Check DLQ
DLQ_COUNT=$(psql_exec "SELECT count(*) FROM ulak.dlq;")
QUEUE_REMAINING=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE endpoint_id = (SELECT id FROM ulak.endpoints WHERE name = 'dlq-source');")
assert_eq "5 messages in DLQ" "5" "$DLQ_COUNT"
assert_eq "0 messages left in queue" "0" "$QUEUE_REMAINING"

# DLQ summary
DLQ_SUMMARY=$(psql_exec "SELECT endpoint_name || ':' || failed_count FROM ulak.dlq_summary();")
assert_contains "DLQ summary shows endpoint" "dlq-source:5" "$DLQ_SUMMARY"

section "Changing endpoint URL to success + redrive..."
# Update endpoint config to point to success URL + reset circuit breaker
psql_quiet "UPDATE ulak.endpoints SET config = '{\"url\": \"${WEBHOOK_URL}/ok/dlq-redrived\", \"method\": \"POST\"}'::jsonb, circuit_state = 'closed', circuit_failure_count = 0 WHERE name = 'dlq-source';"
psql_quiet "SELECT ulak.redrive_endpoint('dlq-source');"

wait_queue_drain 15

REDRIVED_COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
REDRIVED_DELIVERED=$(webhook_count "ok/dlq-redrived")

assert_eq "5 redrived messages completed" "5" "$REDRIVED_COMPLETED"
assert_eq "5 redrived messages delivered" "5" "$REDRIVED_DELIVERED"

# ═══════════════════════════════════════════════════
# TEST 6: Concurrent DDL
# ═══════════════════════════════════════════════════
header "TEST 6: Concurrent DDL"

fresh_env "ulak.workers=4" "ulak.poll_interval=100" "ulak.batch_size=50"
webhook_clear

create_endpoint "ddl-test" "{\"url\": \"${WEBHOOK_URL}/ok/ddl-test\", \"method\": \"POST\"}"

section "Enqueuing 1000 messages..."
send_batch "ddl-test" 1000

section "Concurrent DDL operations during dispatch..."

# Fire DDL operations in background while messages are being dispatched
(
  sleep 1
  # Config reload (SIGHUP)
  docker exec ulak-postgres-1 psql -U postgres -d ulak_test -q -t -A \
    -c "ALTER SYSTEM SET ulak.batch_size = 100;" 2>/dev/null
  docker exec ulak-postgres-1 psql -U postgres -d ulak_test -q -t -A \
    -c "SELECT pg_reload_conf();" 2>/dev/null
  # Create new endpoint + send message
  docker exec ulak-postgres-1 psql -U postgres -d ulak_test -q -t -A \
    -c "SELECT ulak.create_endpoint('ddl-extra', 'http', '{\"url\": \"http://host.docker.internal:9876/ok/ddl-extra\", \"method\": \"POST\"}'::jsonb);" 2>/dev/null
  docker exec ulak-postgres-1 psql -U postgres -d ulak_test -q -t -A \
    -c "SELECT ulak.send('ddl-extra', '{\"ddl\": \"concurrent\"}'::jsonb);" 2>/dev/null
) &
DDL_BG_PID=$!

wait_queue_drain 30
wait "$DDL_BG_PID" 2>/dev/null

# Give extra time for ddl-extra message
sleep 3

COMPLETED=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status = 'completed';")
DDL_DELIVERED=$(webhook_count "ok/ddl-test")
EXTRA_DELIVERED=$(webhook_count "ok/ddl-extra")

assert_gt "DDL test: >= 1000 messages completed" "999" "$COMPLETED"
assert_eq "DDL test: 1000 original messages delivered" "1000" "$DDL_DELIVERED"
assert_eq "DDL test: concurrent endpoint message delivered" "1" "$EXTRA_DELIVERED"

# Check batch_size was changed (wait for SIGHUP to take effect)
sleep 1
NEW_BS=$(psql_exec "SELECT current_setting('ulak.batch_size');")
assert_eq "Config reload applied" "100" "$NEW_BS"

# Check for PANIC only (FATAL from earlier crash recovery test is expected)
PANICS=$(docker logs ulak-postgres-1 2>&1 | grep -c "PANIC" || true)
TOTAL=$((TOTAL + 1))
if [ "$PANICS" = "0" ]; then
  echo -e "  ${GREEN}✓${NC} No PANIC in PostgreSQL logs"
  PASS=$((PASS + 1))
else
  echo -e "  ${RED}✗${NC} Found $PANICS PANIC entries"
  FAIL=$((FAIL + 1))
fi

# ═══════════════════════════════════════════════════
# TEST 7: Webhook Signature Verification
# ═══════════════════════════════════════════════════
header "TEST 7: Webhook Signature Verification"

fresh_env "ulak.workers=1" "ulak.poll_interval=100"
webhook_clear

SIGNING_SECRET="test-secret-key-12345"
create_endpoint "signed" "{\"url\": \"${WEBHOOK_URL}/echo/signed\", \"method\": \"POST\", \"signing_secret\": \"${SIGNING_SECRET}\"}"

section "Sending 5 signed messages..."
for i in $(seq 1 5); do
  send_msg "signed" "{\"sig_test\": $i}"
done

wait_queue_drain 15

DELIVERED=$(webhook_count "echo/signed")
assert_eq "5 signed messages delivered" "5" "$DELIVERED"

section "Verifying HMAC signatures..."
SIG_RESULT=$(webhook_get "echo/signed" | python3 -c "
import sys, json, hmac, hashlib, base64

SECRET = '${SIGNING_SECRET}'
data = json.load(sys.stdin)['requests']
valid = 0
invalid = 0
missing = 0

for r in data:
    h = r.get('headers', {})
    msg_id = h.get('webhook-id', '')
    ts = h.get('webhook-timestamp', '')
    sig = h.get('webhook-signature', '')
    body = r.get('body', '')

    if not msg_id or not ts or not sig:
        missing += 1
        continue

    # Verify: HMAC-SHA256(secret, '{id}.{ts}.{body}')
    sign_content = f'{msg_id}.{ts}.{body}'
    expected = hmac.new(SECRET.encode(), sign_content.encode(), hashlib.sha256).digest()
    expected_b64 = 'v1,' + base64.b64encode(expected).decode()

    if sig == expected_b64:
        valid += 1
    else:
        invalid += 1

print(f'valid={valid} invalid={invalid} missing={missing}')
" 2>/dev/null)

echo "  $SIG_RESULT"
assert_contains "All signatures valid" "valid=5" "$SIG_RESULT"
assert_contains "No invalid signatures" "invalid=0" "$SIG_RESULT"

# Negative test: wrong secret
section "Negative test: wrong secret..."
NEG_RESULT=$(webhook_get "echo/signed" | python3 -c "
import sys, json, hmac, hashlib, base64

WRONG_SECRET = 'wrong-secret'
data = json.load(sys.stdin)['requests']
r = data[0]
h = r.get('headers', {})
sign_content = f\"{h['webhook-id']}.{h['webhook-timestamp']}.{r['body']}\"
expected = hmac.new(WRONG_SECRET.encode(), sign_content.encode(), hashlib.sha256).digest()
expected_b64 = 'v1,' + base64.b64encode(expected).decode()
actual = h.get('webhook-signature', '')
print('MISMATCH' if actual != expected_b64 else 'MATCH')
" 2>/dev/null)

assert_eq "Wrong secret fails verification" "MISMATCH" "$NEG_RESULT"

# ═══════════════════════════════════════════════════
# TEST 8: Payload Edge Cases
# ═══════════════════════════════════════════════════
header "TEST 8: Payload Edge Cases"

fresh_env "ulak.workers=1" "ulak.poll_interval=100"
webhook_clear

create_endpoint "edge" "{\"url\": \"${WEBHOOK_URL}/echo/edge-cases\", \"method\": \"POST\"}"

section "Sending edge case payloads..."

# 1. Empty object
send_msg "edge" "{}"

# 2. Unicode
psql_quiet "SELECT ulak.send('edge', '{\"msg\": \"Merhaba Dünya 🌍 日本語 العربية\"}'::jsonb);"

# 3. Null/boolean mix
send_msg "edge" "{\"a\": null, \"b\": true, \"c\": false, \"d\": 0}"

# 4. Deeply nested (10 levels)
psql_quiet "SELECT ulak.send('edge', '{\"l1\":{\"l2\":{\"l3\":{\"l4\":{\"l5\":{\"l6\":{\"l7\":{\"l8\":{\"l9\":{\"l10\":\"deep\"}}}}}}}}}}'::jsonb);"

# 5. Large array
psql_quiet "SELECT ulak.send('edge', jsonb_build_object('items', (SELECT jsonb_agg(g) FROM generate_series(1,100) g)));"

# 6. Escape characters
psql_quiet "SELECT ulak.send('edge', jsonb_build_object('msg', E'line1\nline2\ttab'));"

# 7. Numeric edges
psql_quiet "SELECT ulak.send('edge', '{\"big_int\": 9223372036854775807, \"neg\": -999999999, \"decimal\": 3.14159265358979}'::jsonb);"

# 8. Large key (255 chars — PG JSONB limit is much higher)
psql_quiet "SELECT ulak.send('edge', jsonb_build_object(repeat('k', 255), 'long-key-value'));"

wait_queue_drain 15

DELIVERED=$(webhook_count "echo/edge-cases")
assert_eq "8 edge case payloads delivered" "8" "$DELIVERED"

section "Verifying payload integrity..."
INTEGRITY=$(webhook_get "echo/edge-cases" | python3 -c "
import sys, json
data = json.load(sys.stdin)['requests']
ok = 0
fail = 0
for r in data:
    b = r.get('bodyJson')
    if b is not None:
        ok += 1
    else:
        fail += 1
print(f'ok={ok} fail={fail}')
" 2>/dev/null)

assert_contains "All payloads valid JSON" "fail=0" "$INTEGRITY"

# Verify specific payloads
UNICODE_OK=$(webhook_get "echo/edge-cases" | python3 -c "
import sys, json
data = json.load(sys.stdin)['requests']
for r in data:
    b = r.get('bodyJson', {})
    if 'msg' in b and '🌍' in str(b['msg']):
        print('UNICODE_OK')
        break
else:
    print('UNICODE_MISSING')
" 2>/dev/null)
assert_eq "Unicode preserved" "UNICODE_OK" "$UNICODE_OK"

DEEP_OK=$(webhook_get "echo/edge-cases" | python3 -c "
import sys, json
data = json.load(sys.stdin)['requests']
for r in data:
    b = r.get('bodyJson', {})
    try:
        if b['l1']['l2']['l3']['l4']['l5']['l6']['l7']['l8']['l9']['l10'] == 'deep':
            print('DEEP_OK')
            break
    except (KeyError, TypeError):
        pass
else:
    print('DEEP_MISSING')
" 2>/dev/null)
assert_eq "Deep nesting preserved" "DEEP_OK" "$DEEP_OK"

echo "  $INTEGRITY"

# ═══════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════
print_summary
exit $?
