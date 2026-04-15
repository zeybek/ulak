#!/usr/bin/env bash
# ─────────────────────────────────────────────────
# ulak — Shared test helpers
# Source this file: . tests/e2e/test-helpers.sh
# ─────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

WEBHOOK_PORT=9876
WEBHOOK_HOST="host.docker.internal"
WEBHOOK_URL="http://${WEBHOOK_HOST}:${WEBHOOK_PORT}"
DB="ulak_test"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
WEBHOOK_SERVER="$SCRIPT_DIR/fixtures/webhook-server.js"
# Build flags — detect installed protocol libraries automatically
MAKE_FLAGS="${PGX_MAKE_FLAGS:-ENABLE_KAFKA=1 ENABLE_REDIS=1 ENABLE_MQTT=1 ENABLE_AMQP=1 ENABLE_NATS=1}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0
TOTAL=0

# ─── Docker helpers ───

psql_exec() {
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A -c "$1" 2>/dev/null
}

psql_quiet() {
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -q -t -A -c "$1" 2>/dev/null
}

psql_verbose() {
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -c "$1" 2>&1
}

# Fresh Docker environment — pass GUC settings as arguments
# Usage: fresh_env "ulak.workers=1" "ulak.poll_interval=100"
fresh_env() {
  echo -e "${CYAN}Setting up fresh environment...${NC}"

  # Ensure container is running (don't touch Docker daemon — avoid OrbStack crash)
  if ! docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" up -d > /dev/null 2>&1
    for i in $(seq 1 30); do
      docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1 && break
      sleep 1
    done
  fi

  # Rebuild extension (incremental — only recompiles changed files)
  local build_out
  build_out=$(docker exec ulak-postgres-1 bash -c "cd /src/ulak && make $MAKE_FLAGS && make $MAKE_FLAGS install" 2>&1)
  if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Incremental build failed, trying full rebuild...${NC}"
    build_out=$(docker exec ulak-postgres-1 bash -c "cd /src/ulak && make clean && make $MAKE_FLAGS && make $MAKE_FLAGS install" 2>&1)
    if [ $? -ne 0 ]; then
      echo -e "${RED}Build failed: ${build_out}${NC}"
      return 1
    fi
  fi

  # Ensure shared_preload_libraries set
  docker exec ulak-postgres-1 psql -U postgres -t -A \
    -c "ALTER SYSTEM SET shared_preload_libraries = 'ulak';" > /dev/null 2>&1

  # Ensure extension exists
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A \
    -c "CREATE EXTENSION IF NOT EXISTS ulak;" > /dev/null 2>&1

  # Clean slate: truncate all data, drop endpoints
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A \
    -c "TRUNCATE ulak.queue, ulak.dlq, ulak.event_log;" > /dev/null 2>&1
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A -c "
    DO \$\$ DECLARE r RECORD; BEGIN
      FOR r IN SELECT name FROM ulak.endpoints LOOP
        BEGIN PERFORM ulak.drop_endpoint(r.name); EXCEPTION WHEN OTHERS THEN NULL; END;
      END LOOP;
    END; \$\$;" > /dev/null 2>&1

  # Reset circuit breakers
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A \
    -c "UPDATE ulak.endpoints SET circuit_state = 'closed', circuit_failure_count = 0;" > /dev/null 2>&1

  # Reset ALL GUCs to defaults first, then apply test-specific overrides
  local guc_args=()
  guc_args+=(-c "ALTER SYSTEM SET ulak.database = '$DB';")
  guc_args+=(-c "ALTER SYSTEM SET ulak.http_allow_internal_urls = true;")
  guc_args+=(-c "ALTER SYSTEM SET max_worker_processes = 24;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.workers = 4;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.poll_interval = 500;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.batch_size = 200;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.http_batch_capacity = 200;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.default_max_retries = 10;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.retry_base_delay = 10;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.circuit_breaker_threshold = 10;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.circuit_breaker_cooldown = 30;")
  guc_args+=(-c "ALTER SYSTEM SET ulak.stale_recovery_timeout = 300;")
  for guc in "$@"; do
    guc_args+=(-c "ALTER SYSTEM SET ${guc};")
  done
  docker exec ulak-postgres-1 psql -U postgres -d "$DB" -t -A \
    "${guc_args[@]}" > /dev/null 2>&1

  # Restart for PGC_POSTMASTER GUCs (workers, max_worker_processes) + reload .so
  docker restart ulak-postgres-1 > /dev/null 2>&1

  for i in $(seq 1 30); do
    docker exec ulak-postgres-1 pg_isready -U postgres > /dev/null 2>&1 && break
    sleep 1
  done
  sleep 2  # Workers need time to start

  # Verify
  local workers
  workers=$(psql_exec "SELECT current_setting('ulak.workers');")
  echo -e "${GREEN}Fresh env ready${NC} — workers=$workers"
}

# ─── Webhook helpers ───

webhook_clear() {
  curl -sf -X POST "http://localhost:${WEBHOOK_PORT}/api/clear" > /dev/null 2>&1
}

webhook_count() {
  local tag="$1"
  curl -sf "http://localhost:${WEBHOOK_PORT}/api/received/${tag}" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0"
}

webhook_get() {
  local tag="$1"
  curl -sf "http://localhost:${WEBHOOK_PORT}/api/received/${tag}" 2>/dev/null
}

webhook_health() {
  curl -sf "http://localhost:${WEBHOOK_PORT}/health" > /dev/null 2>&1
}

# ─── Queue helpers ───

create_endpoint() {
  local name="$1" config="$2" protocol="${3:-http}"
  psql_quiet "SELECT ulak.create_endpoint('${name}', '${protocol}', '${config}'::jsonb);"
}

send_msg() {
  local endpoint="$1" payload="$2"
  psql_quiet "SELECT ulak.send('${endpoint}', '${payload}'::jsonb);"
}

send_batch() {
  local endpoint="$1" count="$2"
  psql_quiet "
  DO \$\$ DECLARE a jsonb[];
  BEGIN
    FOR s IN 1..${count} BY 500 LOOP
      a := ARRAY(SELECT jsonb_build_object('i', g) FROM generate_series(s, LEAST(s+499, ${count})) g);
      PERFORM ulak.send_batch('${endpoint}', a);
    END LOOP;
  END; \$\$;"
}

wait_queue_drain() {
  local max_wait=${1:-30}
  for i in $(seq 1 $((max_wait * 2))); do
    local pending
    pending=$(psql_exec "SELECT count(*) FROM ulak.queue WHERE status IN ('pending','processing');")
    [ "$pending" = "0" ] && return 0
    sleep 0.5
  done
  echo -e "  ${YELLOW}Queue not drained after ${max_wait}s${NC}"
  return 1
}

queue_status() {
  psql_exec "SELECT status, count(*) FROM ulak.queue GROUP BY status ORDER BY status;"
}

# Count completed messages scoped to a specific endpoint (or LIKE pattern)
completed_count() {
  local ep_pattern="$1"
  psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = 'completed' AND e.name LIKE '${ep_pattern}';"
}

# Count messages by status scoped to endpoint
queue_count_status() {
  local ep_name="$1" status="$2"
  psql_exec "SELECT count(*) FROM ulak.queue q JOIN ulak.endpoints e ON q.endpoint_id = e.id WHERE q.status = '${status}' AND e.name = '${ep_name}';"
}

# ─── Assertion helpers ───

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

assert_true() {
  local label="$1" condition="$2"
  TOTAL=$((TOTAL + 1))
  if eval "$condition" 2>/dev/null; then
    echo -e "  ${GREEN}✓${NC} $label"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}✗${NC} $label"
    FAIL=$((FAIL + 1))
  fi
}

# ─── Output helpers ───

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

check_no_crashes() {
  local crashes
  crashes=$(docker logs ulak-postgres-1 2>&1 | grep -c "FATAL\|PANIC" || true)
  TOTAL=$((TOTAL + 1))
  if [ "$crashes" = "0" ]; then
    echo -e "  ${GREEN}✓${NC} No FATAL/PANIC in PostgreSQL logs"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}✗${NC} Found $crashes FATAL/PANIC entries in logs"
    FAIL=$((FAIL + 1))
  fi
}

print_summary() {
  echo ""
  echo -e "${CYAN}═══════════════════════════════════════════${NC}"
  echo -e "${CYAN}  RESULTS: ${PASS}/${TOTAL} passed, ${FAIL} failed${NC}"
  echo -e "${CYAN}═══════════════════════════════════════════${NC}"
  if [ $FAIL -gt 0 ]; then
    echo -e "${RED}  SOME TESTS FAILED${NC}"
    return 1
  else
    echo -e "${GREEN}  ALL TESTS PASSED${NC}"
    return 0
  fi
}
