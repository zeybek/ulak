# Building & Testing

ulak builds with PGXS, runs in Docker with all protocol services, and is tested with pg_regress, pg_isolation_regress, TAP, and end-to-end shell suites. Code quality is enforced by clang-format, cppcheck, clang-tidy, and AddressSanitizer.

---

## Build from Source

### Prerequisites

- PostgreSQL 14-18 development headers (`postgresql-server-dev-XX`)
- `libcurl` development headers (required -- HTTP is always enabled)
- Optional protocol libraries (see build flags below)
- `make` and a C99-compatible compiler

### Basic Build (HTTP Only)

```bash
make && sudo make install
```

### Full Build (All Protocols)

```bash
make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 \
  && sudo make install
```

### Build Flags

Each protocol is an optional compile-time module controlled by a `make` flag:

| Flag | Library | Package (Debian/Ubuntu) |
|------|---------|------------------------|
| *(always enabled)* | libcurl | `libcurl4-openssl-dev` |
| `ENABLE_KAFKA=1` | librdkafka | `librdkafka-dev` |
| `ENABLE_MQTT=1` | libmosquitto | `libmosquitto-dev` |
| `ENABLE_REDIS=1` | hiredis | `libhiredis-dev` |
| `ENABLE_AMQP=1` | librabbitmq | `librabbitmq-dev` |
| `ENABLE_NATS=1` | cnats | `libnats-dev` |

HTTP also requires `libssl-dev` for TLS, OAuth2, and AWS SigV4 support. Redis requires both `libhiredis-dev` and `libssl-dev` for TLS connections.

---

## Docker Development

The Docker Compose environment provides PostgreSQL plus all protocol services in a single command.

### Start the Environment

```bash
docker compose up -d
```

### Build and Install Inside the Container

```bash
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make clean && make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && make install"
```

### Restart PostgreSQL to Load the Extension

```bash
docker restart ulak-postgres-1
```

### Services

| Service | Image | Ports | Notes |
|---------|-------|-------|-------|
| PostgreSQL | `postgres:18` (configurable) | 5433:5432 | Source mounted at `/src/ulak` |
| Apache Kafka | `apache/kafka:3.9.0` | 9092 | KRaft mode (no ZooKeeper) |
| Redis | `redis:7-alpine` | 6379 | AOF persistence |
| Mosquitto | `eclipse-mosquitto:2` | 1883 | Anonymous access enabled |
| RabbitMQ | `rabbitmq:4-management` | 5672, 15672 | Management UI at `http://localhost:15672` |
| NATS | `nats:latest` | 4222, 8222 | JetStream enabled, HTTP monitoring at 8222 |

### Testing Against Different PostgreSQL Versions

The Dockerfile accepts a `PG_MAJOR` build argument. The default is PostgreSQL 18:

```bash
PG_MAJOR=15 docker compose up -d
```

Or build explicitly:

```bash
docker compose build --build-arg PG_MAJOR=15 postgres
docker compose up -d postgres
```

---

## Running Tests

All tests run inside the Docker container.

### Regression + Isolation Tests

```bash
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make installcheck-regress"
```

This runs 27 regression tests followed by 12 isolation tests in a single pass.

### TAP Tests

```bash
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make installcheck-tap"
```

Requires `libipc-run-perl` (included in the Dockerfile). TAP tests cover worker lifecycle scenarios: startup, SIGHUP reload, and stale worker recovery.

### All Tests (Regression + Isolation + TAP)

```bash
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make installcheck"
```

### End-to-End Tests

E2E tests run from the host machine against the Docker services:

```bash
# All protocols
make test-e2e

# Individual protocol suites
make test-http
make test-kafka
make test-redis
make test-mqtt
make test-amqp
make test-nats

# Advanced scenarios (batch, circuit breaker, etc.)
make test-advanced

# Stress tests
make test-stress
```

### Quick Smoke Test

```bash
make test-smoke    # Regression + HTTP E2E only
```

---

## Test Coverage

| Category | Count | What's Tested |
|----------|-------|---------------|
| Regression | 27 | Schema creation, endpoint CRUD, validation, queue operations, constraints, triggers, HTTP config, SQL functions, RBAC, error handling, event types, pub/sub, ordering keys, HTTP auth, production hardening, message lifecycle, CloudEvents, advanced operations, HTTP proxy, protocol configs (Kafka, Redis, MQTT, AMQP, NATS), metrics |
| Isolation | 12 | `FOR UPDATE SKIP LOCKED`, modulo partitioning, ordering key serialization, circuit breaker (threshold, recovery), batch mark-processing, retry visibility, priority contention, idempotency conflict, DLQ concurrent redrive |
| TAP | 3 | Worker startup, SIGHUP configuration reload, stale worker recovery |
| E2E | ~113 | Live protocol delivery, batching, circuit breaker behavior, stress testing across HTTP, Kafka, Redis, MQTT, AMQP, NATS |
| CI Matrix | PG 14-18 | All PostgreSQL versions with all protocols enabled |

---

## CI Pipeline

The CI pipeline runs on every push to `main` and every pull request. Draft PRs are skipped.

### Jobs

| Job | Description | PG Versions |
|-----|-------------|-------------|
| **Static Analysis** | clang-format-22 (formatting) + cppcheck (warnings, performance, portability) | N/A |
| **clang-tidy** | Deep AST-based analysis with `compile_commands.json` via `bear` | PG 18 |
| **Regression + Isolation** | `make installcheck-regress` with all protocols | 14, 15, 16, 17, 18 |
| **TAP** | `make installcheck-tap` worker lifecycle tests | 14, 15, 16, 17, 18 |
| **ASan + UBSan** | AddressSanitizer and UndefinedBehaviorSanitizer | PG 18 |

The regression and TAP jobs each run a 5-version matrix (`fail-fast: false`), so a failure on one PostgreSQL version does not cancel the others.

### Workflow Files

| File | Trigger |
|------|---------|
| `.github/workflows/ci.yml` | Push to `main` |
| `.github/workflows/pr.yml` | Pull request to `main` |
| `.github/workflows/_reusable-regression.yml` | Reusable: regression + isolation |
| `.github/workflows/_reusable-tap.yml` | Reusable: TAP tests |
| `.github/workflows/_reusable-static-analysis.yml` | Reusable: cppcheck + clang-format |
| `.github/workflows/_reusable-sanitizers.yml` | Reusable: ASan + UBSan |

---

## Code Quality Tools

```bash
make tools-install    # Install clang-format, cppcheck, lefthook (Homebrew or apt)
make tools-versions   # Show installed tool versions
make format           # Auto-format all C code with clang-format
make lint             # Run cppcheck static analysis
make tidy             # Run clang-tidy (generates compile_commands.json if missing)
make hooks-install    # Install lefthook git hooks
make hooks-run        # Run pre-commit checks manually
```

### Sanitizer Build

```bash
make sanitize         # Build with -fsanitize=address,undefined
make sanitize-check   # Build, install, and run all tests with sanitizers
```

---

## Git Hooks

ulak uses [lefthook](https://github.com/evilmartians/lefthook) to keep local commits aligned with CI:

### Pre-commit

- **clang-format**: Auto-formats staged `.c` and `.h` files in `src/` and `include/`, then re-stages them.
- **cppcheck**: Runs static analysis on `src/` with `--error-exitcode=1`.

### Commit-msg

- **conventional-commit**: Validates the commit message format.

---

## Conventional Commits

All commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) format, enforced by the `commit-msg` hook:

```
type(scope): subject
type: subject
type(scope)!: subject     # breaking change
```

### Allowed Types

| Type | Purpose |
|------|---------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no logic change |
| `refactor` | Code restructuring |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `build` | Build system or dependencies |
| `ci` | CI configuration |
| `chore` | Maintenance tasks |
| `revert` | Reverts a previous commit |

---

## PR Checklist

Before submitting a pull request:

1. `make format` -- code formatting passes
2. `make lint` -- no static analysis warnings
3. `make hooks-run` -- pre-commit checks pass
4. `make installcheck` -- all regression, isolation, and TAP tests pass
5. Add or update tests in `tests/regress/sql/`, `tests/regress/expected/`, or `tests/isolation/` for new functionality
6. Maintain PostgreSQL 14-18 compatibility (use `#if PG_VERSION_NUM` guards for version-specific code)
7. Write commit messages explaining "why", not just "what"
8. Update `CHANGELOG.md` to reflect the changes

---

## Adding a New Protocol

ulak uses a dispatcher factory pattern with a vtable (`DispatcherOperations`) for protocol polymorphism. Adding a new protocol requires these steps:

### 1. Create the Dispatcher Directory

```bash
mkdir -p src/dispatchers/{protocol}/
```

### 2. Implement the DispatcherOperations Interface

Create at minimum `{protocol}_dispatcher.c` and `{protocol}_dispatcher.h`. The required vtable operations are:

```c
typedef struct DispatcherOperations {
    /* Required */
    bool (*dispatch)(Dispatcher *self, const char *payload, char **error_msg);
    bool (*validate_config)(Jsonb *config);
    void (*cleanup)(Dispatcher *self);

    /* Optional (set to NULL if unsupported) */
    bool (*produce)(Dispatcher *self, const char *payload, int64 msg_id, char **error_msg);
    int  (*flush)(Dispatcher *self, int timeout_ms, int64 **failed_ids, int *failed_count, char ***failed_errors);
    bool (*supports_batch)(Dispatcher *self);
    DispatchResult *(*dispatch_extended)(Dispatcher *self, const char *payload, Jsonb *headers, Jsonb *metadata, char **error_msg);
} DispatcherOperations;
```

### 3. Register in the Factory

Add a `case` to `dispatcher_create()` in `src/dispatchers/dispatcher.c`:

```c
#ifdef ENABLE_MYPROTO
case PROTOCOL_TYPE_MYPROTO:
    return myproto_dispatcher_create(config);
#endif
```

### 4. Add Conditional Compilation

In the `Makefile`, add an `ifdef ENABLE_MYPROTO` block:

```makefile
ifdef ENABLE_MYPROTO
    OBJS += src/dispatchers/myproto/myproto_dispatcher.o
    PG_CPPFLAGS += -DENABLE_MYPROTO
    SHLIB_LINK += -lmyproto
endif
```

### 5. Add GUC Parameters

If the protocol needs configuration parameters, add `DefineCustom*Variable()` calls in `src/config/guc.c` and declare externs in `src/config/guc.h`.

### 6. Update Docker and SQL

- Add the library to `Dockerfile` (the `apt-get install` line)
- Add the service to `docker-compose.yml` if a broker is needed
- Add the protocol to the `CHECK (protocol IN (...))` constraint in `sql/ulak.sql`

### 7. Add Tests

- Add a regression test: `tests/regress/sql/XX_{protocol}_config.sql`
- Add E2E tests: `tests/e2e/{protocol}-tests.sh`
- Update the `REGRESS` list in the `Makefile`

---

## See Also

- [Getting Started](Getting-Started) -- Quick start guide with Docker setup
- [Architecture](Architecture) -- System design, worker model, and dispatcher factory
- [Configuration Reference](Configuration-Reference) -- All 57 GUC parameters
- [Protocol HTTP](Protocol-HTTP) -- HTTP dispatcher as a reference implementation
