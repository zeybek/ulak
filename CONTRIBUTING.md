# Contributing to ulak

Thank you for your interest in contributing to ulak! This document provides
guidelines and instructions for contributing.

## Development Environment

### Prerequisites

- Docker and Docker Compose
- PostgreSQL 14-18 development headers (if building locally)
- libcurl development headers (required)
- Optional protocol libraries: librdkafka, libmosquitto, hiredis, librabbitmq
- `lefthook` (installed automatically by `make hooks-install` when Homebrew or Go is available)
- `clang-format`, `cppcheck` (install with `make tools-install` for local/CI parity)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/zeybek/ulak.git
cd ulak

# Start development environment (PostgreSQL + all protocol services)
docker compose up -d

# Build and install the extension
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make clean && make ENABLE_KAFKA=1 ENABLE_MQTT=1 ENABLE_REDIS=1 ENABLE_AMQP=1 ENABLE_NATS=1 && make install"

# Restart PostgreSQL to load the extension
docker restart ulak-postgres-1

# Run regression tests
docker exec ulak-postgres-1 bash -c \
  "cd /src/ulak && make installcheck"
```

### Testing Against Different PostgreSQL Versions

The Dockerfile accepts a `PG_MAJOR` build argument:

```bash
docker compose build --build-arg PG_MAJOR=15 postgres
docker compose up -d postgres
```

## Code Style

### C Code

- Follow the PostgreSQL coding conventions.
- Use `clang-format` for formatting: `make format`
- Use `cppcheck` for static analysis: `make lint`
- Always use `palloc`/`pfree` — never `malloc`/`free`.
- Always use `strlcpy`/`snprintf` — never `strcpy`/`strcat`.
- Zero sensitive data before `pfree`.

### Git Hooks

This repository uses `lefthook` to keep local commits aligned with CI.

Install and wire the local hooks with:

```bash
make tools-install
make hooks-install
```

Check your local toolchain with:

```bash
make tools-versions
```

Configured hooks:

- `pre-commit`
  - `cppcheck`
  - `clang-format -i` on staged `src/` and `include/` C headers/sources, then auto re-stage
- `commit-msg`
  - conventional-commit style validation

Accepted commit header format:

```text
type(scope): subject
type: subject
type(scope)!: subject
```

Allowed commit types:

```text
feat, fix, docs, style, refactor, perf, test, build, ci, chore, revert
```

### SQL

- Use lowercase for SQL keywords.
- Prefix all objects with the `ulak` schema.
- Add `GRANT EXECUTE` for all three RBAC roles on every new function.
- Include `SECURITY DEFINER` only when elevated privileges are required.

## Pull Requests

### Before Submitting

1. Run `make format` to ensure code is formatted correctly.
2. Run `make lint` to check for static analysis warnings.
3. Run `make hooks-run` to exercise the same local pre-commit checks manually when needed.
4. Run `make installcheck` to verify the regression and isolation suites pass.
5. Add or update tests in `tests/regress/sql/`, `tests/regress/expected/`, or `tests/isolation/` for new functionality.
6. Update `CHANGELOG.md` to reflect the changes included in the next release.

### PR Guidelines

- Keep PRs focused on a single change.
- Write clear commit messages explaining the "why", not just the "what".
- Reference related issues in the PR description.
- Ensure backward compatibility with PostgreSQL 14-18.
- Use `#if PG_VERSION_NUM` guards for version-specific code.

## Architecture Guidelines

### Adding a New Protocol Dispatcher

See `src/dispatchers/dispatcher.h` for the dispatcher interface. In summary:

1. Create `src/dispatchers/{protocol}/` directory.
2. Implement the `DispatcherOperations` interface.
3. Register in the factory (`src/dispatchers/dispatcher.c`).
4. Add conditional compilation with `ENABLE_{PROTOCOL}` flag.
5. Update the Makefile, Dockerfile, and docker-compose.yml.
6. Add regression tests and documentation.

### Adding a New GUC Parameter

1. Add the `DefineCustom*Variable()` call in `src/config/guc.c`.
2. Add a corresponding `config_is_valid_*()` validation function.
3. Keep the validation range in sync with the `DefineCustomVariable` bounds.
4. Declare the extern in `src/config/guc.h`.
5. Update `CHANGELOG.md`.

### Memory Safety

- Always wrap SPI calls in `PG_TRY`/`PG_CATCH` blocks.
- Never call `SPI_finish()` in `PG_CATCH` — only `AbortCurrentTransaction()`.
- Never use `REPEATABLE READ` in workers — it causes serialization failures.
- Use `MemoryContext` switches for long-lived allocations.

## Reporting Issues

When reporting bugs, please include:

- PostgreSQL version (`SELECT version()`)
- ulak version
- Enabled protocols and build flags
- Steps to reproduce
- Relevant log output (`SET ulak.log_level = 'DEBUG'`)

## License

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0, consistent with the rest of the repository.
