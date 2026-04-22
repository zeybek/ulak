# ulak - PostgreSQL Transactional Outbox Extension
# Makefile for building and installing the extension

# Default target - must be before any rules
.DEFAULT_GOAL := all

EXTENSION = ulak
EXTVERSION = $(shell cat version.txt 2>/dev/null || echo "0.0.0")

MODULE_big = $(EXTENSION)

# Protocol flags (uncomment to enable)
# ENABLE_KAFKA = 1
# ENABLE_MQTT = 1
# ENABLE_REDIS = 1
# ENABLE_AMQP = 1

# Core module objects
OBJS = src/ulak.o
OBJS += src/worker.o
OBJS += src/shmem.o

# Modular architecture objects
OBJS += src/core/entities.o
OBJS += src/config/guc.o
OBJS += src/dispatchers/dispatcher.o
OBJS += src/dispatchers/http/http_dispatcher.o
OBJS += src/dispatchers/http/http_security.o
OBJS += src/dispatchers/http/http_request.o
OBJS += src/dispatchers/http/http_sync.o
OBJS += src/dispatchers/http/http_batch.o
OBJS += src/dispatchers/http/http_auth.o
OBJS += src/dispatchers/http/http_auth_oauth2.o
OBJS += src/dispatchers/http/http_auth_sigv4.o
OBJS += src/queue/queue_manager.o
OBJS += src/utils/json_utils.o
OBJS += src/utils/logging.o
OBJS += src/utils/cloudevents.o
OBJS += src/utils/rate_limit.o
OBJS += src/utils/retry_policy.o

# Conditional protocol objects
ifdef ENABLE_KAFKA
    OBJS += src/dispatchers/kafka/kafka_dispatcher.o
    OBJS += src/dispatchers/kafka/kafka_config.o
    OBJS += src/dispatchers/kafka/kafka_callback.o
    OBJS += src/dispatchers/kafka/kafka_delivery.o
    PG_CPPFLAGS += -DENABLE_KAFKA
    SHLIB_LINK += -lrdkafka
endif

ifdef ENABLE_MQTT
    OBJS += src/dispatchers/mqtt/mqtt_dispatcher.o
    OBJS += src/dispatchers/mqtt/mqtt_connection.o
    PG_CPPFLAGS += -DENABLE_MQTT
    SHLIB_LINK += -lmosquitto
endif

ifdef ENABLE_REDIS
    OBJS += src/dispatchers/redis/redis_dispatcher.o
    OBJS += src/dispatchers/redis/redis_config.o
    OBJS += src/dispatchers/redis/redis_connection.o
    OBJS += src/dispatchers/redis/redis_tls.o
    PG_CPPFLAGS += -DENABLE_REDIS
    SHLIB_LINK += -lhiredis -lhiredis_ssl -lssl -lcrypto
endif

ifdef ENABLE_AMQP
    OBJS += src/dispatchers/amqp/amqp_dispatcher.o
    OBJS += src/dispatchers/amqp/amqp_config.o
    OBJS += src/dispatchers/amqp/amqp_connection.o
    OBJS += src/dispatchers/amqp/amqp_delivery.o
    PG_CPPFLAGS += -DENABLE_AMQP
    SHLIB_LINK += -lrabbitmq
endif

ifdef ENABLE_NATS
    OBJS += src/dispatchers/nats/nats_dispatcher.o
    OBJS += src/dispatchers/nats/nats_config.o
    OBJS += src/dispatchers/nats/nats_callback.o
    OBJS += src/dispatchers/nats/nats_delivery.o
    PG_CPPFLAGS += -DENABLE_NATS
    SHLIB_LINK += -lnats
endif

# External libraries - HTTP always enabled
SHLIB_LINK += -lcurl -lcrypto

# Include paths for modular architecture
PG_CPPFLAGS += -I$(srcdir)/include
PG_CPPFLAGS += -I$(srcdir)/src
PG_CPPFLAGS += -I$(srcdir)/src/core
PG_CPPFLAGS += -I$(srcdir)/src/config
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/http
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/kafka
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/mqtt
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/redis
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/amqp
PG_CPPFLAGS += -I$(srcdir)/src/dispatchers/nats
PG_CPPFLAGS += -I$(srcdir)/src/queue
PG_CPPFLAGS += -I$(srcdir)/src/utils

PG_CONFIG ?= pg_config

# Helper and host-side e2e targets should not require pg_config/PGXS.
NO_PGXS_GOALS = format lint tidy version new-version hooks-install hooks-run
NO_PGXS_GOALS += tools-install tools-versions
NO_PGXS_GOALS += test-http test-kafka test-redis test-mqtt test-amqp test-nats
NO_PGXS_GOALS += test-e2e test-advanced test-stress
NO_PGXS_GOALS += test-docker-up test-docker-down
NO_PGXS_GOALS += docs docs-open docs-clean
NO_PGXS_GOALS += dist distcheck release-check version-check

ifeq ($(strip $(MAKECMDGOALS)),)
    NEED_PGXS = 1
else ifneq ($(strip $(filter-out $(NO_PGXS_GOALS),$(MAKECMDGOALS))),)
    NEED_PGXS = 1
else
    NEED_PGXS =
endif

ifdef NEED_PGXS
    ifeq ($(shell command -v $(PG_CONFIG) >/dev/null 2>&1 && echo yes),)
        $(error PGXS-backed targets require '$(PG_CONFIG)' in PATH. Use the Docker PostgreSQL container for build/installcheck targets, or set PG_CONFIG=/path/to/pg_config)
    endif

    PGXS := $(shell $(PG_CONFIG) --pgxs)

    # Include PostgreSQL headers only for PGXS-backed targets.
    PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir)
    PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir-server)
endif

PG_LDFLAGS =

# Add system paths for external libraries
ifeq ($(shell uname), Darwin)
    PG_CPPFLAGS += -I/opt/homebrew/include
    PG_LDFLAGS += -L/opt/homebrew/lib
endif

ifeq ($(shell uname), Linux)
    PG_CPPFLAGS += -I/usr/include
    PG_LDFLAGS += -L/usr/lib -L/usr/lib/x86_64-linux-gnu
endif

# SQL files: main install script + future upgrade scripts
# Main versioned SQL file (generated from source)
DATA_built = sql/$(EXTENSION)--$(EXTVERSION).sql

# Future upgrade scripts (used once a later version exists)
# These are committed to the repository and not auto-generated
UPGRADE_SCRIPTS = $(wildcard sql/$(EXTENSION)--*--*.sql)

DATA = $(EXTENSION).control $(UPGRADE_SCRIPTS)
DOCS = README.md

DISTVERSION = $(EXTVERSION)
DIST_BASENAME = $(EXTENSION)-$(DISTVERSION)
DIST_DIR = dist
DIST_FILE = $(DIST_DIR)/$(DIST_BASENAME).zip

# Generate versioned SQL file from source
sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql version.txt
	cp $< $@

# Only clean generated install files, NOT future upgrade scripts (--X.Y.Z--A.B.C.sql)
# Also clean protocol dispatcher object files (always, regardless of build flags)
EXTRA_CLEAN = sql/$(EXTENSION)--[0-9].[0-9].[0-9].sql
EXTRA_CLEAN += src/dispatchers/http/http_security.o src/dispatchers/http/http_security.bc
EXTRA_CLEAN += src/dispatchers/http/http_request.o src/dispatchers/http/http_request.bc
EXTRA_CLEAN += src/dispatchers/http/http_sync.o src/dispatchers/http/http_sync.bc
EXTRA_CLEAN += src/dispatchers/http/http_batch.o src/dispatchers/http/http_batch.bc
EXTRA_CLEAN += src/dispatchers/kafka/kafka_dispatcher.o src/dispatchers/kafka/kafka_dispatcher.bc
EXTRA_CLEAN += src/dispatchers/kafka/kafka_config.o src/dispatchers/kafka/kafka_config.bc
EXTRA_CLEAN += src/dispatchers/kafka/kafka_callback.o src/dispatchers/kafka/kafka_callback.bc
EXTRA_CLEAN += src/dispatchers/kafka/kafka_delivery.o src/dispatchers/kafka/kafka_delivery.bc
EXTRA_CLEAN += src/dispatchers/mqtt/mqtt_dispatcher.o src/dispatchers/mqtt/mqtt_dispatcher.bc
EXTRA_CLEAN += src/dispatchers/mqtt/mqtt_connection.o src/dispatchers/mqtt/mqtt_connection.bc
EXTRA_CLEAN += src/dispatchers/redis/redis_dispatcher.o src/dispatchers/redis/redis_dispatcher.bc
EXTRA_CLEAN += src/dispatchers/redis/redis_config.o src/dispatchers/redis/redis_config.bc
EXTRA_CLEAN += src/dispatchers/redis/redis_connection.o src/dispatchers/redis/redis_connection.bc
EXTRA_CLEAN += src/dispatchers/redis/redis_tls.o src/dispatchers/redis/redis_tls.bc
EXTRA_CLEAN += src/dispatchers/amqp/amqp_dispatcher.o src/dispatchers/amqp/amqp_dispatcher.bc
EXTRA_CLEAN += src/dispatchers/amqp/amqp_config.o src/dispatchers/amqp/amqp_config.bc
EXTRA_CLEAN += src/dispatchers/amqp/amqp_connection.o src/dispatchers/amqp/amqp_connection.bc
EXTRA_CLEAN += src/dispatchers/amqp/amqp_delivery.o src/dispatchers/amqp/amqp_delivery.bc
EXTRA_CLEAN += src/dispatchers/nats/nats_dispatcher.o src/dispatchers/nats/nats_dispatcher.bc
EXTRA_CLEAN += src/dispatchers/nats/nats_config.o src/dispatchers/nats/nats_config.bc
EXTRA_CLEAN += src/dispatchers/nats/nats_callback.o src/dispatchers/nats/nats_callback.bc
EXTRA_CLEAN += src/dispatchers/nats/nats_delivery.o src/dispatchers/nats/nats_delivery.bc

# ============================================================================
# REGRESSION TEST CONFIGURATION
# ============================================================================

# Test cases in tests/regress/sql/ directory (run in order)
REGRESS = 00_setup 01_schema 02_endpoints_crud 03_endpoints_validation \
          04_queue_operations 05_constraints 06_triggers 07_http_config \
          08_sql_functions 09_rbac 10_error_handling \
          11_event_types 12_publish 13_ordering_key 14_http_auth \
          15_production_hardening 16_message_lifecycle 17_cloudevents \
          18_advanced_operations 19_http_proxy 20_kafka_config 21_redis_config \
          22_mqtt_config 23_amqp_config 24_nats_config \
          25_metrics 99_cleanup

# Test options: use tests/regress/ subdirectory for input/output, dedicated test database
REGRESS_OPTS = --inputdir=tests/regress --outputdir=tests/regress --dbname=ulak_test

# Isolation tests for concurrency scenarios (FOR UPDATE SKIP LOCKED, circuit breaker, ordering)
ISOLATION = skip_locked modulo_partition ordering_key circuit_breaker batch_mark_processing \
            circuit_breaker_threshold circuit_breaker_recovery ordering_key_completion \
            retry_visibility priority_contention idempotency_conflict dlq_concurrent_redrive
ISOLATION_OPTS = --inputdir=tests/isolation --outputdir=tests/isolation --dbname=ulak_test

# TAP tests for worker lifecycle scenarios (requires IPC::Run Perl module)
TAP_TESTS = 1

# Separate CI-facing test targets so workflows can run regress/isolation and
# TAP independently without duplicating the full installcheck matrix.
.PHONY: installcheck-regress installcheck-tap

installcheck-regress:
	echo "# +++ regress install-check in $(CURDIR) +++" && \
	$(top_builddir)/src/test/regress/pg_regress \
		--inputdir=tests/regress \
		--outputdir=tests/regress \
		--bindir='$(bindir)' \
		$(pg_regress_locale_flags) \
		$(REGRESS_OPTS) \
		$(REGRESS)
	echo "# +++ isolation install-check in $(CURDIR) +++" && \
	$(top_builddir)/src/test/isolation/pg_isolation_regress \
		--inputdir=tests/isolation \
		--outputdir=tests/isolation \
		--bindir='$(bindir)' \
		$(pg_regress_locale_flags) \
		$(ISOLATION_OPTS) \
		$(ISOLATION)

installcheck-tap:
	$(prove_installcheck)

ifdef NEED_PGXS
include $(PGXS)
endif

# macOS: Override architecture for Redis (hiredis from Homebrew is arm64-only)
ifdef ENABLE_REDIS
ifeq ($(shell uname), Darwin)
override CFLAGS := $(subst -arch x86_64,,$(CFLAGS))
override CFLAGS_SL := $(subst -arch x86_64,,$(CFLAGS_SL))
override LDFLAGS_SL := $(subst -arch x86_64,,$(LDFLAGS_SL))
endif
endif

# Development targets
.PHONY: all install clean distclean
.PHONY: hooks-install hooks-run
.PHONY: tools-install tools-versions
.PHONY: dist distcheck release-check version-check

# Development helpers
tools-install:
	@set -e; \
	if command -v brew >/dev/null 2>&1; then \
		brew install clang-format cppcheck lefthook; \
	elif command -v apt-get >/dev/null 2>&1; then \
		sudo apt-get update; \
		sudo apt-get install -y clang-format cppcheck; \
		command -v lefthook >/dev/null 2>&1 || { \
			if command -v go >/dev/null 2>&1; then \
				go install github.com/evilmartians/lefthook@latest; \
			else \
				echo "Install Go or lefthook manually to enable git hooks."; \
				exit 1; \
			fi; \
		}; \
	else \
		echo "Unsupported host package manager. Install clang-format, cppcheck, and lefthook manually."; \
		exit 1; \
	fi

tools-versions:
	@printf "clang-format: "; (clang-format --version || echo "not installed")
	@printf "cppcheck: "; (cppcheck --version || echo "not installed")
	@printf "lefthook: "; (lefthook version || echo "not installed")
	@printf "docker: "; (docker --version || echo "not installed")

format:
	find src -type f -name "*.c" -exec clang-format -i {} \; || echo "clang-format not available, skipping format"
	find src -type f -name "*.h" -exec clang-format -i {} \; || echo "clang-format not available, skipping format"
	clang-format -i include/*.h || echo "clang-format not available, skipping format"

lint:
	find src -name "*.c" -exec cppcheck --enable=all --std=c99 {} \; || echo "cppcheck not available, skipping lint"
	cppcheck --enable=all --std=c99 include/*.h || echo "cppcheck not available, skipping lint"

hooks-install:
	@command -v lefthook >/dev/null 2>&1 || { \
		echo "lefthook not found in PATH, attempting automatic installation..."; \
		if command -v brew >/dev/null 2>&1; then \
			brew install lefthook; \
		elif command -v go >/dev/null 2>&1; then \
			go install github.com/evilmartians/lefthook@latest; \
			export PATH="$$PATH:$$(go env GOPATH)/bin"; \
		else \
			echo "Could not auto-install lefthook."; \
			echo "Install Homebrew or Go, then re-run 'make hooks-install'."; \
			exit 1; \
		fi; \
	}
	@command -v lefthook >/dev/null 2>&1 || { \
		export PATH="$$PATH:$$(go env GOPATH 2>/dev/null)/bin"; \
		command -v lefthook >/dev/null 2>&1 || { \
			echo "lefthook is still not available in PATH after installation attempt."; \
			echo "If Go was used, add '$$(go env GOPATH)/bin' to your shell PATH."; \
			exit 1; \
		}; \
		lefthook install; \
		exit 0; \
	}
	lefthook install

hooks-run:
	@command -v lefthook >/dev/null 2>&1 || { \
		echo "lefthook not found in PATH"; \
		echo "Run 'make hooks-install' first, then re-run 'make hooks-run'"; \
		exit 1; \
	}
	lefthook run pre-commit

# Static analysis with clang-tidy (requires compile_commands.json)
# Generate compile_commands.json: bear -- make clean && bear -- make
tidy:
	@if [ ! -f compile_commands.json ]; then \
		echo "compile_commands.json not found. Generating with bear..."; \
		bear -- $(MAKE) clean > /dev/null 2>&1; \
		bear -- $(MAKE) > /dev/null 2>&1; \
	fi
	@echo "Running clang-tidy..."
	@find src -name "*.c" | xargs clang-tidy -p . 2>&1 | tee /tmp/clang-tidy-output.txt
	@echo ""
	@WARNS=$$(grep -c "warning:" /tmp/clang-tidy-output.txt 2>/dev/null || echo 0); \
	ERRS=$$(grep -c "error:" /tmp/clang-tidy-output.txt 2>/dev/null || echo 0); \
	echo "clang-tidy: $$WARNS warning(s), $$ERRS error(s)"

# Build with AddressSanitizer + UndefinedBehaviorSanitizer
sanitize:
	$(MAKE) clean
	$(MAKE) CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer -O1"

# Run tests with sanitizers enabled
sanitize-check: sanitize
	$(MAKE) install
	ASAN_OPTIONS="detect_leaks=0:abort_on_error=1:print_stacktrace=1" \
	UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
	$(MAKE) installcheck

# Version management helpers
version:
	@echo "Current version: $(EXTVERSION)"
	@echo "Future upgrade scripts found:"
	@ls -1 sql/$(EXTENSION)--*--*.sql 2>/dev/null || echo "  (none)"

# Prepare a future version - usage: make new-version NEW_VERSION=0.0.1
new-version:
ifndef NEW_VERSION
	$(error NEW_VERSION is not set. Usage: make new-version NEW_VERSION=0.0.1)
endif
	@echo "Preparing version $(NEW_VERSION)..."
	@echo "$(NEW_VERSION)" > version.txt
	@sed -i.bak "s/default_version = '.*'/default_version = '$(NEW_VERSION)'/" $(EXTENSION).control && rm -f $(EXTENSION).control.bak
	@python3 -c 'import json; from pathlib import Path; path = Path("META.json"); meta = json.loads(path.read_text()); meta["version"] = "$(NEW_VERSION)"; meta.setdefault("provides", {}).setdefault("ulak", {})["version"] = "$(NEW_VERSION)"; path.write_text(json.dumps(meta, indent=2) + "\n")'
	@echo "Updated version.txt, $(EXTENSION).control, and META.json to $(NEW_VERSION)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. If an earlier version already exists, create: sql/$(EXTENSION)--$(EXTVERSION)--$(NEW_VERSION).sql"
	@echo "  2. Update sql/$(EXTENSION).sql with new features"
	@echo "  3. Run 'make clean && make' to generate new versioned SQL"

# Release metadata checks for PGXN/GitHub packaging
version-check:
	@python3 scripts/release_check.py

dist: version-check sql/$(EXTENSION)--$(EXTVERSION).sql
	@rm -rf $(DIST_DIR)/$(DIST_BASENAME)
	@mkdir -p $(DIST_DIR)
	@rsync -a \
		--exclude='.git/' \
		--exclude='dist/' \
		--exclude='.DS_Store' \
		--exclude='docs/' \
		--exclude='results/' \
		--exclude='tmp_check/' \
		--exclude='tmp_check_iso/' \
		--exclude='compile_commands.json' \
		--exclude='*.so' \
		--exclude='*.o' \
		./ $(DIST_DIR)/$(DIST_BASENAME)/
	@rm -rf $(DIST_DIR)/$(DIST_BASENAME)/.github
	@rm -rf $(DIST_DIR)/$(DIST_BASENAME)/docs
	@rm -f $(DIST_DIR)/$(DIST_BASENAME)/Dockerfile
	@rm -f $(DIST_DIR)/$(DIST_BASENAME)/Dockerfile.publish
	@rm -f $(DIST_DIR)/$(DIST_BASENAME)/Doxyfile
	@rm -f $(DIST_DIR)/$(DIST_BASENAME)/compile_commands.json
	@rm -f $(DIST_DIR)/$(DIST_BASENAME)/lefthook.yml
	@rm -f $(DIST_FILE)
	@cd $(DIST_DIR) && zip -qr $(DIST_BASENAME).zip $(DIST_BASENAME)
	@rm -rf $(DIST_DIR)/$(DIST_BASENAME)
	@echo "Created $(DIST_FILE)"

distcheck: dist
	@python3 scripts/distcheck.py

release-check: version-check distcheck
	@echo "Release checks completed successfully"

# ============================================================================
# END-TO-END TEST TARGETS
# ============================================================================
.PHONY: test-all test-unit test-e2e
.PHONY: test-http test-kafka test-mqtt test-redis test-amqp test-nats
.PHONY: test-advanced test-stress
.PHONY: test-docker-up test-docker-down

# Combined test targets
test-all: test-unit test-e2e

test-unit: installcheck

test-e2e: test-http test-kafka test-redis test-mqtt test-amqp test-nats

# Individual e2e suites (run from host, not inside container)
test-http:
	bash tests/e2e/run-tests.sh

test-kafka:
	bash tests/e2e/kafka-tests.sh

test-redis:
	bash tests/e2e/redis-tests.sh

test-mqtt:
	bash tests/e2e/mqtt-tests.sh

test-amqp:
	bash tests/e2e/amqp-tests.sh

test-nats:
	bash tests/e2e/nats-tests.sh

test-advanced:
	bash tests/e2e/advanced-tests.sh

test-stress:
	bash tests/e2e/stress-test.sh

# Docker environment management
test-docker-up:
	docker compose up -d

test-docker-down:
	docker compose down -v

# Quick smoke test (regression + HTTP e2e)
test-smoke: test-unit test-http

# Documentation generation
docs:
	@echo "Generating Doxygen documentation..."
	@mkdir -p docs
	@doxygen Doxyfile
	@echo "Documentation generated at docs/html/index.html"

docs-open: docs
	@xdg-open docs/html/index.html 2>/dev/null || open docs/html/index.html 2>/dev/null || echo "Open docs/html/index.html in browser"

docs-clean:
	@rm -rf docs
	@echo "Documentation cleaned"
