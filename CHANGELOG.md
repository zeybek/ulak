# Changelog

All notable changes to ulak will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0](https://github.com/zeybek/ulak/compare/v0.0.3...v0.1.0) (2026-09-07)


### ⚠ BREAKING CHANGES

* **sql:** ulak_application and ulak_monitor lose SELECT on ulak.endpoints (use ulak.endpoint_status); ulak.prevent_payload_modification() is removed; redrive_message() raises for DLQ rows that are not in failed state.
* **core:** ulak.database requires a server restart to change (pg_reload_conf no longer applies it), and metrics() no longer returns spawns_total, spawn_failures_total or restarts_total.

### Features

* **sql:** endpoint_status view, tighter grants, single queue trigger and 0.1.0 upgrade path ([0ffc72e](https://github.com/zeybek/ulak/commit/0ffc72e16d5dd96e99f41d4a350976aa60f15cd3))


### Bug Fixes

* **http:** stable webhook ids, whsec_ secrets, stricter SSRF guard and 4xx handling ([9f2d3d4](https://github.com/zeybek/ulak/commit/9f2d3d49c96bfb47f1c6696d8d2f1fc282d6cafb))
* **kafka:** fix use-after-free in batch growth and stale delivery callbacks ([cdc4aec](https://github.com/zeybek/ulak/commit/cdc4aec83812f40c7c1dc65e094ea1bbb037471f))
* **mqtt:** wait for the broker acknowledgement on synchronous QoS 1/2 publishes ([52a4059](https://github.com/zeybek/ulak/commit/52a405999f00d90623aef20a72b485ab958d56f2))
* **nats:** confirm JetStream publishes via AckHandler instead of dropping errors ([02ae498](https://github.com/zeybek/ulak/commit/02ae498a1341cd673c24cc9de7c89b35763c5c6b))
* **nats:** require the result buffer in dispatch_ex ([2f85dcb](https://github.com/zeybek/ulak/commit/2f85dcbb27b91a4c46de11d41b3981e5447f0763))
* resolve v0.0.3 review findings and prepare the 0.1.0 upgrade path ([64aafb5](https://github.com/zeybek/ulak/commit/64aafb51919a4c706f4b442d4011d72172de5061))
* **worker:** drain LISTEN queue, defer rate-limited rows and expire messages ([948ed2f](https://github.com/zeybek/ulak/commit/948ed2f4aab45d94a8258c470bd3cea5ea7993a1))


### Code Refactoring

* **core:** remove dead launcher, DSM and entity layers; log endpoint DDL at LOG level ([6438255](https://github.com/zeybek/ulak/commit/643825589644f851fb9ad57c1caa2da5fbb8de9b))


### Documentation

* **readme:** link test directories with relative paths ([19aca5c](https://github.com/zeybek/ulak/commit/19aca5ca1097ea74c76422324131a33fdc419017))


### Continuous Integration

* fail the clang-tidy gate on errors and suppress cppcheck's branch-limit notice ([73b1d31](https://github.com/zeybek/ulak/commit/73b1d314827d963a226c9446a57ba3bbff08521a))


### Miscellaneous

* **release:** list every commit type in the changelog ([f8b29ae](https://github.com/zeybek/ulak/commit/f8b29ae039f37c2997977348b460cf3a8c3278c7))
* **release:** verify the extension upgrade chain reaches the released version ([569fa79](https://github.com/zeybek/ulak/commit/569fa792e1ddc4c3ed63fa0718904e44da8e4a82))

## [0.0.3](https://github.com/zeybek/ulak/compare/v0.0.2...v0.0.3) (2026-05-12)


### Bug Fixes

* trigger release for accumulated worker and ci fixes ([#6](https://github.com/zeybek/ulak/issues/6)) ([4e7b59f](https://github.com/zeybek/ulak/commit/4e7b59f218b5cf1fcfa857a498300536b43cd741))

## [0.0.2](https://github.com/zeybek/ulak/compare/v0.0.1...v0.0.2) (2026-04-15)


### Bug Fixes

* add manual trigger to Docker publish workflow ([dfa5fa8](https://github.com/zeybek/ulak/commit/dfa5fa824f6e4ecd7951a47ac4d1946980508dd0))

## 0.0.1 (2026-04-15)


### Features

* initial commit ([a928cf0](https://github.com/zeybek/ulak/commit/a928cf0e4394bc781ab0350db0c11d6fdd255c90))

## [0.0.0]

### Added

- Core transactional ulak with `send()`, `send_with_options()`, `send_batch()`,
  and `send_batch_with_priority()` functions.
- Pub/Sub system with `publish()`, `publish_batch()`, event types, and subscriptions.
- Six protocol dispatchers: HTTP (always enabled), Kafka, MQTT, Redis Streams,
  AMQP, and NATS (conditional compilation via `ENABLE_*` flags).
- Batch dispatch support for Kafka, MQTT, and AMQP with async produce/flush.
- Multi-worker background processing with modulo-based queue partitioning
  (`bgw_main_arg = (total_workers << 16) | worker_id`).
- Circuit breaker per endpoint with configurable threshold and cooldown.
- Dead-letter queue (DLQ) with `redrive_message()`, `redrive_endpoint()`,
  `redrive_all()`, and `dlq_summary()`.
- Monthly-partitioned archive table with `replay_message()` and `replay_range()`.
- Backpressure control via `max_queue_size` GUC.
- GUC parameters covering core, retry, circuit breaker, HTTP, Kafka, Redis,
  MQTT, AMQP, NATS, data retention, and response capture.
- RBAC with three roles: `ulak_admin`, `ulak_application`,
  `ulak_monitor`.
- HTTP dispatcher with libcurl, curl_multi batching, SSRF protection, OAuth2,
  SigV4, and Bearer/Basic/API-key authentication.
- Kafka dispatcher with librdkafka, delivery reports, and batch produce.
- MQTT dispatcher with libmosquitto, QoS levels, and PUBACK-based batch mode.
- Redis Streams dispatcher with hiredis, TLS support, and consumer groups.
- AMQP dispatcher with librabbitmq, publisher confirms, and batch produce.
- NATS dispatcher with Core NATS / JetStream publishing and batch flush support.
- Shared memory worker registry with LWLock coordination.
- LISTEN/NOTIFY integration for low-latency message wake-up.
- `FOR UPDATE SKIP LOCKED` concurrent queue consumption.
- Ordering key support for per-key sequential delivery.
- Idempotency key support with conflict detection.
- Scheduled message delivery via `scheduled_at` parameter.
- Message TTL with automatic expiration via `mark_expired_messages()`.
- CloudEvents envelope support for HTTP dispatcher.
- Webhook HMAC signature verification.
- 27 regression tests, 12 isolation tests, and 3 TAP tests with `make installcheck`.
- Docker Compose development environment with all protocol services.
- GitHub Actions CI for PostgreSQL 14-18 testing and static analysis.

This entry describes the initial contents of version 0.0.0.
