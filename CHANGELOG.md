# Changelog

All notable changes to ulak will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
