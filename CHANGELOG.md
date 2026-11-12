# Changelog

All notable changes to ulak will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/zeybek/ulak/compare/v0.2.0...v0.3.0) (2026-11-12)


### ⚠ BREAKING CHANGES

* **redis:** TLS connections fail when the server certificate does not match the configured host. OOM and READONLY replies are retried instead of going to the DLQ.
* **nats:** Nats-Msg-Id is ulak-<system identifier>-<database oid>-<row id>. Publishes to a subject without a stream are retried on the synchronous path too.
* **amqp:** an unroutable mandatory publish is retried instead of completing. The message_id property is msg_<queue row id> instead of a per-connection delivery tag.
* **mqtt:** ulak.mqtt_default_qos defaults to 1. With ulak.workers > 1 a configured client_id gets a _<worker number> suffix. CONNACK refusals such as "server unavailable" are retried instead of going to the DLQ.
* **http:** HTTP 408 and 425 responses are retried instead of going to the DLQ.
* **worker:** a row found stuck in 'processing' is charged a retry and moves to the DLQ once its retries are used up. A worker ended with pg_terminate_backend() is restarted after ulak.worker_restart_delay. Worker sessions run with synchronous_commit = local and lock_timeout = 10s.
* **sql:** send_with_options(), send_batch(), send_batch_with_priority(), publish() and publish_batch() reject a payload larger than ulak.max_payload_size (SQLSTATE 54000), and send_batch() / send_batch_with_priority() count every row against ulak.max_queue_size (SQLSTATE 53400).

### Features

* **mqtt:** report publishes the broker refuses on MQTT 5 connections ([6e4002c](https://github.com/zeybek/ulak/commit/6e4002cca083d0a1b4d7cfdd2b9835487ac13092))


### Bug Fixes

* **amqp:** fail returned mandatory publishes and bound broker RPCs ([b932935](https://github.com/zeybek/ulak/commit/b93293563aeb8b7e2e85e53f7389b9651a520109))
* **core:** keep the trigger wake-up after a send() that failed ([ad4eafb](https://github.com/zeybek/ulak/commit/ad4eafbf446d887b2b62dd74c0cc1f93d2d7f2b4))
* **core:** start two timeout GUCs at their boot values ([f787d77](https://github.com/zeybek/ulak/commit/f787d774cf44983238af8cd4d03e1569d20fd615))
* **http:** extend the SSRF guard to IPv6, NAT64 and broadcast addresses ([b7f5190](https://github.com/zeybek/ulak/commit/b7f5190b721e9419086682dac4ba126d9d1640e5))
* **http:** retry 408 and 425, keep batch-mode headers and Retry-After ([7683656](https://github.com/zeybek/ulak/commit/76836562520cae3afad33c02bcf1d58d2af81e58))
* **http:** size the OAuth2 token buffer to the response ([f78faa9](https://github.com/zeybek/ulak/commit/f78faa9618702c9d32ade589553484c0700ea15f))
* **kafka:** tie sync reports to their produce and accept acks 0 and 1 ([b902767](https://github.com/zeybek/ulak/commit/b902767be56255c2d7b91026cf0e6da51316af36))
* **mqtt:** publish with QoS 1 by default and bound the connect ([c886bcb](https://github.com/zeybek/ulak/commit/c886bcb8cf67ee1626036dd6582ee92dbdf7484a))
* **nats:** retry publishes no stream takes and namespace Nats-Msg-Id ([7024104](https://github.com/zeybek/ulak/commit/7024104dd1d2bcb2e9ecf3fa9ce7a3930847678f))
* **redis:** check the TLS host name and retry OOM and READONLY ([6165e0e](https://github.com/zeybek/ulak/commit/6165e0e5b423b36a2ce784804cf0f3c1140d3cfc))
* **sql:** apply payload and queue limits on every enqueue function ([86c97e8](https://github.com/zeybek/ulak/commit/86c97e88e27d7dd19040553c3f281eb77b589c84))
* **sql:** name the members of each ulak role in the uninstall script ([3ab3269](https://github.com/zeybek/ulak/commit/3ab3269839e89c7c1a400d3863d8570bc65020a3))
* **sql:** refuse to run the extension scripts through psql ([dffe8e5](https://github.com/zeybek/ulak/commit/dffe8e51bd7981f465fcd2ef106ae2ea8b8deedd))
* **sql:** warn when the ulak roles already exist with members ([04a6c78](https://github.com/zeybek/ulak/commit/04a6c78f85f825f10eae6179624d4349b1780a06))
* **worker:** keep delivering through errors, stuck rows and restarts ([8174c7b](https://github.com/zeybek/ulak/commit/8174c7b3513c3350caf1c0ce14b53173d87d4e46))


### Performance Improvements

* **core:** keep senders off the registry lock in the backpressure check ([7b2b420](https://github.com/zeybek/ulak/commit/7b2b420a4bf8605361ed65796ff44b9c946a997b))

## [0.2.0](https://github.com/zeybek/ulak/compare/v0.1.2...v0.2.0) (2026-09-09)


### ⚠ BREAKING CHANGES

* **worker:** the queue trigger and the send_batch/publish functions no longer issue NOTIFY 'ulak_new_msg'. External processes that LISTEN on that channel to learn about new messages must set ulak.wake_notify = on (the NOTIFY is then throttled per session by ulak.notify_throttle_ms) or fall back to polling.

### Features

* **worker:** wake workers through shared memory at commit instead of NOTIFY ([27c2e26](https://github.com/zeybek/ulak/commit/27c2e26c6b702cd28caa7924b3305ccd24b83e21))


### Bug Fixes

* **redis:** bound host name resolution by connect_timeout ([9e92878](https://github.com/zeybek/ulak/commit/9e92878a73f4bccceb9f48d33ba7243af4eaab30))
* **worker:** claim in one transaction, deliver, then write results in another ([a089c8e](https://github.com/zeybek/ulak/commit/a089c8e181bd53f589a537337817a948f5b58ae8))

## [0.1.2](https://github.com/zeybek/ulak/compare/v0.1.1...v0.1.2) (2026-09-08)


### Bug Fixes

* **sql:** add the 0.1.0 -&gt; 0.1.1 and 0.1.1 -&gt; 0.1.2 upgrade paths ([#14](https://github.com/zeybek/ulak/issues/14)) ([afd7fae](https://github.com/zeybek/ulak/commit/afd7fae58b6f44bc56cfb0efd8f5a1337351c481))

## [0.1.1](https://github.com/zeybek/ulak/compare/v0.1.0...v0.1.1) (2026-09-07)


### Bug Fixes

* **docker:** install every runtime library ulak.so needs in the published image ([6a1df54](https://github.com/zeybek/ulak/commit/6a1df542e304ca114daea851a1e2bc4d22a8e11f))
* **docker:** install every runtime library ulak.so needs in the published image ([de15b93](https://github.com/zeybek/ulak/commit/de15b930a3b61047f49da038e967550f6220b27b))

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
