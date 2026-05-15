# Kafka Basics in Rust

Small Rust project for learning Kafka fundamentals with real code and integration tests.

The repository focuses on a single `orders` topic, JSON order events, and the core Kafka behaviors that matter on day one:

- producing keyed events
- consuming and committing offsets
- observing partition ordering
- seeing how consumer groups share or duplicate work
- creating and describing topics locally

The code is library-first. There are no CLI binaries yet.

## What Is Implemented

The library is split into a few small modules:

- `src/order_event.rs`: order event model and sample event generation
- `src/producer.rs`: Kafka producer for serialized `OrderEvent` payloads
- `src/consumer.rs`: Kafka consumer utilities for reading validated order events
- `src/topic_admin.rs`: topic creation, deletion, and metadata inspection
- `src/config.rs`: app config loading plus shared Kafka client configuration

The integration suite in `tests/kafka_experiments.rs` exercises the main learning scenarios:

- topic creation and description
- ordering for messages with the same key
- work sharing across consumers in the same group
- independent reads across different groups
- restart behavior with committed offsets
- timeout behavior when not enough records arrive
- invalid payload handling

## Prerequisites

- Rust toolchain
- Docker with `docker compose`

Kafka is expected on `localhost:9092`. The included compose file starts a single Kafka broker in KRaft mode.

## Quick Start

Start Kafka:

```bash
docker compose up -d
docker compose ps
```

Run the test suite:

```bash
cargo test
```

Run lint checks:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Stop Kafka:

```bash
docker compose down
```

For the step-by-step local Kafka walkthrough, see [docs/local-kafka.md](docs/local-kafka.md).

## Configuration

Default config lives in `config/app.toml`.

You can override the config path with:

```bash
export KAFKA_BASICS_CONFIG=/path/to/app.toml
```

Current settings cover:

- Kafka broker and default topic
- producer timeout and sample event count
- consumer group defaults and offset reset policy

## Project Scope

This repository intentionally stays narrow:

- one broker for local learning
- one topic model: `orders`
- JSON payloads only
- no database
- no HTTP API
- no UI
- no schema registry

That keeps the code small enough to study while still covering the important Kafka mechanics.
