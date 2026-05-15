# Kafka Cheatsheet

Quick recap of the main Kafka concepts this repository is meant to teach.

## Core Concepts

### Broker

A broker is a Kafka server. In this project, local Docker runs a single broker, which is enough to learn the flow without worrying about a cluster.

### Topic

A topic is the named stream where messages are written and read. Here, the default topic is `orders`.

### Partition

A topic is split into partitions. Kafka guarantees ordering only inside one partition, not across the whole topic.

Practical rule:

- same key usually goes to the same partition
- different keys may land in different partitions
- ordering across partitions is not global

### Offset

An offset is the position of a message inside a partition.

Important detail:

- offsets are per partition
- offset `5` in partition `0` has nothing to do with offset `5` in partition `1`

### Producer

A producer sends records to Kafka.

In this repo, the producer:

- serializes `OrderEvent` as JSON
- can send with a key
- can optionally target a specific partition

### Consumer

A consumer reads records from Kafka.

In this repo, the consumer:

- subscribes to a topic
- reads up to `N` messages
- deserializes JSON into `OrderEvent`
- can commit offsets when requested

### Consumer Group

A consumer group is how Kafka shares work between multiple consumers.

Behavior to remember:

- consumers in the same group split partitions
- consumers in different groups each get their own view of the topic

## Key Learning Rules

### Ordering

Kafka ordering is per partition.

What that means:

- if messages for `order-123` always use the same key, they should stay in one partition
- inside that partition, their order is preserved
- messages for other keys can be processed elsewhere at the same time

### Commits and Restarts

Consumers track progress through committed offsets.

If offsets are committed:

- restarting the same group resumes from the committed position

If offsets are not committed:

- the group can read those messages again, depending on its offset state

In this repo, `consume_n(..., commit = true, ...)` explicitly commits progress.

### `auto.offset.reset`

This setting matters when a group has no committed offset yet.

Common values:

- `earliest`: start from the beginning of the partition
- `latest`: start from new messages only

This repo defaults to `earliest`.

## What The Tests Demonstrate

- topic creation and metadata inspection
- same-key ordering within one partition
- work sharing inside one consumer group
- independent reads across different groups
- restart behavior after committing offsets
- timeout behavior when not enough messages arrive
- invalid payload rejection

## Mental Model

You can think about the flow like this:

1. A producer sends a record to a topic.
2. Kafka stores that record in one partition.
3. The record gets an offset inside that partition.
4. A consumer group reads from the topic.
5. Each partition is assigned to at most one consumer in that group.
6. Consumers commit offsets to record progress.

## Scope Of This Repo

This repository is intentionally small.

It does not try to teach:

- schema registry
- Avro or Protobuf
- retries and dead-letter queues
- exactly-once processing
- Kafka Streams
- multi-broker production design

It is aimed at getting the day-one mechanics clear first.
