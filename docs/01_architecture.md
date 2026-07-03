# Buraq Architecture

This document describes how Buraq is structured, how data flows through the
system, and why each design decision was made.

## Component Overview

Buraq is a distributed task queue built on Redis Streams. It has five logical
layers:

```
+-------------------+       +-------------------+
|   HTTP API (8080) |       | Prometheus (2112) |
|   SSE / REST      |       |   /metrics        |
+--------+----------+       +--------+----------+
         |                           |
         v                           v
+--------+---------------------------+----------+
|              Producer / Consumer              |
|  - producer.Producer   (enqueue)              |
|  - consumer.Consumer   (fetch + process)      |
+--------+---------------------------+----------+
         |                           |
         v                           v
+--------+----------+       +--------+----------+
|   task (domain)   |       |  event (pub/sub)  |
|   Task, Event,    |       |  Publisher         |
|   interfaces      |       |                    |
+--------+----------+       +--------+----------+
         |                           |
         v                           v
+--------+---------------------------+----------+
|            redisadapter.Store                  |
|  XADD / XREADGROUP / XACK / XDEL / XRANGE    |
+-------------------+---------------------------+
                    |
                    v
              +-----+-----+
              |   Redis   |
              |  Streams  |
              +-----------+
```

### Package Responsibilities

| Package         | Responsibility                                           |
|-----------------|----------------------------------------------------------|
| `main`          | Wiring: creates Redis client, instantiates all services, |
|                 | handles OS signals for graceful shutdown.                 |
| `config`        | Reads environment variables into a typed `Config` struct.|
| `task`          | Domain types (`Task`, `Event`) and interfaces            |
|                 | (`StreamStore`, `EventPublisher`, `TaskProcessor`).      |
| `producer`      | Serializes a `Task` and pushes it onto a Redis Stream.   |
| `consumer`      | Reads from a stream via a consumer group, dispatches     |
|                 | tasks to a worker pool, handles retries and DLQ routing. |
| `redisadapter`  | Implements `StreamStore` against `go-redis`.             |
| `event`         | Implements `EventPublisher` via Redis Pub/Sub.           |
| `metrics`       | Prometheus counters, histograms, and gauges.             |
| `api`           | HTTP server: SSE stream, health check, DLQ replay, CORS.|

## Data Flow

### Enqueue Path

1. An external caller (or the mock producer) creates a `task.Task`.
2. `producer.Produce()` calls `store.Add()` which runs `XADD` against the
   configured stream (default `buraq_tasks`).
3. A `Pending` event is published to the `buraq_events` Pub/Sub channel.

### Processing Path

1. `consumer.Start()` creates the consumer group if it does not exist
   (`XGROUP CREATE MKSTREAM`).
2. A single fetcher goroutine calls `XREADGROUP` in a loop, blocking for up
   to `BLOCK_TIMEOUT` when the stream is empty.
3. Messages are pushed into a buffered channel (`tasksCh`).
4. A pool of `WORKER_COUNT` goroutines reads from the channel.
5. Each worker deserializes the JSON payload into a `Task`.
6. The injected `TaskProcessor` executes the actual work.
7. On success the message is acknowledged (`XACK`).
8. On failure the retry/DLQ logic runs (see Reliability Patterns).

### Event Path

Every state transition (Pending, Processing, Completed, Failed, DLQ) is
broadcast as a JSON event on the `buraq_events` Pub/Sub channel. The API
server subscribes to this channel and relays events to browser clients over
Server-Sent Events (SSE) at `GET /api/stream`.

## Design Decisions

### Why Redis Streams over a dedicated broker?

- Redis is already a dependency for most Go web services.
- Streams give us consumer groups, acknowledgement, and trimming out of the
  box -- the same primitives RabbitMQ or Kafka provide, with far less
  operational overhead.
- The `XREADGROUP` blocking read model maps naturally to a fetcher loop.

### Why interfaces for storage and publishing?

The `task.StreamStore`, `task.EventPublisher`, and `task.TaskProcessor`
interfaces decouple the domain logic from Redis. This lets you:

- Swap Redis for NATS or Postgres by writing a new adapter.
- Write unit tests with in-memory fakes.
- Inject a mock processor that sleeps or fails deterministically.

### Why a single fetcher + channel dispatch?

A naive approach spawns N goroutines each calling `XREADGROUP` independently.
That works but creates redundant round-trips when the stream is idle. Buraq
uses a single fetcher that blocks on `XREADGROUP` and fans out messages over a
buffered channel. Workers are purely CPU-bound -- they never block on I/O while
waiting for new work.

The channel capacity is `WORKER_COUNT * 10`, giving each worker a small local
buffer so the fetcher is not blocked by a slow task.

### Why structured logging?

Buraq uses Go's standard `log/slog` with JSON output. JSON logs are easy to
ship to Loki, Elasticsearch, or CloudWatch without regex parsing. Every log
line includes contextual fields (`task_id`, `worker_id`, `error`) so you can
filter and correlate in your log aggregator.

## Configuration Reference

All configuration is via environment variables with sensible defaults:

| Variable            | Default                | Description                              |
|---------------------|------------------------|------------------------------------------|
| `REDIS_ADDR`        | `localhost:6379`       | Redis address                            |
| `STREAM_NAME`       | `buraq_tasks`          | Main task stream                         |
| `DLQ_STREAM_NAME`   | `buraq_tasks_dlq`      | Dead-Letter Queue stream                 |
| `GROUP_NAME`        | `buraq_workers`        | Consumer group name                      |
| `CONSUMER_NAME`     | `worker_node_1`        | This consumer's identity                 |
| `WORKER_COUNT`      | `5`                    | Number of worker goroutines              |
| `FETCH_BATCH_SIZE`  | `10`                   | Messages per `XREADGROUP` call           |
| `BLOCK_TIMEOUT`     | `2s`                   | How long `XREADGROUP` blocks             |
| `API_PORT`          | `:8080`                | HTTP API listen address                  |
| `METRICS_PORT`      | `:2112`                | Prometheus metrics listen address        |
| `MAX_RETRIES`       | `3`                    | Retries before DLQ                       |
| `ENABLE_MOCK_TASKS` | `true`                 | Produce synthetic tasks on a timer       |
| `MOCK_TASK_INTERVAL`| `2s`                   | Interval between mock tasks              |
| `API_KEY`           | (empty)                | Bearer token for sensitive endpoints     |
| `CORS_ORIGINS`      | `http://localhost:3000`| Comma-separated allowed origins          |

## Running Locally

```bash
# Start Redis
docker compose up -d redis

# Run the application
go run .

# In another terminal, watch the SSE stream
curl -N http://localhost:8080/api/stream
```
