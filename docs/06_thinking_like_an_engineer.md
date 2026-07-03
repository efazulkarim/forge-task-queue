# Thinking Like an Engineer

Technical skills get you hired. Engineering thinking gets you promoted. This
document covers the mental models, debugging strategies, and decision-making
frameworks that separate coders from engineers. Every example uses Buraq.

## How to Read Unfamiliar Go Codebases

When you open a project like Buraq for the first time, do not read every file.
Follow this sequence:

### 1. Start with go.mod

```go
module buraq

go 1.21

require (
    github.com/redis/go-redis/v9 v9.7.3
    github.com/prometheus/client_golang v1.20.0
)
```

This tells you the project's dependencies. Buraq uses Redis and Prometheus. That
means it is a data-processing service with metrics. Two dependencies is a green
flag -- focused, not bloated.

### 2. Read main.go

`main.go` is the wiring diagram. It shows you every component and how they
connect. In Buraq:

```go
func main() {
    logger := slog.New(...)           // Structured logging
    cfg := config.Load()              // Config from env vars
    ctx, cancel := context.WithCancel(context.Background()) // Graceful shutdown

    rdb := redis.NewClient(...)       // Redis connection
    store := redisadapter.NewStore(rdb) // Storage adapter
    publisher := event.NewPublisher(rdb, ...) // Event broadcasting

    p := producer.New(store, publisher, ...) // Producer
    apiSrv := api.NewServer(store, ...)      // HTTP API
    c := consumer.New(store, publisher, ...)  // Consumer

    c.Start(ctx) // Blocks until shutdown
}
```

In 30 seconds you know: this project produces tasks, consumes them via a worker
pool, exposes an API, and shuts down gracefully.

### 3. Find the interfaces

Interfaces tell you the contracts between components. In Buraq, look at
`task/store.go`:

```go
type StreamStore interface {
    Add(ctx context.Context, stream string, t *Task) (string, error)
    ReadGroup(ctx context.Context, ...) ([]StreamMessage, error)
    Ack(ctx context.Context, stream, group, msgID string) error
}

type TaskProcessor interface {
    Process(ctx context.Context, t *Task) error
}
```

These three interfaces define the entire system. Everything else is
implementation detail.

### 4. Trace one request end-to-end

Pick one task and follow it through the system:

```
Producer.Produce()
  → store.Add()        (XADD to Redis)
  → publisher.Publish() (Pub/Sub event)

Consumer.Start()
  → fetchTasks()       (XREADGROUP loop)
  → tasksCh <- msg     (channel)
  → worker()           (goroutine pool)
  → processMessage()   (deserialize, check idempotency, process)
  → acknowledge()      (XACK)
```

Once you trace one request, you understand the whole system.

### 5. Look for error handling

How a system handles failure tells you more about its maturity than how it
handles success. In Buraq, look at:

- `handleTaskFailure()` -- retry or DLQ logic
- `defer func() { if r := recover() ... }()` -- panic recovery
- `select { case <-ctx.Done(): ... }` -- cancellation handling

## Mental Models for Distributed Systems

### Delivery Guarantees

There are exactly three delivery guarantees:

```
At-most-once:   Send and forget. Messages can be lost.
                 Use when: losing a few items is acceptable (metrics, logs).

At-least-once:  Send, wait for ACK, retry if no ACK. Messages can duplicate.
                 Use when: every item matters, duplicates are tolerable (Buraq).

Exactly-once:   Impossible in distributed systems. What people mean is
                 "effectively once" = at-least-once + idempotency.
                 Use when: duplicates cause real harm (payments, inventory).
```

Buraq implements at-least-once. To get effectively-once, add idempotency keys
(see reliability patterns).

### Ordering

```
Single producer → single stream → single consumer:
  Strict FIFO ordering guaranteed.

Single producer → single stream → multiple consumers:
  Per-consumer ordering guaranteed (each consumer processes sequentially).
  Global ordering NOT guaranteed (tasks interleaved across consumers).

Multiple producers → single stream:
  Per-producer ordering NOT guaranteed (XADD from different producers
  can interleave). Global ordering IS guaranteed (stream is total order).
```

Buraq uses multiple consumers reading from one stream. This means task-1 might
finish after task-2 even if task-1 was enqueued first. If ordering matters,
use a single consumer or partition by key.

### Partitioning

```
                  +-------------------+
                  |   Stream: orders  |
                  +--------+----------+
                           |
              +------------+------------+
              |                         |
     +--------+--------+      +--------+--------+
     |  Stream: orders  |      |  Stream: orders  |
     |  (user A)        |      |  (user B)        |
     +------------------+      +------------------+
```

Buraq's `PriorityProducer` demonstrates partitioning by priority:

```go
func (pp *PriorityProducer) streamForPriority(priority string) string {
    switch priority {
    case task.PriorityHigh:
        return fmt.Sprintf("%s_high", pp.baseStream)  // buraq_tasks_high
    case task.PriorityLow:
        return fmt.Sprintf("%s_low", pp.baseStream)   // buraq_tasks_low
    default:
        return pp.baseStream                          // buraq_tasks
    }
}
```

This gives high-priority tasks their own stream, so they are never blocked
behind a queue of low-priority work.

## Debugging Strategies

### Strategy 1: Read the logs

Buraq uses structured JSON logging. Every log line includes context:

```json
{
  "time": "2024-06-28T10:30:00Z",
  "level": "ERROR",
  "msg": "task failed",
  "error": "simulated network timeout",
  "worker_id": 3,
  "task_id": "task-42"
}
```

To debug a failing task:

```bash
# Find all log lines for task-42
grep '"task_id":"task-42"' /var/log/buraq.json

# Find all errors in the last hour
grep '"level":"ERROR"' /var/log/buraq.json | tail -50

# Find all DLQ events
grep '"msg":"task exceeded max retries"' /var/log/buraq.json
```

### Strategy 2: Check Prometheus metrics

Metrics tell you what is happening at the system level:

```promql
# Are tasks being processed?
rate(buraq_tasks_processed_total[5m])

# Are tasks failing?
rate(buraq_tasks_failed_total[5m])

# How long do tasks take?
histogram_quantile(0.99, rate(buraq_task_duration_seconds_bucket[5m]))

# Is the DLQ growing?
rate(buraq_tasks_dlq_total[5m])

# Are workers panicking?
rate(buraq_worker_panics_total[5m])
```

If `buraq_tasks_processed_total` is flat (zero rate), the consumer is stuck.
Check: is Redis reachable? Is the consumer group created? Are workers deadlocked?

### Strategy 3: Inspect Redis directly

```bash
# Stream length
XLEN buraq_tasks

# Consumer group info
XINFO GROUPS buraq_tasks

# Pending messages (delivered but not ACKed)
XPENDING buraq_tasks buraq_workers - + 10

# Last 10 messages
XRANGE buraq_tasks - + COUNT 10

# DLQ contents
XRANGE buraq_tasks_dlq - + COUNT 10
```

If `XPENDING` shows many messages, workers are slow or crashed. If `XLEN` is
growing, producers are outpacing consumers.

### Strategy 4: Check the SSE stream

Buraq broadcasts every state transition as an SSE event:

```bash
curl -N http://localhost:8080/api/stream

# Output:
# data: {"type":"Pending","task_id":"task-1"}
# data: {"type":"Processing","task_id":"task-1","worker_id":"worker_node_1-3"}
# data: {"type":"Completed","task_id":"task-1","worker_id":"worker_node_1-3"}
```

If you see `Pending` but no `Processing`, the consumer is not fetching. If you
see `Processing` but no `Completed`, the processor is hanging or crashing.

## When to Use Interfaces vs Concrete Types

Use an interface when:

1. **You need to swap implementations.** `StreamStore` lets you use Redis in
   production and an in-memory fake in tests.

2. **You want to decouple packages.** The `consumer` package depends on
   `task.StreamStore`, not `redisadapter.Store`. It does not import Redis.

3. **You have multiple implementations.** `Store` (standalone Redis) and
   `ClusterStore` (Redis Cluster) both implement `StreamStore`.

Do NOT use an interface when:

1. **There is only one implementation.** If you never swap it, a concrete type
   is simpler and the compiler can inline calls.

2. **The interface has only one method and one caller.** That is just a function
   type with extra steps.

3. **You are guessing at future needs.** "We might need this someday" is not
   a reason. Add the interface when you actually need it.

Buraq's interfaces are justified because they enable testing and support both
standalone Redis and Redis Cluster.

## The "What If It Fails?" Checklist

Before shipping any component, walk through this list:

```
[ ] What if Redis is down?
    → Fetcher logs error, sleeps, retries. No data loss.

[ ] What if a worker panics?
    → Panic recovered, logged, worker continues. Other workers unaffected.

[ ] What if the channel is full?
    → Fetcher blocks (backpressure). select on ctx.Done() prevents deadlock.

[ ] What if we receive a malformed message?
    → Logged as warning, message ACKed (avoids infinite redelivery).

[ ] What if idempotency check fails?
    → Processing continues. Safer to process twice than to drop.

[ ] What if DLQ write fails?
    → Error logged, original message NOT acked (will be retried).

[ ] What if the process is killed with SIGKILL?
    → In-flight messages stay in PEL. On restart, they are re-delivered.

[ ] What if two consumers start with the same name?
    → Redis handles this. They share the PEL. Messages split between them.

[ ] What if the stream grows unbounded?
    → Set up XTRIM cron job. Buraq does not trim automatically.
```

## Career Growth Through Open Source

Contributing to projects like Buraq teaches you skills that tutorials cannot:

### Code review teaches you to read

When you review someone else's PR, you learn to trace logic, spot edge cases,
and think about failure modes. This is the same skill you use debugging
production issues.

### Issues teach you to scope work

"Fix the DLQ replay endpoint" sounds simple. Then you discover: what happens if
a task in the DLQ has a different schema version? What if the DLQ is empty? What
if Redis is down during replay? Scoping is a skill.

### Architecture discussions teach you to reason

"Why use Redis Streams instead of RabbitMQ?" forces you to compare trade-offs:
operational complexity, feature set, team familiarity, performance. This is
engineering thinking.

### Concrete ways to contribute to Buraq

1. **Add exponential backoff with jitter** to `handleTaskFailure()`.
2. **Implement XCLAIM** for automatic dead-worker detection.
3. **Add stream trimming** via a configurable MAXLEN.
4. **Write integration tests** using testcontainers-go.
5. **Add request tracing** with OpenTelemetry.

Each of these teaches a different engineering skill.

## Decision-Making Framework

When faced with a design choice, use this framework:

### 1. Define the constraint

"We need to process 10,000 tasks/second with at-least-once delivery."

### 2. List the options

```
Option A: Single Redis stream, 20 workers
Option B: 4 Redis streams (partitioned), 5 workers each
Option C: Kafka with 8 partitions
```

### 3. Evaluate trade-offs

```
                Option A    Option B    Option C
Throughput      ~15k/s      ~40k/s      ~100k/s
Complexity      Low         Medium      High
Ops overhead    Low         Medium      High
Ordering        Per-group   Per-stream  Per-partition
Team skill      Go+Redis    Go+Redis    Go+Kafka
```

### 4. Choose and document why

"We chose Option A because our current load is 2,000/s and the team knows Redis.
We will revisit at 10,000/s."

### 5. Set a trigger to revisit

"When processing latency p99 exceeds 5 seconds, evaluate Option B."

This prevents both premature optimization and analysis paralysis.

## Summary

| Skill                    | How Buraq teaches it                        |
|--------------------------|---------------------------------------------|
| Reading codebases        | `go.mod` → `main.go` → interfaces → trace   |
| Delivery guarantees      | At-least-once via XREADGROUP + XACK         |
| Ordering                 | Single stream = total order, partitions = per-partition |
| Debugging                | Logs, Prometheus, Redis CLI, SSE events     |
| Interface design         | StreamStore, TaskProcessor, EventPublisher  |
| Failure thinking         | Panic recovery, idempotency, DLQ, context   |
| Decision-making          | Constraints → options → trade-offs → choose  |
