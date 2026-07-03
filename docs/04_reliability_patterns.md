# Reliability Patterns in Buraq

Distributed systems fail. Networks partition, processes crash, Redis runs out
of memory. Reliability is not about preventing failure -- it is about designing
systems that degrade gracefully and recover automatically. This document
explains the reliability patterns Buraq implements and why each one exists.

## At-Least-Once Delivery

Buraq guarantees at-least-once delivery. Every task submitted to the queue will
be processed at least one time. It might be processed more than once (see
idempotency below), but it will never be silently dropped.

### How Redis Streams enables this

The mechanism is the Pending Entry List (PEL). When `XREADGROUP` delivers a
message, Redis records it in the PEL. The message stays there until `XACK` is
called. If the consumer crashes, the message remains in the PEL and can be
re-delivered.

```
Producer                     Redis                        Consumer
   |                           |                              |
   |-- XADD ------------------>|                              |
   |                           |                              |
   |                           |<--- XREADGROUP (>) ---------|
   |                           |   [message enters PEL]       |
   |                           |                              |
   |                           |       [consumer crashes]     |
   |                           |                              |
   |                           |<--- XREADGROUP (0) ---------|  (new consumer)
   |                           |   [re-delivers from PEL]     |
   |                           |                              |
   |                           |<--- XACK -------------------|
   |                           |   [message leaves PEL]       |
```

The `>` in `XREADGROUP` means "new messages only." The `0` means "give me my
pending messages." Buraq currently uses `>` because it relies on restart for
re-delivery rather than `XCLAIM`.

### Why not exactly-once?

Exactly-once delivery is a myth in distributed systems. What systems like Kafka
call "exactly-once" is really "effectively once" -- they use idempotency keys
and transactional offsets to deduplicate. Buraq takes the same approach:
at-least-once delivery plus idempotency keys equals effective exactly-once.

## Retry Strategies

When a task fails, Buraq has three options:

1. **Retry immediately** -- re-enqueue with no delay.
2. **Retry with backoff** -- wait longer between each retry.
3. **Give up** -- move to the Dead-Letter Queue.

Buraq currently implements immediate retry, but the architecture supports all
three.

### Immediate Retry

This is what Buraq does today. Failed tasks are re-enqueued to the same stream
with an incremented retry counter:

```go
// consumer/consumer.go — handleTaskFailure()
func (c *Consumer) handleTaskFailure(ctx context.Context, workerID int,
    t *task.Task, originalMsgID string, processErr error, workerStrID string) {

    t.Error = processErr.Error()

    if t.CurrentRetries < t.MaxRetries {
        t.CurrentRetries++
        c.logger.Info("re-queueing task",
            "worker_id", workerID,
            "task_id", t.ID,
            "retry", fmt.Sprintf("%d/%d", t.CurrentRetries, t.MaxRetries),
        )

        if _, err := c.store.Add(ctx, c.stream, t); err != nil {
            c.logger.Error("failed to re-queue task", ...)
            return
        }
    } else {
        // Move to DLQ
        // ...
    }

    c.acknowledge(ctx, originalMsgID)
}
```

The sequence is:
1. Process the task -- it fails.
2. If retries remain, `XADD` a new copy of the task with `CurrentRetries++`.
3. `XACK` the original message (so it is not re-delivered from the PEL).
4. If retries are exhausted, `XADD` to the DLQ stream instead.

### Exponential Backoff

Immediate retry can overwhelm a failing downstream service. Exponential backoff
adds a delay that doubles with each attempt:

```go
// Example: add to Buraq for exponential backoff
func backoffDelay(retry int) time.Duration {
    base := time.Second * 2
    delay := base * time.Duration(1<<uint(retry)) // 2s, 4s, 8s, 16s...
    if delay > time.Minute {
        delay = time.Minute // cap at 60s
    }
    return delay
}

// In handleTaskFailure:
delay := backoffDelay(t.CurrentRetries)
time.Sleep(delay) // Simple approach — see jitter below for production
```

### Jitter

Pure exponential backoff can cause "thundering herd" -- if many tasks fail at
the same time, they all retry at the same time. Jitter adds randomness:

```go
func backoffWithJitter(retry int) time.Duration {
    base := time.Second * 2
    maxDelay := base * time.Duration(1<<uint(retry))
    if maxDelay > time.Minute {
        maxDelay = time.Minute
    }
    // Random duration between 0 and maxDelay
    return time.Duration(rand.Int63n(int64(maxDelay)))
}
```

```
Without jitter (all tasks retry at 4s):
  Task A: ---retry---->  (4s)
  Task B: ---retry---->  (4s)
  Task C: ---retry---->  (4s)
  [All hit the failing service at the same instant]

With jitter (spread across 0-4s):
  Task A: -retry-->       (1.2s)
  Task B: ---retry---->   (3.1s)
  Task C: --retry--->     (2.4s)
  [Load is spread over time]
```

## Dead-Letter Queue (DLQ)

A Dead-Letter Queue is a holding area for tasks that have exhausted all retries.
Instead of silently dropping them, you move them to a separate stream where
operators can inspect, debug, and replay them.

### When a task moves to the DLQ

Buraq checks `CurrentRetries < MaxRetries` before each retry. When the counter
reaches the limit, the task is routed to the DLQ:

```go
// consumer/consumer.go
if t.CurrentRetries < t.MaxRetries {
    t.CurrentRetries++
    c.store.Add(ctx, c.stream, t) // Re-queue
} else {
    c.logger.Warn("task exceeded max retries, moving to DLQ",
        "worker_id", workerID,
        "task_id", t.ID,
    )
    metrics.TasksDLQTotal.WithLabelValues(t.Type).Inc()

    c.publishEvent(ctx, task.Event{
        Type:     "DLQ",
        TaskID:   t.ID,
        WorkerID: workerStrID,
    })

    c.store.Add(ctx, c.dlqStream, t) // Move to DLQ stream
}

c.acknowledge(ctx, originalMsgID) // Always ack the original
```

### DLQ stream structure

The DLQ is just another Redis stream (`buraq_tasks_dlq` by default). Tasks
stored there retain all their original fields plus the error message:

```json
{
    "id": "task-42",
    "type": "email_notification",
    "payload": {"user_id": 123, "template": "welcome"},
    "max_retries": 3,
    "current_retries": 3,
    "error": "simulated network timeout while calling external API",
    "created_at": "2024-06-28T10:30:00Z"
}
```

### DLQ Replay

The Buraq API exposes a `POST /api/retry-dlq` endpoint that reads all tasks
from the DLQ, resets their retry counters, and re-enqueues them to the main
stream:

```go
// api/server.go — handleRetryDLQ()
func (s *Server) handleRetryDLQ(w http.ResponseWriter, r *http.Request) {
    // Fetch all tasks from DLQ
    messages, err := s.store.Range(ctx, s.dlqStream, "-", "+", 0)
    // ...

    retried := 0
    for _, msg := range messages {
        t, _ := task.Unmarshal([]byte(payload))

        // Reset retries
        t.CurrentRetries = 0

        // Add back to main stream
        s.store.Add(ctx, s.stream, t)

        // Delete from DLQ
        s.store.Delete(ctx, s.dlqStream, msg.ID)
        retried++

        // Publish Pending event
        s.publisher.Publish(ctx, task.Event{Type: "Pending", TaskID: t.ID})
    }

    json.NewEncoder(w).Encode(map[string]interface{}{
        "success": true,
        "retried": retried,
    })
}
```

This is an operational tool. Use it when:
- A downstream service was down and is now healthy.
- A bug in your task processor has been fixed and deployed.
- You want to re-process historical tasks.

### DLQ monitoring

Alert on DLQ growth. If the DLQ is growing, something is systematically failing:

```promql
# Rate of tasks entering the DLQ (per task type)
rate(buraq_tasks_dlq_total[5m])

# Alert if any tasks enter the DLQ
ALERT DLQTaskReceived
  IF rate(buraq_tasks_dlq_total[1m]) > 0
  FOR 2m
```

## Idempotency Keys

At-least-once delivery means tasks can be processed twice. Idempotency keys
prevent duplicate side effects.

### How it works

The producer sets an `idempotency_key` on the task. The consumer checks a
deduplication store before processing. If the key has been seen, the task is
skipped.

```go
// Producer side
t := &task.Task{
    ID:             "task-1",
    Type:           "email_notification",
    IdempotencyKey: "email:user-123:welcome:2024-06-28", // Unique per action
    // ...
}
```

```go
// Consumer side — consumer/consumer.go
if c.idempotency != nil && t.IdempotencyKey != "" {
    isNew, checkErr := c.idempotency.Check(ctx, t.IdempotencyKey)
    if checkErr != nil {
        c.logger.Error("idempotency check failed", "error", checkErr, "task_id", t.ID)
        // Continue processing on check failure — safer than dropping
    } else if !isNew {
        c.logger.Info("skipping duplicate task", "task_id", t.ID,
            "idempotency_key", t.IdempotencyKey)
        metrics.TasksDuplicateTotal.Inc()
        c.acknowledge(context.Background(), msg.ID)
        return
    }
}
```

### Redis implementation

Buraq uses `SET NX` (set if not exists) with a TTL:

```go
// redisadapter/idempotency.go
func (s *IdempotencyStore) Check(ctx context.Context, key string) (bool, error) {
    ok, err := s.client.SetNX(ctx, "idemp:"+key, "1", s.ttl).Result()
    if err != nil {
        return false, err
    }
    return ok, nil // true = new, false = duplicate
}
```

`SET NX` is atomic -- two concurrent consumers checking the same key will not
both see "new." The TTL (default 24 hours) automatically cleans up old keys.

### Choosing idempotency keys

Good keys are deterministic and scoped to the action:

```
Good:  "email:user-123:welcome:2024-06-28"
Good:  "payment:order-456:charge"
Bad:   "task-1"  (task ID changes on retry)
Bad:   ""        (empty, check is skipped)
```

## The ACK Dance

Every message must be acknowledged, regardless of whether processing succeeded
or failed. Here is the complete decision tree:

```
                    +-------------------+
                    |  Fetch message    |
                    |  (XREADGROUP)     |
                    +--------+----------+
                             |
                             v
                    +-------------------+
                    |  Parse payload    |
                    |  (unmarshal JSON) |
                    +--------+----------+
                             |
                    +--------+--------+
                    |                  |
               Valid payload     Invalid payload
                    |                  |
                    v                  v
           +----------------+    +----------+
           | Idempotency    |    | XACK     |
           | check          |    | (discard)|
           +--------+-------+    +----------+
                    |
           +--------+--------+
           |                  |
        New key           Duplicate key
           |                  |
           v                  v
    +----------------+   +----------+
    | Process task   |   | XACK     |
    +--------+-------+   | (skip)   |
             |           +----------+
    +--------+--------+
    |                  |
 Success            Failure
    |                  |
    v                  v
+----------+   +------------------+
| XACK     |   | Retries left?    |
| (done)   |   +--------+---------+
+----------+            |
                 +------+------+
                 |             |
               Yes            No
                 |             |
                 v             v
          +------------+ +------------+
          | XADD to    | | XADD to    |
          | main stream| | DLQ stream |
          | (re-queue) | +------+-----+
          +------+-----+        |
                 |              |
                 v              v
          +-------------------------+
          | XACK original message   |
          | (always, in all cases)  |
          +-------------------------+
```

The critical rule: **always XACK the original message**. Even if the task fails
and gets re-queued, the original message must be acknowledged. Otherwise Redis
will re-deliver it from the PEL, creating a duplicate.

```go
// consumer/consumer.go — handleTaskFailure()
// After re-queuing OR moving to DLQ:
c.acknowledge(ctx, originalMsgID) // Always ack the original
```

## What If It Fails? Checklist

When building any component, ask these questions:

| Scenario                          | What happens in Buraq                     |
|-----------------------------------|-------------------------------------------|
| Consumer crashes mid-processing?  | Message stays in PEL, re-delivered on restart |
| Redis is temporarily down?        | Fetcher logs error, sleeps 1s, retries    |
| Task processor panics?            | Panic recovered, worker logs and continues|
| DLQ XADD fails?                  | Error logged, original message NOT acked  |
| Idempotency check fails?          | Processing continues (safer than dropping)|
| Publisher (event) fails?          | Warning logged, task still processes      |
| Channel is full during shutdown?  | select on ctx.Done() prevents deadlock    |

## Summary

| Pattern              | Purpose                          | Buraq Implementation           |
|----------------------|----------------------------------|--------------------------------|
| At-least-once        | No message loss                  | XREADGROUP + PEL + XACK        |
| Immediate retry      | Simple failure recovery           | Re-XADD with CurrentRetries++  |
| Exponential backoff  | Protect downstream services       | Not yet implemented            |
| Jitter               | Prevent thundering herd           | Not yet implemented            |
| DLQ                  | Hold failed tasks for inspection  | Separate stream + replay API   |
| Idempotency keys     | Prevent duplicate processing      | Redis SET NX with TTL          |
| Always ACK           | Prevent PEL growth                | XACK in all code paths         |
