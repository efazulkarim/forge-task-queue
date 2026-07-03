# Building a Task Queue from Scratch

This guide walks through building Buraq incrementally. Each step adds one
capability. By the end, you will understand every component and why it exists.

## Step 1: The Simplest Producer

Start with the absolute minimum: push a JSON message into a Redis stream.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/redis/go-redis/v9"
)

type Task struct {
    ID      string `json:"id"`
    Type    string `json:"type"`
    Payload string `json:"payload"`
}

func main() {
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    task := Task{
        ID:      "task-1",
        Type:    "email_notification",
        Payload: `{"user_id": 123, "template": "welcome"}`,
    }

    data, _ := json.Marshal(task)

    // XADD appends a message to the stream
    msgID, err := rdb.XAdd(ctx, &redis.XAddArgs{
        Stream: "buraq_tasks",
        Values: map[string]interface{}{
            "payload": data,
        },
    }).Result()

    if err != nil {
        panic(err)
    }

    fmt.Printf("Enqueued task %s, message ID: %s\n", task.ID, msgID)
}
```

Run it:

```bash
go run main.go
# Enqueued task task-1, message ID: 1719580800000-0
```

Verify with Redis CLI:

```bash
redis-cli XRANGE buraq_tasks - +
# 1) 1) "1719580800000-0"
#    2) 1) "payload"
#       2) "{\"id\":\"task-1\",\"type\":\"email_notification\",\"payload\":\"{...}\"}"
```

This is a producer. It writes to an append-only log. No consumers yet -- messages
just pile up in the stream.

## Step 2: Adding a Consumer

Now read those messages. Start with plain `XREAD` (no consumer group):

```go
// Simple XREAD — every consumer sees every message (broadcast)
streams, err := rdb.XRead(ctx, &redis.XReadArgs{
    Streams: []string{"buraq_tasks", "0"}, // "0" = read from beginning
    Count:   10,
    Block:   5 * time.Second,
}).Result()

for _, stream := range streams {
    for _, msg := range stream.Messages {
        fmt.Printf("Message %s: %s\n", msg.ID, msg.Values["payload"])
    }
}
```

This works but has a problem: if you run two consumers, both see every message.
For a task queue, you want each message processed exactly once. That requires
consumer groups.

### Consumer Groups

```go
// Create a consumer group (idempotent — ignores "already exists")
rdb.XGroupCreateMkStream(ctx, "buraq_tasks", "buraq_workers", "0")

// Read with XREADGROUP — each message delivered to one consumer
streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
    Group:    "buraq_workers",
    Consumer: "worker-1",
    Streams:  []string{"buraq_tasks", ">"}, // ">" = only new messages
    Count:    10,
    Block:    5 * time.Second,
}).Result()
```

The `>` is critical. It means "give me messages that have never been delivered
to this group." After processing, acknowledge with `XACK`:

```go
for _, msg := range messages {
    // Process the task...
    fmt.Printf("Processing: %s\n", msg.Values["payload"])

    // Acknowledge — removes from Pending Entry List
    rdb.XAck(ctx, "buraq_tasks", "buraq_workers", msg.ID)
}
```

## Step 3: Adding Concurrency

A single goroutine processing tasks sequentially is too slow. Add a worker pool:

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    rdb.XGroupCreateMkStream(ctx, "buraq_tasks", "buraq_workers", "0")

    tasksCh := make(chan redis.XMessage, 50) // Buffered channel
    var wg sync.WaitGroup

    // Start 5 workers
    for i := 1; i <= 5; i++ {
        wg.Add(1)
        go worker(ctx, &wg, rdb, tasksCh, i)
    }

    // Fetcher goroutine
    wg.Add(1)
    go fetcher(ctx, &wg, rdb, tasksCh)

    // Wait for shutdown signal
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    <-sigCh

    cancel()    // Signal all goroutines to stop
    wg.Wait()   // Wait for clean exit
}

func fetcher(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client,
    tasksCh chan<- redis.XMessage) {

    defer wg.Done()
    defer close(tasksCh)

    for {
        select {
        case <-ctx.Done():
            return
        default:
            streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
                Group: "buraq_workers", Consumer: "fetcher",
                Streams: []string{"buraq_tasks", ">"},
                Count: 10, Block: 2 * time.Second,
            }).Result()

            if err != nil {
                if ctx.Err() != nil { return }
                time.Sleep(time.Second)
                continue
            }

            for _, stream := range streams {
                for _, msg := range stream.Messages {
                    select {
                    case tasksCh <- msg:
                    case <-ctx.Done():
                        return
                    }
                }
            }
        }
    }
}

func worker(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client,
    tasksCh <-chan redis.XMessage, id int) {

    defer wg.Done()

    for msg := range tasksCh {
        fmt.Printf("Worker %d processing %s\n", id, msg.ID)
        time.Sleep(500 * time.Millisecond) // Simulate work
        rdb.XAck(ctx, "buraq_tasks", "buraq_workers", msg.ID)
    }
}
```

This is the core architecture Buraq uses. One fetcher, N workers, one buffered
channel. The `select` with `ctx.Done()` prevents deadlock during shutdown.

## Step 4: Adding Retries and DLQ

Tasks can fail. Add retry logic and a dead-letter queue:

```go
type Task struct {
    ID             string `json:"id"`
    Type           string `json:"type"`
    Payload        string `json:"payload"`
    MaxRetries     int    `json:"max_retries"`
    CurrentRetries int    `json:"current_retries"`
    Error          string `json:"error,omitempty"`
}

func processWithRetry(ctx context.Context, rdb *redis.Client,
    msg redis.XMessage, task Task) {

    err := processTask(task) // Your actual processing logic

    if err == nil {
        // Success — acknowledge
        rdb.XAck(ctx, "buraq_tasks", "buraq_workers", msg.ID)
        return
    }

    // Failed — decide what to do
    task.Error = err.Error()

    if task.CurrentRetries < task.MaxRetries {
        // Re-queue with incremented retry counter
        task.CurrentRetries++
        data, _ := json.Marshal(task)
        rdb.XAdd(ctx, &redis.XAddArgs{
            Stream: "buraq_tasks",
            Values: map[string]interface{}{"payload": data},
        })
    } else {
        // Move to DLQ
        data, _ := json.Marshal(task)
        rdb.XAdd(ctx, &redis.XAddArgs{
            Stream: "buraq_tasks_dlq",
            Values: map[string]interface{}{"payload": data},
        })
    }

    // Always ack the original message
    rdb.XAck(ctx, "buraq_tasks", "buraq_workers", msg.ID)
}
```

The key insight: you always `XACK` the original message. The retry is a new
`XADD`. This prevents the PEL from growing and avoids duplicate delivery of
the same message ID.

## Step 5: Adding Interfaces

Hardcoding Redis makes testing impossible. Extract interfaces:

```go
// task/store.go
type StreamStore interface {
    Add(ctx context.Context, stream string, t *Task) (string, error)
    ReadGroup(ctx context.Context, stream, group, consumer string,
        count int64, block interface{}) ([]StreamMessage, error)
    Ack(ctx context.Context, stream, group, msgID string) error
    CreateGroup(ctx context.Context, stream, group string) error
}

type TaskProcessor interface {
    Process(ctx context.Context, t *Task) error
}

// Convenience adapter for functions
type TaskProcessorFunc func(ctx context.Context, t *Task) error
func (f TaskProcessorFunc) Process(ctx context.Context, t *Task) error {
    return f(ctx, t)
}
```

Now the consumer depends on interfaces, not Redis:

```go
// consumer/consumer.go
type Consumer struct {
    store     task.StreamStore     // Interface, not *redis.Client
    processor task.TaskProcessor   // Interface, not hardcoded logic
    // ...
}
```

For testing, create an in-memory fake:

```go
type FakeStore struct {
    messages []task.StreamMessage
}

func (f *FakeStore) Add(ctx context.Context, stream string, t *task.Task) (string, error) {
    // Store in memory
    return "fake-id", nil
}
// ... implement other methods
```

## Step 6: Adding Observability

### Prometheus metrics

Add counters and histograms to track what is happening:

```go
// metrics/metrics.go
var (
    TasksProcessedTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "buraq_tasks_processed_total",
            Help: "Total number of tasks processed",
        },
        []string{"task_type", "status"},
    )

    TaskDurationSeconds = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "buraq_task_duration_seconds",
            Help:    "Task processing duration",
            Buckets: prometheus.DefBuckets,
        },
        []string{"task_type"},
    )
)
```

Use them in the consumer:

```go
startTime := time.Now()
err = c.processor.Process(ctx, t)
duration := time.Since(startTime).Seconds()

metrics.TaskDurationSeconds.WithLabelValues(t.Type).Observe(duration)

if err != nil {
    metrics.TasksProcessedTotal.WithLabelValues(t.Type, "error").Inc()
} else {
    metrics.TasksProcessedTotal.WithLabelValues(t.Type, "success").Inc()
}
```

Expose metrics over HTTP:

```go
go func() {
    http.Handle("/metrics", promhttp.Handler())
    http.ListenAndServe(":2112", nil)
}()
```

### Structured logging

Replace `fmt.Printf` with `log/slog`:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

logger.Info("processing task",
    "worker_id", id,
    "task_id", t.ID,
    "task_type", t.Type,
    "retry", fmt.Sprintf("%d/%d", t.CurrentRetries, t.MaxRetries),
)
```

JSON output is easy to parse in log aggregators:

```json
{"time":"2024-06-28T10:30:00Z","level":"INFO","msg":"processing task",
 "worker_id":1,"task_id":"task-42","task_type":"email_notification","retry":"1/3"}
```

### Server-Sent Events

Broadcast task lifecycle events to the dashboard:

```go
// event/publisher.go — Redis Pub/Sub
func (p *Publisher) Publish(ctx context.Context, e task.Event) error {
    data, _ := json.Marshal(e)
    return p.client.Publish(ctx, p.channel, string(data)).Err()
}

// api/server.go — SSE endpoint
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")

    pubsub := s.client.Subscribe(r.Context(), "buraq_events")
    defer pubsub.Close()

    for msg := range pubsub.Channel() {
        fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
        w.(http.Flusher).Flush()
    }
}
```

## Step 7: Adding a Dashboard

The dashboard is a Next.js app that connects to the SSE endpoint and displays
real-time task events. It shows:

- Task throughput (tasks/second)
- Worker status
- DLQ depth
- Processing latency percentiles

The architecture at this point:

```
+-------------+     +---------------+     +-------------+
|   Producer  |     |   Consumer    |     |  Dashboard  |
|   (Go API)  |     |  (Worker Pool)|     |  (Next.js)  |
+------+------+     +-------+-------+     +------+------+
       |                    |                     |
       v                    v                     v
+------+--------------------+---------------------+------+
|                    Redis Streams + Pub/Sub              |
+--------------------------------------------------------+
       |                    |                     |
       v                    v                     v
+------+-------+   +--------+------+    +---------+-----+
| Prometheus   |   | Structured    |    | SSE Events    |
| /metrics     |   | Logs (slog)   |    | /api/stream   |
+--------------+   +---------------+    +---------------+
```

## The Complete main.go

Here is how all the pieces wire together in Buraq's `main.go`:

```go
func main() {
    // 1. Logger
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

    // 2. Config from environment
    cfg := config.Load()

    // 3. Context with signal-based cancellation
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
    go func() { <-sigCh; cancel() }()

    // 4. Redis client + store
    rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
    store := redisadapter.NewStore(rdb)

    // 5. Event publisher
    publisher := event.NewPublisher(rdb, "buraq_events", logger)

    // 6. Producer
    p := producer.New(store, publisher, cfg.StreamName, logger)

    // 7. API server
    apiSrv := api.NewServer(store, publisher, rdb, cfg.StreamName,
        cfg.DLQStreamName, cfg.APIKey, cfg.CORSOrigins, logger)
    go apiSrv.Start(ctx, cfg.APIPort)

    // 8. Task processor (your business logic)
    processor := task.TaskProcessorFunc(func(ctx context.Context, t *task.Task) error {
        // Do actual work here
        return nil
    })

    // 9. Consumer with worker pool
    c := consumer.New(store, publisher, processor,
        cfg.StreamName, cfg.GroupName, cfg.ConsumerName,
        cfg.WorkerCount, cfg.FetchBatchSize, cfg.BlockTimeout,
        cfg.DLQStreamName, cfg.MaxRetries, logger)

    // 10. Start (blocks until shutdown)
    c.Start(ctx)
}
```

## What You Learned

| Step | Concept                        | Go Pattern                      |
|------|--------------------------------|---------------------------------|
| 1    | Enqueue a message              | `XADD`                          |
| 2    | Read messages reliably         | `XREADGROUP` + `XACK`           |
| 3    | Concurrent processing          | goroutine pool + channel        |
| 4    | Handle failures                | retry counter + DLQ stream      |
| 5    | Testability                    | interfaces + dependency injection|
| 6    | Observability                  | Prometheus + slog + SSE         |
| 7    | User interface                 | Next.js dashboard               |

Each step is a natural progression. You do not need to understand the full
architecture to start contributing. Pick a step, understand the pattern, and
build from there.
