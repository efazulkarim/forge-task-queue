# Concurrency Patterns in Buraq

This document covers the concurrency primitives and patterns that power Buraq's
task processing pipeline. If you understand goroutines and channels but have
never built a production worker pool, this guide bridges that gap.

## The Worker Pool Pattern

A worker pool is the most common concurrency pattern in Go services. The idea is
simple: instead of spawning a goroutine per task (which can exhaust memory), you
create a fixed pool of goroutines that pull work from a shared channel.

```
                    +-----------+
                    |  Fetcher  |
                    | goroutine |
                    +-----+-----+
                          |
                          | sends messages
                          v
                    +-----------+
                    |  Channel  |
                    | (buffered)|
                    +-----+-----+
                          |
              +-----------+-----------+
              |           |           |
              v           v           v
         +--------+  +--------+  +--------+
         |Worker 1|  |Worker 2|  |Worker 3|
         +--------+  +--------+  +--------+
```

Buraq implements this in `consumer/consumer.go`:

```go
// consumer/consumer.go — Start()
func (c *Consumer) Start(ctx context.Context) error {
    if err := c.store.CreateGroup(ctx, c.stream, c.group); err != nil {
        return fmt.Errorf("failed to create consumer group: %w", err)
    }

    // Buffered channel to decouple fetch speed from processing speed
    tasksCh := make(chan task.StreamMessage, c.workerCount*10)
    var wg sync.WaitGroup

    // Start worker pool
    for i := 1; i <= c.workerCount; i++ {
        wg.Add(1)
        go c.worker(ctx, &wg, tasksCh, i)
    }

    // Start task fetcher
    wg.Add(1)
    go c.fetchTasks(ctx, &wg, tasksCh)

    wg.Wait()
    return nil
}
```

Key points:
- `c.workerCount` goroutines are created upfront, not per-task.
- A single fetcher goroutine reads from Redis and pushes into the channel.
- `wg.Wait()` blocks until both the fetcher and all workers exit.

## sync.WaitGroup for Lifecycle Management

A `sync.WaitGroup` tracks how many goroutines are still running. You call
`wg.Add(1)` before starting a goroutine, and `wg.Done()` inside the goroutine
when it exits. `wg.Wait()` blocks until the counter reaches zero.

Buraq uses WaitGroup in two places:

### 1. Tracking the fetcher and all workers

```go
var wg sync.WaitGroup

// Each worker increments the counter
for i := 1; i <= c.workerCount; i++ {
    wg.Add(1)
    go c.worker(ctx, &wg, tasksCh, i)
}

// The fetcher also increments the counter
wg.Add(1)
go c.fetchTasks(ctx, &wg, tasksCh)

// Block until everything is done
wg.Wait()
```

### 2. Deferring Done inside goroutines

Every goroutine calls `defer wg.Done()` as its first statement. This guarantees
the counter decrements even if the function panics:

```go
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup,
    tasksCh <-chan task.StreamMessage, id int) {

    defer wg.Done()  // Always runs, even on panic

    defer func() {
        if r := recover(); r != nil {
            c.logger.Error("worker panicked", "worker_id", id, "panic", r)
            metrics.WorkerPanicsTotal.Inc()
        }
    }()

    for msg := range tasksCh {
        c.processMessage(context.Background(), id, msg)
    }
}
```

The panic recovery defer runs before `wg.Done()` because defers execute in
LIFO order. This means a panicked worker is logged and counted, then the
WaitGroup counter is decremented, and the system continues.

## context.Context for Cancellation Propagation

A `context.Context` carries cancellation signals and deadlines across goroutine
boundaries. Buraq creates a root context in `main.go` and cancels it when the
process receives SIGTERM:

```go
// main.go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
go func() {
    sig := <-sigCh
    logger.Info("received signal, initiating graceful shutdown", "signal", sig)
    cancel()
}()
```

Every goroutine receives this `ctx` and checks it:

```go
func (c *Consumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup,
    tasksCh chan<- task.StreamMessage) {

    defer wg.Done()
    defer close(tasksCh)

    for {
        select {
        case <-ctx.Done():
            c.logger.Info("context cancelled, consumer fetcher stopping")
            return
        default:
            messages, err := c.store.ReadGroup(ctx, ...)
            // ...
        }
    }
}
```

The `select` with `ctx.Done()` is the idiomatic way to check for cancellation.
When `cancel()` is called, the `ctx.Done()` channel closes, and every goroutine
monitoring it exits.

### Per-task timeouts

Buraq also supports per-task timeouts using `context.WithTimeout`:

```go
processCtx := ctx
if t.Timeout != nil {
    var cancel context.CancelFunc
    processCtx, cancel = context.WithTimeout(ctx, *t.Timeout)
    defer cancel()
}

err = c.processor.Process(processCtx, t)
```

This creates a child context that cancels itself after the specified duration.
If the task takes too long, the processor receives a cancelled context and can
abort early.

## The Shutdown Dance

Graceful shutdown in a worker pool follows a specific sequence. Getting this
wrong leads to lost messages, duplicate processing, or goroutine leaks.

```
Step 1: Stop fetching new messages
         |
         v
Step 2: Close the channel (signals workers to drain)
         |
         v
Step 3: Workers finish current task, then exit range loop
         |
         v
Step 4: WaitGroup reaches zero, Start() returns
         |
         v
Step 5: main() exits
```

Here is how Buraq implements each step:

### Step 1: Stop fetching

The fetcher goroutine checks `ctx.Done()` on every loop iteration. When the
context is cancelled, it returns immediately -- no new messages enter the channel.

```go
for {
    select {
    case <-ctx.Done():
        c.logger.Info("context cancelled, consumer fetcher stopping")
        return
    default:
        // ... fetch and dispatch
    }
}
```

### Step 2: Close the channel

The fetcher calls `defer close(tasksCh)` before entering its loop. When the
fetcher returns, the channel is closed. This is the signal for workers to stop.

```go
func (c *Consumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup,
    tasksCh chan<- task.StreamMessage) {

    defer wg.Done()
    defer close(tasksCh)  // Closes when fetcher exits
    // ...
}
```

### Step 3: Workers drain the channel

Workers use `for msg := range tasksCh`. When the channel is closed and empty,
the range loop exits naturally. Workers finish their current task before
checking the channel again.

```go
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup,
    tasksCh <-chan task.StreamMessage, id int) {

    defer wg.Done()
    // ...
    for msg := range tasksCh {
        c.processMessage(context.Background(), id, msg)
    }
    // Channel closed, worker exits
}
```

### Step 4: WaitGroup reaches zero

`Start()` calls `wg.Wait()` which blocks until all workers and the fetcher have
called `wg.Done()`. Only then does `Start()` return.

## Buffered vs Unbuffered Channels

Buraq uses a buffered channel with capacity `workerCount * 10`:

```go
tasksCh := make(chan task.StreamMessage, c.workerCount*10)
```

### Unbuffered channels (synchronous)

An unbuffered channel blocks the sender until a receiver is ready:

```go
ch := make(chan int)     // capacity 0

ch <- 42                 // Blocks until someone reads
val := <-ch              // Blocks until someone writes
```

This creates tight coupling between the fetcher and workers. If all workers are
busy, the fetcher blocks and cannot read more messages from Redis.

### Buffered channels (asynchronous)

A buffered channel blocks the sender only when the buffer is full:

```go
ch := make(chan int, 10) // capacity 10

ch <- 42                 // Does not block (buffer has space)
ch <- 43                 // Still fine
// ... fill the buffer ...
ch <- 999                // BLOCKS — buffer is full
```

This decouples the fetcher from workers. The fetcher can dump a batch of messages
into the channel and immediately go back to Redis, while workers process at their
own pace.

### Why workerCount * 10?

The buffer size is a tuning knob:

```
Buffer too small (1-2):
  Fetcher blocks frequently → Redis connection sits idle → low throughput

Buffer too large (1000+):
  Messages pile up in memory → high memory usage → stale messages if crash

Buffer = workerCount * 10:
  Each worker has ~10 messages of runway → fetcher rarely blocks → good balance
```

### Backpressure

When the buffer is full, the fetcher blocks on the channel send. This is
backpressure -- the system naturally slows down fetching when workers cannot keep
up. Buraq's fetcher handles this gracefully:

```go
for _, msg := range messages {
    select {
    case tasksCh <- msg:
        // Message dispatched
    case <-ctx.Done():
        c.logger.Info("context cancelled while dispatching, stopping fetcher")
        return
    }
}
```

The `select` ensures that even if the channel is full, the fetcher can still
respond to cancellation. Without this, a full channel during shutdown would
deadlock.

## Direction-Typed Channels

Notice that Buraq uses different channel types for the fetcher and workers:

```go
// Fetcher receives a send-only channel
func (c *Consumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup,
    tasksCh chan<- task.StreamMessage) { ... }

// Workers receive a receive-only channel
func (c *Consumer) worker(ctx context.Context, wg *sync.WaitGroup,
    tasksCh <-chan task.StreamMessage, id int) { ... }
```

The `chan<-` (send-only) and `<-chan` (receive-only) types prevent misuse at
compile time. The fetcher cannot accidentally read from the channel, and workers
cannot accidentally close it.

## The Priority Consumer Variant

Buraq includes a `PriorityConsumer` that reads from multiple streams in priority
order. Instead of one channel, it has one fetcher that checks high-priority
streams first:

```go
// consumer/priority_consumer.go
func (pc *PriorityConsumer) fetchTasks(ctx context.Context, wg *sync.WaitGroup,
    tasksCh chan<- task.StreamMessage, streams []string) {

    defer wg.Done()
    defer close(tasksCh)

    for {
        select {
        case <-ctx.Done():
            return
        default:
            foundAny := false
            for _, stream := range streams {
                // Try high-priority first, then normal, then low
                messages, err := pc.store.ReadGroup(ctx, stream, ...)
                for _, msg := range messages {
                    foundAny = true
                    select {
                    case tasksCh <- msg:
                    case <-ctx.Done():
                        return
                    }
                }
            }
            if !foundAny {
                time.Sleep(100 * time.Millisecond) // Avoid busy loop
            }
        }
    }
}
```

The streams are ordered `["high", "normal", "low"]`. The fetcher always tries
the high-priority stream first. Only if it is empty does it check the next one.
This ensures critical tasks are processed first without starving lower priorities
(they get checked on every iteration).

## Common Mistakes

### 1. Forgetting to close the channel

```go
// WRONG — workers block forever
go func() {
    for _, msg := range messages {
        tasksCh <- msg
    }
    // Channel never closed, workers never exit
}()

// RIGHT — close when done
go func() {
    defer close(tasksCh)
    for _, msg := range messages {
        tasksCh <- msg
    }
}()
```

### 2. Closing a channel from the wrong side

```go
// WRONG — receiver should not close
go func() {
    for msg := range tasksCh {
        process(msg)
    }
    close(tasksCh) // Race condition: multiple workers, who closes?
}()

// RIGHT — sender closes, receivers range
```

### 3. Not checking ctx.Done() during channel send

```go
// WRONG — can deadlock during shutdown
for _, msg := range messages {
    tasksCh <- msg  // Blocks if channel full and workers stopped
}

// RIGHT — select on both channel and context
for _, msg := range messages {
    select {
    case tasksCh <- msg:
    case <-ctx.Done():
        return
    }
}
```

## Summary

| Pattern               | Where Used              | Why                                      |
|-----------------------|-------------------------|------------------------------------------|
| Worker pool           | `consumer.Start()`      | Fixed goroutines, bounded memory         |
| sync.WaitGroup        | `Start()`, `worker()`   | Track goroutine lifecycle                |
| context.Context       | All goroutines           | Propagate cancellation from SIGTERM      |
| Buffered channel      | `tasksCh`               | Decouple fetch from process, backpressure|
| Channel direction     | `chan<-` / `<-chan`      | Compile-time safety                      |
| defer close           | `fetchTasks()`          | Signal workers to stop                   |
| Panic recovery        | `worker()`              | Prevent one bad task from killing worker  |
