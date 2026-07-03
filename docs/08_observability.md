# Observability in Buraq

Observability answers the question: "What is happening inside my system right
now?" It has three pillars -- metrics, logs, and traces. Buraq implements the
first two and provides the hooks for the third. This document covers every
observability feature in Buraq and how to use them in production.

## Prometheus Metrics

Prometheus scrapes a `/metrics` HTTP endpoint every 15 seconds and stores
time-series data. Buraq exposes metrics on port 2112 by default.

### Metric Types

Buraq uses three Prometheus metric types:

**Counter** -- a value that only goes up. Use for totals.

```go
// metrics/metrics.go
TasksProcessedTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "buraq_tasks_processed_total",
        Help: "Total number of tasks processed by Buraq consumers",
    },
    []string{"task_type", "status"}, // Labels: task type and success/error
)
```

**Histogram** -- a distribution of values. Use for latency.

```go
TaskDurationSeconds = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name:    "buraq_task_duration_seconds",
        Help:    "Histogram of task processing duration in seconds",
        Buckets: prometheus.DefBuckets, // 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
    },
    []string{"task_type"},
)
```

**Gauge** -- a value that goes up and down. Use for current state.

```go
ChannelDepth = promauto.NewGauge(
    prometheus.GaugeOpts{
        Name: "buraq_channel_depth",
        Help: "Current number of tasks waiting in the dispatch channel",
    },
)
```

### All Buraq Metrics

| Metric Name                      | Type      | Labels              | Description                        |
|----------------------------------|-----------|---------------------|------------------------------------|
| `buraq_tasks_processed_total`    | Counter   | task_type, status   | Total tasks processed              |
| `buraq_tasks_failed_total`       | Counter   | task_type           | Total processing failures          |
| `buraq_tasks_dlq_total`          | Counter   | task_type           | Tasks moved to DLQ                 |
| `buraq_tasks_duplicate_total`    | Counter   | (none)              | Duplicate tasks skipped            |
| `buraq_task_duration_seconds`    | Histogram | task_type           | Processing duration distribution   |
| `buraq_worker_panics_total`      | Counter   | (none)              | Worker goroutine panics recovered  |
| `buraq_channel_depth`            | Gauge     | (none)              | Current channel buffer fill level  |

### How Metrics Are Recorded

In the consumer, every task processing attempt records metrics:

```go
// consumer/consumer.go — processMessage()
startTime := time.Now()
err = c.processor.Process(processCtx, t)
duration := time.Since(startTime).Seconds()

// Always record duration
metrics.TaskDurationSeconds.WithLabelValues(t.Type).Observe(duration)

if err != nil {
    // Record failure
    metrics.TasksProcessedTotal.WithLabelValues(t.Type, "error").Inc()
    metrics.TasksFailedTotal.WithLabelValues(t.Type).Inc()
} else {
    // Record success
    metrics.TasksProcessedTotal.WithLabelValues(t.Type, "success").Inc()
}
```

Panics are caught by the recovery handler:

```go
defer func() {
    if r := recover(); r != nil {
        c.logger.Error("worker panicked", "worker_id", id, "panic", r)
        metrics.WorkerPanicsTotal.Inc()
    }
}()
```

## Key PromQL Queries

PromQL is Prometheus's query language. Here are the queries you need for Buraq.

### Throughput

```promql
# Tasks processed per second (all types)
rate(buraq_tasks_processed_total[5m])

# Tasks processed per second by type
rate(buraq_tasks_processed_total{task_type="email_notification"}[5m])

# Tasks processed per second by status
rate(buraq_tasks_processed_total{status="success"}[5m])
rate(buraq_tasks_processed_total{status="error"}[5m])
```

### Error Rate

```promql
# Error rate as a percentage
rate(buraq_tasks_processed_total{status="error"}[5m])
/
rate(buraq_tasks_processed_total[5m])
* 100

# Failed tasks per second
rate(buraq_tasks_failed_total[5m])
```

### Latency

```promql
# Median processing time (p50)
histogram_quantile(0.50, rate(buraq_task_duration_seconds_bucket[5m]))

# 95th percentile processing time (p95)
histogram_quantile(0.95, rate(buraq_task_duration_seconds_bucket[5m]))

# 99th percentile processing time (p99)
histogram_quantile(0.99, rate(buraq_task_duration_seconds_bucket[5m]))

# Average processing time
rate(buraq_task_duration_seconds_sum[5m])
/
rate(buraq_task_duration_seconds_count[5m])
```

### DLQ Monitoring

```promql
# DLQ growth rate (tasks entering DLQ per second)
rate(buraq_tasks_dlq_total[5m])

# Total tasks in DLQ (cumulative)
buraq_tasks_dlq_total

# DLQ rate by task type
rate(buraq_tasks_dlq_total{task_type="email_notification"}[5m])
```

### Duplicate Detection

```promql
# Duplicate tasks per second
rate(buraq_tasks_duplicate_total[5m])

# Duplicate percentage
rate(buraq_tasks_duplicate_total[5m])
/
rate(buraq_tasks_processed_total[5m])
* 100
```

### Worker Health

```promql
# Worker panics per second
rate(buraq_worker_panics_total[5m])

# Channel depth (how backed up workers are)
buraq_channel_depth
```

## Grafana Dashboard Setup

Create a Grafana dashboard with these panels:

### Row 1: Overview

```
+-------------------+-------------------+-------------------+
|  Tasks/sec        |  Error Rate       |  DLQ Depth        |
|  (stat panel)     |  (stat panel)     |  (stat panel)     |
|  rate(processed)  |  rate(failed)/    |  dlq_total        |
|                   |  rate(processed)  |                   |
+-------------------+-------------------+-------------------+
```

PromQL for each:

```promql
# Tasks/sec
sum(rate(buraq_tasks_processed_total{status="success"}[5m]))

# Error Rate
sum(rate(buraq_tasks_processed_total{status="error"}[5m]))
/
sum(rate(buraq_tasks_processed_total[5m]))
* 100

# DLQ Depth
sum(buraq_tasks_dlq_total)
```

### Row 2: Latency

```
+-----------------------------------------------+
|  Processing Latency (time series)              |
|  p50, p95, p99 on same chart                  |
+-----------------------------------------------+
```

```promql
# p50
histogram_quantile(0.50, sum(rate(buraq_task_duration_seconds_bucket[5m])) by (le))

# p95
histogram_quantile(0.95, sum(rate(buraq_task_duration_seconds_bucket[5m])) by (le))

# p99
histogram_quantile(0.99, sum(rate(buraq_task_duration_seconds_bucket[5m])) by (le))
```

### Row 3: Throughput by Type

```
+-----------------------------------------------+
|  Tasks/sec by Type (stacked area)             |
+-----------------------------------------------+
```

```promql
sum(rate(buraq_tasks_processed_total[5m])) by (task_type)
```

### Row 4: Failures and Retries

```
+-------------------+-------------------+-------------------+
|  Failures/sec     |  DLQ rate         |  Panics           |
|  rate(failed)     |  rate(dlq)        |  rate(panics)     |
+-------------------+-------------------+-------------------+
```

## Server-Sent Events (SSE)

Buraq broadcasts real-time task lifecycle events over SSE. This is how the
dashboard shows tasks moving through the system.

### How it works

1. The consumer publishes events to a Redis Pub/Sub channel (`buraq_events`).
2. The API server subscribes to that channel.
3. Browser clients connect to `GET /api/stream` and receive events as they happen.

```go
// api/server.go — SSE endpoint
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, `{"error":"Streaming unsupported"}`, http.StatusInternalServerError)
        return
    }

    pubsub := s.client.Subscribe(r.Context(), "buraq_events")
    defer pubsub.Close()
    ch := pubsub.Channel()

    // Initial heartbeat
    fmt.Fprintf(w, "data: {\"type\":\"ping\"}\n\n")
    flusher.Flush()

    for {
        select {
        case <-r.Context().Done():
            return
        case msg, ok := <-ch:
            if !ok {
                return
            }
            fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
            flusher.Flush()
        }
    }
}
```

### Event format

Each event is a JSON object:

```json
{
    "type": "Processing",
    "task_id": "task-42",
    "worker_id": "worker_node_1-3"
}
```

Event types:
- `Pending` -- task enqueued
- `Processing` -- worker picked up the task
- `Completed` -- task finished successfully
- `Failed` -- task processing failed
- `DLQ` -- task moved to dead-letter queue

### Consuming SSE in JavaScript

```javascript
const events = new EventSource('/api/stream');

events.onmessage = (event) => {
    const data = JSON.parse(event.data);

    switch (data.type) {
        case 'Pending':
            showTaskPending(data.task_id);
            break;
        case 'Processing':
            showTaskProcessing(data.task_id, data.worker_id);
            break;
        case 'Completed':
            showTaskCompleted(data.task_id);
            break;
        case 'Failed':
            showTaskFailed(data.task_id);
            break;
        case 'DLQ':
            showTaskDLQ(data.task_id);
            break;
        case 'ping':
            // Heartbeat, ignore
            break;
    }
};

events.onerror = () => {
    console.log('SSE connection lost, reconnecting...');
    // EventSource auto-reconnects
};
```

### Consuming SSE with curl

```bash
curl -N http://localhost:8080/api/stream

# Output:
# data: {"type":"ping"}
# data: {"type":"Pending","task_id":"task-1"}
# data: {"type":"Processing","task_id":"task-1","worker_id":"worker_node_1-3"}
# data: {"type":"Completed","task_id":"task-1","worker_id":"worker_node_1-3"}
```

## Structured Logging with log/slog

Buraq uses Go's standard `log/slog` package with JSON output. Every log line
is a structured JSON object with contextual fields.

### Logger setup

```go
// main.go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))
```

### Log levels

```go
logger.Debug("fetching messages", "batch_size", count)      // Development only
logger.Info("processing task", "task_id", t.ID)              // Normal operations
logger.Warn("invalid payload", "msg_id", msg.ID)             // Unexpected but handled
logger.Error("task failed", "error", err, "task_id", t.ID)  // Failures
```

### Contextual fields

Every log line includes relevant context:

```go
// In the consumer
c.logger.Info("processing task",
    "worker_id", workerID,
    "task_id", t.ID,
    "task_type", t.Type,
    "retry", fmt.Sprintf("%d/%d", t.CurrentRetries, t.MaxRetries),
)

// Output:
// {"time":"2024-06-28T10:30:00Z","level":"INFO","msg":"processing task",
//  "worker_id":3,"task_id":"task-42","task_type":"email_notification","retry":"1/3"}
```

### Querying logs

With JSON logs, you can filter in any log aggregator:

```bash
# Find all errors for task-42
grep '"task_id":"task-42"' buraq.json | grep '"level":"ERROR"'

# Find all DLQ events
grep '"msg":"task exceeded max retries"' buraq.json

# Find slow tasks (logged with duration)
grep '"duration_s"' buraq.json | jq 'select(.duration_s > 5)'
```

## Health Check Endpoint

Buraq exposes `GET /api/health` for liveness and readiness probes:

```go
// api/server.go
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
    defer cancel()

    redisOK := s.client.Ping(ctx).Err() == nil

    status := "healthy"
    code := http.StatusOK
    if !redisOK {
        status = "degraded"
        code = http.StatusServiceUnavailable
    }

    w.WriteHeader(code)
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status":  status,
        "uptime":  time.Since(s.startTime).String(),
        "redis":   redisOK,
    })
}
```

### Response examples

Healthy:

```json
{
    "status": "healthy",
    "uptime": "72h15m30s",
    "redis": true
}
```

Degraded (Redis down):

```json
{
    "status": "degraded",
    "uptime": "72h15m30s",
    "redis": false
}
```

### Kubernetes integration

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: buraq
      livenessProbe:
        httpGet:
          path: /api/health
          port: 8080
        initialDelaySeconds: 5
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /api/health
          port: 8080
        initialDelaySeconds: 5
        periodSeconds: 5
```

## What to Alert On

Set up alerts for these conditions:

### Critical (page someone immediately)

```promql
# DLQ is growing — something is systematically failing
ALERT DLQGrowing
  IF rate(buraq_tasks_dlq_total[5m]) > 0
  FOR 5m
  ANNOTATIONS {
    summary = "Tasks entering DLQ",
    description = "{{ $value }} tasks/sec entering DLQ"
  }

# Worker panics — code bug causing crashes
ALERT WorkerPanics
  IF rate(buraq_worker_panics_total[5m]) > 0
  FOR 1m
  ANNOTATIONS {
    summary = "Worker goroutine panics detected"
  }

# No tasks being processed — consumer is dead
ALERT ConsumerDown
  IF rate(buraq_tasks_processed_total[5m]) == 0
  FOR 5m
  ANNOTATIONS {
    summary = "No tasks being processed"
  }
```

### Warning (investigate during business hours)

```promql
# High error rate
ALERT HighErrorRate
  IF (
    rate(buraq_tasks_processed_total{status="error"}[5m])
    /
    rate(buraq_tasks_processed_total[5m])
  ) > 0.05
  FOR 10m
  ANNOTATIONS {
    summary = "Task error rate exceeds 5%"
  }

# High p99 latency
ALERT HighLatency
  IF histogram_quantile(0.99, rate(buraq_task_duration_seconds_bucket[5m])) > 10
  FOR 10m
  ANNOTATIONS {
    summary = "Task p99 latency exceeds 10 seconds"
  }

# Channel depth high — workers are backed up
ALERT ChannelBackedUp
  IF buraq_channel_depth > 50
  FOR 5m
  ANNOTATIONS {
    summary = "Worker channel depth exceeds 50"
  }
```

### Informational (dashboard only)

```promql
# Duplicate tasks detected (may indicate idempotency issues)
rate(buraq_tasks_duplicate_total[5m])

# Processing throughput trend
rate(buraq_tasks_processed_total[1h])
```

## The Three Pillars Summary

| Pillar    | Tool      | Buraq Implementation                    |
|-----------|-----------|-----------------------------------------|
| Metrics   | Prometheus| Counters, histograms, gauges on :2112   |
| Logs      | log/slog  | JSON structured logs to stdout          |
| Traces    | (future)  | OpenTelemetry spans not yet implemented |

| Feature           | Endpoint            | Purpose                          |
|-------------------|---------------------|----------------------------------|
| Prometheus metrics| `GET /metrics:2112` | Scraped by Prometheus            |
| SSE events        | `GET /api/stream`   | Real-time task lifecycle events  |
| Health check      | `GET /api/health`   | Liveness/readiness probes        |
| DLQ replay        | `POST /api/retry-dlq` | Operational recovery          |
| Workers info      | `GET /api/workers`  | Worker status (mock data)        |
