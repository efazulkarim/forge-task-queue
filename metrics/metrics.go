package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TasksProcessedTotal tracks the total number of tasks successfully processed.
	TasksProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_processed_total",
			Help: "Total number of tasks processed by Buraq consumers",
		},
		[]string{"task_type", "status"}, // Status can be "success" or "error"
	)

	// TasksFailedTotal tracks the number of times a task processing attempt failed.
	TasksFailedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_failed_total",
			Help: "Total number of task processing failures (retries)",
		},
		[]string{"task_type"},
	)

	// TasksDLQTotal tracks the total number of tasks moved to the Dead-Letter Queue.
	TasksDLQTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buraq_tasks_dlq_total",
			Help: "Total number of tasks moved to the DLQ stream after exhausted retries",
		},
		[]string{"task_type"},
	)

	// TaskDurationSeconds tracks the duration of task processing.
	TaskDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "buraq_task_duration_seconds",
			Help:    "Histogram of task processing duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"task_type"},
	)

	// WorkerPanicsTotal tracks the number of worker goroutine panics recovered.
	WorkerPanicsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "buraq_worker_panics_total",
			Help: "Total number of worker goroutine panics recovered",
		},
	)

	// ChannelDepth tracks the current depth of the task dispatch channel.
	ChannelDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "buraq_channel_depth",
			Help: "Current number of tasks waiting in the dispatch channel",
		},
	)

	// TasksDuplicateTotal tracks the number of duplicate tasks skipped via idempotency.
	TasksDuplicateTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "buraq_tasks_duplicate_total",
			Help: "Total number of duplicate tasks skipped due to idempotency check",
		},
	)
)
