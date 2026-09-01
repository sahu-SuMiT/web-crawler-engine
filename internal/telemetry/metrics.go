package telemetry

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// PagesCrawledTotal tracks total pages crawled grouped by HTTP status code
	PagesCrawledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "crawler_pages_crawled_total",
			Help: "Total number of web pages crawled by HTTP status code",
		},
		[]string{"status"},
	)

	// ActiveWorkers tracks the current number of active fetcher goroutines
	ActiveWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "crawler_active_workers",
			Help: "Current count of active fetcher worker goroutines",
		},
	)

	// QueueLength tracks the number of URLs currently waiting in the frontier queue
	QueueLength = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "crawler_queue_length",
			Help: "Current count of URLs queued in the frontier backlog",
		},
	)

	// TotalBytesFetched tracks cumulative bandwidth consumed by the fetcher
	TotalBytesFetched = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "crawler_bytes_fetched_total",
			Help: "Total volume of payload data downloaded in bytes",
		},
	)

	// RobotsBlockedTotal tracks how many URLs were blocked by Robots.txt rules
	RobotsBlockedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "crawler_robots_blocked_total",
			Help: "Total URLs blocked by Robots.txt compliance rules",
		},
	)

	// FetchDurationSeconds tracks HTTP response latency distributions
	FetchDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "crawler_fetch_duration_seconds",
			Help:    "Latency distribution of HTTP fetch operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)

// RecordFetch metrics updates Prometheus counters for a completed fetch operation.
func RecordFetch(statusCode int, bodySize int, duration time.Duration) {
	statusStr := strconv.Itoa(statusCode)
	if statusCode == 0 {
		statusStr = "ERROR"
	}

	PagesCrawledTotal.WithLabelValues(statusStr).Inc()
	TotalBytesFetched.Add(float64(bodySize))
	FetchDurationSeconds.Observe(duration.Seconds())
}

// RecordRobotsBlock increments the robots.txt disallow counter.
func RecordRobotsBlock() {
	RobotsBlockedTotal.Inc()
}

// UpdateQueueMetrics updates the active worker and queue backlog gauges.
func UpdateQueueMetrics(workers int, queueLen int) {
	ActiveWorkers.Set(float64(workers))
	QueueLength.Set(float64(queueLen))
}
