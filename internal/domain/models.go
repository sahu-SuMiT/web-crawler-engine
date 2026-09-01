package domain

import (
	"time"
)

// URLStatus represents the lifecycle state of a URL.
type URLStatus string

const (
	StatusQueued   URLStatus = "QUEUED"
	StatusFetching URLStatus = "FETCHING"
	StatusCrawled  URLStatus = "CRAWLED"
	StatusFailed   URLStatus = "FAILED"
)

// URLItem represents a target URL in the frontier queue.
type URLItem struct {
	URL        string    `json:"url"`
	Domain     string    `json:"domain"`
	Depth      int       `json:"depth"`
	Priority   int       `json:"priority"`
	RetryCount int       `json:"retry_count"`
	Status     URLStatus `json:"status"`
	AddedAt    time.Time `json:"added_at"`
}

// FetchResult represents the HTTP response payload from the fetcher engine.
type FetchResult struct {
	URL          string            `json:"url"`
	Domain       string            `json:"domain"`
	Depth        int               `json:"depth"`
	StatusCode   int               `json:"status_code"`
	ContentType  string            `json:"content_type"`
	Body         []byte            `json:"-"` // Raw payload (omitted from JSON)
	Headers      map[string]string `json:"headers,omitempty"`
	FetchTime    time.Duration     `json:"fetch_time_ms"`
	OutboundURLs []string          `json:"outbound_urls,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// CrawlStats represents live telemetry metrics for observability.
type CrawlStats struct {
	ActiveWorkers int           `json:"active_workers"`
	TotalCrawled  uint64        `json:"total_crawled"`
	TotalQueued   uint64        `json:"total_queued"`
	TotalErrors   uint64        `json:"total_errors"`
	BytesFetched  uint64        `json:"bytes_fetched"`
	PagesPerSec   float64       `json:"pages_per_sec"`
	Uptime        time.Duration `json:"uptime_sec"`
}
