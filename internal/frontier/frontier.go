package frontier

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/parser"
)

// Frontier coordinates URL deduplication, persistence, and worker dispatch via Go channels.
type Frontier struct {
	mu           sync.Mutex
	store        *PebbleStore
	bloom        *parser.BloomDeduplicator
	queue        chan domain.URLItem
	totalPushed  uint64
	totalCrawled uint64
}

// NewFrontier initializes a URL Frontier queue with a Pebble storage backend and Bloom Filter.
func NewFrontier(store *PebbleStore, bloom *parser.BloomDeduplicator, bufferSize int) *Frontier {
	if bufferSize <= 0 {
		bufferSize = 10000
	}

	return &Frontier{
		store: store,
		bloom: bloom,
		queue: make(chan domain.URLItem, bufferSize),
	}
}

// Push adds a URLItem to the frontier if it hasn't been seen before.
// Returns true if the URL was accepted and queued, false if it was a duplicate.
func (f *Frontier) Push(item domain.URLItem) (bool, error) {
	if item.URL == "" {
		return false, nil
	}

	// 1. Fast in-memory Bloom filter deduplication check
	if !f.bloom.Add(item.URL) {
		return false, nil // Already seen
	}

	// 2. Persist URL item to Pebble DB
	item.Status = domain.StatusQueued
	if err := f.store.SaveURL(item); err != nil {
		return false, fmt.Errorf("failed to persist URL to frontier store: %w", err)
	}

	// 3. Push to active worker dispatch channel (non-blocking if buffer has space)
	select {
	case f.queue <- item:
		atomic.AddUint64(&f.totalPushed, 1)
		return true, nil
	default:
		// Channel buffer full - still stored in Pebble for future polling
		atomic.AddUint64(&f.totalPushed, 1)
		return true, nil
	}
}

// Channel returns the read-only Go channel used by fetcher workers to receive target URLs.
func (f *Frontier) Channel() <-chan domain.URLItem {
	return f.queue
}

// MarkCompleted marks a URL as crawled in Pebble DB and increments progress counters.
func (f *Frontier) MarkCompleted(item domain.URLItem) error {
	item.Status = domain.StatusCrawled
	atomic.AddUint64(&f.totalCrawled, 1)
	return f.store.SaveURL(item)
}

// TotalPushed returns the count of unique URLs added to the queue.
func (f *Frontier) TotalPushed() uint64 {
	return atomic.LoadUint64(&f.totalPushed)
}

// TotalCrawled returns the count of URLs successfully processed.
func (f *Frontier) TotalCrawled() uint64 {
	return atomic.LoadUint64(&f.totalCrawled)
}

// QueueLength returns the current number of in-flight items in the Go channel.
func (f *Frontier) QueueLength() int {
	return len(f.queue)
}

// Close closes the underlying channel and Pebble store.
func (f *Frontier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	close(f.queue)
	return f.store.Close()
}
