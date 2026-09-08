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
	inFlight     int64 // items dequeued by workers but not yet completed
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

// ResetDeduplicator resets the in-memory Bloom filter for a fresh crawl session.
func (f *Frontier) ResetDeduplicator() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bloom = parser.NewBloomDeduplicator(1000000, 0.01)
}

// IsIdle returns true only when both the channel queue and all in-flight worker
// items are fully drained. Use this to determine if a crawl session has finished.
func (f *Frontier) IsIdle() bool {
	return len(f.queue) == 0 && atomic.LoadInt64(&f.inFlight) == 0
}

// MarkInFlight signals that a worker has dequeued an item and is processing it.
// Call this immediately after receiving from Channel().
func (f *Frontier) MarkInFlight() {
	atomic.AddInt64(&f.inFlight, 1)
}

// Push adds a URLItem to the frontier if it hasn't been seen before.
// Returns true if the URL was accepted and queued, false if it was a duplicate.
func (f *Frontier) Push(item domain.URLItem) (bool, error) {
	if item.URL == "" {
		return false, nil
	}

	// 1. Fast in-memory Bloom filter deduplication check
	f.mu.Lock()
	added := f.bloom.Add(item.URL)
	f.mu.Unlock()

	if !added {
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
// Must be called after MarkInFlight() to correctly track idle state.
func (f *Frontier) MarkCompleted(item domain.URLItem) error {
	atomic.AddInt64(&f.inFlight, -1)
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

// QueueLength returns the current number of pending items in the Go channel buffer.
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
