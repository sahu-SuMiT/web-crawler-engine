package frontier

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/parser"
)

type Frontier struct {
	mu           sync.Mutex
	store        *PebbleStore
	bloom        *parser.BloomDeduplicator
	queue        chan domain.URLItem
	totalPushed  uint64
	totalCrawled uint64
	inFlight     int64
}

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

func (f *Frontier) ResetDeduplicator() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bloom = parser.NewBloomDeduplicator(1000000, 0.01)
}

func (f *Frontier) IsIdle() bool {
	return len(f.queue) == 0 && atomic.LoadInt64(&f.inFlight) == 0
}

func (f *Frontier) MarkInFlight() {
	atomic.AddInt64(&f.inFlight, 1)
}

func (f *Frontier) Push(item domain.URLItem) (bool, error) {
	if item.URL == "" {
		return false, nil
	}

	f.mu.Lock()
	added := f.bloom.Add(item.URL)
	f.mu.Unlock()

	if !added {
		return false, nil
	}

	item.Status = domain.StatusQueued
	if err := f.store.SaveURL(item); err != nil {
		return false, fmt.Errorf("failed to persist URL to frontier store: %w", err)
	}

	select {
	case f.queue <- item:
		atomic.AddUint64(&f.totalPushed, 1)
		return true, nil
	default:
		atomic.AddUint64(&f.totalPushed, 1)
		return true, nil
	}
}
func (f *Frontier) Channel() <-chan domain.URLItem {
	return f.queue
}

func (f *Frontier) MarkCompleted(item domain.URLItem) error {
	atomic.AddInt64(&f.inFlight, -1)
	item.Status = domain.StatusCrawled
	atomic.AddUint64(&f.totalCrawled, 1)
	return f.store.SaveURL(item)
}

func (f *Frontier) TotalPushed() uint64 {
	return atomic.LoadUint64(&f.totalPushed)
}

func (f *Frontier) TotalCrawled() uint64 {
	return atomic.LoadUint64(&f.totalCrawled)
}

func (f *Frontier) QueueLength() int {
	return len(f.queue)
}

func (f *Frontier) GetStore() *PebbleStore {
	return f.store
}
func (f *Frontier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	close(f.queue)
	return f.store.Close()
}
