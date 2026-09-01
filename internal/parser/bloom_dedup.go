package parser

import (
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// BloomDeduplicator provides thread-safe in-memory probabilistic URL deduplication.
type BloomDeduplicator struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
	count  uint64
}

// NewBloomDeduplicator creates a Bloom filter configured for estimated n items and false positive rate p.
// Example: n = 1,000,000 URLs, p = 0.01 (1% false positive rate).
func NewBloomDeduplicator(expectedItems uint, falsePositiveRate float64) *BloomDeduplicator {
	if expectedItems == 0 {
		expectedItems = 1000000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	return &BloomDeduplicator{
		filter: bloom.NewWithEstimates(expectedItems, falsePositiveRate),
	}
}

// MightContain returns true if the URL might have been seen before.
// If it returns false, the URL is 100% GUARANTEED to be new.
func (b *BloomDeduplicator) MightContain(url string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.filter.Test([]byte(url))
}

// Add adds a URL to the Bloom filter. Returns true if the URL was newly added,
// or false if it was already marked as seen.
func (b *BloomDeduplicator) Add(url string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	urlBytes := []byte(url)
	if b.filter.Test(urlBytes) {
		return false // Likely already seen
	}

	b.filter.Add(urlBytes)
	b.count++
	return true // Newly added
}

// Count returns the number of unique URLs added to the Bloom filter.
func (b *BloomDeduplicator) Count() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.count
}
