package politeness

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter manages per-host politeness rate-limiting to prevent IP bans and server overload.
type RateLimiter struct {
	mu           sync.RWMutex
	limiters     map[string]*rate.Limiter
	defaultDelay time.Duration
}

// NewRateLimiter creates a RateLimiter with a default per-host request interval.
// Example: delayPerHost = 500ms means a max of 2 requests/sec per domain.
func NewRateLimiter(delayPerHost time.Duration) *RateLimiter {
	if delayPerHost <= 0 {
		delayPerHost = 500 * time.Millisecond
	}

	return &RateLimiter{
		limiters:     make(map[string]*rate.Limiter),
		defaultDelay: delayPerHost,
	}
}

// getLimiter returns or initializes the rate.Limiter for a given domain.
func (r *RateLimiter) getLimiter(domain string) *rate.Limiter {
	r.mu.RLock()
	limiter, exists := r.limiters[domain]
	r.mu.RUnlock()

	if exists {
		return limiter
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double check after acquiring write lock
	if limiter, exists = r.limiters[domain]; exists {
		return limiter
	}

	// 1 token per delayPerHost, burst capacity of 1 token
	limit := rate.Every(r.defaultDelay)
	limiter = rate.NewLimiter(limit, 1)
	r.limiters[domain] = limiter

	return limiter
}

// Wait blocks until the politeness policy allows a request to the given domain.
func (r *RateLimiter) Wait(ctx context.Context, domain string) error {
	limiter := r.getLimiter(domain)
	return limiter.Wait(ctx)
}
