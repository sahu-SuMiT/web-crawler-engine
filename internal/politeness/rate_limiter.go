package politeness

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter struct {
	mu           sync.RWMutex
	limiters     map[string]*rate.Limiter
	defaultDelay time.Duration
}

func NewRateLimiter(delayPerHost time.Duration) *RateLimiter {
	if delayPerHost <= 0 {
		delayPerHost = 500 * time.Millisecond
	}

	return &RateLimiter{
		limiters:     make(map[string]*rate.Limiter),
		defaultDelay: delayPerHost,
	}
}

func (r *RateLimiter) getLimiter(domain string) *rate.Limiter {
	r.mu.RLock()
	limiter, exists := r.limiters[domain]
	r.mu.RUnlock()

	if exists {
		return limiter
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if limiter, exists = r.limiters[domain]; exists {
		return limiter
	}
	limit := rate.Every(r.defaultDelay)
	limiter = rate.NewLimiter(limit, 1)
	r.limiters[domain] = limiter

	return limiter
}

func (r *RateLimiter) Wait(ctx context.Context, domain string) error {
	limiter := r.getLimiter(domain)
	return limiter.Wait(ctx)
}
