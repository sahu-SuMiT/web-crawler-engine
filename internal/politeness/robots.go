package politeness

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
	"github.com/valyala/fasthttp"
)

// RobotsEngine fetches, parses, and caches Robots.txt compliance rules per domain.
type RobotsEngine struct {
	mu        sync.RWMutex
	cache     map[string]*robotstxt.Group
	userAgent string
	client    *fasthttp.Client
}

// NewRobotsEngine initializes a Robots.txt compliance manager with in-memory domain caching.
func NewRobotsEngine(userAgent string) *RobotsEngine {
	if userAgent == "" {
		userAgent = "SOTACrawler"
	}

	return &RobotsEngine{
		cache:     make(map[string]*robotstxt.Group),
		userAgent: userAgent,
		client: &fasthttp.Client{
			Name:                userAgent,
			ReadTimeout:         5 * time.Second,
			WriteTimeout:        5 * time.Second,
			MaxResponseBodySize: 1 * 1024 * 1024, // 1MB max robots.txt
		},
	}
}

// IsAllowed checks if a target URL is permitted to be crawled according to its domain's robots.txt rules.
// Returns (isAllowed, crawlDelay). If robots.txt cannot be fetched or doesn't exist (HTTP 404), crawling is allowed by default.
func (r *RobotsEngine) IsAllowed(rawURL string) (bool, time.Duration) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return true, 0 // Allow on parse error
	}

	domain := strings.ToLower(parsed.Hostname())
	if domain == "" {
		return true, 0
	}

	group := r.getOrFetchRobots(parsed.Scheme, domain)
	if group == nil {
		return true, 0 // Default allow if no robots.txt rules apply
	}

	allowed := group.Test(parsed.Path)
	crawlDelay := group.CrawlDelay
	return allowed, crawlDelay
}

// getOrFetchRobots retrieves the cached robots.txt Group or fetches it from the host.
func (r *RobotsEngine) getOrFetchRobots(scheme, domain string) *robotstxt.Group {
	r.mu.RLock()
	group, exists := r.cache[domain]
	r.mu.RUnlock()

	if exists {
		return group
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double check after acquiring write lock
	if group, exists = r.cache[domain]; exists {
		return group
	}

	if scheme == "" {
		scheme = "https"
	}
	robotsURL := fmt.Sprintf("%s://%s/robots.txt", scheme, domain)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(robotsURL)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.SetUserAgent(r.userAgent)

	err := r.client.DoTimeout(req, resp, 5*time.Second)
	if err != nil || resp.StatusCode() != fasthttp.StatusOK {
		// If 404 Not Found or fetch error, store empty group (allowed by default)
		r.cache[domain] = nil
		return nil
	}

	data, err := robotstxt.FromBytes(resp.Body())
	if err != nil {
		r.cache[domain] = nil
		return nil
	}

	// Find matching User-Agent group (first search for custom agent, fallback to '*')
	group = data.FindGroup(r.userAgent)
	if group == nil {
		group = data.FindGroup("*")
	}

	r.cache[domain] = group
	return group
}
