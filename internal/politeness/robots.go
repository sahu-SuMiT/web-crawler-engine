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

type RobotsEngine struct {
	mu        sync.RWMutex
	cache     map[string]*robotstxt.Group
	userAgent string
	client    *fasthttp.Client
}

func NewRobotsEngine(userAgent string) *RobotsEngine {
	if userAgent == "" {
		userAgent = "WebCrawlerEngine"
	}

	return &RobotsEngine{
		cache:     make(map[string]*robotstxt.Group),
		userAgent: userAgent,
		client: &fasthttp.Client{
			Name:                userAgent,
			ReadTimeout:         5 * time.Second,
			WriteTimeout:        5 * time.Second,
			MaxResponseBodySize: 1 * 1024 * 1024,
		},
	}
}

func (r *RobotsEngine) IsAllowed(rawURL string) (bool, time.Duration) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return true, 0
	}

	domain := strings.ToLower(parsed.Hostname())
	if domain == "" {
		return true, 0
	}

	group := r.getOrFetchRobots(parsed.Scheme, domain)
	if group == nil {
		return true, 0
	}

	allowed := group.Test(parsed.Path)
	crawlDelay := group.CrawlDelay
	return allowed, crawlDelay
}

func (r *RobotsEngine) getOrFetchRobots(scheme, domain string) *robotstxt.Group {
	r.mu.RLock()
	group, exists := r.cache[domain]
	r.mu.RUnlock()

	if exists {
		return group
	}

	r.mu.Lock()
	defer r.mu.Unlock()

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
		r.cache[domain] = nil
		return nil
	}

	data, err := robotstxt.FromBytes(resp.Body())
	if err != nil {
		r.cache[domain] = nil
		return nil
	}

	group = data.FindGroup(r.userAgent)
	if group == nil {
		group = data.FindGroup("*")
	}

	r.cache[domain] = group
	return group
}
