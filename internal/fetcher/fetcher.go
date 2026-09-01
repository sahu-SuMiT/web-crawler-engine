package fetcher

import (
	"context"
	"fmt"
	"time"

	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/valyala/fasthttp"
)

// AsyncFetcher handles high-throughput non-blocking HTTP requests using fasthttp.
type AsyncFetcher struct {
	client    *fasthttp.Client
	timeout   time.Duration
	userAgent string
}

// NewAsyncFetcher creates a fasthttp client configured with connection pooling and timeouts.
func NewAsyncFetcher(timeout time.Duration, userAgent string) *AsyncFetcher {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if userAgent == "" {
		userAgent = "SOTACrawler/1.0 (+https://github.com/sahu-SuMiT/web-crawler-engine)"
	}

	return &AsyncFetcher{
		client: &fasthttp.Client{
			Name:                     userAgent,
			MaxConnsPerHost:          500,
			ReadTimeout:              timeout,
			WriteTimeout:             timeout,
			MaxResponseBodySize:      10 * 1024 * 1024, // 10MB max page size
			NoDefaultUserAgentHeader: false,
		},
		timeout:   timeout,
		userAgent: userAgent,
	}
}

// Fetch executes a non-blocking HTTP GET request for the given target URL.
func (f *AsyncFetcher) Fetch(ctx context.Context, item domain.URLItem) domain.FetchResult {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(item.URL)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.SetUserAgent(f.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")

	startTime := time.Now()
	err := f.client.DoTimeout(req, resp, f.timeout)
	latency := time.Since(startTime)

	result := domain.FetchResult{
		URL:        item.URL,
		Domain:     item.Domain,
		Depth:      item.Depth,
		FetchTime:  latency,
		StatusCode: resp.StatusCode(),
	}

	if err != nil {
		result.Error = fmt.Sprintf("fetch error: %v", err)
		return result
	}

	// Capture Content-Type
	result.ContentType = string(resp.Header.ContentType())

	// Capture Headers
	headers := make(map[string]string)
	resp.Header.VisitAll(func(key, value []byte) {
		headers[string(key)] = string(value)
	})
	result.Headers = headers

	// Copy body bytes (fasthttp reuses response buffers, so we copy)
	bodyBytes := resp.Body()
	result.Body = make([]byte, len(bodyBytes))
	copy(result.Body, bodyBytes)

	return result
}
