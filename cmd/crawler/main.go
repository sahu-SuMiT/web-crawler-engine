package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/fetcher"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/frontier"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/parser"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/politeness"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/storage"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/web"
)

func main() {
	// 1. CLI Flags Configuration
	seedURLFlag := flag.String("seed", "https://books.toscrape.com", "Seed URL to start crawling")
	maxDepthFlag := flag.Int("depth", 3, "Maximum crawl depth limit")
	workerCountFlag := flag.Int("workers", 10, "Number of concurrent fetcher workers")
	portFlag := flag.Int("port", 8080, "Web dashboard HTTP port")
	dataDirFlag := flag.String("data", "./data/pebble", "Pebble DB storage directory")
	warcDirFlag := flag.String("warc", "./data/warc", "WARC archives storage directory")
	flag.Parse()

	log.Println("==========================================================")
	log.Printf("🚀 Starting SOTA Web Crawler Engine")
	log.Printf("📍 Seed URL    : %s", *seedURLFlag)
	log.Printf("📊 Max Depth   : %d", *maxDepthFlag)
	log.Printf("⚡ Workers     : %d", *workerCountFlag)
	log.Printf("🌐 Dashboard   : http://localhost:%d", *portFlag)
	log.Println("==========================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Initialize Pebble DB Persistent Frontier Queue
	pebbleStore, err := frontier.NewPebbleStore(*dataDirFlag)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize Pebble DB: %v", err)
	}

	// 3. Initialize In-Memory Bloom Filter Deduplicator (1M items estimate, 1% false positive rate)
	bloomFilter := parser.NewBloomDeduplicator(1000000, 0.01)

	// 4. Initialize Frontier Manager
	urlFrontier := frontier.NewFrontier(pebbleStore, bloomFilter, 50000)

	// 5. Initialize Politeness Rate Limiter (500ms per host)
	rateLimiter := politeness.NewRateLimiter(500 * time.Millisecond)

	// 6. Initialize Async HTTP Fetcher Engine
	asyncFetcher := fetcher.NewAsyncFetcher(10*time.Second, "")

	// 7. Initialize HTML Parser & Link Extractor
	htmlParser := parser.NewHTMLParser()

	// 8. Initialize WARC Exporter
	warcWriter, err := storage.NewWARCWriter(*warcDirFlag)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize WARC Exporter: %v", err)
	}
	defer warcWriter.Close()

	log.Printf("📦 WARC Archive initialized at: %s", warcWriter.FilePath())

	// 9. Start Embedded Web UI Server
	webServer := web.NewServer(*portFlag)
	if err := webServer.Start(); err != nil {
		log.Printf("Warning: Failed to start web dashboard: %v", err)
	}

	// 10. Push Seed URL into Frontier Queue
	seedParsed, err := url.Parse(*seedURLFlag)
	if err != nil {
		log.Fatalf("Fatal: Invalid seed URL: %v", err)
	}

	seedItem := domain.URLItem{
		URL:        *seedURLFlag,
		Domain:     seedParsed.Hostname(),
		Depth:      1,
		Priority:   1,
		Status:     domain.StatusQueued,
		AddedAt:    time.Now(),
	}

	if pushed, err := urlFrontier.Push(seedItem); err != nil || !pushed {
		log.Fatalf("Fatal: Failed to push seed URL to frontier: %v", err)
	}

	// Metrics counters
	var activeWorkers int32
	var totalErrors uint64

	// 11. Periodic Telemetry Broadcast Goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := domain.CrawlStats{
					ActiveWorkers: int(atomic.LoadInt32(&activeWorkers)),
					TotalCrawled:  urlFrontier.TotalCrawled(),
					TotalQueued:   uint64(urlFrontier.QueueLength()),
					TotalErrors:   atomic.LoadUint64(&totalErrors),
				}
				webServer.BroadcastStats(stats)
			}
		}
	}()

	// 12. Spawn Worker Pool
	var wg sync.WaitGroup
	for i := 1; i <= *workerCountFlag; i++ {
		wg.Add(1)
		workerID := i
		go func(id int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-urlFrontier.Channel():
					if !ok {
						return
					}

					if item.Depth > *maxDepthFlag {
						continue
					}

					atomic.AddInt32(&activeWorkers, 1)

					// A. Enforce Politeness Rate Limiting per Domain
					_ = rateLimiter.Wait(ctx, item.Domain)

					// B. Fetch Page Asynchronously via fasthttp
					res := asyncFetcher.Fetch(ctx, item)

					atomic.AddInt32(&activeWorkers, -1)

					if res.Error != "" || res.StatusCode >= 400 {
						atomic.AddUint64(&totalErrors, 1)
						log.Printf("[Worker %2d] ❌ ERROR [%d] | Depth: %d | %s | %s",
							id, res.StatusCode, item.Depth, item.URL, res.Error)
						webServer.BroadcastLog(item.URL, "ERROR", item.Depth)
						continue
					}

					log.Printf("[Worker %2d] ✅ HTTP 200 | Depth: %d | Links: %d | Latency: %v | %s",
						id, item.Depth, len(res.OutboundURLs), res.FetchTime, item.URL)

					webServer.BroadcastLog(item.URL, fmt.Sprintf("%d", res.StatusCode), item.Depth)

					// C. Write Response Payload to WARC Archive
					_ = warcWriter.WriteRecord(res)

					// D. Extract Links and Canonicalize Outbound URLs
					if item.Depth < *maxDepthFlag && len(res.Body) > 0 {
						links, err := htmlParser.ExtractLinks(res.Body, res.URL)
						if err == nil {
							for _, link := range links {
								parsed, err := url.Parse(link)
								if err != nil {
									continue
								}

								nextItem := domain.URLItem{
									URL:      link,
									Domain:   parsed.Hostname(),
									Depth:    item.Depth + 1,
									Priority: 2,
									AddedAt:  time.Now(),
								}

								_, _ = urlFrontier.Push(nextItem)
							}
						}
					}

					// E. Mark URL Completed in Pebble DB
					_ = urlFrontier.MarkCompleted(item)
				}
			}
		}(workerID)
	}

	// 13. Graceful Shutdown Signal Listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("\n🛑 Shutdown signal received. Closing crawler engine...")
	}

	cancel()
	_ = urlFrontier.Close()
	log.Println("✨ Crawler engine stopped cleanly.")
}
