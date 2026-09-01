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

	"github.com/joho/godotenv"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/fetcher"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/frontier"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/parser"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/politeness"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/storage"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/telemetry"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/web"
)

func main() {
	// 0. Load .env file if present
	_ = godotenv.Load()

	// 1. CLI Flags Configuration
	seedURLFlag := flag.String("seed", "https://books.toscrape.com", "Seed URL to start crawling")
	maxDepthFlag := flag.Int("depth", 3, "Maximum crawl depth limit")
	workerCountFlag := flag.Int("workers", 10, "Number of concurrent fetcher workers")
	portFlag := flag.Int("port", 8080, "Web dashboard HTTP port")
	dataDirFlag := flag.String("data", "./data/pebble", "Pebble DB storage directory")
	warcDirFlag := flag.String("warc", "./data/warc", "WARC archives storage directory")
	flag.Parse()

	log.Println("==========================================================")
	log.Printf("🚀 Starting Web Crawler Engine")
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

	// 5. Initialize Politeness Rate Limiter & Robots.txt Compliance Engine
	rateLimiter := politeness.NewRateLimiter(500 * time.Millisecond)
	robotsEngine := politeness.NewRobotsEngine("SOTACrawler")

	// 6. Initialize Async HTTP Fetcher Engine
	asyncFetcher := fetcher.NewAsyncFetcher(10*time.Second, "")

	// 7. Initialize HTML Parser & Link Extractor
	htmlParser := parser.NewHTMLParser()

	// 8. Initialize Local WARC Exporter & Cloudflare R2 Cloud Storage
	warcWriter, err := storage.NewWARCWriter(*warcDirFlag)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize WARC Exporter: %v", err)
	}
	defer warcWriter.Close()

	r2Storage, err := storage.NewR2Storage(ctx, "", "", "", "")
	if err != nil {
		log.Printf("Warning: Failed to initialize Cloudflare R2: %v", err)
	}
	if r2Storage != nil && r2Storage.IsEnabled() {
		log.Println("☁️ Cloudflare R2 Object Storage: CONNECTED & ACTIVE")
	} else {
		log.Println("ℹ️ Cloudflare R2 Object Storage: LOCAL MODE (Set R2_ACCOUNT_ID to enable cloud uploads)")
	}

	// 9. Initialize Neon PostgreSQL Metadata Store
	neonStore, err := storage.NewNeonMetadataStore(ctx, "")
	if err != nil {
		log.Printf("Warning: Failed to initialize Neon PostgreSQL: %v", err)
	} else if neonStore.IsEnabled() {
		log.Println("🐘 Neon PostgreSQL Metadata Store: CONNECTED & ACTIVE")
		defer neonStore.Close()
	} else {
		log.Println("ℹ️ Neon PostgreSQL Metadata Store: LOCAL MODE (Set NEON_DATABASE_URL to enable cloud metadata sync)")
	}

	log.Printf("📦 WARC Archive initialized at: %s", warcWriter.FilePath())

	// 10. Start Embedded Web UI Server
	webServer := web.NewServer(*portFlag)
	if err := webServer.Start(); err != nil {
		log.Printf("Warning: Failed to start web dashboard: %v", err)
	}

	// 11. Push Seed URL into Frontier Queue
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

	// 12. Periodic Telemetry Broadcast Goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				workers := int(atomic.LoadInt32(&activeWorkers))
				queueLen := urlFrontier.QueueLength()
				telemetry.UpdateQueueMetrics(workers, queueLen)

				stats := domain.CrawlStats{
					ActiveWorkers: workers,
					TotalCrawled:  urlFrontier.TotalCrawled(),
					TotalQueued:   uint64(queueLen),
					TotalErrors:   atomic.LoadUint64(&totalErrors),
				}
				webServer.BroadcastStats(stats)
			}
		}
	}()

	// 13. Spawn Worker Pool
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

					// A. Check Robots.txt Compliance Policy
					allowed, crawlDelay := robotsEngine.IsAllowed(item.URL)
					if !allowed {
						log.Printf("[Worker %2d] 🛑 SKIPPED (Robots.txt Disallowed) | %s", id, item.URL)
						webServer.BroadcastLog(item.URL, "BLOCKED", item.Depth)
						telemetry.RecordRobotsBlock()
						_ = urlFrontier.MarkCompleted(item)
						continue
					}

					if crawlDelay > 0 {
						time.Sleep(crawlDelay)
					}

					atomic.AddInt32(&activeWorkers, 1)

					// B. Enforce Politeness Rate Limiting per Domain
					_ = rateLimiter.Wait(ctx, item.Domain)

					// C. Fetch Page Asynchronously via fasthttp
					res := asyncFetcher.Fetch(ctx, item)

					atomic.AddInt32(&activeWorkers, -1)
					telemetry.RecordFetch(res.StatusCode, len(res.Body), res.FetchTime)

					if res.Error != "" || res.StatusCode >= 400 {
						atomic.AddUint64(&totalErrors, 1)
						log.Printf("[Worker %2d] ❌ ERROR [%d] | Depth: %d | %s | %s",
							id, res.StatusCode, item.Depth, item.URL, res.Error)
						webServer.BroadcastLog(item.URL, "ERROR", item.Depth)

						if neonStore != nil && neonStore.IsEnabled() {
							_ = neonStore.SaveRecord(ctx, res)
						}
						continue
					}

					log.Printf("[Worker %2d] ✅ HTTP 200 | Depth: %d | Links: %d | Latency: %v | %s",
						id, item.Depth, len(res.OutboundURLs), res.FetchTime, item.URL)

					webServer.BroadcastLog(item.URL, fmt.Sprintf("%d", res.StatusCode), item.Depth)

					// D. Sync Metadata to Neon PostgreSQL Cloud DB
					if neonStore != nil && neonStore.IsEnabled() {
						_ = neonStore.SaveRecord(ctx, res)
					}

					// E. Write Response Payload to WARC Archive
					_ = warcWriter.WriteRecord(res)

					// F. Extract Links and Canonicalize Outbound URLs
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

					// G. Mark URL Completed in Pebble DB
					_ = urlFrontier.MarkCompleted(item)
				}
			}
		}(workerID)
	}

	// 14. Graceful Shutdown Signal Listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("\n🛑 Shutdown signal received. Closing crawler engine...")
	}

	cancel()
	_ = urlFrontier.Close()

	// Upload WARC archive to Cloudflare R2 if enabled
	if r2Storage != nil && r2Storage.IsEnabled() {
		log.Println("☁️ Uploading WARC archive to Cloudflare R2...")
		remoteURI, err := r2Storage.UploadWARC(context.Background(), warcWriter.FilePath())
		if err != nil {
			log.Printf("Warning: Failed to upload WARC to Cloudflare R2: %v", err)
		} else {
			log.Printf("✅ WARC Archive successfully uploaded to Cloudflare R2: %s", remoteURI)
		}
	}

	log.Println("✨ Crawler engine stopped cleanly.")
}
