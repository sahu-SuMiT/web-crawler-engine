package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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
	_ = godotenv.Load()
	defaultPort := 8080
	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			defaultPort = p
		}
	}
	defaultSeed := ""
	if envSeed := os.Getenv("CRAWLER_SEED"); envSeed != "" {
		defaultSeed = envSeed
	}
	defaultDepth := 3
	if envDepth := os.Getenv("CRAWLER_DEPTH"); envDepth != "" {
		if d, err := strconv.Atoi(envDepth); err == nil {
			defaultDepth = d
		}
	}
	defaultWorkers := 10
	if envWorkers := os.Getenv("CRAWLER_WORKERS"); envWorkers != "" {
		if w, err := strconv.Atoi(envWorkers); err == nil {
			defaultWorkers = w
		}
	}
	seedURLFlag := flag.String("seed", defaultSeed, "Seed URL to start crawling")
	maxDepthFlag := flag.Int("depth", defaultDepth, "Maximum crawl depth limit")
	workerCountFlag := flag.Int("workers", defaultWorkers, "Number of concurrent fetcher workers")
	portFlag := flag.Int("port", defaultPort, "Web dashboard HTTP port")
	dataDirFlag := flag.String("data", "./data/pebble", "Pebble DB storage directory")
	warcDirFlag := flag.String("warc", "./data/warc", "WARC archives storage directory")
	flag.Parse()

	log.Println("==========================================================")
	log.Printf("Engine started...")
	if *seedURLFlag != "" {
		log.Printf("Seed URL: %s", *seedURLFlag)
	} else {
		log.Printf("Mode    : IDLE (Awaiting seed URL via Web Dashboard)")
	}
	log.Printf("Max Depth   : %d", *maxDepthFlag)
	log.Printf("Workers     : %d", *workerCountFlag)
	log.Printf("Dashboard   : http://localhost:%d", *portFlag)
	log.Println("==========================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pebbleStore, err := frontier.NewPebbleStore(*dataDirFlag)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize Pebble DB: %v", err)
	}

	bloomFilter := parser.NewBloomDeduplicator(1000000, 0.01)
	urlFrontier := frontier.NewFrontier(pebbleStore, bloomFilter, 50000)
	rateLimiter := politeness.NewRateLimiter(500 * time.Millisecond)
	robotsEngine := politeness.NewRobotsEngine("WebCrawlerEngine")
	asyncFetcher := fetcher.NewAsyncFetcher(10*time.Second, "")
	htmlParser := parser.NewHTMLParser()
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
		log.Println("Cloudflare R2 Object Storage: LOCAL MODE")
	}

	neonStore, err := storage.NewNeonMetadataStore(ctx, "")
	if err != nil {
		log.Printf("Warning: Failed to initialize Neon PostgreSQL: %v", err)
	} else if neonStore.IsEnabled() {
		log.Println("🐘 Neon PostgreSQL Metadata Store: CONNECTED & ACTIVE")
		defer neonStore.Close()
	} else {
		log.Println("Neon PostgreSQL Metadata Store: LOCAL MODE")
	}

	log.Println("WARC Archive initialized (Dynamic multi-domain storage)")

	webServer := web.NewServer(*portFlag)
	webServer.SetFrontier(urlFrontier)
	webServer.SetWARCDir(*warcDirFlag)
	if err := webServer.Start(); err != nil {
		log.Printf("Warning: Failed to start web dashboard: %v", err)
	}

	if *seedURLFlag != "" {
		seedParsed, err := url.Parse(*seedURLFlag)
		if err != nil {
			log.Fatalf("Fatal: Invalid seed URL: %v", err)
		}

		seedItem := domain.URLItem{
			URL:      *seedURLFlag,
			Domain:   seedParsed.Hostname(),
			Depth:    1,
			Priority: 1,
			Status:   domain.StatusQueued,
			AddedAt:  time.Now(),
		}

		if pushed, err := urlFrontier.Push(seedItem); err != nil || !pushed {
			log.Printf("Warning: Seed URL already processed or queued: %v", err)
		}
	}

	var activeWorkers int32
	var totalErrors uint64

	var crawlWasActive bool
	var finishedBroadcasted bool

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
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

				if workers > 0 || queueLen > 0 {
					crawlWasActive = true
					finishedBroadcasted = false
				} else if crawlWasActive && !finishedBroadcasted && urlFrontier.IsIdle() {
					webServer.BroadcastLog("", "SUCCESS", 0)
					finishedBroadcasted = true
					crawlWasActive = false
				}
			}
		}
	}()

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

					urlFrontier.MarkInFlight()
					atomic.AddInt32(&activeWorkers, 1)

					if item.Depth > *maxDepthFlag {
						_ = urlFrontier.MarkCompleted(item)
						atomic.AddInt32(&activeWorkers, -1)
						continue
					}

					allowed, crawlDelay := robotsEngine.IsAllowed(item.URL)
					if !allowed {
						log.Printf("[Worker %2d] -- SKIPPED (Robots.txt Disallowed) | %s", id, item.URL)
						webServer.BroadcastLog(item.URL, "BLOCKED", item.Depth)
						telemetry.RecordRobotsBlock()
						_ = urlFrontier.MarkCompleted(item)
						atomic.AddInt32(&activeWorkers, -1)
						continue
					}

					if crawlDelay > 0 {
						time.Sleep(crawlDelay)
					}

					_ = rateLimiter.Wait(ctx, item.Domain)

					res := asyncFetcher.Fetch(ctx, item)
					telemetry.RecordFetch(res.StatusCode, len(res.Body), res.FetchTime)

					if res.Error != "" || res.StatusCode >= 400 {
						atomic.AddUint64(&totalErrors, 1)
						log.Printf("[Worker %2d] ❌ ERROR [%d] | Depth: %d | %s | %s",
							id, res.StatusCode, item.Depth, item.URL, res.Error)
						webServer.BroadcastLog(item.URL, "ERROR", item.Depth)

						if neonStore != nil && neonStore.IsEnabled() {
							_ = neonStore.SaveRecord(ctx, res)
						}
						_ = urlFrontier.MarkCompleted(item)
						atomic.AddInt32(&activeWorkers, -1)
						continue
					}

					log.Printf("[Worker %2d] HTTP 200 |Depth: %d |Links: %d |Latency: %v | %s",
						id, item.Depth, len(res.OutboundURLs), res.FetchTime, item.URL)

					webServer.BroadcastLog(item.URL, fmt.Sprintf("%d", res.StatusCode), item.Depth)

					if neonStore != nil && neonStore.IsEnabled() {
						_ = neonStore.SaveRecord(ctx, res)
					}

					_ = warcWriter.WriteRecord(res)

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

					_ = urlFrontier.MarkCompleted(item)
					atomic.AddInt32(&activeWorkers, -1)
				}
			}
		}(workerID)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Crawler running. Web dashboard alive at port", *portFlag)
	log.Println("Open /metrics for Prometheus telemetry.")

	<-sigChan
	log.Println("\n🛑 Shutdown signal received...")

	cancel()
	wg.Wait()
	_ = urlFrontier.Close()

	if r2Storage != nil && r2Storage.IsEnabled() {
		log.Println("☁️ Uploading WARC archives to Cloudflare R2...")
		for _, fp := range warcWriter.FilePaths() {
			remoteURI, err := r2Storage.UploadWARC(context.Background(), fp)
			if err != nil {
				log.Printf("!!! Warning: Failed to upload WARC %s to Cloudflare R2: %v", fp, err)
			} else {
				log.Printf("WARC Archive successfully uploaded to Cloudflare R2: %s", remoteURI)
			}
		}
	}

	log.Println("✨ Engine stopped gracefully.")
}
