package web

import (
	"bufio"
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/frontier"
)

//go:embed static/*
var staticFS embed.FS

// Server hosts the embedded Web UI control center and SSE event stream.
type Server struct {
	port      int
	clients   map[chan string]bool
	mu        sync.Mutex
	broadcast chan string
	frontier  *frontier.Frontier
	warcDir   string
}

// NewServer creates a new web dashboard server instance.
func NewServer(port int) *Server {
	if port <= 0 {
		port = 8080
	}

	s := &Server{
		port:      port,
		clients:   make(map[chan string]bool),
		broadcast: make(chan string, 1000),
	}

	go s.listenBroadcast()
	return s
}

// SetFrontier attaches the Frontier instance for interactive URL submission.
func (s *Server) SetFrontier(f *frontier.Frontier) {
	s.frontier = f
}

func (s *Server) SetWARCDir(dir string) {
	s.warcDir = dir
}

// listenBroadcast distributes messages to all active SSE subscribers.
func (s *Server) listenBroadcast() {
	for msg := range s.broadcast {
		s.mu.Lock()
		for clientChan := range s.clients {
			select {
			case clientChan <- msg:
			default:
				// Skip if client buffer is full
			}
		}
		s.mu.Unlock()
	}
}

// BroadcastStats broadcasts live CrawlStats to all connected Web UI clients.
func (s *Server) BroadcastStats(stats domain.CrawlStats) {
	payload := map[string]interface{}{
		"type":           "stats",
		"active_workers": stats.ActiveWorkers,
		"total_crawled":  stats.TotalCrawled,
		"total_queued":   stats.TotalQueued,
		"total_errors":   stats.TotalErrors,
	}

	bytes, err := json.Marshal(payload)
	if err == nil {
		s.broadcast <- string(bytes)
	}
}

// BroadcastLog broadcasts an individual URL fetch event log to the Web UI.
func (s *Server) BroadcastLog(urlStr string, status string, depth int) {
	payload := map[string]interface{}{
		"type":   "log",
		"time":   time.Now().Format("15:04:05"),
		"url":    urlStr,
		"status": status,
		"depth":  depth,
	}

	bytes, err := json.Marshal(payload)
	if err == nil {
		s.broadcast <- string(bytes)
	}
}

type crawlReqPayload struct {
	URL     string `json:"url"`
	Depth   int    `json:"depth"`
	Workers int    `json:"workers"`
}

// Start launches the HTTP web server in a background goroutine.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		statusFilter := r.URL.Query().Get("status")

		type WARCFile struct {
			Name    string `json:"name"`
			SizeMB  string `json:"size_mb"`
			ModTime string `json:"mod_time"`
		}
		type WARCDomain struct {
			Domain string     `json:"domain"`
			Files  []WARCFile `json:"files"`
		}

		var warcGroups []WARCDomain
		if s.warcDir != "" {
			domainEntries, _ := os.ReadDir(s.warcDir)
			for _, de := range domainEntries {
				if !de.IsDir() {
					continue
				}
				domainPath := filepath.Join(s.warcDir, de.Name())
				fileEntries, _ := os.ReadDir(domainPath)
				var files []WARCFile
				for _, fe := range fileEntries {
					if fe.IsDir() {
						continue
					}
					info, err := fe.Info()
					if err != nil {
						continue
					}
					files = append(files, WARCFile{
						Name:    fe.Name(),
						SizeMB:  fmt.Sprintf("%.2f MB", float64(info.Size())/1024/1024),
						ModTime: info.ModTime().Format("2006-01-02 15:04:05"),
					})
				}
				// Always add the directory to show empty folders (like 'unsorted')
				if files == nil {
					files = []WARCFile{}
				}
				warcGroups = append(warcGroups, WARCDomain{
					Domain: de.Name(),
					Files:  files,
				})
			}
		}

		// Pebble: return only domain summaries (count per domain), no individual URLs.
		// URLs are loaded on-demand via /api/pebble?domain=X
		type PebbleDomainSummary struct {
			Domain string `json:"domain"`
			Count  int    `json:"count"`
		}
		var pebbleSummary []PebbleDomainSummary
		if s.frontier != nil {
			counts, err := s.frontier.GetStore().ScanDomains(statusFilter)
			if err == nil {
				domains := make([]string, 0, len(counts))
				for d := range counts {
					domains = append(domains, d)
				}
				sort.Strings(domains)
				for _, d := range domains {
					pebbleSummary = append(pebbleSummary, PebbleDomainSummary{
						Domain: d,
						Count:  counts[d],
					})
				}
			}
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"pebble_domains": pebbleSummary,
			"warc_domains":   warcGroups,
		})
	})

	// On-demand URL loader: called when user expands a domain folder in the sidebar.
	mux.HandleFunc("/api/pebble", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		domainName := r.URL.Query().Get("domain")
		if domainName == "" {
			http.Error(w, "domain param required", http.StatusBadRequest)
			return
		}
		statusFilter := r.URL.Query().Get("status")
		limit := 200
		if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 2000 {
			limit = n
		}

		var records []domain.URLItem
		if s.frontier != nil {
			records, _ = s.frontier.GetStore().ScanByDomain(domainName, statusFilter, limit)
		}
		if records == nil {
			records = []domain.URLItem{}
		}
		json.NewEncoder(w).Encode(records)
	})


	mux.HandleFunc("/api/warc-preview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		domainName := filepath.Base(r.URL.Query().Get("domain"))
		fileName := filepath.Base(r.URL.Query().Get("file"))
		if fileName == "" || fileName == "." {
			http.Error(w, "file param required", http.StatusBadRequest)
			return
		}

		nLines := 100
		if n, err := strconv.Atoi(r.URL.Query().Get("lines")); err == nil && n > 0 && n <= 2000 {
			nLines = n
		}

		var filePath string
		if domainName != "" && domainName != "." {
			filePath = filepath.Join(s.warcDir, domainName, fileName)
		} else {
			filePath = filepath.Join(s.warcDir, fileName)
		}
		f, err := os.Open(filePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot open file: %v", err), http.StatusNotFound)
			return
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			http.Error(w, fmt.Sprintf("not a valid gzip file: %v", err), http.StatusBadRequest)
			return
		}
		defer gr.Close()

		scanner := bufio.NewScanner(gr)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		count := 0
		for scanner.Scan() && count < nLines {
			fmt.Fprintln(w, scanner.Text())
			count++
		}
	})

	mux.HandleFunc("/api/crawl", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload crawlReqPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		rawURL := strings.TrimSpace(payload.URL)
		if rawURL == "" {
			http.Error(w, "URL is required", http.StatusBadRequest)
			return
		}

		if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
			rawURL = "https://" + rawURL
		}

		parsed, err := url.Parse(rawURL)
		if err != nil || parsed.Hostname() == "" {
			http.Error(w, "Invalid URL format", http.StatusBadRequest)
			return
		}

		if s.frontier == nil {
			http.Error(w, "Frontier engine not connected", http.StatusInternalServerError)
			return
		}

		depth := payload.Depth
		if depth <= 0 {
			depth = 3
		}

		item := domain.URLItem{
			URL:      rawURL,
			Domain:   parsed.Hostname(),
			Depth:    1,
			Priority: 1,
			Status:   domain.StatusQueued,
			AddedAt:  time.Now(),
		}

		// Reset deduplicator for a new crawl session if frontier is fully idle
		if s.frontier.IsIdle() {
			s.frontier.ResetDeduplicator()
		}

		pushed, err := s.frontier.Push(item)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to queue URL: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if !pushed {
			s.BroadcastLog(rawURL, "ALREADY IN FRONTIER", 1)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "URL already crawled or queued in frontier",
			})
			return
		}

		s.BroadcastLog(rawURL, "QUEUED", 1)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("Seed URL queued: %s (Depth: %d)", rawURL, depth),
		})
	})

	// Erase Disk — deletes all Pebble DB data and WARC files.
	// Only allowed when the engine is idle (no active workers).
	mux.HandleFunc("/api/erase-disk", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		var warcDeleted int

		// Erase Pebble DB
		if s.frontier != nil {
			if err := s.frontier.GetStore().EraseAll(); err != nil {
				http.Error(w, fmt.Sprintf("pebble erase failed: %v", err), http.StatusInternalServerError)
				return
			}
		}

		// Erase WARC files — remove every subdirectory under warcDir
		if s.warcDir != "" {
			entries, _ := os.ReadDir(s.warcDir)
			for _, e := range entries {
				// Skip the fallback storage directory
				if e.IsDir() && e.Name() == "unsorted" {
					continue
				}
				path := filepath.Join(s.warcDir, e.Name())
				if e.IsDir() {
					// Count .warc.gz files before deleting
					files, _ := os.ReadDir(path)
					warcDeleted += len(files)
					os.RemoveAll(path)
				} else {
					warcDeleted++
					os.Remove(path)
				}
			}
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":       true,
			"warc_deleted":  warcDeleted,
			"message":       fmt.Sprintf("Disk erased. %d WARC file(s) deleted. Pebble DB reset.", warcDeleted),
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		indexBytes, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "Index file not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(indexBytes)
	})

	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		messageChan := make(chan string, 100)

		s.mu.Lock()
		s.clients[messageChan] = true
		s.mu.Unlock()

		defer func() {
			s.mu.Lock()
			delete(s.clients, messageChan)
			s.mu.Unlock()
			close(messageChan)
		}()

		notify := r.Context().Done()
		for {
			select {
			case msg := <-messageChan:
				fmt.Fprintf(w, "data: %s\n\n", msg)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			case <-notify:
				return
			}
		}
	})

	addr := fmt.Sprintf(":%d", s.port)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Printf("[Web Server] Error: %v\n", err)
		}
	}()

	return nil
}
