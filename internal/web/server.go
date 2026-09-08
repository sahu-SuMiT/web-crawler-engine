package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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
