package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
)

//go:embed static/*
var staticFS embed.FS

// Server hosts the embedded Web UI control center and SSE event stream.
type Server struct {
	port      int
	clients   map[chan string]bool
	mu        sync.Mutex
	broadcast chan string
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

// Start launches the HTTP web server in a background goroutine.
func (s *Server) Start() error {
	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		indexBytes, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "Index file not found", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(indexBytes)
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
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
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("[Web Server] Error: %v\n", err)
		}
	}()

	return nil
}
