package storage

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
)

type domainWriter struct {
	file     *os.File
	gzWriter *gzip.Writer
	filePath string
}

type WARCWriter struct {
	mu        sync.Mutex
	outputDir string
	writers   map[string]*domainWriter
}

func NewWARCWriter(outputDir string) (*WARCWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create warc base dir: %w", err)
	}
	return &WARCWriter{
		outputDir: outputDir,
		writers:   make(map[string]*domainWriter),
	}, nil
}

func (w *WARCWriter) getDomainWriter(domainName string) (*domainWriter, error) {
	if domainName == "" {
		domainName = "unsorted"
	}

	if dw, exists := w.writers[domainName]; exists {
		return dw, nil
	}

	domainDir := filepath.Join(w.outputDir, domainName)
	if err := os.MkdirAll(domainDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create domain warc dir: %w", err)
	}
	fileName := fmt.Sprintf("crawl_%s.warc.gz", time.Now().Format("20060102_150405"))
	fullPath := filepath.Join(domainDir, fileName)

	f, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create warc file: %w", err)
	}

	dw := &domainWriter{
		file:     f,
		gzWriter: gzip.NewWriter(f),
		filePath: fullPath,
	}
	w.writers[domainName] = dw
	return dw, nil
}

func (w *WARCWriter) WriteRecord(result domain.FetchResult) error {
	if len(result.Body) == 0 || result.StatusCode == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	dw, err := w.getDomainWriter(result.Domain)
	if err != nil {
		return err
	}

	nowStr := time.Now().UTC().Format(time.RFC3339)
	httpHeaderBlock := fmt.Sprintf("HTTP/1.1 %d OK\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n",
		result.StatusCode, result.ContentType, len(result.Body))

	fullPayload := append([]byte(httpHeaderBlock), result.Body...)

	warcHeader := fmt.Sprintf(
		"WARC/1.0\r\n"+
			"WARC-Type: response\r\n"+
			"WARC-Record-ID: <urn:uuid:%d>\r\n"+
			"WARC-Date: %s\r\n"+
			"WARC-Target-URI: %s\r\n"+
			"Content-Type: application/http; msgtype=response\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n",
		time.Now().UnixNano(),
		nowStr,
		result.URL,
		len(fullPayload),
	)

	if _, err := dw.gzWriter.Write([]byte(warcHeader)); err != nil {
		return fmt.Errorf("warc write header error: %w", err)
	}
	if _, err := dw.gzWriter.Write(fullPayload); err != nil {
		return fmt.Errorf("warc write payload error: %w", err)
	}
	if _, err := dw.gzWriter.Write([]byte("\r\n\r\n")); err != nil {
		return fmt.Errorf("warc write footer error: %w", err)
	}

	return dw.gzWriter.Flush()
}

func (w *WARCWriter) FilePaths() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	paths := make([]string, 0, len(w.writers))
	for _, dw := range w.writers {
		paths = append(paths, dw.filePath)
	}
	return paths
}

func (w *WARCWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var lastErr error
	for _, dw := range w.writers {
		if err := dw.gzWriter.Close(); err != nil {
			lastErr = err
		}
		if err := dw.file.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
