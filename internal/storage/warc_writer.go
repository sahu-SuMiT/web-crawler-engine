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

// WARCWriter exports fetched page payloads into ISO 28500 standard .warc.gz archives.
type WARCWriter struct {
	mu       sync.Mutex
	filePath string
	file     *os.File
	gzWriter *gzip.Writer
}

// NewWARCWriter opens or creates a gzipped WARC archive file at the destination path.
func NewWARCWriter(outputDir string) (*WARCWriter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create warc output dir: %w", err)
	}

	fileName := fmt.Sprintf("crawl_%s.warc.gz", time.Now().Format("20060102_150405"))
	fullPath := filepath.Join(outputDir, fileName)

	f, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create warc file: %w", err)
	}

	gz := gzip.NewWriter(f)
	return &WARCWriter{
		filePath: fullPath,
		file:     f,
		gzWriter: gz,
	}, nil
}

// WriteRecord writes a fetched page payload into the gzipped WARC archive.
func (w *WARCWriter) WriteRecord(result domain.FetchResult) error {
	if len(result.Body) == 0 || result.StatusCode == 0 {
		return nil // Skip empty responses
	}

	w.mu.Lock()
	defer w.mu.Unlock()

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

	if _, err := w.gzWriter.Write([]byte(warcHeader)); err != nil {
		return fmt.Errorf("warc write header error: %w", err)
	}
	if _, err := w.gzWriter.Write(fullPayload); err != nil {
		return fmt.Errorf("warc write payload error: %w", err)
	}
	if _, err := w.gzWriter.Write([]byte("\r\n\r\n")); err != nil {
		return fmt.Errorf("warc write footer error: %w", err)
	}

	return w.gzWriter.Flush()
}

// FilePath returns the absolute path of the WARC archive file.
func (w *WARCWriter) FilePath() string {
	return w.filePath
}

// Close flushes and closes the WARC writer.
func (w *WARCWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.gzWriter != nil {
		w.gzWriter.Close()
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
