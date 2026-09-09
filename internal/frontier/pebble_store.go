package frontier

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
)

type PebbleStore struct {
	db      *pebble.DB
	dirPath string
}

func NewPebbleStore(dirPath string) (*PebbleStore, error) {
	opts := &pebble.Options{}
	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dirPath, err)
	}

	return &PebbleStore{db: db, dirPath: dirPath}, nil
}

func (p *PebbleStore) SaveURL(item domain.URLItem) error {
	key := []byte("url:" + item.URL)
	value, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal URLItem: %w", err)
	}

	if err := p.db.Set(key, value, pebble.Sync); err != nil {
		return fmt.Errorf("failed to write key to pebble: %w", err)
	}

	return nil
}

func (p *PebbleStore) GetURL(urlStr string) (*domain.URLItem, bool, error) {
	key := []byte("url:" + urlStr)
	val, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("pebble get error: %w", err)
	}
	defer closer.Close()

	var item domain.URLItem
	if err := json.Unmarshal(val, &item); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal URLItem: %w", err)
	}

	return &item, true, nil
}

func (p *PebbleStore) ScanAll(statusFilter string, limit int) ([]domain.URLItem, error) {
	if limit <= 0 {
		limit = 500
	}
	var results []domain.URLItem

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble iterator: %w", err)
	}
	defer iter.Close()

	prefix := []byte("url:")
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if len(results) >= limit {
			break
		}
		key := iter.Key()
		if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
			break
		}
		var item domain.URLItem
		if err := json.Unmarshal(iter.Value(), &item); err != nil {
			continue
		}
		if statusFilter == "" || string(item.Status) == statusFilter {
			results = append(results, item)
		}
	}
	return results, nil
}

// ScanDomains does a single key-only pass over all url: entries and returns
// a map of domain → record count. No JSON unmarshaling — fast even with 50k records.
func (p *PebbleStore) ScanDomains(statusFilter string) (map[string]int, error) {
	counts := make(map[string]int)

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble iterator: %w", err)
	}
	defer iter.Close()

	prefix := []byte("url:")
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !strings.HasPrefix(string(key), "url:") {
			break
		}
		if statusFilter != "" {
			var item domain.URLItem
			if err := json.Unmarshal(iter.Value(), &item); err != nil {
				continue
			}
			if string(item.Status) != statusFilter {
				continue
			}
			counts[item.Domain]++
		} else {
			// Fast path: extract domain from key without full unmarshal.
			// Key format: "url:https://domain.com/path"
			urlStr := string(key[4:]) // strip "url:"
			if d := extractDomainFromURL(urlStr); d != "" {
				counts[d]++
			}
		}
	}
	return counts, nil
}

// ScanByDomain returns up to limit URLItems whose Domain field matches domainName.
func (p *PebbleStore) ScanByDomain(domainName, statusFilter string, limit int) ([]domain.URLItem, error) {
	if limit <= 0 {
		limit = 200
	}
	var results []domain.URLItem

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble iterator: %w", err)
	}
	defer iter.Close()

	// Seek directly to the url:https://domainName prefix for efficiency.
	seekKey := []byte("url:https://" + domainName)
	prefix := []byte("url:")
	for iter.SeekGE(seekKey); iter.Valid(); iter.Next() {
		if len(results) >= limit {
			break
		}
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break
		}
		var item domain.URLItem
		if err := json.Unmarshal(iter.Value(), &item); err != nil {
			continue
		}
		if item.Domain != domainName {
			break // Past this domain alphabetically
		}
		if statusFilter == "" || string(item.Status) == statusFilter {
			results = append(results, item)
		}
	}
	return results, nil
}

// extractDomainFromURL extracts the hostname from a raw URL string without net/url import.
func extractDomainFromURL(rawURL string) string {
	// Strip scheme: https:// or http://
	s := rawURL
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	// Strip path, query, fragment
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	// Strip port
	if i := strings.LastIndex(s, ":"); i >= 0 {
		s = s[:i]
	}
	return s
}

// EraseAll closes the Pebble DB, deletes all its data files, and reopens a
// fresh empty database at the same path. Used by the /api/erase-disk endpoint.
func (p *PebbleStore) EraseAll() error {
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			return fmt.Errorf("pebble close before erase: %w", err)
		}
		p.db = nil
	}
	if err := os.RemoveAll(p.dirPath); err != nil {
		return fmt.Errorf("failed to remove pebble dir: %w", err)
	}
	if err := os.MkdirAll(p.dirPath, 0755); err != nil {
		return fmt.Errorf("failed to recreate pebble dir: %w", err)
	}
	db, err := pebble.Open(p.dirPath, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("failed to reopen pebble after erase: %w", err)
	}
	p.db = db
	return nil
}

func (p *PebbleStore) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
