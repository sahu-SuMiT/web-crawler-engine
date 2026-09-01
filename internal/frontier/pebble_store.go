package frontier

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
)

// PebbleStore wraps the embedded Pebble KV database for persistent URL queue backlog storage.
type PebbleStore struct {
	db *pebble.DB
}

// NewPebbleStore opens or creates a Pebble database at the given path directory.
func NewPebbleStore(dirPath string) (*PebbleStore, error) {
	opts := &pebble.Options{}
	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dirPath, err)
	}

	return &PebbleStore{db: db}, nil
}

// SaveURL persists a URLItem into Pebble DB using the key format "url:<url_string>".
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

// GetURL retrieves a persisted URLItem by its URL string.
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

// Close closes the Pebble DB instance cleanly.
func (p *PebbleStore) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
