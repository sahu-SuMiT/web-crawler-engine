package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sahu-SuMiT/web-crawler-engine/internal/domain"
)

// NeonMetadataStore synchronizes crawl metadata and telemetry logs to Neon PostgreSQL.
type NeonMetadataStore struct {
	pool    *pgxpool.Pool
	enabled bool
}

// NewNeonMetadataStore initializes a connection pool to Neon PostgreSQL.
// If connString is empty, it checks NEON_DATABASE_URL env var.
// If no database URL is set, it operates in disabled mode gracefully.
func NewNeonMetadataStore(ctx context.Context, connString string) (*NeonMetadataStore, error) {
	if connString == "" {
		connString = os.Getenv("NEON_DATABASE_URL")
	}

	if connString == "" {
		return &NeonMetadataStore{enabled: false}, nil
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Neon PostgreSQL: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping Neon PostgreSQL: %w", err)
	}

	store := &NeonMetadataStore{
		pool:    pool,
		enabled: true,
	}

	if err := store.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to init neon database schema: %w", err)
	}

	return store, nil
}

// initSchema creates the required crawl_metadata table if it doesn't exist.
func (n *NeonMetadataStore) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS crawl_records (
		id SERIAL PRIMARY KEY,
		url TEXT NOT NULL,
		domain TEXT NOT NULL,
		status_code INT NOT NULL,
		depth INT NOT NULL,
		fetch_time_ms BIGINT NOT NULL,
		error TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_crawl_domain ON crawl_records(domain);
	`
	_, err := n.pool.Exec(ctx, schema)
	return err
}

// IsEnabled returns true if Neon PostgreSQL is connected and active.
func (n *NeonMetadataStore) IsEnabled() bool {
	return n.enabled
}

// SaveRecord persists a FetchResult record into Neon PostgreSQL.
func (n *NeonMetadataStore) SaveRecord(ctx context.Context, res domain.FetchResult) error {
	if !n.enabled {
		return nil
	}

	query := `
	INSERT INTO crawl_records (url, domain, status_code, depth, fetch_time_ms, error)
	VALUES ($1, $2, $3, $4, $5, $6);
	`
	_, err := n.pool.Exec(ctx, query,
		res.URL,
		res.Domain,
		res.StatusCode,
		res.Depth,
		res.FetchTime.Milliseconds(),
		res.Error,
	)
	return err
}

// Close closes the database connection pool.
func (n *NeonMetadataStore) Close() {
	if n.enabled && n.pool != nil {
		n.pool.Close()
	}
}
