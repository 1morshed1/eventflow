// Package db holds small helpers for connecting to Postgres and running the SQL
// migrations at startup. It is intentionally tiny - no ORM, no migration
// framework - so the mechanics stay visible for learning.
package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pgx connection pool and waits (with retries) until Postgres is
// reachable. In Docker Compose the app container can start before Postgres is
// ready to accept connections, so we retry rather than crash-loop.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pool.Ping(pingCtx)
		cancel()
		if lastErr == nil {
			return pool, nil
		}
		time.Sleep(time.Second)
	}
	pool.Close()
	return nil, fmt.Errorf("postgres not reachable: %w", lastErr)
}

// Migrate applies every *.sql file in dir, in filename order. The migration SQL is
// written to be idempotent (CREATE TABLE IF NOT EXISTS ...), so re-running it on
// an already-migrated database is safe. This keeps the demo simple; a real system
// would track applied migrations in a table.
func Migrate(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sql" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		sqlBytes, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}
