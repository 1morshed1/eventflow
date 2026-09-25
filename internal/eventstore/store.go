package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1morshed1/eventflow/internal/metrics"
)

// ErrVersionConflict is returned by Append when another writer already wrote the
// version we tried to write. The caller (a command handler) reacts by reloading
// the aggregate and retrying - this is optimistic concurrency control.
var ErrVersionConflict = errors.New("eventstore: version conflict")

// pgUniqueViolation is the SQLSTATE Postgres returns for a UNIQUE constraint
// violation. We use it to detect a concurrency conflict on (aggregate_id, version).
const pgUniqueViolation = "23505"

// Snapshot is a cached, materialized state of an aggregate at a given version.
// Replaying thousands of events on every load is slow; a snapshot lets us start
// from a known state and replay only the events after it.
type Snapshot struct {
	AggregateID   uuid.UUID
	AggregateType string
	Version       int
	State         json.RawMessage
}

// Store is the Postgres-backed event store. It knows nothing about orders or any
// specific domain - it only appends and reads generic events plus snapshots.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps a pgx pool as a Store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Append writes new events for one aggregate atomically.
//
// All events must belong to the same aggregate and carry strictly increasing
// versions. The UNIQUE(aggregate_id, version) constraint means that if a
// concurrent command already committed the same version, our INSERT fails and we
// translate that into ErrVersionConflict so the caller can retry with fresh state.
func (s *Store) Append(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// A transaction makes the batch all-or-nothing: either every event lands or
	// none do. That keeps the version sequence gap-free per aggregate.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback is a no-op after commit

	for _, e := range events {
		_, err := tx.Exec(ctx, `
			INSERT INTO events
				(aggregate_id, aggregate_type, event_type, event_data, metadata, version)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, e.AggregateID, e.AggregateType, e.EventType, e.EventData, e.Metadata, e.Version)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
				return ErrVersionConflict
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	metrics.EventsAppended.Add(float64(len(events)))
	return nil
}

// Load returns all events for an aggregate in version order. Callers replay them
// to reconstruct current state.
func (s *Store) Load(ctx context.Context, aggregateID uuid.UUID) ([]Event, error) {
	return s.query(ctx, `
		SELECT id, aggregate_id, aggregate_type, event_type, event_data, metadata, version, created_at
		FROM events
		WHERE aggregate_id = $1
		ORDER BY version ASC
	`, aggregateID)
}

// LoadAfterVersion returns events with version strictly greater than the given
// one. Used together with a snapshot: load snapshot at version N, then replay
// only events after N.
func (s *Store) LoadAfterVersion(ctx context.Context, aggregateID uuid.UUID, version int) ([]Event, error) {
	return s.query(ctx, `
		SELECT id, aggregate_id, aggregate_type, event_type, event_data, metadata, version, created_at
		FROM events
		WHERE aggregate_id = $1 AND version > $2
		ORDER BY version ASC
	`, aggregateID, version)
}

// LoadBefore returns all events for an aggregate that were created at or before
// the given time. This is the "time travel" query that makes temporal state
// reconstruction possible ("what did this order look like yesterday at 3pm?").
func (s *Store) LoadBefore(ctx context.Context, aggregateID uuid.UUID, at time.Time) ([]Event, error) {
	return s.query(ctx, `
		SELECT id, aggregate_id, aggregate_type, event_type, event_data, metadata, version, created_at
		FROM events
		WHERE aggregate_id = $1 AND created_at <= $2
		ORDER BY version ASC
	`, aggregateID, at)
}

// LoadAllAfter returns every event across all aggregates with a global id greater
// than afterID, up to limit rows. Projectors poll this to build read models in
// global order. (In the Kafka-driven flow projectors consume the topic instead,
// but this method lets a projector rebuild directly from the store too.)
func (s *Store) LoadAllAfter(ctx context.Context, afterID int64, limit int) ([]Event, error) {
	return s.query(ctx, `
		SELECT id, aggregate_id, aggregate_type, event_type, event_data, metadata, version, created_at
		FROM events
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, afterID, limit)
}

// query runs a SELECT and scans rows into Events.
func (s *Store) query(ctx context.Context, sql string, args ...any) ([]Event, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.ID, &e.AggregateID, &e.AggregateType, &e.EventType,
			&e.EventData, &e.Metadata, &e.Version, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// RelayCheckpoint returns the id of the last event the relay published to Kafka.
func (s *Store) RelayCheckpoint(ctx context.Context) (int64, error) {
	var last int64
	err := s.pool.QueryRow(ctx, `SELECT last_event_id FROM relay_checkpoint WHERE id = 1`).Scan(&last)
	if err != nil {
		return 0, fmt.Errorf("read relay checkpoint: %w", err)
	}
	return last, nil
}

// SetRelayCheckpoint advances the relay's published-up-to marker.
func (s *Store) SetRelayCheckpoint(ctx context.Context, lastEventID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE relay_checkpoint SET last_event_id = $1, updated_at = NOW() WHERE id = 1
	`, lastEventID)
	if err != nil {
		return fmt.Errorf("write relay checkpoint: %w", err)
	}
	return nil
}

// LatestSnapshot returns the newest snapshot for an aggregate, or (nil, nil) if
// none exists yet.
func (s *Store) LatestSnapshot(ctx context.Context, aggregateID uuid.UUID) (*Snapshot, error) {
	var snap Snapshot
	err := s.pool.QueryRow(ctx, `
		SELECT aggregate_id, aggregate_type, version, state
		FROM snapshots
		WHERE aggregate_id = $1
		ORDER BY version DESC
		LIMIT 1
	`, aggregateID).Scan(&snap.AggregateID, &snap.AggregateType, &snap.Version, &snap.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	return &snap, nil
}

// SaveSnapshot stores a materialized state. Snapshots are an optimization only -
// they can always be recomputed by replaying events, so we upsert freely.
func (s *Store) SaveSnapshot(ctx context.Context, snap Snapshot) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO snapshots (aggregate_id, aggregate_type, version, state)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (aggregate_id, version) DO UPDATE SET state = EXCLUDED.state
	`, snap.AggregateID, snap.AggregateType, snap.Version, snap.State)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}
