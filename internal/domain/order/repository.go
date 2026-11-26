package order

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/google/uuid"
)

// snapshotEvery controls how often we persist a snapshot. Replaying up to this
// many events on a cold load is cheap; beyond it we snapshot to bound load cost.
const snapshotEvery = 50

// Repository loads and saves Order aggregates on top of the generic event store.
// It is the bridge between the domain (Order, events) and persistence (eventstore).
type Repository struct {
	store *eventstore.Store
}

// NewRepository wires a Repository to an event store.
func NewRepository(store *eventstore.Store) *Repository {
	return &Repository{store: store}
}

// Load reconstructs the current state of an order.
//
// Fast path: if a snapshot exists, start from it and replay only events written
// after the snapshot's version. Slow path (no snapshot): replay all events.
func (r *Repository) Load(ctx context.Context, id uuid.UUID) (*Order, error) {
	snap, err := r.store.LatestSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}

	o := &Order{}
	if snap != nil {
		if err := json.Unmarshal(snap.State, o); err != nil {
			return nil, fmt.Errorf("decode snapshot: %w", err)
		}
		o.exists = true
		events, err := r.store.LoadAfterVersion(ctx, id, snap.Version)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			o.Apply(e)
		}
		return o, nil
	}

	events, err := r.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	return replay(events), nil
}

// LoadAt reconstructs the state an order had at a point in time (temporal query).
// It cannot use snapshots because a snapshot may be newer than `at`, so it always
// replays from the full history up to `at`.
func (r *Repository) LoadAt(ctx context.Context, id uuid.UUID, at time.Time) (*Order, error) {
	events, err := r.store.LoadBefore(ctx, id, at)
	if err != nil {
		return nil, err
	}
	return replay(events), nil
}

// Save appends new events for an order and opportunistically writes a snapshot.
//
// expectedVersion is the aggregate version the caller based its decision on. The
// new events are numbered expectedVersion+1, +2, ... The event store's UNIQUE
// (aggregate_id, version) constraint rejects the append with ErrVersionConflict
// if another writer already committed expectedVersion+1 - that is the optimistic
// concurrency check.
func (r *Repository) Save(ctx context.Context, o *Order, expectedVersion int, newEvents []eventstore.Event) error {
	if err := r.store.Append(ctx, newEvents); err != nil {
		return err
	}

	// Snapshot when the version crosses a multiple of snapshotEvery. Snapshots are
	// pure optimization, so a failure here is logged by the caller but not fatal.
	finalVersion := expectedVersion + len(newEvents)
	if finalVersion/snapshotEvery > expectedVersion/snapshotEvery {
		state, err := json.Marshal(o)
		if err != nil {
			return fmt.Errorf("marshal snapshot: %w", err)
		}
		_ = r.store.SaveSnapshot(ctx, eventstore.Snapshot{
			AggregateID:   o.ID,
			AggregateType: AggregateType,
			Version:       finalVersion,
			State:         state,
		})
	}
	return nil
}
