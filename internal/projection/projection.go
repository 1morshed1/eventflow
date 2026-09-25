// Package projection holds the projectors that consume events and build read
// models. Each projector owns one or more denormalized tables and can be rebuilt
// from scratch by replaying the whole event log.
package projection

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/1morshed1/eventflow/internal/kafka"
	"github.com/1morshed1/eventflow/internal/metrics"
)

// Projector builds one read model. Name is used both as the Kafka consumer group
// and the key in projector_checkpoint. Handle applies a single event within a DB
// transaction that also advances the checkpoint - so a read-model update and its
// checkpoint move commit atomically.
type Projector interface {
	Name() string
	Handle(ctx context.Context, tx pgx.Tx, e eventstore.Event) error
}

// Runner drives a Projector: it consumes its topic and, for each event, runs the
// projector's Handle inside a transaction with an idempotency guard.
type Runner struct {
	readPool *pgxpool.Pool
	consumer *kafka.Consumer
	proj     Projector
}

// NewRunner wires a projector to its read DB and a Kafka consumer. groupSuffix lets
// you start an independent replay (a fresh consumer group + checkpoint row) by
// passing e.g. a version string; empty means the default group.
func NewRunner(readPool *pgxpool.Pool, brokers []string, topic string, proj Projector, groupSuffix string) *Runner {
	group := proj.Name()
	if groupSuffix != "" {
		group = proj.Name() + "-" + groupSuffix
	}
	return &Runner{
		readPool: readPool,
		consumer: kafka.NewConsumer(brokers, topic, group),
		proj:     proj,
	}
}

// Run consumes until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	defer r.consumer.Close()
	return r.consumer.Run(ctx, r.handle)
}

// handle applies one event with exactly-once *effect* semantics on the read model,
// despite at-least-once *delivery* from Kafka:
//   - open a transaction,
//   - read this projector's checkpoint,
//   - if the event id is not newer than the checkpoint, skip it (already applied),
//   - otherwise apply it and advance the checkpoint,
//   - commit.
// Because the read-model write and the checkpoint update are in one transaction, a
// redelivered event can never be applied twice.
func (r *Runner) handle(ctx context.Context, e eventstore.Event) error {
	tx, err := r.readPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var checkpoint int64
	err = tx.QueryRow(ctx, `
		SELECT last_event_id FROM projector_checkpoint WHERE projector = $1
	`, r.proj.Name()).Scan(&checkpoint)
	if err == pgx.ErrNoRows {
		checkpoint = 0
	} else if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}

	if e.ID <= checkpoint {
		return nil // already applied - idempotent skip
	}

	if err := r.proj.Handle(ctx, tx, e); err != nil {
		return fmt.Errorf("project %s: %w", e.EventType, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO projector_checkpoint (projector, last_event_id, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (projector) DO UPDATE SET last_event_id = EXCLUDED.last_event_id, updated_at = NOW()
	`, r.proj.Name(), e.ID)
	if err != nil {
		return fmt.Errorf("advance checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Only record progress after the commit succeeds, so metrics never over-count.
	metrics.ProjectorProcessed.WithLabelValues(r.proj.Name()).Inc()
	metrics.ProjectorPosition.WithLabelValues(r.proj.Name()).Set(float64(e.ID))
	return nil
}
