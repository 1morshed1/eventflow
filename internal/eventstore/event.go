// Package eventstore is the heart of the system.
//
// In event sourcing we never store the "current state" of an entity as a row we
// overwrite. Instead we store every state change as an immutable Event and derive
// the current state by replaying those events in order. This file defines the
// Event value object; store.go defines the persistence layer.
package eventstore

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event is one immutable fact that happened to an aggregate.
//
// "Aggregate" is the DDD term for a consistency boundary - here, a single Order.
// Events are append-only: once written they are never updated or deleted. The
// full history of an aggregate is its ordered list of events.
type Event struct {
	// ID is the global, monotonically increasing sequence number (BIGSERIAL).
	// It gives every event across every aggregate a total order, which is what
	// projectors follow when rebuilding read models.
	ID int64 `json:"id"`

	// AggregateID identifies which entity this event belongs to (an order id).
	AggregateID uuid.UUID `json:"aggregate_id"`

	// AggregateType groups aggregates of the same kind ("Order", "Product"...).
	AggregateType string `json:"aggregate_type"`

	// EventType names what happened ("OrderCreated", "ItemAdded"...). The apply
	// logic switches on this string.
	EventType string `json:"event_type"`

	// EventData is the event-specific payload, stored as JSONB. Kept as raw JSON
	// here so the store stays domain-agnostic; the domain layer marshals/unmarshals.
	EventData json.RawMessage `json:"event_data"`

	// Metadata carries cross-cutting context (trace_id, user_id, causation_id)
	// that is useful for auditing and distributed tracing but is not domain data.
	Metadata json.RawMessage `json:"metadata,omitempty"`

	// Version is the per-aggregate version number. The first event of an
	// aggregate is version 1, the next is 2, and so on. Combined with the
	// UNIQUE(aggregate_id, version) DB constraint this gives us optimistic
	// concurrency control - see store.go Append.
	Version int `json:"version"`

	// CreatedAt is when the event was appended. Never mutated.
	CreatedAt time.Time `json:"created_at"`
}
