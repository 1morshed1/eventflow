package order

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/1morshed1/eventflow/internal/eventstore"
)

// mkEvent is a tiny helper to build an event with a marshaled payload.
func mkEvent(id uuid.UUID, version int, etype string, data any) eventstore.Event {
	raw := json.RawMessage("{}")
	if data != nil {
		b, _ := json.Marshal(data)
		raw = b
	}
	return eventstore.Event{AggregateID: id, AggregateType: AggregateType, EventType: etype, EventData: raw, Version: version}
}

// TestReplayBuildsCurrentState is the canonical event-sourcing property test:
// folding a sequence of events must yield the correct current state, including the
// derived total and version.
func TestReplayBuildsCurrentState(t *testing.T) {
	id := uuid.New()
	cust := uuid.New()
	prodA := uuid.New()
	prodB := uuid.New()

	events := []eventstore.Event{
		mkEvent(id, 1, EventOrderCreated, OrderCreatedData{CustomerID: cust}),
		mkEvent(id, 2, EventItemAdded, ItemAddedData{ProductID: prodA, Quantity: 2, Price: 500}), // 1000
		mkEvent(id, 3, EventItemAdded, ItemAddedData{ProductID: prodB, Quantity: 1, Price: 250}), // +250
		mkEvent(id, 4, EventOrderConfirmed, nil),
	}

	o := replay(events)

	if !o.Exists() {
		t.Fatal("order should exist after OrderCreated")
	}
	if o.CustomerID != cust {
		t.Errorf("customer id = %s, want %s", o.CustomerID, cust)
	}
	if o.Status != StatusConfirmed {
		t.Errorf("status = %s, want %s", o.Status, StatusConfirmed)
	}
	if o.Total != 1250 {
		t.Errorf("total = %d, want 1250", o.Total)
	}
	if o.ItemCount() != 2 {
		t.Errorf("item count = %d, want 2", o.ItemCount())
	}
	if o.Version != 4 {
		t.Errorf("version = %d, want 4", o.Version)
	}
}

// TestItemRemovedAdjustsTotal verifies removing an item updates both the item list
// and the derived total.
func TestItemRemovedAdjustsTotal(t *testing.T) {
	id := uuid.New()
	prodA := uuid.New()

	o := replay([]eventstore.Event{
		mkEvent(id, 1, EventOrderCreated, OrderCreatedData{CustomerID: uuid.New()}),
		mkEvent(id, 2, EventItemAdded, ItemAddedData{ProductID: prodA, Quantity: 3, Price: 100}), // 300
		mkEvent(id, 3, EventItemRemoved, ItemRemovedData{ProductID: prodA}),
	})

	if o.Total != 0 {
		t.Errorf("total = %d, want 0 after removal", o.Total)
	}
	if o.ItemCount() != 0 {
		t.Errorf("item count = %d, want 0", o.ItemCount())
	}
}

// TestUnknownEventIgnored confirms replay tolerates event types it does not know,
// which is what makes adding new event types backward-compatible.
func TestUnknownEventIgnored(t *testing.T) {
	id := uuid.New()
	o := replay([]eventstore.Event{
		mkEvent(id, 1, EventOrderCreated, OrderCreatedData{CustomerID: uuid.New()}),
		mkEvent(id, 2, "SomeFutureEvent", nil),
	})
	if o.Status != StatusDraft {
		t.Errorf("status = %s, want draft (unknown event must not change state)", o.Status)
	}
	if o.Version != 2 {
		t.Errorf("version = %d, want 2 (version still advances)", o.Version)
	}
}
