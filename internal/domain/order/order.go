package order

import (
	"encoding/json"

	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/google/uuid"
)

// Order is the in-memory state of an order aggregate, reconstructed by replaying
// its events. It is NEVER persisted as-is (except opportunistically as a snapshot);
// the events are the source of truth.
type Order struct {
	ID         uuid.UUID   `json:"id"`
	CustomerID uuid.UUID   `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	Status     string      `json:"status"`
	Total      int64       `json:"total"` // cents, derived from items
	Version    int         `json:"version"`

	// exists is false until an OrderCreated event has been applied. It lets
	// command handlers distinguish "order not found" from "order in draft".
	exists bool
}

// Exists reports whether the aggregate has been created (has at least one event).
func (o *Order) Exists() bool { return o.exists }

// ItemCount returns the number of distinct line items currently on the order.
func (o *Order) ItemCount() int { return len(o.Items) }

// Apply mutates the aggregate to reflect a single event. This is the core of
// event sourcing: state = fold(Apply, events). Apply must be:
//   - deterministic (same events => same state), and
//   - side-effect free (no DB, no IO) - it only mutates the struct.
//
// It must handle every event type the aggregate can emit. Unknown event types are
// ignored so that adding a new event type never breaks replay of old aggregates.
func (o *Order) Apply(e eventstore.Event) {
	switch e.EventType {
	case EventOrderCreated:
		var d OrderCreatedData
		_ = json.Unmarshal(e.EventData, &d)
		o.ID = e.AggregateID
		o.CustomerID = d.CustomerID
		o.Status = StatusDraft
		o.exists = true

	case EventItemAdded:
		var d ItemAddedData
		_ = json.Unmarshal(e.EventData, &d)
		o.Items = append(o.Items, OrderItem{ProductID: d.ProductID, Quantity: d.Quantity, Price: d.Price})
		o.Total += d.Price * int64(d.Quantity)

	case EventItemRemoved:
		var d ItemRemovedData
		_ = json.Unmarshal(e.EventData, &d)
		for i, it := range o.Items {
			if it.ProductID == d.ProductID {
				o.Total -= it.Price * int64(it.Quantity)
				o.Items = append(o.Items[:i], o.Items[i+1:]...)
				break
			}
		}

	case EventOrderConfirmed:
		o.Status = StatusConfirmed

	case EventPaymentReceived:
		o.Status = StatusPaid

	case EventOrderShipped:
		o.Status = StatusShipped

	case EventOrderCancelled:
		o.Status = StatusCancelled
	}

	// Every applied event advances the aggregate version to match the event's
	// version. After replaying all events, o.Version == the highest event version.
	o.Version = e.Version
}

// replay folds a list of events into a fresh Order.
func replay(events []eventstore.Event) *Order {
	o := &Order{}
	for _, e := range events {
		o.Apply(e)
	}
	return o
}
