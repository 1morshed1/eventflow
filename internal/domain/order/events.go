// Package order is the Order aggregate: the write model of the system.
//
// It defines the events that can happen to an order, the in-memory state derived
// from them (the aggregate), the business rules that decide which new events a
// command may produce, and a repository that loads/saves via the event store.
package order

import "github.com/google/uuid"

// AggregateType is the value stored in events.aggregate_type for orders.
const AggregateType = "Order"

// Event type names. These strings are the contract between the write model, the
// event store, Kafka, and every projector. Treat them as append-only and stable:
// once an event type is in production its shape should only ever be extended in a
// backward-compatible way (add fields, never repurpose them).
const (
	EventOrderCreated   = "OrderCreated"
	EventItemAdded      = "ItemAdded"
	EventItemRemoved    = "ItemRemoved"
	EventOrderConfirmed = "OrderConfirmed"
	EventPaymentReceived = "PaymentReceived"
	EventOrderShipped   = "OrderShipped"
	EventOrderCancelled = "OrderCancelled"
)

// Order status values. The aggregate is a small state machine; these are its states.
const (
	StatusDraft     = "draft"
	StatusConfirmed = "confirmed"
	StatusPaid      = "paid"
	StatusShipped   = "shipped"
	StatusDelivered = "delivered"
	StatusCancelled = "cancelled"
)

// --- Event payloads (stored in events.event_data as JSONB) ---

// OrderCreatedData is emitted once, as the first event of every order.
type OrderCreatedData struct {
	CustomerID uuid.UUID `json:"customer_id"`
}

// ItemAddedData records a line item being added to a draft order. Price is in
// cents (integer) to avoid floating-point money bugs.
type ItemAddedData struct {
	ProductID uuid.UUID `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Price     int64     `json:"price"` // unit price in cents
}

// ItemRemovedData records a line item being removed from a draft order.
type ItemRemovedData struct {
	ProductID uuid.UUID `json:"product_id"`
}

// PaymentReceivedData records a successful payment.
type PaymentReceivedData struct {
	Amount        int64  `json:"amount"` // cents
	PaymentRef    string `json:"payment_ref"`
}

// OrderShippedData records shipment with a tracking number.
type OrderShippedData struct {
	TrackingNumber string `json:"tracking_number"`
}

// OrderCancelledData records why an order was cancelled (useful for analytics and
// for saga compensation, which cancels an order when a later step fails).
type OrderCancelledData struct {
	Reason string `json:"reason"`
}

// OrderItem is a line item in the in-memory aggregate state (not persisted
// directly - it is derived by replaying ItemAdded/ItemRemoved events).
type OrderItem struct {
	ProductID uuid.UUID `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Price     int64     `json:"price"`
}
