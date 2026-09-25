package projection

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/1morshed1/eventflow/internal/domain/order"
	"github.com/1morshed1/eventflow/internal/eventstore"
)

// HistoryProjector maintains customer_order_history_view: a human-readable, append-
// only timeline of everything that happened to an order, for customer support. It
// reacts to EVERY event type and turns each into a friendly description.
type HistoryProjector struct{}

func (HistoryProjector) Name() string { return "customer_order_history_view" }

func (HistoryProjector) Handle(ctx context.Context, tx pgx.Tx, e eventstore.Event) error {
	customerID, description, err := describe(ctx, tx, e)
	if err != nil {
		return err
	}
	if description == "" {
		return nil
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO customer_order_history_view (order_id, customer_id, event_type, description, occurred_at)
		VALUES ($1, $2, $3, $4, $5)
	`, e.AggregateID, customerID, e.EventType, description, e.CreatedAt)
	return err
}

// describe turns an event into (customer id, human sentence). Only OrderCreated
// carries the customer id in its payload; for later events we look it up from the
// first history row of this order.
func describe(ctx context.Context, tx pgx.Tx, e eventstore.Event) (uuid.UUID, string, error) {
	switch e.EventType {
	case order.EventOrderCreated:
		var d order.OrderCreatedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return uuid.Nil, "", err
		}
		return d.CustomerID, "Order created", nil

	case order.EventItemAdded:
		var d order.ItemAddedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return uuid.Nil, "", err
		}
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, fmt.Sprintf("Added %d x product %s at %d cents", d.Quantity, d.ProductID, d.Price), err

	case order.EventItemRemoved:
		var d order.ItemRemovedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return uuid.Nil, "", err
		}
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, fmt.Sprintf("Removed product %s", d.ProductID), err

	case order.EventOrderConfirmed:
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, "Order confirmed", err

	case order.EventPaymentReceived:
		var d order.PaymentReceivedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return uuid.Nil, "", err
		}
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, fmt.Sprintf("Payment of %d cents received (ref %s)", d.Amount, d.PaymentRef), err

	case order.EventOrderShipped:
		var d order.OrderShippedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return uuid.Nil, "", err
		}
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, fmt.Sprintf("Shipped (tracking %s)", d.TrackingNumber), err

	case order.EventOrderCancelled:
		var d order.OrderCancelledData
		_ = json.Unmarshal(e.EventData, &d)
		cid, err := lookupCustomer(ctx, tx, e.AggregateID)
		return cid, fmt.Sprintf("Order cancelled: %s", d.Reason), err
	}
	return uuid.Nil, "", nil
}

// lookupCustomer finds the order's customer id from an earlier history row. The
// OrderCreated row is always written first (events are ordered per aggregate), so
// by the time we handle a later event the row exists.
func lookupCustomer(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) (uuid.UUID, error) {
	var cid uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT customer_id FROM customer_order_history_view
		WHERE order_id = $1 ORDER BY id ASC LIMIT 1
	`, orderID).Scan(&cid)
	if err == pgx.ErrNoRows {
		return uuid.Nil, nil
	}
	return cid, err
}
