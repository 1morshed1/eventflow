package projection

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/1morshed1/eventflow/internal/domain/order"
	"github.com/1morshed1/eventflow/internal/eventstore"
)

// OrdersProjector maintains orders_view: one denormalized row per order, shaped for
// the "my orders" list and order-detail pages. Note it stores current status,
// running total, and item count directly, so those pages need no joins or replays.
type OrdersProjector struct{}

func (OrdersProjector) Name() string { return "orders_view" }

func (OrdersProjector) Handle(ctx context.Context, tx pgx.Tx, e eventstore.Event) error {
	switch e.EventType {
	case order.EventOrderCreated:
		var d order.OrderCreatedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO orders_view (id, customer_id, status, total, item_count, created_at, updated_at)
			VALUES ($1, $2, $3, 0, 0, $4, $4)
			ON CONFLICT (id) DO NOTHING
		`, e.AggregateID, d.CustomerID, order.StatusDraft, e.CreatedAt)
		return err

	case order.EventItemAdded:
		var d order.ItemAddedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE orders_view
			SET total = total + $1, item_count = item_count + 1, updated_at = $2
			WHERE id = $3
		`, d.Price*int64(d.Quantity), e.CreatedAt, e.AggregateID)
		return err

	case order.EventItemRemoved:
		// We do not carry the removed item's price in ItemRemovedData, so we cannot
		// cheaply adjust the total here. This is an intentional teaching example: a
		// projector is only as rich as the events feeding it. The fix is to enrich
		// the event (include price/quantity in ItemRemovedData) rather than to query
		// back into the write model. For now we just decrement the count.
		_, err := tx.Exec(ctx, `
			UPDATE orders_view
			SET item_count = GREATEST(item_count - 1, 0), updated_at = $1
			WHERE id = $2
		`, e.CreatedAt, e.AggregateID)
		return err

	case order.EventOrderConfirmed:
		return setStatus(ctx, tx, e, order.StatusConfirmed)

	case order.EventPaymentReceived:
		return setStatus(ctx, tx, e, order.StatusPaid)

	case order.EventOrderShipped:
		var d order.OrderShippedData
		if err := json.Unmarshal(e.EventData, &d); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE orders_view
			SET status = $1, tracking_number = $2, shipped_at = $3, updated_at = $3
			WHERE id = $4
		`, order.StatusShipped, d.TrackingNumber, e.CreatedAt, e.AggregateID)
		return err

	case order.EventOrderCancelled:
		return setStatus(ctx, tx, e, order.StatusCancelled)
	}
	return nil
}

func setStatus(ctx context.Context, tx pgx.Tx, e eventstore.Event, status string) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders_view SET status = $1, updated_at = $2 WHERE id = $3
	`, status, e.CreatedAt, e.AggregateID)
	return err
}
