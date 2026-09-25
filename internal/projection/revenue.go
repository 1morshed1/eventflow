package projection

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/1morshed1/eventflow/internal/domain/order"
	"github.com/1morshed1/eventflow/internal/eventstore"
)

// RevenueProjector maintains revenue_daily_view: money recognized per day per
// customer. It reacts only to PaymentReceived events - a great illustration that
// different read models care about different subsets of the same event stream.
type RevenueProjector struct{}

func (RevenueProjector) Name() string { return "revenue_daily_view" }

func (RevenueProjector) Handle(ctx context.Context, tx pgx.Tx, e eventstore.Event) error {
	if e.EventType != order.EventPaymentReceived {
		return nil
	}
	var d order.PaymentReceivedData
	if err := json.Unmarshal(e.EventData, &d); err != nil {
		return err
	}

	// We need the customer id, which is not in the payment payload. Rather than
	// reach into the write model, we read it from a read model we already populate.
	// (Projectors sharing the read DB is fine; sharing the write model is not.)
	var customerID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT customer_id FROM orders_view WHERE id = $1`, e.AggregateID).Scan(&customerID)
	if err == pgx.ErrNoRows {
		// The orders_view projector may not have caught up yet. Returning an error
		// makes Kafka redeliver this event later, once the row exists.
		return err
	} else if err != nil {
		return err
	}

	day := e.CreatedAt.UTC().Format("2006-01-02")
	_, err = tx.Exec(ctx, `
		INSERT INTO revenue_daily_view (day, customer_id, revenue, order_count)
		VALUES ($1, $2, $3, 1)
		ON CONFLICT (day, customer_id)
		DO UPDATE SET revenue = revenue_daily_view.revenue + EXCLUDED.revenue,
		              order_count = revenue_daily_view.order_count + 1
	`, day, customerID, d.Amount)
	return err
}
