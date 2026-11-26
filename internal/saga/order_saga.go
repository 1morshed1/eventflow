package saga

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
)

// Orchestrator builds and runs the place-order saga.
//
// Inventory and payment are simulated in-process here (there are no separate
// inventory/payment microservices in this demo), while the final "mark paid" step
// is a real call to the command service that appends a PaymentReceived event. That
// mix is deliberate: it shows how a saga step can be a local action, a remote call,
// or a simulation, all coordinated the same way.
type Orchestrator struct {
	cmd *CommandClient
}

// NewOrchestrator wires the orchestrator to a command-service client.
func NewOrchestrator(cmd *CommandClient) *Orchestrator {
	return &Orchestrator{cmd: cmd}
}

// PlaceOrderInput is the request to run the saga for one confirmed order.
type PlaceOrderInput struct {
	OrderID uuid.UUID
	Amount  int64 // cents to charge
	// FailPayment forces the payment step to fail. It exists so you can test that
	// compensation runs (inventory is released) - see the load-testing section of
	// the spec: "kill the payment service mid-saga and verify inventory is released".
	FailPayment bool
}

// errPaymentDeclined is returned by the simulated payment step when forced to fail.
var errPaymentDeclined = errors.New("payment declined")

// Run executes the place-order saga for the given input.
func (o *Orchestrator) Run(ctx context.Context, in PlaceOrderInput) error {
	s := &Saga{
		Name: "place-order",
		Steps: []Step{
			{
				Name:       "reserve_inventory",
				Execute:    o.reserveInventory,
				Compensate: o.releaseInventory,
			},
			{
				Name:       "process_payment",
				Execute:    o.processPayment,
				Compensate: o.refundPayment,
			},
			{
				Name:    "mark_order_paid",
				Execute: o.markPaid,
				// Compensating a confirmed payment means cancelling the order.
				Compensate: o.cancelOrder,
			},
		},
	}

	data := Data{
		"order_id":     in.OrderID,
		"amount":       in.Amount,
		"fail_payment": in.FailPayment,
	}
	return s.Run(ctx, data)
}

// --- Step implementations ---

// reserveInventory simulates reserving stock and records a reservation id so the
// compensation can release exactly what was reserved.
func (o *Orchestrator) reserveInventory(_ context.Context, d Data) error {
	reservationID := uuid.New().String()
	d["reservation_id"] = reservationID
	slog.Info("inventory reserved", "reservation_id", reservationID)
	return nil
}

func (o *Orchestrator) releaseInventory(_ context.Context, d Data) error {
	slog.Info("inventory released", "reservation_id", d["reservation_id"])
	return nil
}

// processPayment simulates charging a card. It fails when FailPayment was set,
// which triggers compensation of the inventory reservation.
func (o *Orchestrator) processPayment(_ context.Context, d Data) error {
	if fail, _ := d["fail_payment"].(bool); fail {
		return errPaymentDeclined
	}
	paymentRef := "pay_" + uuid.New().String()
	d["payment_ref"] = paymentRef
	slog.Info("payment processed", "payment_ref", paymentRef)
	return nil
}

func (o *Orchestrator) refundPayment(_ context.Context, d Data) error {
	slog.Info("payment refunded", "payment_ref", d["payment_ref"])
	return nil
}

// markPaid appends the real PaymentReceived event via the command service.
func (o *Orchestrator) markPaid(ctx context.Context, d Data) error {
	orderID := d["order_id"].(uuid.UUID)
	amount := d["amount"].(int64)
	ref, _ := d["payment_ref"].(string)
	return o.cmd.Pay(ctx, orderID, amount, ref)
}

func (o *Orchestrator) cancelOrder(ctx context.Context, d Data) error {
	orderID := d["order_id"].(uuid.UUID)
	return o.cmd.Cancel(ctx, orderID, "saga compensation")
}
