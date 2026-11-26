package order

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/google/uuid"
)

// Domain errors returned by command handlers. They map to HTTP 4xx in the API
// layer - they are expected business outcomes, not bugs.
var (
	ErrOrderNotFound       = errors.New("order not found")
	ErrOrderNotEditable    = errors.New("order is not editable")
	ErrTooManyItems        = errors.New("order has too many items")
	ErrEmptyOrder          = errors.New("cannot confirm an empty order")
	ErrInvalidState        = errors.New("operation not allowed in current state")
	ErrConcurrencyConflict = errors.New("concurrency conflict, please retry")
)

// maxRetries bounds the optimistic-concurrency retry loop. Under contention a
// command reloads fresh state and re-decides; if it still loses after this many
// tries we give up and surface ErrConcurrencyConflict.
const maxRetries = 3

// maxItems caps line items per order - an example business invariant enforced by
// the write model (never by the read model).
const maxItems = 50

// Service is the command side of CQRS: it validates commands against current
// aggregate state and appends new events. It performs NO reads for query
// purposes - reads for clients go to the query service and its read models.
type Service struct {
	repo *Repository
}

// NewService wires a command Service to a repository.
func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// decision is the result of applying business rules to a loaded aggregate: the
// new events to append (may be empty if the command is a no-op).
type decision func(o *Order) ([]pendingEvent, error)

// pendingEvent is an event the domain wants to emit, before it is assigned a
// version and marshaled. Keeping it version-free here means the retry loop can
// re-number events after reloading, without the domain caring about versions.
type pendingEvent struct {
	eventType string
	data      any
}

// execute runs the optimistic-concurrency loop shared by every command:
//  1. load fresh aggregate state,
//  2. let the caller's decision function apply business rules,
//  3. number the resulting events and append them,
//  4. on a version conflict, loop and try again with fresher state.
func (s *Service) execute(ctx context.Context, id uuid.UUID, requireExists bool, fn decision) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		o, err := s.repo.Load(ctx, id)
		if err != nil {
			return err
		}
		if requireExists && !o.Exists() {
			return ErrOrderNotFound
		}

		pending, err := fn(o)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil // no-op command
		}

		events, err := numberEvents(id, o.Version, pending)
		if err != nil {
			return err
		}

		err = s.repo.Save(ctx, applyAll(o, events), o.Version, events)
		if errors.Is(err, eventstore.ErrVersionConflict) {
			continue // another writer won; reload and retry
		}
		return err
	}
	return ErrConcurrencyConflict
}

// Create starts a new draft order and returns its id.
func (s *Service) Create(ctx context.Context, customerID uuid.UUID) (uuid.UUID, error) {
	id := uuid.New()
	err := s.execute(ctx, id, false, func(o *Order) ([]pendingEvent, error) {
		if o.Exists() {
			return nil, ErrInvalidState // id collision - practically impossible
		}
		return []pendingEvent{{EventOrderCreated, OrderCreatedData{CustomerID: customerID}}}, nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// AddItem adds a line item to a draft order, enforcing the editable-state and
// max-items invariants.
func (s *Service) AddItem(ctx context.Context, id uuid.UUID, productID uuid.UUID, qty int, price int64) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		if o.Status != StatusDraft {
			return nil, ErrOrderNotEditable
		}
		if o.ItemCount() >= maxItems {
			return nil, ErrTooManyItems
		}
		return []pendingEvent{{EventItemAdded, ItemAddedData{ProductID: productID, Quantity: qty, Price: price}}}, nil
	})
}

// RemoveItem removes a line item from a draft order.
func (s *Service) RemoveItem(ctx context.Context, id uuid.UUID, productID uuid.UUID) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		if o.Status != StatusDraft {
			return nil, ErrOrderNotEditable
		}
		return []pendingEvent{{EventItemRemoved, ItemRemovedData{ProductID: productID}}}, nil
	})
}

// Confirm moves a draft order to confirmed, rejecting empty orders.
func (s *Service) Confirm(ctx context.Context, id uuid.UUID) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		if o.Status != StatusDraft {
			return nil, ErrInvalidState
		}
		if o.ItemCount() == 0 {
			return nil, ErrEmptyOrder
		}
		return []pendingEvent{{EventOrderConfirmed, nil}}, nil
	})
}

// MarkPaid records a payment. Called by the saga after the payment step succeeds.
func (s *Service) MarkPaid(ctx context.Context, id uuid.UUID, amount int64, ref string) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		if o.Status != StatusConfirmed {
			return nil, ErrInvalidState
		}
		return []pendingEvent{{EventPaymentReceived, PaymentReceivedData{Amount: amount, PaymentRef: ref}}}, nil
	})
}

// Ship records shipment with a tracking number.
func (s *Service) Ship(ctx context.Context, id uuid.UUID, tracking string) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		if o.Status != StatusPaid {
			return nil, ErrInvalidState
		}
		return []pendingEvent{{EventOrderShipped, OrderShippedData{TrackingNumber: tracking}}}, nil
	})
}

// Cancel cancels an order that is not already shipped/delivered/cancelled. Used
// both by clients and by saga compensation.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID, reason string) error {
	return s.execute(ctx, id, true, func(o *Order) ([]pendingEvent, error) {
		switch o.Status {
		case StatusShipped, StatusDelivered, StatusCancelled:
			return nil, ErrInvalidState
		}
		return []pendingEvent{{EventOrderCancelled, OrderCancelledData{Reason: reason}}}, nil
	})
}

// numberEvents assigns sequential versions (baseVersion+1, +2, ...) and marshals
// payloads into eventstore.Event values ready to append.
func numberEvents(id uuid.UUID, baseVersion int, pending []pendingEvent) ([]eventstore.Event, error) {
	events := make([]eventstore.Event, 0, len(pending))
	for i, p := range pending {
		var raw json.RawMessage
		if p.data != nil {
			b, err := json.Marshal(p.data)
			if err != nil {
				return nil, err
			}
			raw = b
		} else {
			raw = json.RawMessage("{}")
		}
		events = append(events, eventstore.Event{
			AggregateID:   id,
			AggregateType: AggregateType,
			EventType:     p.eventType,
			EventData:     raw,
			Version:       baseVersion + i + 1,
		})
	}
	return events, nil
}

// applyAll folds new events into the aggregate so that a snapshot taken right
// after Save reflects the final state.
func applyAll(o *Order, events []eventstore.Event) *Order {
	for _, e := range events {
		o.Apply(e)
	}
	return o
}
