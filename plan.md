## Project 8: Event-Driven E-Commerce Platform with CQRS & Event Sourcing

### Concept & Why It Matters

You are building an e-commerce backend that uses event sourcing as its primary data model and CQRS (Command Query Responsibility Segregation) to separate writes from reads. Instead of storing the "current state" of an order in a row, you store every event that ever happened to it (OrderCreated, ItemAdded, PaymentReceived, OrderShipped). The current state is derived by replaying these events.

This matters because event sourcing and CQRS are increasingly adopted at companies like Netflix, Uber, and all major financial institutions. They provide a complete audit trail, enable temporal queries ("what did this order look like yesterday at 3pm?"), and decouple the write model from the read model so each can scale independently. Most engineers have heard of these patterns but have never implemented them. Building one is a strong differentiator.

### Architecture Diagram

```
  Clients                                                                     
   │                                                                          
   │  Commands (POST/PUT/DELETE)          Queries (GET)                       
   ▼                                      ▼                                  
┌──────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES CLUSTER                                    │
│                                                                              │
│  ┌────────────────────────┐       ┌────────────────────────┐                 │
│  │  Command Service (Go)  │       │  Query Service (Go)    │                │
│  │                        │       │                        │                │
│  │  - Validates commands  │       │  - Reads from          │                │
│  │  - Loads aggregate     │       │    materialized views  │                │
│  │    from event store    │       │  - Optimized for       │                │
│  │  - Applies business    │       │    read patterns       │                │
│  │    rules               │       │  - Independently       │                │
│  │  - Appends new events  │       │    scalable            │                │
│  │    to event store      │       │                        │                │
│  └───────────┬────────────┘       └───────────┬────────────┘                │
│              │                                │                             │
│              ▼                                │  reads from                 │
│  ┌────────────────────────┐                   │                             │
│  │  Event Store           │                   │                             │
│  │  (PostgreSQL)          │                   │                             │
│  │                        │                   │                             │
│  │  events table:         │                   │                             │
│  │  - aggregate_id        │                   │                             │
│  │  - aggregate_type      │                   │                             │
│  │  - event_type          │                   │                             │
│  │  - event_data (JSONB)  │                   │                             │
│  │  - version (optimistic │                   │                             │
│  │    concurrency)        │                   │                             │
│  │  - timestamp           │                   │                             │
│  └───────────┬────────────┘                   │                             │
│              │                                │                             │
│              │  CDC (Debezium) or             │                             │
│              │  application-level publish      │                             │
│              ▼                                │                             │
│  ┌────────────────────────┐                   │                             │
│  │  Apache Kafka          │                   │                             │
│  │                        │                   │                             │
│  │  topic: order-events   │                   │                             │
│  │  topic: payment-events │                   │                             │
│  │  topic: inventory-events│                  │                             │
│  └───────────┬────────────┘                   │                             │
│              │                                │                             │
│     ┌────────┴────────┬────────────┐          │                             │
│     ▼                 ▼            ▼          │                             │
│  ┌──────────┐  ┌───────────┐  ┌─────────┐    │                             │
│  │Projector │  │Projector  │  │Projector│    │                             │
│  │(Orders   │  │(Inventory │  │(Revenue │    │                             │
│  │ Read     │  │ Read      │  │ Read    │    │                             │
│  │ Model)   │  │ Model)    │  │ Model)  │    │                             │
│  └────┬─────┘  └─────┬─────┘  └────┬────┘    │                             │
│       │              │              │         │                             │
│       ▼              ▼              ▼         │                             │
│  ┌──────────────────────────────────────┐     │                             │
│  │  Read Database (PostgreSQL)          │◀────┘                             │
│  │                                      │                                   │
│  │  orders_view (denormalized)         │                                   │
│  │  inventory_view                     │                                   │
│  │  revenue_daily_view                 │                                   │
│  │  customer_order_history_view        │                                   │
│  └──────────────────────────────────────┘                                    │
│                                                                              │
│  ┌──────────────────────────────────────┐                                    │
│  │  Saga Orchestrator (Go)             │                                    │
│  │                                     │                                    │
│  │  OrderSaga:                         │                                    │
│  │    1. ReserveInventory              │                                    │
│  │    2. ProcessPayment                │                                    │
│  │    3. ConfirmOrder                  │                                    │
│  │    (with compensating actions       │                                    │
│  │     on failure)                     │                                    │
│  └──────────────────────────────────────┘                                    │
│                                                                              │
│  Observability: Prometheus + Grafana + Jaeger + Loki                         │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Key Implementation Details

#### The Event Store

This is the heart of the system. Every state change is stored as an immutable event:

```sql
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL,       -- 'Order', 'Product', 'Customer'
    event_type      VARCHAR(100) NOT NULL,       -- 'OrderCreated', 'ItemAdded', etc.
    event_data      JSONB NOT NULL,
    metadata        JSONB,                       -- trace_id, user_id, causation_id
    version         INTEGER NOT NULL,            -- per-aggregate version for optimistic concurrency
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(aggregate_id, version)                -- prevents concurrent writes to same aggregate
);

CREATE INDEX idx_events_aggregate ON events (aggregate_id, version ASC);
CREATE INDEX idx_events_type ON events (aggregate_type, created_at);
```

**Optimistic Concurrency:**

The `UNIQUE(aggregate_id, version)` constraint is critical. When two concurrent commands try to modify the same order, one of them will fail with a unique constraint violation. The application catches this and retries:

```go
func (s *OrderCommandService) AddItem(ctx context.Context, orderID uuid.UUID, item OrderItem) error {
    for retries := 0; retries < 3; retries++ {
        // Load current state by replaying events
        order, currentVersion := s.loadAggregate(ctx, orderID)
        
        // Apply business rules
        if order.Status != "draft" {
            return ErrOrderNotEditable
        }
        if order.ItemCount() >= 50 {
            return ErrTooManyItems
        }
        
        // Create the event
        event := Event{
            AggregateID:   orderID,
            AggregateType: "Order",
            EventType:     "ItemAdded",
            EventData:     toJSON(ItemAddedData{ProductID: item.ProductID, Quantity: item.Quantity, Price: item.Price}),
            Version:       currentVersion + 1,
        }
        
        // Append (will fail if another writer incremented the version first)
        err := s.eventStore.Append(ctx, event)
        if err == ErrVersionConflict {
            continue  // retry with fresh state
        }
        return err
    }
    return ErrConcurrencyConflict
}
```

#### Aggregate Reconstruction

To get the current state of an order, replay all its events:

```go
type Order struct {
    ID         uuid.UUID
    CustomerID uuid.UUID
    Items      []OrderItem
    Status     string       // draft, confirmed, paid, shipped, delivered, cancelled
    Total      int64        // cents
    Version    int
}

func (o *Order) Apply(event Event) {
    switch event.EventType {
    case "OrderCreated":
        var data OrderCreatedData
        json.Unmarshal(event.EventData, &data)
        o.ID = event.AggregateID
        o.CustomerID = data.CustomerID
        o.Status = "draft"
        
    case "ItemAdded":
        var data ItemAddedData
        json.Unmarshal(event.EventData, &data)
        o.Items = append(o.Items, OrderItem{
            ProductID: data.ProductID,
            Quantity:  data.Quantity,
            Price:     data.Price,
        })
        o.Total += data.Price * int64(data.Quantity)
        
    case "ItemRemoved":
        // find and remove the item, update total
        
    case "OrderConfirmed":
        o.Status = "confirmed"
        
    case "PaymentReceived":
        o.Status = "paid"
        
    case "OrderShipped":
        var data OrderShippedData
        json.Unmarshal(event.EventData, &data)
        o.Status = "shipped"
        
    case "OrderCancelled":
        o.Status = "cancelled"
    }
    
    o.Version = event.Version
}

func (s *Store) LoadAggregate(ctx context.Context, id uuid.UUID) (*Order, int) {
    events := s.eventStore.GetEvents(ctx, id)
    order := &Order{}
    for _, e := range events {
        order.Apply(e)
    }
    return order, order.Version
}
```

**Snapshotting (performance optimization):**

For aggregates with hundreds of events, replaying from the beginning is slow. Store periodic snapshots:

```sql
CREATE TABLE snapshots (
    aggregate_id    UUID NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL,
    version         INTEGER NOT NULL,
    state           JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_id, version)
);
```

When loading, first check for the latest snapshot, then replay only events after the snapshot's version. Create a new snapshot every 50 events.

#### Projectors (Building Read Models)

Projectors consume events from Kafka and build denormalized read models optimized for specific query patterns:

```go
// The Orders Read Model projector
type OrderProjector struct {
    readDB *sql.DB
}

func (p *OrderProjector) HandleEvent(event Event) error {
    switch event.EventType {
    case "OrderCreated":
        var data OrderCreatedData
        json.Unmarshal(event.EventData, &data)
        _, err := p.readDB.Exec(`
            INSERT INTO orders_view (id, customer_id, status, total, item_count, created_at)
            VALUES ($1, $2, 'draft', 0, 0, $3)
        `, event.AggregateID, data.CustomerID, event.CreatedAt)
        return err
        
    case "ItemAdded":
        var data ItemAddedData
        json.Unmarshal(event.EventData, &data)
        _, err := p.readDB.Exec(`
            UPDATE orders_view 
            SET total = total + $1, item_count = item_count + 1, updated_at = NOW()
            WHERE id = $2
        `, data.Price*int64(data.Quantity), event.AggregateID)
        return err
        
    case "OrderShipped":
        _, err := p.readDB.Exec(`
            UPDATE orders_view SET status = 'shipped', shipped_at = NOW() WHERE id = $1
        `, event.AggregateID)
        return err
    }
    return nil
}
```

**Multiple Read Models from the Same Events:**

This is the power of CQRS. The same `order-events` Kafka topic feeds multiple projectors:

- `orders_view` — for the "my orders" page (denormalized with customer name, latest status, item count).
- `revenue_daily_view` — for the admin dashboard (aggregated daily revenue by product category).
- `customer_order_history_view` — for customer support (full order history with timeline).
- `inventory_view` — updated by the inventory projector when `ItemAdded` events are processed (decrement available stock).

Each projector runs independently and can be rebuilt from scratch by replaying all events from Kafka (set consumer group offset to 0). This means if you discover a bug in how the revenue view was calculated, you fix the projector code and replay — no data migration needed.

#### The Saga Orchestrator

Placing an order involves multiple services (inventory, payment, shipping). If payment fails after inventory was reserved, you need to release the inventory. The Saga pattern handles this:

```go
type OrderSaga struct {
    steps []SagaStep
}

type SagaStep struct {
    Name       string
    Execute    func(ctx context.Context, data SagaData) error
    Compensate func(ctx context.Context, data SagaData) error
}

func NewOrderSaga() *OrderSaga {
    return &OrderSaga{
        steps: []SagaStep{
            {
                Name:       "reserve_inventory",
                Execute:    reserveInventory,
                Compensate: releaseInventory,
            },
            {
                Name:       "process_payment",
                Execute:    processPayment,
                Compensate: refundPayment,
            },
            {
                Name:       "confirm_order",
                Execute:    confirmOrder,
                Compensate: cancelOrder,
            },
        },
    }
}

func (s *OrderSaga) Run(ctx context.Context, data SagaData) error {
    completedSteps := []int{}
    
    for i, step := range s.steps {
        if err := step.Execute(ctx, data); err != nil {
            // Compensate in reverse order
            for j := len(completedSteps) - 1; j >= 0; j-- {
                idx := completedSteps[j]
                if compErr := s.steps[idx].Compensate(ctx, data); compErr != nil {
                    // Log compensation failure for manual intervention
                    log.Error("compensation failed", "step", s.steps[idx].Name, "error", compErr)
                }
            }
            return fmt.Errorf("saga failed at step %s: %w", step.Name, err)
        }
        completedSteps = append(completedSteps, i)
    }
    return nil
}
```

**Temporal Query (time-travel):**

One of the unique benefits of event sourcing. To see what an order looked like at a specific point in time:

```go
func (s *Store) LoadAggregateAt(ctx context.Context, id uuid.UUID, at time.Time) *Order {
    events := s.eventStore.GetEventsBefore(ctx, id, at)
    order := &Order{}
    for _, e := range events {
        order.Apply(e)
    }
    return order
}
```

This is invaluable for debugging ("what was the order state when the payment was attempted?") and compliance ("show me the account balance on December 31st for the audit").

### Load Testing

- Measure write throughput to the event store (target: 10,000+ events/sec).
- Measure read model query latency (target: under 10ms p99, since it is just reading from a denormalized table).
- Measure projection lag: the delay between an event being written and the read model being updated (target: under 1 second).
- Test saga compensation: kill the payment service mid-saga and verify that inventory is correctly released.

### Cost Management

- Everything runs in Docker Compose locally. PostgreSQL for the event store, PostgreSQL for the read database (or the same instance with separate schemas), Kafka for event distribution.
- No cloud services required for development. This is one of the cheapest projects to run.

---
---