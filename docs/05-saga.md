# 05 — The Saga Pattern

## The problem

Placing an order spans multiple concerns: reserve inventory, charge the card, mark
the order paid. These touch different services/resources, so you **cannot** wrap them
in one ACID transaction. If payment fails *after* inventory was reserved, you must
*release* that inventory — otherwise stock leaks.

A **saga** is the answer: model the workflow as a sequence of local steps, each with
a **compensating action** that semantically undoes it. If a step fails, run the
compensations for all completed steps, in reverse. You trade atomicity for
**eventual consistency with explicit rollback**.

## The generic engine

`internal/saga/saga.go` — a step is an execute + a compensate:

```go
type Step struct {
    Name       string
    Execute    func(ctx, Data) error
    Compensate func(ctx, Data) error
}
```

`Saga.Run` executes steps in order, tracking which completed. On the first failure it
compensates the completed steps **in reverse order**:

```
exec A ✓   exec B ✓   exec C ✗
                    └─ compensate B, then compensate A   (C never completed → not compensated)
```

The unit test `TestSagaCompensatesInReverseOrder` pins exactly this sequence:
`exec-A, exec-B, exec-C, comp-B, comp-A`.

A `Data map[string]any` is threaded through every step so outputs of one step
(a payment ref, a reservation id) are available to later steps and to compensations.

## The place-order saga

`internal/saga/order_saga.go` builds three steps:

| Step               | Execute                          | Compensate            |
|--------------------|----------------------------------|-----------------------|
| `reserve_inventory`| reserve stock (simulated)        | release inventory     |
| `process_payment`  | charge card (simulated)          | refund payment        |
| `mark_order_paid`  | **real** call to command-service `POST /pay` (appends `PaymentReceived`) | cancel the order |

The mix is intentional and instructive: a saga step can be a **local action**, a
**simulation**, or a **remote service call** — all coordinated the same way. The saga
talks to other services through their **APIs** (`internal/saga/client.go`), never
their databases. It is a coordinator, not a back door.

## Watch compensation happen

`PlaceOrderInput.FailPayment` forces `process_payment` to fail. This is the plan's
load-test scenario — "kill the payment service mid-saga and verify inventory is
released" — made reproducible:

```bash
SAGA=http://localhost:18082 ; CMD=http://localhost:18080
ORDER=$(curl -s -X POST $CMD/orders -d '{"customer_id":"'$(uuidgen)'"}' | jq -r .id)
curl -s -X POST $CMD/orders/$ORDER/items -d '{"product_id":"'$(uuidgen)'","quantity":1,"price":999}'
curl -s -X POST $CMD/orders/$ORDER/confirm
curl -s -X POST $SAGA/sagas/place-order -d "{\"order_id\":\"$ORDER\",\"amount\":999,\"fail_payment\":true}" | jq
```

In the saga logs you'll see `reserve_inventory` execute, `process_payment` fail, then
`inventory released` (compensation). Because `mark_order_paid` never ran, the order
stays `confirmed` — there was nothing to cancel. The endpoint returns `409` with
`{"status":"failed_and_compensated"}`.

## Orchestration vs. choreography

There are two saga styles:

- **Orchestration** (used here): one coordinator explicitly drives the steps. Easy to
  read, easy to see the whole flow and its rollback in one place.
- **Choreography**: no coordinator; each service reacts to events and emits the next.
  More decoupled, but the flow is implicit and harder to follow.

Orchestration is the right teaching choice and a common production choice for flows
with clear, ordered steps.

## The hard part: compensation can fail

A compensating action is itself a distributed call and can fail. `Saga.compensate`
logs such failures loudly (`compensation FAILED - manual intervention required`) and
continues compensating the rest. Real systems back this with **retries + a
dead-letter queue + alerting**, and design compensations to be **idempotent** (safe
to run twice). There is no magic that makes rollback across services free — sagas make
the rollback *explicit and testable*, which is the point.
