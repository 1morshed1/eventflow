#!/usr/bin/env bash
# End-to-end smoke test for EventFlow.
#
# Walks a full order lifecycle through the real services and verifies the CQRS read
# side eventually reflects the writes:
#   1. create order + add items + confirm  (command service, write side)
#   2. run the place-order saga            (saga -> command service)
#   3. read back from the query service    (read side, populated by projectors)
#   4. exercise the temporal query         (event store time travel)
#   5. prove saga compensation on payment failure
#
# Requires the stack to be up (make up) and `jq`.
set -euo pipefail

CMD=${CMD:-http://localhost:8080}
QRY=${QRY:-http://localhost:8081}
SAGA=${SAGA:-http://localhost:8082}

CUSTOMER=$(cat /proc/sys/kernel/random/uuid)
PRODUCT=$(cat /proc/sys/kernel/random/uuid)

say() { printf "\n\033[1;36m== %s\033[0m\n" "$1"; }

wait_for() { # url
  for _ in $(seq 1 60); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timeout waiting for $1" >&2; exit 1
}

say "Waiting for services to be healthy"
wait_for "$CMD/healthz"
wait_for "$QRY/healthz"
wait_for "$SAGA/healthz"

say "Create order"
ORDER=$(curl -fsS -X POST "$CMD/orders" -d "{\"customer_id\":\"$CUSTOMER\"}" | jq -r .id)
echo "order id: $ORDER"

say "Add two items"
curl -fsS -X POST "$CMD/orders/$ORDER/items" -d "{\"product_id\":\"$PRODUCT\",\"quantity\":2,\"price\":1500}" >/dev/null
curl -fsS -X POST "$CMD/orders/$ORDER/items" -d "{\"product_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"quantity\":1,\"price\":500}" >/dev/null

say "Confirm order"
curl -fsS -X POST "$CMD/orders/$ORDER/confirm" >/dev/null

say "Run place-order saga (amount = 3500 cents)"
curl -fsS -X POST "$SAGA/sagas/place-order" -d "{\"order_id\":\"$ORDER\",\"amount\":3500}" | jq .

say "Wait for read model to catch up, then read order (query side)"
for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$QRY/orders/$ORDER" | jq -r .status 2>/dev/null || echo "")
  if [ "$STATUS" = "paid" ]; then break; fi
  sleep 1
done
curl -fsS "$QRY/orders/$ORDER" | jq .
[ "$STATUS" = "paid" ] || { echo "FAIL: expected status=paid, got '$STATUS'"; exit 1; }

say "Order history (support timeline read model)"
curl -fsS "$QRY/orders/$ORDER/history" | jq .

say "Temporal query: state as of now"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -fsS "$QRY/orders/$ORDER/at?t=$NOW" | jq '{status, total, version}'

say "Daily revenue (admin read model)"
curl -fsS "$QRY/admin/revenue" | jq .

say "Saga compensation: force payment failure on a new order"
ORDER2=$(curl -fsS -X POST "$CMD/orders" -d "{\"customer_id\":\"$CUSTOMER\"}" | jq -r .id)
curl -fsS -X POST "$CMD/orders/$ORDER2/items" -d "{\"product_id\":\"$PRODUCT\",\"quantity\":1,\"price\":999}" >/dev/null
curl -fsS -X POST "$CMD/orders/$ORDER2/confirm" >/dev/null
echo "saga (expected to fail + compensate, HTTP 409):"
# Not -f here: a 409 is the CORRECT outcome (saga failed and compensated).
curl -sS -X POST "$SAGA/sagas/place-order" -d "{\"order_id\":\"$ORDER2\",\"amount\":999,\"fail_payment\":true}" | jq .
# The order stays 'confirmed' (never paid): mark_paid never ran, so nothing to cancel.
sleep 3
STATUS2=$(curl -fsS "$QRY/orders/$ORDER2" | jq -r .status)
echo "order2 status after failed saga: $STATUS2"
[ "$STATUS2" = "confirmed" ] || { echo "FAIL: expected confirmed, got '$STATUS2'"; exit 1; }

say "SMOKE TEST PASSED"
