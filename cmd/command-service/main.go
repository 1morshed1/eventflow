// Command service: the WRITE side of CQRS.
//
// It exposes an HTTP API for commands (create order, add item, confirm, cancel...).
// Each command loads the order aggregate from the event store, applies business
// rules, and appends new events. It never serves query/read traffic - that is the
// query service's job. This separation is the whole point of CQRS.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/1morshed1/eventflow/internal/config"
	"github.com/1morshed1/eventflow/internal/db"
	"github.com/1morshed1/eventflow/internal/domain/order"
	"github.com/1morshed1/eventflow/internal/eventstore"
	"github.com/1morshed1/eventflow/internal/httpx"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.Env("EVENTSTORE_DSN", "postgres://eventflow:eventflow@localhost:5432/eventstore?sslmode=disable")
	pool, err := db.Connect(ctx, dsn)
	if err != nil {
		slog.Error("connect event store", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool, config.Env("EVENTSTORE_MIGRATIONS", "/migrations/eventstore")); err != nil {
		slog.Error("migrate event store", "error", err)
		os.Exit(1)
	}

	svc := order.NewService(order.NewRepository(eventstore.New(pool)))
	h := &handler{svc: svc}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", h.createOrder)
	mux.HandleFunc("POST /orders/{id}/items", h.addItem)
	mux.HandleFunc("DELETE /orders/{id}/items/{productID}", h.removeItem)
	mux.HandleFunc("POST /orders/{id}/confirm", h.confirm)
	mux.HandleFunc("POST /orders/{id}/cancel", h.cancel)
	mux.HandleFunc("POST /orders/{id}/pay", h.pay)
	mux.HandleFunc("POST /orders/{id}/ship", h.ship)

	addr := config.Env("HTTP_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: httpx.Logging(mux)}

	go func() {
		slog.Info("command service listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type handler struct {
	svc *order.Service
}

// httpStatus maps domain errors to HTTP status codes. Business-rule violations are
// client errors (4xx); anything unrecognized is a 500.
func httpStatus(err error) int {
	switch {
	case errors.Is(err, order.ErrOrderNotFound):
		return http.StatusNotFound
	case errors.Is(err, order.ErrOrderNotEditable),
		errors.Is(err, order.ErrTooManyItems),
		errors.Is(err, order.ErrEmptyOrder),
		errors.Is(err, order.ErrInvalidState):
		return http.StatusConflict
	case errors.Is(err, order.ErrConcurrencyConflict):
		return http.StatusServiceUnavailable // transient; client may retry
	default:
		return http.StatusInternalServerError
	}
}

func (h *handler) fail(w http.ResponseWriter, err error) {
	status := httpStatus(err)
	if status == http.StatusInternalServerError {
		slog.Error("command failed", "error", err)
		httpx.Error(w, status, "internal error")
		return
	}
	httpx.Error(w, status, err.Error())
}

func (h *handler) createOrder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CustomerID uuid.UUID `json:"customer_id"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	id, err := h.svc.Create(r.Context(), body.CustomerID)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]string{"id": id.String()})
}

func (h *handler) addItem(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var body struct {
		ProductID uuid.UUID `json:"product_id"`
		Quantity  int       `json:"quantity"`
		Price     int64     `json:"price"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	if body.Quantity <= 0 || body.Price < 0 {
		httpx.Error(w, http.StatusBadRequest, "quantity must be > 0 and price >= 0")
		return
	}
	if err := h.svc.AddItem(r.Context(), id, body.ProductID, body.Quantity, body.Price); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

func (h *handler) removeItem(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	productID, ok := parseID(w, r.PathValue("productID"))
	if !ok {
		return
	}
	if err := h.svc.RemoveItem(r.Context(), id, productID); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

func (h *handler) confirm(w http.ResponseWriter, r *http.Request) {
	h.simpleCommand(w, r, func(ctx context.Context, id uuid.UUID) error {
		return h.svc.Confirm(ctx, id)
	})
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	_ = httpx.Decode(w, r, &body) // reason is optional
	if err := h.svc.Cancel(r.Context(), id, body.Reason); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

func (h *handler) pay(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var body struct {
		Amount     int64  `json:"amount"`
		PaymentRef string `json:"payment_ref"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	if err := h.svc.MarkPaid(r.Context(), id, body.Amount, body.PaymentRef); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

func (h *handler) ship(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var body struct {
		TrackingNumber string `json:"tracking_number"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	if err := h.svc.Ship(r.Context(), id, body.TrackingNumber); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

// simpleCommand handles the common shape: parse id, run a command, return 202.
func (h *handler) simpleCommand(w http.ResponseWriter, r *http.Request, fn func(context.Context, uuid.UUID) error) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	if err := fn(r.Context(), id); err != nil {
		h.fail(w, err)
		return
	}
	httpx.JSON(w, http.StatusAccepted, nil)
}

func parseID(w http.ResponseWriter, raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid id: "+raw)
		return uuid.Nil, false
	}
	return id, true
}
