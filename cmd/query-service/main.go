// Query service: the READ side of CQRS.
//
// It serves GET endpoints straight from the denormalized read models, so queries
// are simple, index-friendly, and fast (no event replay on the hot path). It also
// exposes one endpoint that DOES touch the event store: the temporal "what did this
// order look like at time T?" query, which is a superpower unique to event sourcing.
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

	readPool, err := db.Connect(ctx, config.Env("READMODEL_DSN", "postgres://eventflow:eventflow@localhost:5433/readmodel?sslmode=disable"))
	if err != nil {
		slog.Error("connect read model", "error", err)
		os.Exit(1)
	}
	defer readPool.Close()

	// The event-store connection is used only for the temporal query endpoint.
	eventPool, err := db.Connect(ctx, config.Env("EVENTSTORE_DSN", "postgres://eventflow:eventflow@localhost:5432/eventstore?sslmode=disable"))
	if err != nil {
		slog.Error("connect event store", "error", err)
		os.Exit(1)
	}
	defer eventPool.Close()

	h := &handler{
		read: readPool,
		repo: order.NewRepository(eventstore.New(eventPool)),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	mux.HandleFunc("GET /orders/{id}/history", h.getHistory)
	mux.HandleFunc("GET /orders/{id}/at", h.getOrderAt)
	mux.HandleFunc("GET /customers/{id}/orders", h.listCustomerOrders)
	mux.HandleFunc("GET /admin/revenue", h.getRevenue)

	addr := config.Env("HTTP_ADDR", ":8081")
	srv := &http.Server{Addr: addr, Handler: httpx.Logging(mux)}
	go func() {
		slog.Info("query service listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type handler struct {
	read *pgxpool.Pool
	repo *order.Repository
}

type orderView struct {
	ID             string     `json:"id"`
	CustomerID     string     `json:"customer_id"`
	Status         string     `json:"status"`
	Total          int64      `json:"total"`
	ItemCount      int        `json:"item_count"`
	TrackingNumber *string    `json:"tracking_number,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ShippedAt      *time.Time `json:"shipped_at,omitempty"`
}

func (h *handler) getOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var o orderView
	err := h.read.QueryRow(r.Context(), `
		SELECT id, customer_id, status, total, item_count, tracking_number, created_at, updated_at, shipped_at
		FROM orders_view WHERE id = $1
	`, id).Scan(&o.ID, &o.CustomerID, &o.Status, &o.Total, &o.ItemCount, &o.TrackingNumber, &o.CreatedAt, &o.UpdatedAt, &o.ShippedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.Error(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		slog.Error("get order", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	httpx.JSON(w, http.StatusOK, o)
}

func (h *handler) listCustomerOrders(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	rows, err := h.read.Query(r.Context(), `
		SELECT id, customer_id, status, total, item_count, tracking_number, created_at, updated_at, shipped_at
		FROM orders_view WHERE customer_id = $1 ORDER BY created_at DESC
	`, id)
	if err != nil {
		slog.Error("list orders", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()

	out := []orderView{}
	for rows.Next() {
		var o orderView
		if err := rows.Scan(&o.ID, &o.CustomerID, &o.Status, &o.Total, &o.ItemCount, &o.TrackingNumber, &o.CreatedAt, &o.UpdatedAt, &o.ShippedAt); err != nil {
			slog.Error("scan order", "error", err)
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		out = append(out, o)
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (h *handler) getHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	rows, err := h.read.Query(r.Context(), `
		SELECT event_type, description, occurred_at
		FROM customer_order_history_view WHERE order_id = $1 ORDER BY occurred_at ASC, id ASC
	`, id)
	if err != nil {
		slog.Error("get history", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()

	type entry struct {
		EventType   string    `json:"event_type"`
		Description string    `json:"description"`
		OccurredAt  time.Time `json:"occurred_at"`
	}
	out := []entry{}
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.EventType, &e.Description, &e.OccurredAt); err != nil {
			slog.Error("scan history", "error", err)
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		out = append(out, e)
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (h *handler) getRevenue(w http.ResponseWriter, r *http.Request) {
	day := r.URL.Query().Get("day") // optional YYYY-MM-DD filter
	var (
		rows pgx.Rows
		err  error
	)
	if day != "" {
		rows, err = h.read.Query(r.Context(), `
			SELECT day, customer_id, revenue, order_count FROM revenue_daily_view WHERE day = $1 ORDER BY revenue DESC
		`, day)
	} else {
		rows, err = h.read.Query(r.Context(), `
			SELECT day, customer_id, revenue, order_count FROM revenue_daily_view ORDER BY day DESC, revenue DESC LIMIT 100
		`)
	}
	if err != nil {
		slog.Error("get revenue", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rows.Close()

	type row struct {
		Day        time.Time `json:"day"`
		CustomerID string    `json:"customer_id"`
		Revenue    int64     `json:"revenue"`
		OrderCount int       `json:"order_count"`
	}
	out := []row{}
	for rows.Next() {
		var rw row
		if err := rows.Scan(&rw.Day, &rw.CustomerID, &rw.Revenue, &rw.OrderCount); err != nil {
			slog.Error("scan revenue", "error", err)
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		out = append(out, rw)
	}
	httpx.JSON(w, http.StatusOK, out)
}

// getOrderAt is the temporal query: reconstruct the order's state as of ?t=RFC3339
// by replaying events up to that instant. This reads the event store, not a read
// model, because read models only hold "now".
func (h *handler) getOrderAt(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r.PathValue("id"))
	if !ok {
		return
	}
	raw := r.URL.Query().Get("t")
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "query param t must be RFC3339, e.g. 2026-09-25T15:00:00Z")
		return
	}
	o, err := h.repo.LoadAt(r.Context(), id, at)
	if err != nil {
		slog.Error("load at", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !o.Exists() {
		httpx.Error(w, http.StatusNotFound, "order did not exist at that time")
		return
	}
	httpx.JSON(w, http.StatusOK, o)
}

func parseID(w http.ResponseWriter, raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid id: "+raw)
		return uuid.Nil, false
	}
	return id, true
}
