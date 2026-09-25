// Saga orchestrator service.
//
// Exposes POST /sagas/place-order to run the place-order saga (reserve inventory ->
// process payment -> mark order paid) with automatic compensation on failure. It
// calls the command service to append the real payment/cancel events.
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
	"github.com/1morshed1/eventflow/internal/httpx"
	"github.com/1morshed1/eventflow/internal/saga"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	commandURL := config.Env("COMMAND_SERVICE_URL", "http://localhost:8080")
	orch := saga.NewOrchestrator(saga.NewCommandClient(commandURL, &http.Client{Timeout: 10 * time.Second}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /sagas/place-order", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			OrderID     uuid.UUID `json:"order_id"`
			Amount      int64     `json:"amount"`
			FailPayment bool      `json:"fail_payment"`
		}
		if !httpx.Decode(w, r, &body) {
			return
		}
		err := orch.Run(r.Context(), saga.PlaceOrderInput{
			OrderID:     body.OrderID,
			Amount:      body.Amount,
			FailPayment: body.FailPayment,
		})
		if err != nil {
			// The saga already compensated; report the failure so the caller knows the
			// order was not placed.
			httpx.JSON(w, http.StatusConflict, map[string]string{
				"status": "failed_and_compensated",
				"error":  err.Error(),
			})
			return
		}
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "completed"})
	})

	addr := config.Env("HTTP_ADDR", ":8082")
	srv := &http.Server{Addr: addr, Handler: httpx.Logging(mux)}
	go func() {
		slog.Info("saga service listening", "addr", addr)
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
