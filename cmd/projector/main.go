// Projector service: the read-model builders.
//
// It runs several projectors concurrently, each an independent Kafka consumer group
// reading the order-events topic and writing its own denormalized table in the read
// database. Because each projector has its own group and its own checkpoint, they
// progress independently and any one can be rebuilt without touching the others.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/1morshed1/eventflow/internal/config"
	"github.com/1morshed1/eventflow/internal/db"
	"github.com/1morshed1/eventflow/internal/kafka"
	"github.com/1morshed1/eventflow/internal/projection"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := config.Env("READMODEL_DSN", "postgres://eventflow:eventflow@localhost:5433/readmodel?sslmode=disable")
	pool, err := db.Connect(ctx, dsn)
	if err != nil {
		slog.Error("connect read model", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool, config.Env("READMODEL_MIGRATIONS", "/migrations/readmodel")); err != nil {
		slog.Error("migrate read model", "error", err)
		os.Exit(1)
	}

	brokers := config.EnvList("KAFKA_BROKERS", "localhost:9092")
	topic := config.Env("ORDER_TOPIC", "order-events")

	// Create the topic before any consumer joins, avoiding the zero-partition
	// startup race (see kafka.EnsureTopics).
	if err := kafka.EnsureTopics(ctx, brokers, topic); err != nil {
		slog.Error("ensure topics", "error", err)
		os.Exit(1)
	}

	// A rebuild suffix lets you replay from scratch: set REBUILD=v2 to start fresh
	// consumer groups and checkpoints without disturbing the running ones. You would
	// TRUNCATE the target tables first, then swap reads over once caught up.
	rebuild := config.Env("REBUILD", "")

	projectors := []projection.Projector{
		projection.OrdersProjector{},
		projection.RevenueProjector{},
		projection.HistoryProjector{},
	}

	var wg sync.WaitGroup
	runCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	for _, p := range projectors {
		p := p
		runner := projection.NewRunner(pool, brokers, topic, p, rebuild)
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("projector started", "name", p.Name())
			if err := runner.Run(runCtx); err != nil {
				slog.Error("projector stopped", "name", p.Name(), "error", err)
				cancelAll() // one projector dying tears down the rest for a clean restart
			}
		}()
	}

	<-runCtx.Done()
	slog.Info("projector service shutting down")
	wg.Wait()
}
