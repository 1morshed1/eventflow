// Package saga implements the Saga pattern for coordinating a multi-step business
// transaction across services without distributed (2-phase-commit) transactions.
//
// A saga is a sequence of local steps. Each step has an Execute action and a
// Compensate action that semantically undoes it. If any step fails, the already-
// completed steps are compensated in reverse order. This gives us eventual
// consistency and a clear recovery story, which is how real systems (payments,
// travel booking, e-commerce checkout) coordinate work that spans services.
package saga

import (
	"context"
	"fmt"
	"log/slog"
)

// Data is the shared state threaded through every step. Steps read inputs from it
// and write outputs (e.g. a payment reference) back into it for later steps and for
// compensation.
type Data map[string]any

// Step is one unit of a saga.
type Step struct {
	Name       string
	Execute    func(ctx context.Context, d Data) error
	Compensate func(ctx context.Context, d Data) error
}

// Saga is an ordered list of steps.
type Saga struct {
	Name  string
	Steps []Step
}

// Run executes the steps in order. On the first failure it compensates every
// completed step in reverse order, then returns the original error. A compensation
// that itself fails is logged (it needs manual intervention) but does not stop the
// remaining compensations - we still try to undo everything we can.
func (s *Saga) Run(ctx context.Context, d Data) error {
	completed := make([]int, 0, len(s.Steps))

	for i, step := range s.Steps {
		slog.Info("saga step executing", "saga", s.Name, "step", step.Name)
		if err := step.Execute(ctx, d); err != nil {
			slog.Warn("saga step failed, compensating", "saga", s.Name, "step", step.Name, "error", err)
			s.compensate(ctx, d, completed)
			return fmt.Errorf("saga %s failed at step %s: %w", s.Name, step.Name, err)
		}
		completed = append(completed, i)
	}
	slog.Info("saga completed", "saga", s.Name)
	return nil
}

// compensate undoes completed steps in reverse order.
func (s *Saga) compensate(ctx context.Context, d Data, completed []int) {
	for j := len(completed) - 1; j >= 0; j-- {
		step := s.Steps[completed[j]]
		if step.Compensate == nil {
			continue
		}
		slog.Info("saga compensating", "saga", s.Name, "step", step.Name)
		if err := step.Compensate(ctx, d); err != nil {
			// A failed compensation is the dangerous case: the system is now in a
			// state that needs a human or a retry job. We surface it loudly.
			slog.Error("saga compensation FAILED - manual intervention required",
				"saga", s.Name, "step", step.Name, "error", err)
		}
	}
}
