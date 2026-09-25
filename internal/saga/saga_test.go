package saga

import (
	"context"
	"errors"
	"testing"
)

// TestSagaCompensatesInReverseOrder verifies the core saga guarantee: when a step
// fails, all previously completed steps are compensated in reverse order.
func TestSagaCompensatesInReverseOrder(t *testing.T) {
	var order []string

	s := &Saga{
		Name: "test",
		Steps: []Step{
			{
				Name:       "A",
				Execute:    func(context.Context, Data) error { order = append(order, "exec-A"); return nil },
				Compensate: func(context.Context, Data) error { order = append(order, "comp-A"); return nil },
			},
			{
				Name:       "B",
				Execute:    func(context.Context, Data) error { order = append(order, "exec-B"); return nil },
				Compensate: func(context.Context, Data) error { order = append(order, "comp-B"); return nil },
			},
			{
				Name:       "C",
				Execute:    func(context.Context, Data) error { order = append(order, "exec-C"); return errors.New("boom") },
				Compensate: func(context.Context, Data) error { order = append(order, "comp-C"); return nil },
			},
		},
	}

	err := s.Run(context.Background(), Data{})
	if err == nil {
		t.Fatal("expected error from failing step C")
	}

	// C failed, so C is NOT compensated (it never completed). B then A are, in reverse.
	want := []string{"exec-A", "exec-B", "exec-C", "comp-B", "comp-A"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestSagaHappyPathRunsAllSteps verifies no compensation runs when all steps pass.
func TestSagaHappyPathRunsAllSteps(t *testing.T) {
	executed := 0
	compensated := 0
	step := func(name string) Step {
		return Step{
			Name:       name,
			Execute:    func(context.Context, Data) error { executed++; return nil },
			Compensate: func(context.Context, Data) error { compensated++; return nil },
		}
	}
	s := &Saga{Name: "happy", Steps: []Step{step("A"), step("B")}}
	if err := s.Run(context.Background(), Data{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if executed != 2 {
		t.Errorf("executed = %d, want 2", executed)
	}
	if compensated != 0 {
		t.Errorf("compensated = %d, want 0", compensated)
	}
}
