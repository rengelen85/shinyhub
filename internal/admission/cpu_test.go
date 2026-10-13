package admission

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestAvailableCPURejectsInvalidCapacity(t *testing.T) {
	for _, cores := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := AvailableCPUCores(context.Background(), cores, "config"); err == nil {
			t.Fatalf("invalid capacity %v admitted", cores)
		}
	}
}

func TestAvailableCPUHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := AvailableCPUCores(ctx, 4, "config"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sample: %v", err)
	}
}
