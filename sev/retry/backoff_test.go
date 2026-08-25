package retry

import (
	"testing"
	"time"
)

func TestCalculateNextRetryBaseAndCap(t *testing.T) {
	base := 5 * time.Second
	max := 30 * time.Minute

	if got := CalculateNextRetry(1, base, max, 0, nil); got != 5*time.Second {
		t.Fatalf("retry 1: expected 5s, got %s", got)
	}
	if got := CalculateNextRetry(3, base, max, 0, nil); got != 20*time.Second {
		t.Fatalf("retry 3: expected 20s, got %s", got)
	}
	if got := CalculateNextRetry(20, base, max, 0, nil); got != max {
		t.Fatalf("retry 20: expected max delay, got %s", got)
	}
}

func TestCalculateNextRetryDeterministicJitter(t *testing.T) {
	got := CalculateNextRetry(2, 5*time.Second, 30*time.Minute, 0.2, func() float64 { return 0.5 })
	if got != 10*time.Second {
		t.Fatalf("expected jitter-neutral 10s, got %s", got)
	}
}
