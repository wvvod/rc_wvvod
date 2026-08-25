package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryLimiterFixedWindow(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	limiter := NewMemoryLimiter(func() time.Time { return now })

	allowed, err := limiter.Allow(context.Background(), "crm_payment", 2, time.Second)
	if err != nil || !allowed {
		t.Fatalf("first allow failed: allowed=%v err=%v", allowed, err)
	}
	allowed, err = limiter.Allow(context.Background(), "crm_payment", 2, time.Second)
	if err != nil || !allowed {
		t.Fatalf("second allow failed: allowed=%v err=%v", allowed, err)
	}
	allowed, err = limiter.Allow(context.Background(), "crm_payment", 2, time.Second)
	if err != nil {
		t.Fatalf("third allow err: %v", err)
	}
	if allowed {
		t.Fatalf("expected third call to be denied")
	}

	now = now.Add(time.Second)
	allowed, err = limiter.Allow(context.Background(), "crm_payment", 2, time.Second)
	if err != nil || !allowed {
		t.Fatalf("window reset allow failed: allowed=%v err=%v", allowed, err)
	}
}

func TestMemoryLimiterInvalidConfig(t *testing.T) {
	limiter := NewMemoryLimiter(time.Now)
	if allowed, err := limiter.Allow(context.Background(), "", 1, time.Second); err == nil || allowed {
		t.Fatalf("expected invalid endpoint config error")
	}
}
