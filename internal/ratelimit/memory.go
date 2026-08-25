package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrInvalidConfig = errors.New("invalid rate limit config")

type Limiter interface {
	Allow(ctx context.Context, endpoint string, limit int, window time.Duration) (bool, error)
}

type MemoryLimiter struct {
	mu      sync.Mutex
	clock   func() time.Time
	buckets map[string]*windowBucket
}

type windowBucket struct {
	windowStart time.Time
	count       int
}

func NewMemoryLimiter(clock func() time.Time) *MemoryLimiter {
	if clock == nil {
		clock = time.Now
	}
	return &MemoryLimiter{
		clock:   clock,
		buckets: make(map[string]*windowBucket),
	}
}

func (l *MemoryLimiter) Allow(_ context.Context, endpoint string, limit int, window time.Duration) (bool, error) {
	if endpoint == "" {
		return false, ErrInvalidConfig
	}
	if limit <= 0 || window <= 0 {
		return false, ErrInvalidConfig
	}

	now := l.clock().UTC()

	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, ok := l.buckets[endpoint]
	if !ok || !now.Before(bucket.windowStart.Add(window)) {
		bucket = &windowBucket{windowStart: now, count: 0}
		l.buckets[endpoint] = bucket
	}

	bucket.count++
	if bucket.count > limit {
		return false, nil
	}
	return true, nil
}
