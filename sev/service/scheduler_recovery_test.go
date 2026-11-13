package service

import (
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/repository"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type flakyRepository struct {
	repository.Repository
	calls atomic.Int32
}

func (r *flakyRepository) ListRunnableJobs(ctx context.Context, now time.Time, limit int) ([]model.NotificationJob, error) {
	if r.calls.Add(1) == 1 {
		return nil, errors.New("temporary DB failure")
	}
	return r.Repository.ListRunnableJobs(ctx, now, limit)
}
func TestSchedulerRecoversAfterScanFailure(t *testing.T) {
	r := &flakyRepository{Repository: repository.NewMemoryRepository()}
	scheduler := NewRetryScheduler(r, nil, time.Now, 10, 1, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for !scheduler.Healthy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if r.calls.Load() < 2 || !scheduler.Healthy() {
		t.Fatal("scheduler did not recover")
	}
}
