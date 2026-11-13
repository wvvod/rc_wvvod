package repository

import (
	"awesomeProject1/sev/model"
	"context"
	"errors"
	"testing"
	"time"
)

func TestStaleCompletionCannotOverwriteNewLease(t *testing.T) {
	ctx := context.Background()
	r := NewMemoryRepository()
	now := time.Now().UTC()
	_, _, err := r.CreateJob(ctx, model.NotificationJob{NotificationID: "n", Business: "b", RequestID: "r", Status: model.JobStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	old, ok, err := r.ClaimJob(ctx, "n", now, time.Second)
	if err != nil || !ok {
		t.Fatal("claim", err)
	}
	fresh, ok, err := r.ClaimJob(ctx, "n", now.Add(2*time.Second), time.Second)
	if err != nil || !ok {
		t.Fatal("reclaim", err)
	}
	old.Status = model.JobStatusSuccess
	if err := r.FinishAttempt(ctx, old, model.NotificationAttempt{NotificationID: "n"}); !errors.Is(err, ErrConflict) {
		t.Fatal("stale completion accepted", err)
	}
	fresh.Status = model.JobStatusFailed
	if err := r.FinishAttempt(ctx, fresh, model.NotificationAttempt{NotificationID: "n"}); err != nil {
		t.Fatal(err)
	}
	attempts, _ := r.ListAttempts(ctx, "n")
	if len(attempts) != 1 {
		t.Fatal(attempts)
	}
	job, err := r.RetryFailed(ctx, "n", now)
	if err != nil || job.RetryCount != 0 {
		t.Fatal(job, err)
	}
	if _, err := r.RetryFailed(ctx, "n", now); !errors.Is(err, ErrConflict) {
		t.Fatal("active retry accepted", err)
	}
}
