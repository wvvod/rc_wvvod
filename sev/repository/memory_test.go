package repository

import (
	"context"
	"testing"
	"time"

	"awesomeProject1/sev/model"
)

func TestMemoryRepositoryCreateJobIsIdempotent(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()

	job := model.NotificationJob{
		NotificationID: "ntf_1",
		RequestID:      "req_1",
		Business:       "payment",
		EndpointCode:   "crm_payment_success",
		Status:         model.JobStatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	created, existed, err := repo.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if existed {
		t.Fatalf("expected first insert to create new job")
	}
	if created.NotificationID != job.NotificationID {
		t.Fatalf("unexpected notification id: %s", created.NotificationID)
	}

	duplicate, existed, err := repo.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	if !existed {
		t.Fatalf("expected duplicate insert to be detected")
	}
	if duplicate.NotificationID != job.NotificationID {
		t.Fatalf("duplicate should return existing notification id")
	}

	got, ok, err := repo.GetJobByBusinessRequest(ctx, "payment", "req_1")
	if err != nil || !ok {
		t.Fatalf("get by business/request: ok=%v err=%v", ok, err)
	}
	if got.NotificationID != job.NotificationID {
		t.Fatalf("unexpected notification id from lookup: %s", got.NotificationID)
	}
}

func TestMemoryRepositoryRunnableJobsAndAttempts(t *testing.T) {
	repo := NewMemoryRepository()
	ctx := context.Background()
	now := time.Unix(1700000000, 0).UTC()

	job := model.NotificationJob{
		NotificationID: "ntf_2",
		RequestID:      "req_2",
		Business:       "payment",
		EndpointCode:   "crm_payment_success",
		Status:         model.JobStatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if _, _, err := repo.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, ok, err := repo.ClaimJob(ctx, "ntf_2", now, 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("claim pending job: ok=%v err=%v", ok, err)
	}
	if claimed.Status != model.JobStatusProcessing {
		t.Fatalf("expected processing status, got %s", claimed.Status)
	}

	processing := claimed
	past := now.Add(-time.Second)
	processing.LockedUntil = &past
	processing.Status = model.JobStatusProcessing
	processing.UpdatedAt = now
	if err := repo.UpdateJob(ctx, processing); err != nil {
		t.Fatalf("update job: %v", err)
	}

	runnable, err := repo.ListRunnableJobs(ctx, now, 10)
	if err != nil {
		t.Fatalf("list runnable: %v", err)
	}
	if len(runnable) != 1 {
		t.Fatalf("expected one runnable job, got %d", len(runnable))
	}

	if err := repo.AppendAttempt(ctx, model.NotificationAttempt{
		NotificationID: "ntf_2",
		AttemptNo:      2,
		Result:         model.AttemptResultFailed,
		FailReason:     model.FailReasonHTTP5xx,
		CreatedAt:      now.Add(time.Second),
	}); err != nil {
		t.Fatalf("append attempt: %v", err)
	}
	if err := repo.AppendAttempt(ctx, model.NotificationAttempt{
		NotificationID: "ntf_2",
		AttemptNo:      1,
		Result:         model.AttemptResultSuccess,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("append attempt: %v", err)
	}

	attempts, err := repo.ListAttempts(ctx, "ntf_2")
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(attempts))
	}
	if attempts[0].AttemptNo != 1 || attempts[1].AttemptNo != 2 {
		t.Fatalf("attempts not sorted: %#v", attempts)
	}
}
