package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"awesomeProject1/sev/model"
)

func TestRetrySchedulerRunOnceProcessesPendingJob(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	client := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("ok")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	repo, notificationService, deliveryService := newDeliveryHarness(t, now, fakeLimiter{allow: true}, client)
	job := createTestEndpointAndJob(t, notificationService, "https://vendor.example.com/api")

	scheduler := NewRetryScheduler(repo, deliveryService, func() time.Time { return now }, 100, 2, time.Second)
	processed, err := scheduler.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 processed job, got %d", processed)
	}

	stored, ok, err := repo.GetJob(context.Background(), job.NotificationID)
	if err != nil || !ok {
		t.Fatalf("get job: ok=%v err=%v", ok, err)
	}
	if stored.Status != model.JobStatusSuccess {
		t.Fatalf("expected success, got %s", stored.Status)
	}
}
