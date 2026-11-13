package service

import (
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/repository"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestRateLimitDoesNotExhaustRetryBudget(t *testing.T) {
	now := time.Now().UTC()
	repo, notifications, delivery := newDeliveryHarness(t, now, fakeLimiter{allow: false}, &http.Client{})
	job := createTestEndpointAndJob(t, notifications, "https://example.com")
	for i := 0; i < 15; i++ {
		if err := delivery.Deliver(context.Background(), job.NotificationID); err != nil {
			t.Fatal(err)
		}
		stored, _, _ := repo.GetJob(context.Background(), job.NotificationID)
		if stored.Status != model.JobStatusRetryWait || stored.RetryCount != 0 {
			t.Fatal(stored)
		}
		now = now.Add(time.Minute)
		delivery.clock = func() time.Time { return now }
	}
}
func TestIdempotencyRejectsDifferentPayload(t *testing.T) {
	now := time.Now().UTC()
	_, notifications, _ := newDeliveryHarness(t, now, fakeLimiter{allow: true}, &http.Client{})
	job := createTestEndpointAndJob(t, notifications, "https://example.com")
	_, _, err := notifications.CreateNotification(context.Background(), model.CreateNotificationRequest{Business: job.Business, RequestID: job.RequestID, EndpointCode: job.EndpointCode, Payload: map[string]any{"different": true}})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatal(err)
	}
}
