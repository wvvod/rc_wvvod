package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"awesomeProject1/sev/delivery"
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/repository"
)

func TestNotificationServiceCreateNotificationIsIdempotent(t *testing.T) {
	repo := repository.NewMemoryRepository()
	builder := delivery.NewTemplateBuilder()
	now := time.Unix(1700000000, 0).UTC()
	nextID := 0

	svc := NewNotificationService(repo, builder, func() time.Time { return now }, func() string {
		nextID++
		return fmt.Sprintf("ntf_test_%d", nextID)
	})

	ctx := context.Background()
	endpoint, err := svc.CreateEndpoint(ctx, model.CreateEndpointRequest{
		EndpointCode:      "crm_payment_success",
		Name:              "CRM payment notify",
		URL:               "https://crm.example.com/api/contact",
		Method:            "POST",
		ContentType:       model.ContentTypeJSON,
		BodyMode:          model.BodyModeTemplate,
		Headers:           map[string]string{"Authorization": "Bearer {{secret.crm_token}}"},
		BodyTemplate:      map[string]any{"contact_id": "{{payload.user_id}}", "amount": "{{payload.amount}}"},
		Secrets:           map[string]string{"crm_token": "tok_123"},
		TimeoutMS:         5000,
		RateLimit:         100,
		RateLimitWindowMS: 1000,
	})
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}

	job, created, err := svc.CreateNotification(ctx, model.CreateNotificationRequest{
		RequestID:    "payment_1",
		Business:     "payment",
		EndpointCode: endpoint.Code,
		Payload: map[string]any{
			"user_id": 100,
			"amount":  199,
		},
	})
	if err != nil {
		t.Fatalf("create notification: %v", err)
	}
	if !created {
		t.Fatalf("expected notification to be created")
	}
	if job.NotificationID != "ntf_test_1" {
		t.Fatalf("unexpected notification id: %s", job.NotificationID)
	}
	if got := job.RequestHeaders["Idempotency-Key"]; got != job.NotificationID {
		t.Fatalf("expected idempotency key header, got %q", got)
	}
	if job.Status != model.JobStatusPending {
		t.Fatalf("expected pending status, got %s", job.Status)
	}

	duplicate, created, err := svc.CreateNotification(ctx, model.CreateNotificationRequest{
		RequestID:    "payment_1",
		Business:     "payment",
		EndpointCode: endpoint.Code,
		Payload: map[string]any{
			"user_id": 100,
			"amount":  199,
		},
	})
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	if created {
		t.Fatalf("expected duplicate create to be idempotent")
	}
	if duplicate.NotificationID != job.NotificationID {
		t.Fatalf("expected duplicate to return same id")
	}

	job.Status = model.JobStatusFailed
	if err := repo.UpdateJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	retryJob, err := svc.RetryNotification(ctx, job.NotificationID)
	if err != nil {
		t.Fatalf("retry notification: %v", err)
	}
	if retryJob.Status != model.JobStatusRetryWait {
		t.Fatalf("expected retry wait status, got %s", retryJob.Status)
	}
	if retryJob.NextRetryAt == nil || !retryJob.NextRetryAt.Equal(now) {
		t.Fatalf("expected next retry at now, got %#v", retryJob.NextRetryAt)
	}
}
