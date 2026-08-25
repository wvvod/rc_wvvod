package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"awesomeProject1/sev/delivery"
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/ratelimit"
	"awesomeProject1/sev/repository"
)

type fakeLimiter struct {
	allow bool
	err   error
}

func (f fakeLimiter) Allow(context.Context, string, int, time.Duration) (bool, error) {
	return f.allow, f.err
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newDeliveryHarness(t *testing.T, now time.Time, limiter ratelimit.Limiter, client delivery.Doer) (*repository.MemoryRepository, *NotificationService, *DeliveryService) {
	t.Helper()

	repo := repository.NewMemoryRepository()
	builder := delivery.NewTemplateBuilder()
	notificationService := NewNotificationService(repo, builder, func() time.Time { return now }, func() string {
		return fmt.Sprintf("ntf_%d", now.UnixNano())
	})
	deliveryService := NewDeliveryService(repo, limiter, client, func() time.Time { return now }, func() float64 { return 0.5 })
	return repo, notificationService, deliveryService
}

func createTestEndpointAndJob(t *testing.T, svc *NotificationService, endpointURL string) model.NotificationJob {
	t.Helper()

	ctx := context.Background()
	endpoint, err := svc.CreateEndpoint(ctx, model.CreateEndpointRequest{
		EndpointCode:      "crm_payment_success",
		Name:              "CRM payment notify",
		URL:               endpointURL,
		Method:            "POST",
		ContentType:       model.ContentTypeJSON,
		BodyMode:          model.BodyModePassThrough,
		Headers:           map[string]string{"X-App-Id": "notification-service"},
		TimeoutMS:         2000,
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
	return job
}

func TestDeliveryServiceSuccess(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	var capturedMethod string
	var capturedID string
	var capturedBody map[string]any

	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedMethod = req.Method
			capturedID = req.Header.Get("Idempotency-Key")
			body, _ := io.ReadAll(req.Body)
			_ = req.Body.Close()
			_ = jsonUnmarshal(body, &capturedBody)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("ok")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	repo, notificationService, deliveryService := newDeliveryHarness(t, now, fakeLimiter{allow: true}, client)
	job := createTestEndpointAndJob(t, notificationService, "https://vendor.example.com/api")

	if err := deliveryService.Deliver(context.Background(), job.NotificationID); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	stored, ok, err := repo.GetJob(context.Background(), job.NotificationID)
	if err != nil || !ok {
		t.Fatalf("get job: ok=%v err=%v", ok, err)
	}
	if stored.Status != model.JobStatusSuccess {
		t.Fatalf("expected success, got %s", stored.Status)
	}
	if stored.HTTPStatus == nil || *stored.HTTPStatus != http.StatusOK {
		t.Fatalf("expected http 200, got %#v", stored.HTTPStatus)
	}

	attempts, err := repo.ListAttempts(context.Background(), job.NotificationID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
	if attempts[0].Result != model.AttemptResultSuccess {
		t.Fatalf("expected success attempt, got %s", attempts[0].Result)
	}
	if capturedMethod != http.MethodPost {
		t.Fatalf("expected POST, got %s", capturedMethod)
	}
	if capturedID != job.NotificationID {
		t.Fatalf("expected idempotency key %s, got %s", job.NotificationID, capturedID)
	}
	if got := capturedBody["amount"]; got != float64(199) {
		t.Fatalf("expected payload body, got %#v", got)
	}
}

func TestDeliveryServiceRetryableHTTPFailure(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	client := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(bytes.NewBufferString("try later")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	repo, notificationService, deliveryService := newDeliveryHarness(t, now, fakeLimiter{allow: true}, client)
	job := createTestEndpointAndJob(t, notificationService, "https://vendor.example.com/api")

	if err := deliveryService.Deliver(context.Background(), job.NotificationID); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	stored, ok, err := repo.GetJob(context.Background(), job.NotificationID)
	if err != nil || !ok {
		t.Fatalf("get job: ok=%v err=%v", ok, err)
	}
	if stored.Status != model.JobStatusRetryWait {
		t.Fatalf("expected retry wait, got %s", stored.Status)
	}
	if stored.FailReason != model.FailReasonHTTP5xx {
		t.Fatalf("expected HTTP_5XX, got %s", stored.FailReason)
	}
	if stored.RetryCount != 1 {
		t.Fatalf("expected retry count 1, got %d", stored.RetryCount)
	}
	if stored.NextRetryAt == nil || !stored.NextRetryAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("expected next retry at +5s, got %#v", stored.NextRetryAt)
	}

	attempts, err := repo.ListAttempts(context.Background(), job.NotificationID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Result != model.AttemptResultFailed {
		t.Fatalf("unexpected attempts: %#v", attempts)
	}
	if attempts[0].HTTPStatus == nil || *attempts[0].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 attempt status, got %#v", attempts[0].HTTPStatus)
	}
}

func TestDeliveryServiceRateLimited(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	client := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			t.Fatalf("vendor should not be called when rate limited")
			return nil, errors.New("unexpected vendor call")
		}),
	}

	repo, notificationService, deliveryService := newDeliveryHarness(t, now, fakeLimiter{allow: false}, client)
	job := createTestEndpointAndJob(t, notificationService, "https://vendor.example.com/api")

	if err := deliveryService.Deliver(context.Background(), job.NotificationID); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	stored, ok, err := repo.GetJob(context.Background(), job.NotificationID)
	if err != nil || !ok {
		t.Fatalf("get job: ok=%v err=%v", ok, err)
	}
	if stored.Status != model.JobStatusRetryWait {
		t.Fatalf("expected retry wait, got %s", stored.Status)
	}
	if stored.FailReason != model.FailReasonRateLimited {
		t.Fatalf("expected rate limited fail reason, got %s", stored.FailReason)
	}

	attempts, err := repo.ListAttempts(context.Background(), job.NotificationID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Result != model.AttemptResultRateLimited {
		t.Fatalf("unexpected attempts: %#v", attempts)
	}
}

func TestDeliveryServiceNetworkErrorRetries(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	client := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp: connection refused")
		}),
	}

	repo, notificationService, deliveryService := newDeliveryHarness(t, now, fakeLimiter{allow: true}, client)
	job := createTestEndpointAndJob(t, notificationService, "https://vendor.example.com/api")

	if err := deliveryService.Deliver(context.Background(), job.NotificationID); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	stored, ok, err := repo.GetJob(context.Background(), job.NotificationID)
	if err != nil || !ok {
		t.Fatalf("get job: ok=%v err=%v", ok, err)
	}
	if stored.Status != model.JobStatusRetryWait {
		t.Fatalf("expected retry wait, got %s", stored.Status)
	}
	if stored.FailReason != model.FailReasonConnectionRefused {
		t.Fatalf("expected connection refused reason, got %s", stored.FailReason)
	}
}
func jsonUnmarshal(data []byte, dst any) error {
	return json.NewDecoder(bytes.NewReader(data)).Decode(dst)
}
