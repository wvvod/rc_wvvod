package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"awesomeProject1/internal/delivery"
	"awesomeProject1/internal/model"
	"awesomeProject1/internal/ratelimit"
	"awesomeProject1/internal/repository"
	"awesomeProject1/internal/service"
)

type apiEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHandlerCreateQueryAndRetryFlow(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	repo := repository.NewMemoryRepository()
	builder := delivery.NewTemplateBuilder()
	notificationService := service.NewNotificationService(repo, builder, func() time.Time { return now }, func() string {
		return fmt.Sprintf("ntf_%d", now.UnixNano())
	})
	client := &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("ok")),
				Header:     make(http.Header),
			}, nil
		}),
	}
	deliveryService := service.NewDeliveryService(repo, ratelimit.NewMemoryLimiter(time.Now), client, func() time.Time { return now }, func() float64 { return 0.5 })
	handler := NewHandler(notificationService, deliveryService)

	endpointReq := model.CreateEndpointRequest{
		EndpointCode:      "crm_payment_success",
		Name:              "CRM payment notify",
		URL:               "https://vendor.example.com/api",
		Method:            "POST",
		ContentType:       model.ContentTypeJSON,
		BodyMode:          model.BodyModePassThrough,
		Headers:           map[string]string{"X-App-Id": "notification-service"},
		TimeoutMS:         2000,
		RateLimit:         100,
		RateLimitWindowMS: 1000,
	}
	if resp := mustDoJSONRequest(t, handler, http.MethodPost, "/api/v1/endpoints", endpointReq); resp.Code != http.StatusCreated {
		t.Fatalf("expected create endpoint 201, got %d", resp.Code)
	}

	createReq := model.CreateNotificationRequest{
		RequestID:    "payment_1001",
		Business:     "payment",
		EndpointCode: "crm_payment_success",
		Payload: map[string]any{
			"user_id": 100,
			"amount":  199,
		},
	}
	resp := mustDoJSONRequest(t, handler, http.MethodPost, "/api/v1/notifications", createReq)
	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected accepted, got %d", resp.Code)
	}

	var createData model.CreateNotificationResponse
	if err := json.Unmarshal(resp.Data, &createData); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createData.NotificationID == "" {
		t.Fatalf("expected notification id")
	}

	duplicate := mustDoJSONRequest(t, handler, http.MethodPost, "/api/v1/notifications", createReq)
	if duplicate.Code != http.StatusAccepted {
		t.Fatalf("expected duplicate accepted, got %d", duplicate.Code)
	}
	var duplicateData model.CreateNotificationResponse
	if err := json.Unmarshal(duplicate.Data, &duplicateData); err != nil {
		t.Fatalf("decode duplicate response: %v", err)
	}
	if duplicateData.NotificationID != createData.NotificationID {
		t.Fatalf("expected duplicate to return same notification id")
	}

	job := waitForJobStatus(t, repo, createData.NotificationID, model.JobStatusSuccess)
	if job.Status != model.JobStatusSuccess {
		t.Fatalf("expected success, got %s", job.Status)
	}

	statusResp := doJSONRequest(t, handler, http.MethodGet, "/api/v1/notifications/"+createData.NotificationID, nil)
	if statusResp.Code != http.StatusOK {
		t.Fatalf("expected get status ok, got %d", statusResp.Code)
	}
	var statusData model.NotificationStatusResponse
	if err := json.Unmarshal(statusResp.Data, &statusData); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if statusData.Status != model.JobStatusSuccess {
		t.Fatalf("expected success status, got %s", statusData.Status)
	}

	attemptsResp := doJSONRequest(t, handler, http.MethodGet, "/api/v1/notifications/"+createData.NotificationID+"/attempts", nil)
	if attemptsResp.Code != http.StatusOK {
		t.Fatalf("expected get attempts ok, got %d", attemptsResp.Code)
	}
	var attemptsData []model.NotificationAttemptResponse
	if err := json.Unmarshal(attemptsResp.Data, &attemptsData); err != nil {
		t.Fatalf("decode attempts response: %v", err)
	}
	if len(attemptsData) != 1 {
		t.Fatalf("expected one attempt, got %d", len(attemptsData))
	}
}

func mustDoJSONRequest(t *testing.T, handler http.Handler, method, url string, payload any) apiEnvelope {
	t.Helper()
	return doJSONRequest(t, handler, method, url, payload)
}

func doJSONRequest(t *testing.T, handler http.Handler, method, url string, payload any) apiEnvelope {
	t.Helper()

	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	var envelope apiEnvelope
	if err := json.NewDecoder(rr.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return apiEnvelope{Code: rr.Code, Message: envelope.Message, Data: envelope.Data}
}

func waitForJobStatus(t *testing.T, repo repository.Repository, notificationID string, expected model.JobStatus) model.NotificationJob {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, ok, err := repo.GetJob(context.Background(), notificationID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if ok && job.Status == expected {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}

	job, ok, err := repo.GetJob(context.Background(), notificationID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !ok {
		t.Fatalf("job not found after waiting")
	}
	t.Fatalf("expected status %s, got %s", expected, job.Status)
	return job
}
