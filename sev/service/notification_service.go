package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"awesomeProject1/sev/delivery"
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/repository"
)

type Clock func() time.Time

type NotificationService struct {
	repo    repository.Repository
	builder delivery.RequestBuilder
	clock   Clock
	idGen   func() string
}

func NewNotificationService(repo repository.Repository, builder delivery.RequestBuilder, clock Clock, idGen func() string) *NotificationService {
	if clock == nil {
		clock = time.Now
	}
	if idGen == nil {
		idGen = defaultNotificationID
	}
	return &NotificationService{
		repo:    repo,
		builder: builder,
		clock:   clock,
		idGen:   idGen,
	}
}

func (s *NotificationService) CreateEndpoint(ctx context.Context, req model.CreateEndpointRequest) (model.Endpoint, error) {
	if req.EndpointCode == "" {
		return model.Endpoint{}, fmt.Errorf("endpoint_code is required")
	}
	if req.Name == "" {
		return model.Endpoint{}, fmt.Errorf("name is required")
	}
	if req.URL == "" {
		return model.Endpoint{}, fmt.Errorf("url is required")
	}
	if req.Method == "" {
		return model.Endpoint{}, fmt.Errorf("method is required")
	}
	if !req.ContentType.Valid() {
		return model.Endpoint{}, fmt.Errorf("invalid content_type %q", req.ContentType)
	}
	if !req.BodyMode.Valid() {
		return model.Endpoint{}, fmt.Errorf("invalid body_mode %q", req.BodyMode)
	}

	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	rateWindow := time.Duration(req.RateLimitWindowMS) * time.Millisecond
	if rateWindow <= 0 {
		rateWindow = time.Second
	}
	rateLimit := req.RateLimit
	if rateLimit <= 0 {
		rateLimit = 100
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	maxRetryCount := req.MaxRetryCount
	if maxRetryCount <= 0 {
		maxRetryCount = 10
	}

	retryStatuses := defaultRetryHTTPStatuses()
	if len(req.RetryHTTPStatuses) > 0 {
		retryStatuses = make(map[int]struct{}, len(req.RetryHTTPStatuses))
		for _, code := range req.RetryHTTPStatuses {
			retryStatuses[code] = struct{}{}
		}
	}

	endpoint := model.Endpoint{
		Code:              req.EndpointCode,
		Name:              req.Name,
		URL:               req.URL,
		Method:            req.Method,
		ContentType:       req.ContentType,
		BodyMode:          req.BodyMode,
		Headers:           cloneStringMap(req.Headers),
		BodyTemplate:      cloneAnyMap(req.BodyTemplate),
		Secrets:           cloneStringMap(req.Secrets),
		Timeout:           model.NewMillisDuration(timeout),
		RateLimit:         rateLimit,
		RateLimitWindow:   model.NewMillisDuration(rateWindow),
		Enabled:           enabled,
		MaxRetryCount:     maxRetryCount,
		RetryHTTPStatuses: retryStatuses,
	}

	if err := s.repo.UpsertEndpoint(ctx, endpoint); err != nil {
		return model.Endpoint{}, err
	}
	return endpoint, nil
}

func (s *NotificationService) CreateNotification(ctx context.Context, req model.CreateNotificationRequest) (model.NotificationJob, bool, error) {
	if req.RequestID == "" {
		return model.NotificationJob{}, false, fmt.Errorf("request_id is required")
	}
	if req.Business == "" {
		return model.NotificationJob{}, false, fmt.Errorf("business is required")
	}
	if req.EndpointCode == "" {
		return model.NotificationJob{}, false, fmt.Errorf("endpoint_code is required")
	}
	if req.Payload == nil {
		req.Payload = map[string]any{}
	}

	if existing, ok, err := s.repo.GetJobByBusinessRequest(ctx, req.Business, req.RequestID); err != nil {
		return model.NotificationJob{}, false, err
	} else if ok {
		return existing, false, nil
	}

	endpoint, ok, err := s.repo.GetEndpoint(ctx, req.EndpointCode)
	if err != nil {
		return model.NotificationJob{}, false, err
	}
	if !ok {
		return model.NotificationJob{}, false, fmt.Errorf("endpoint %s not found", req.EndpointCode)
	}

	snapshot, err := s.builder.Build(endpoint, req.Payload)
	if err != nil {
		return model.NotificationJob{}, false, err
	}

	notificationID := s.idGen()
	headers := cloneStringMap(snapshot.Headers)
	if headers == nil {
		headers = make(map[string]string)
	}
	if _, ok := headers["Idempotency-Key"]; !ok {
		headers["Idempotency-Key"] = notificationID
	}

	maxRetryCount := endpoint.MaxRetryCount
	if maxRetryCount <= 0 {
		maxRetryCount = 10
	}
	now := s.clock().UTC()
	job := model.NotificationJob{
		NotificationID: notificationID,
		RequestID:      req.RequestID,
		Business:       req.Business,
		EndpointCode:   req.EndpointCode,
		Payload:        cloneAnyMap(req.Payload),
		RequestURL:     snapshot.URL,
		RequestMethod:  snapshot.Method,
		RequestHeaders: headers,
		RequestBody:    snapshot.Body,
		ContentType:    snapshot.ContentType,
		Timeout:        model.NewMillisDuration(snapshot.Timeout),
		Status:         model.JobStatusPending,
		MaxRetryCount:  maxRetryCount,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	created, existed, err := s.repo.CreateJob(ctx, job)
	if err != nil {
		return model.NotificationJob{}, false, err
	}
	if existed {
		return created, false, nil
	}
	return created, true, nil
}

func (s *NotificationService) GetNotification(ctx context.Context, notificationID string) (model.NotificationJob, bool, error) {
	return s.repo.GetJob(ctx, notificationID)
}

func (s *NotificationService) ListAttempts(ctx context.Context, notificationID string) ([]model.NotificationAttempt, error) {
	return s.repo.ListAttempts(ctx, notificationID)
}

func (s *NotificationService) RetryNotification(ctx context.Context, notificationID string) (model.NotificationJob, error) {
	job, ok, err := s.repo.GetJob(ctx, notificationID)
	if err != nil {
		return model.NotificationJob{}, err
	}
	if !ok {
		return model.NotificationJob{}, repository.ErrNotFound
	}
	if job.Status == model.JobStatusSuccess {
		return model.NotificationJob{}, fmt.Errorf("notification already succeeded")
	}

	now := s.clock().UTC()
	job.Status = model.JobStatusRetryWait
	job.FailReason = ""
	job.NextRetryAt = &now
	job.LockedUntil = nil
	job.UpdatedAt = now
	if job.MaxRetryCount <= 0 {
		job.MaxRetryCount = 10
	}
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return model.NotificationJob{}, err
	}
	return job, nil
}

func defaultRetryHTTPStatuses() map[int]struct{} {
	return map[int]struct{}{
		408: {},
		429: {},
		500: {},
		502: {},
		503: {},
		504: {},
	}
}

var defaultIDCounter atomic.Uint64

func defaultNotificationID() string {
	n := defaultIDCounter.Add(1)
	var randomPart [4]byte
	if _, err := rand.Read(randomPart[:]); err != nil {
		binary.BigEndian.PutUint32(randomPart[:], uint32(time.Now().UnixNano()))
	}
	return fmt.Sprintf("ntf_%d_%s_%06d", time.Now().UTC().UnixNano(), hex.EncodeToString(randomPart[:]), n%1000000)
}
