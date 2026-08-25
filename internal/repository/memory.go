package repository

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"awesomeProject1/internal/model"
)

var (
	ErrNotFound = errors.New("not found")
)

type Repository interface {
	UpsertEndpoint(ctx context.Context, endpoint model.Endpoint) error
	GetEndpoint(ctx context.Context, code string) (model.Endpoint, bool, error)
	ListEndpoints(ctx context.Context) ([]model.Endpoint, error)

	CreateJob(ctx context.Context, job model.NotificationJob) (model.NotificationJob, bool, error)
	GetJob(ctx context.Context, notificationID string) (model.NotificationJob, bool, error)
	GetJobByBusinessRequest(ctx context.Context, business, requestID string) (model.NotificationJob, bool, error)
	UpdateJob(ctx context.Context, job model.NotificationJob) error
	ClaimJob(ctx context.Context, notificationID string, now time.Time, lockFor time.Duration) (model.NotificationJob, bool, error)
	ListRunnableJobs(ctx context.Context, now time.Time, limit int) ([]model.NotificationJob, error)

	AppendAttempt(ctx context.Context, attempt model.NotificationAttempt) error
	ListAttempts(ctx context.Context, notificationID string) ([]model.NotificationAttempt, error)
}

type MemoryRepository struct {
	mu                   sync.RWMutex
	endpoints            map[string]model.Endpoint
	jobs                 map[string]model.NotificationJob
	businessRequestIndex map[string]string
	attempts             map[string][]model.NotificationAttempt
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		endpoints:            make(map[string]model.Endpoint),
		jobs:                 make(map[string]model.NotificationJob),
		businessRequestIndex: make(map[string]string),
		attempts:             make(map[string][]model.NotificationAttempt),
	}
}

func businessRequestKey(business, requestID string) string {
	return business + "\x00" + requestID
}

func (r *MemoryRepository) UpsertEndpoint(_ context.Context, endpoint model.Endpoint) error {
	if strings.TrimSpace(endpoint.Code) == "" {
		return errors.New("endpoint code is required")
	}
	if strings.TrimSpace(endpoint.URL) == "" {
		return errors.New("endpoint url is required")
	}
	if strings.TrimSpace(endpoint.Method) == "" {
		return errors.New("endpoint method is required")
	}
	if endpoint.Timeout <= 0 {
		endpoint.Timeout = model.NewMillisDuration(5 * time.Second)
	}
	if endpoint.RateLimitWindow <= 0 {
		endpoint.RateLimitWindow = model.NewMillisDuration(time.Second)
	}
	if endpoint.MaxRetryCount <= 0 {
		endpoint.MaxRetryCount = 10
	}
	if endpoint.RateLimit <= 0 {
		endpoint.RateLimit = 100
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.endpoints[endpoint.Code] = cloneEndpoint(endpoint)
	return nil
}

func (r *MemoryRepository) GetEndpoint(_ context.Context, code string) (model.Endpoint, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	endpoint, ok := r.endpoints[code]
	if !ok {
		return model.Endpoint{}, false, nil
	}
	return cloneEndpoint(endpoint), true, nil
}

func (r *MemoryRepository) ListEndpoints(_ context.Context) ([]model.Endpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	endpoints := make([]model.Endpoint, 0, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		endpoints = append(endpoints, cloneEndpoint(endpoint))
	}
	sort.Slice(endpoints, func(i, j int) bool {
		return endpoints[i].Code < endpoints[j].Code
	})
	return endpoints, nil
}

func (r *MemoryRepository) CreateJob(_ context.Context, job model.NotificationJob) (model.NotificationJob, bool, error) {
	if strings.TrimSpace(job.NotificationID) == "" {
		return model.NotificationJob{}, false, errors.New("notification id is required")
	}
	if strings.TrimSpace(job.Business) == "" {
		return model.NotificationJob{}, false, errors.New("business is required")
	}
	if strings.TrimSpace(job.RequestID) == "" {
		return model.NotificationJob{}, false, errors.New("request id is required")
	}
	if job.Timeout <= 0 {
		job.Timeout = model.NewMillisDuration(5 * time.Second)
	}
	if job.MaxRetryCount <= 0 {
		job.MaxRetryCount = 10
	}

	key := businessRequestKey(job.Business, job.RequestID)

	r.mu.Lock()
	defer r.mu.Unlock()

	if existingID, ok := r.businessRequestIndex[key]; ok {
		return cloneJob(r.jobs[existingID]), true, nil
	}

	createdAt := job.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	job.CreatedAt = createdAt
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = createdAt
	}

	r.jobs[job.NotificationID] = cloneJob(job)
	r.businessRequestIndex[key] = job.NotificationID
	return cloneJob(job), false, nil
}

func (r *MemoryRepository) GetJob(_ context.Context, notificationID string) (model.NotificationJob, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	job, ok := r.jobs[notificationID]
	if !ok {
		return model.NotificationJob{}, false, nil
	}
	return cloneJob(job), true, nil
}

func (r *MemoryRepository) GetJobByBusinessRequest(_ context.Context, business, requestID string) (model.NotificationJob, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.businessRequestIndex[businessRequestKey(business, requestID)]
	if !ok {
		return model.NotificationJob{}, false, nil
	}
	job, ok := r.jobs[id]
	if !ok {
		return model.NotificationJob{}, false, nil
	}
	return cloneJob(job), true, nil
}

func (r *MemoryRepository) UpdateJob(_ context.Context, job model.NotificationJob) error {
	if strings.TrimSpace(job.NotificationID) == "" {
		return errors.New("notification id is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.jobs[job.NotificationID]; !ok {
		return ErrNotFound
	}

	r.jobs[job.NotificationID] = cloneJob(job)
	return nil
}

func (r *MemoryRepository) ClaimJob(_ context.Context, notificationID string, now time.Time, lockFor time.Duration) (model.NotificationJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobs[notificationID]
	if !ok {
		return model.NotificationJob{}, false, ErrNotFound
	}

	if !claimable(job, now) {
		return cloneJob(job), false, nil
	}

	job.Status = model.JobStatusProcessing
	job.LockedUntil = ptrTime(now.Add(lockFor))
	job.UpdatedAt = now.UTC()
	r.jobs[notificationID] = job

	return cloneJob(job), true, nil
}

func claimable(job model.NotificationJob, now time.Time) bool {
	switch job.Status {
	case model.JobStatusPending:
		return true
	case model.JobStatusRetryWait:
		return job.NextRetryAt == nil || !job.NextRetryAt.After(now)
	case model.JobStatusProcessing:
		return job.LockedUntil != nil && !job.LockedUntil.After(now)
	default:
		return false
	}
}

func (r *MemoryRepository) ListRunnableJobs(_ context.Context, now time.Time, limit int) ([]model.NotificationJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]model.NotificationJob, 0)
	for _, job := range r.jobs {
		if !claimable(job, now) {
			continue
		}
		jobs = append(jobs, cloneJob(job))
	}

	sort.Slice(jobs, func(i, j int) bool {
		li := runnableSortTime(jobs[i])
		lj := runnableSortTime(jobs[j])
		if li.Equal(lj) {
			return jobs[i].NotificationID < jobs[j].NotificationID
		}
		return li.Before(lj)
	})

	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}

	return jobs, nil
}

func runnableSortTime(job model.NotificationJob) time.Time {
	switch job.Status {
	case model.JobStatusPending:
		return job.CreatedAt
	case model.JobStatusRetryWait:
		if job.NextRetryAt != nil {
			return *job.NextRetryAt
		}
		return job.CreatedAt
	case model.JobStatusProcessing:
		if job.LockedUntil != nil {
			return *job.LockedUntil
		}
		return job.CreatedAt
	default:
		return job.CreatedAt
	}
}

func (r *MemoryRepository) AppendAttempt(_ context.Context, attempt model.NotificationAttempt) error {
	if strings.TrimSpace(attempt.NotificationID) == "" {
		return errors.New("notification id is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.jobs[attempt.NotificationID]; !ok {
		return ErrNotFound
	}

	if attempt.CreatedAt.IsZero() {
		attempt.CreatedAt = time.Now().UTC()
	}
	r.attempts[attempt.NotificationID] = append(r.attempts[attempt.NotificationID], cloneAttempt(attempt))
	return nil
}

func (r *MemoryRepository) ListAttempts(_ context.Context, notificationID string) ([]model.NotificationAttempt, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	attempts := r.attempts[notificationID]
	out := make([]model.NotificationAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		out = append(out, cloneAttempt(attempt))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AttemptNo == out[j].AttemptNo {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].AttemptNo < out[j].AttemptNo
	})
	return out, nil
}

func cloneEndpoint(in model.Endpoint) model.Endpoint {
	out := in
	out.Headers = cloneStringMap(in.Headers)
	out.BodyTemplate = cloneAnyMap(in.BodyTemplate)
	out.Secrets = cloneStringMap(in.Secrets)
	out.RetryHTTPStatuses = cloneStatusSet(in.RetryHTTPStatuses)
	return out
}

func cloneJob(in model.NotificationJob) model.NotificationJob {
	out := in
	out.Payload = cloneAnyMap(in.Payload)
	out.RequestHeaders = cloneStringMap(in.RequestHeaders)
	if in.NextRetryAt != nil {
		next := *in.NextRetryAt
		out.NextRetryAt = &next
	}
	if in.LockedUntil != nil {
		locked := *in.LockedUntil
		out.LockedUntil = &locked
	}
	if in.HTTPStatus != nil {
		status := *in.HTTPStatus
		out.HTTPStatus = &status
	}
	return out
}

func cloneAttempt(in model.NotificationAttempt) model.NotificationAttempt {
	out := in
	if in.HTTPStatus != nil {
		status := *in.HTTPStatus
		out.HTTPStatus = &status
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStatusSet(in map[int]struct{}) map[int]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[int]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneAny(v)
	}
	return out
}

func cloneAny(in any) any {
	switch v := in.(type) {
	case map[string]any:
		return cloneAnyMap(v)
	case map[string]string:
		return cloneStringMap(v)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = cloneAny(v[i])
		}
		return out
	case []string:
		out := make([]string, len(v))
		copy(out, v)
		return out
	default:
		return v
	}
}

func ptrTime(t time.Time) *time.Time {
	out := t.UTC()
	return &out
}
