package service

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"awesomeProject1/sev/delivery"
	"awesomeProject1/sev/model"
	"awesomeProject1/sev/ratelimit"
	"awesomeProject1/sev/repository"
	"awesomeProject1/sev/retry"
)

type DeliveryService struct {
	repo           repository.Repository
	limiter        ratelimit.Limiter
	client         delivery.Doer
	clock          Clock
	lockFor        time.Duration
	retryBase      time.Duration
	retryMax       time.Duration
	rateLimitBase  time.Duration
	rateLimitMax   time.Duration
	jitterFraction float64
	randomFloat    func() float64
}

func NewDeliveryService(repo repository.Repository, limiter ratelimit.Limiter, client delivery.Doer, clock Clock, randomFloat func() float64) *DeliveryService {
	if clock == nil {
		clock = time.Now
	}
	if client == nil {
		client = delivery.NewHTTPClient()
	}
	if randomFloat == nil {
		randomFloat = rand.Float64
	}
	return &DeliveryService{
		repo:           repo,
		limiter:        limiter,
		client:         client,
		clock:          clock,
		lockFor:        30 * time.Second,
		retryBase:      5 * time.Second,
		retryMax:       30 * time.Minute,
		rateLimitBase:  500 * time.Millisecond,
		rateLimitMax:   2 * time.Second,
		jitterFraction: 0.2,
		randomFloat:    randomFloat,
	}
}

func (s *DeliveryService) Deliver(ctx context.Context, notificationID string) error {
	job, ok, err := s.repo.GetJob(ctx, notificationID)
	if err != nil {
		return err
	}
	if !ok {
		return repository.ErrNotFound
	}
	if job.Status.Terminal() {
		return nil
	}

	now := s.clock().UTC()
	claimed, ok, err := s.repo.ClaimJob(ctx, notificationID, now, s.lockFor)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	job = claimed

	endpoint, ok, err := s.repo.GetEndpoint(ctx, job.EndpointCode)
	if err != nil {
		return err
	}
	if !ok || !endpoint.Enabled {
		return s.finishWithFailure(ctx, job, 0, model.AttemptResultFailed, model.FailReasonInvalidConfig, 0, "endpoint missing or disabled")
	}

	attemptNo := job.RetryCount + 1
	start := s.clock().UTC()

	allowed, limiterErr := s.allow(ctx, endpoint)
	if limiterErr != nil {
		return s.finishRetry(ctx, job, attemptNo, model.AttemptResultRateLimitError, model.FailReasonRateLimitError, 0, limiterErr.Error())
	}
	if !allowed {
		return s.finishRetry(ctx, job, attemptNo, model.AttemptResultRateLimited, model.FailReasonRateLimited, 0, "rate limited")
	}

	req, cancel, err := s.buildRequest(ctx, job)
	if err != nil {
		return s.finishWithFailure(ctx, job, 0, model.AttemptResultFailed, model.FailReasonInvalidConfig, time.Since(start), err.Error())
	}
	defer cancel()

	resp, err := s.client.Do(req)
	if err != nil {
		reason, retryable := classifyError(err)
		if retryable {
			return s.finishRetry(ctx, job, attemptNo, model.AttemptResultFailed, reason, time.Since(start), err.Error())
		}
		return s.finishWithFailure(ctx, job, 0, model.AttemptResultFailed, reason, time.Since(start), err.Error())
	}
	defer resp.Body.Close()

	bodySnippet, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode <= 299:
		return s.finishSuccess(ctx, job, resp.StatusCode, time.Since(start))
	case isRetryableStatus(endpoint, resp.StatusCode):
		return s.finishRetry(ctx, job, attemptNo, model.AttemptResultFailed, reasonForStatus(resp.StatusCode), time.Since(start), string(bodySnippet), resp.StatusCode)
	default:
		return s.finishWithFailure(ctx, job, resp.StatusCode, model.AttemptResultFailed, reasonForStatus(resp.StatusCode), time.Since(start), string(bodySnippet))
	}
}

func (s *DeliveryService) allow(ctx context.Context, endpoint model.Endpoint) (bool, error) {
	if s.limiter == nil {
		return true, nil
	}
	return s.limiter.Allow(ctx, endpoint.Code, endpoint.RateLimit, endpoint.RateLimitWindow.Duration())
}

func (s *DeliveryService) buildRequest(ctx context.Context, job model.NotificationJob) (*http.Request, context.CancelFunc, error) {
	var bodyReader io.Reader
	if job.RequestBody != "" {
		bodyReader = strings.NewReader(job.RequestBody)
	}

	timeout := job.Timeout.Duration()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	req, err := http.NewRequestWithContext(reqCtx, job.RequestMethod, job.RequestURL, bodyReader)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	req.Header = make(http.Header, len(job.RequestHeaders))
	for key, value := range job.RequestHeaders {
		req.Header.Set(key, value)
	}
	if _, ok := req.Header["Idempotency-Key"]; !ok {
		req.Header.Set("Idempotency-Key", job.NotificationID)
	}
	return req, cancel, nil
}

func (s *DeliveryService) finishSuccess(ctx context.Context, job model.NotificationJob, httpStatus int, duration time.Duration) error {
	job.Status = model.JobStatusSuccess
	job.FailReason = ""
	job.RetryCount = job.RetryCount
	job.NextRetryAt = nil
	job.LockedUntil = nil
	job.HTTPStatus = &httpStatus
	job.LastError = ""
	job.UpdatedAt = s.clock().UTC()
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return err
	}
	return s.repo.AppendAttempt(ctx, model.NotificationAttempt{
		NotificationID: job.NotificationID,
		AttemptNo:      job.RetryCount + 1,
		Result:         model.AttemptResultSuccess,
		HTTPStatus:     &httpStatus,
		DurationMS:     int(duration / time.Millisecond),
		CreatedAt:      s.clock().UTC(),
	})
}

func (s *DeliveryService) finishWithFailure(ctx context.Context, job model.NotificationJob, httpStatus int, result model.AttemptResult, reason model.FailReason, duration time.Duration, errMsg string) error {
	job.Status = model.JobStatusFailed
	job.FailReason = reason
	job.NextRetryAt = nil
	job.LockedUntil = nil
	job.HTTPStatus = nil
	if httpStatus > 0 {
		job.HTTPStatus = &httpStatus
	}
	job.LastError = truncateError(errMsg)
	job.UpdatedAt = s.clock().UTC()
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return err
	}
	return s.repo.AppendAttempt(ctx, model.NotificationAttempt{
		NotificationID: job.NotificationID,
		AttemptNo:      job.RetryCount + 1,
		Result:         result,
		FailReason:     reason,
		HTTPStatus:     job.HTTPStatus,
		DurationMS:     int(duration / time.Millisecond),
		ErrorMessage:   truncateError(errMsg),
		CreatedAt:      s.clock().UTC(),
	})
}

func (s *DeliveryService) finishRetry(ctx context.Context, job model.NotificationJob, attemptNo int, result model.AttemptResult, reason model.FailReason, duration time.Duration, errMsg string, httpStatuses ...int) error {
	if attemptNo > job.MaxRetryCount && job.MaxRetryCount > 0 {
		return s.finishWithFailure(ctx, job, firstHTTPStatus(httpStatuses...), model.AttemptResultFailed, model.FailReasonMaxRetryExceeded, duration, errMsg)
	}

	job.Status = model.JobStatusRetryWait
	job.FailReason = reason
	job.RetryCount = attemptNo
	job.LockedUntil = nil
	if len(httpStatuses) > 0 && httpStatuses[0] > 0 {
		job.HTTPStatus = &httpStatuses[0]
	} else {
		job.HTTPStatus = nil
	}
	job.LastError = truncateError(errMsg)
	delay := s.nextDelay(reason, attemptNo)
	nextRetry := s.clock().UTC().Add(delay)
	job.NextRetryAt = &nextRetry
	job.UpdatedAt = s.clock().UTC()
	if err := s.repo.UpdateJob(ctx, job); err != nil {
		return err
	}
	return s.repo.AppendAttempt(ctx, model.NotificationAttempt{
		NotificationID: job.NotificationID,
		AttemptNo:      attemptNo,
		Result:         result,
		FailReason:     reason,
		HTTPStatus:     job.HTTPStatus,
		DurationMS:     int(duration / time.Millisecond),
		ErrorMessage:   truncateError(errMsg),
		CreatedAt:      s.clock().UTC(),
	})
}

func (s *DeliveryService) nextDelay(reason model.FailReason, attemptNo int) time.Duration {
	switch reason {
	case model.FailReasonRateLimited, model.FailReasonRateLimitError:
		return retry.CalculateNextRetry(attemptNo, s.rateLimitBase, s.rateLimitMax, s.jitterFraction, s.randomFloat)
	default:
		return retry.CalculateNextRetry(attemptNo, s.retryBase, s.retryMax, s.jitterFraction, s.randomFloat)
	}
}

func isRetryableStatus(endpoint model.Endpoint, statusCode int) bool {
	if _, ok := endpoint.RetryHTTPStatuses[statusCode]; ok {
		return true
	}
	return false
}

func reasonForStatus(statusCode int) model.FailReason {
	switch statusCode {
	case http.StatusTooManyRequests:
		return model.FailReasonHTTP429
	case http.StatusRequestTimeout:
		return model.FailReasonRequestTimeout
	default:
		if statusCode >= 500 && statusCode <= 599 {
			return model.FailReasonHTTP5xx
		}
		if statusCode >= 400 && statusCode <= 499 {
			return model.FailReasonHTTP4xx
		}
		return model.FailReasonHTTPStatus
	}
}

func classifyError(err error) (model.FailReason, bool) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return model.FailReasonRequestTimeout, true
	case isTimeoutError(err):
		return model.FailReasonRequestTimeout, true
	case isDNSError(err):
		return model.FailReasonDNSError, true
	case strings.Contains(strings.ToLower(err.Error()), "connection refused"):
		return model.FailReasonConnectionRefused, true
	default:
		return model.FailReasonNetworkError, true
	}
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if strings.Contains(strings.ToLower(err.Error()), "timeout") {
		return true
	}
	return false
}

func isDNSError(err error) bool {
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "no such host") || strings.Contains(lower, "server misbehaving") || strings.Contains(lower, "temporary failure in name resolution")
}

func truncateError(errMsg string) string {
	const maxLen = 2048
	if len(errMsg) <= maxLen {
		return errMsg
	}
	return errMsg[:maxLen]
}

func firstHTTPStatus(statuses ...int) int {
	for _, status := range statuses {
		if status > 0 {
			return status
		}
	}
	return 0
}
