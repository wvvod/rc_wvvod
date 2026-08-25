package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"awesomeProject1/sev/model"
	"awesomeProject1/sev/repository"
	"awesomeProject1/sev/service"
)

type Handler struct {
	notifications *service.NotificationService
	delivery      *service.DeliveryService
}

func NewHandler(notifications *service.NotificationService, delivery *service.DeliveryService) *Handler {
	return &Handler{
		notifications: notifications,
		delivery:      delivery,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/notifications":
		h.createNotification(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/notifications/"):
		h.getNotificationSubresource(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/notifications/") && strings.HasSuffix(r.URL.Path, "/retry"):
		h.retryNotification(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/endpoints":
		h.createEndpoint(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) createNotification(w http.ResponseWriter, r *http.Request) {
	var req model.CreateNotificationRequest
	if err := decodeJSONBody(w, r, &req, 256<<10); err != nil {
		return
	}

	job, created, err := h.notifications.CreateNotification(r.Context(), req)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	if created {
		h.triggerDelivery(job.NotificationID)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"code":    0,
		"message": "accepted",
		"data": model.CreateNotificationResponse{
			NotificationID: job.NotificationID,
			Status:         string(job.Status),
		},
	})
}

func (h *Handler) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req model.CreateEndpointRequest
	if err := decodeJSONBody(w, r, &req, 256<<10); err != nil {
		return
	}

	endpoint, err := h.notifications.CreateEndpoint(r.Context(), req)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"code":    0,
		"message": "created",
		"data":    sanitizeEndpoint(endpoint),
	})
}

func (h *Handler) getNotificationSubresource(w http.ResponseWriter, r *http.Request) {
	notificationID, subresource, ok := parseNotificationPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	switch subresource {
	case "":
		job, found, err := h.notifications.GetNotification(r.Context(), notificationID)
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "notification not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code":    0,
			"data":    toStatusResponse(job),
			"message": "ok",
		})
	case "attempts":
		attempts, err := h.notifications.ListAttempts(r.Context(), notificationID)
		if err != nil {
			writeError(w, statusForError(err), err.Error())
			return
		}
		responses := make([]model.NotificationAttemptResponse, 0, len(attempts))
		for _, attempt := range attempts {
			responses = append(responses, toAttemptResponse(attempt))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code":    0,
			"data":    responses,
			"message": "ok",
		})
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Handler) retryNotification(w http.ResponseWriter, r *http.Request) {
	notificationID, subresource, ok := parseNotificationPath(r.URL.Path)
	if !ok || subresource != "retry" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	job, err := h.notifications.RetryNotification(r.Context(), notificationID)
	if err != nil {
		writeError(w, statusForError(err), err.Error())
		return
	}

	h.triggerDelivery(job.NotificationID)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"code":    0,
		"message": "retry scheduled",
		"data":    toStatusResponse(job),
	})
}

func (h *Handler) triggerDelivery(notificationID string) {
	go func() {
		_ = h.delivery.Deliver(context.Background(), notificationID)
	}()
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return errors.New("multiple json values")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{
		"code":    status,
		"message": message,
	})
}

func statusForError(err error) int {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return http.StatusNotFound
	case strings.Contains(strings.ToLower(err.Error()), "invalid"):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func parseNotificationPath(path string) (notificationID string, subresource string, ok bool) {
	const prefix = "/api/v1/notifications/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	if rest == "" {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) == 1 {
		return parts[0], "", true
	}
	if len(parts) == 2 && parts[1] != "" {
		return parts[0], parts[1], true
	}
	return "", "", false
}

func toStatusResponse(job model.NotificationJob) model.NotificationStatusResponse {
	return model.NotificationStatusResponse{
		NotificationID: job.NotificationID,
		EndpointCode:   job.EndpointCode,
		Status:         job.Status,
		RetryCount:     job.RetryCount,
		NextRetryAt:    job.NextRetryAt,
		FailReason:     job.FailReason,
		HTTPStatus:     job.HTTPStatus,
		LastError:      job.LastError,
	}
}

func toAttemptResponse(attempt model.NotificationAttempt) model.NotificationAttemptResponse {
	return model.NotificationAttemptResponse{
		Attempt:      attempt.AttemptNo,
		Result:       attempt.Result,
		FailReason:   attempt.FailReason,
		HTTPStatus:   attempt.HTTPStatus,
		DurationMS:   attempt.DurationMS,
		ErrorMessage: attempt.ErrorMessage,
		CreatedAt:    attempt.CreatedAt,
	}
}

func sanitizeEndpoint(endpoint model.Endpoint) map[string]any {
	headers := make(map[string]string, len(endpoint.Headers))
	for key, value := range endpoint.Headers {
		headers[key] = value
	}
	return map[string]any{
		"endpoint_code":        endpoint.Code,
		"name":                 endpoint.Name,
		"url":                  endpoint.URL,
		"method":               endpoint.Method,
		"content_type":         endpoint.ContentType,
		"body_mode":            endpoint.BodyMode,
		"headers":              headers,
		"timeout_ms":           endpoint.Timeout.Milliseconds(),
		"rate_limit":           endpoint.RateLimit,
		"rate_limit_window_ms": endpoint.RateLimitWindow.Milliseconds(),
		"enabled":              endpoint.Enabled,
		"max_retry_count":      endpoint.MaxRetryCount,
	}
}
