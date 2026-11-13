package service

import (
	"awesomeProject1/sev/model"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }
func invalid(message string) error       { return &ValidationError{Message: message} }
func validText(name, value string, limit int) error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > limit {
		return invalid(fmt.Sprintf("%s is required and must be at most %d characters", name, limit))
	}
	return nil
}
func validateNotification(r model.CreateNotificationRequest) error {
	for _, f := range []struct {
		name, value string
		limit       int
	}{{"request_id", r.RequestID, 128}, {"business", r.Business, 64}, {"endpoint_code", r.EndpointCode, 128}} {
		if err := validText(f.name, f.value, f.limit); err != nil {
			return err
		}
	}
	return nil
}
func validateEndpoint(r model.CreateEndpointRequest) error {
	for _, f := range []struct {
		name, value string
		limit       int
	}{{"endpoint_code", r.EndpointCode, 128}, {"name", r.Name, 255}, {"url", r.URL, 2048}, {"method", r.Method, 16}} {
		if err := validText(f.name, f.value, f.limit); err != nil {
			return err
		}
	}
	u, err := url.Parse(r.URL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
		return invalid("url must be an absolute HTTP(S) URL without credentials or fragment")
	}
	switch strings.ToUpper(r.Method) {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return invalid("unsupported method")
	}
	if !r.ContentType.Valid() || !r.BodyMode.Valid() {
		return invalid("invalid content_type or body_mode")
	}
	if r.TimeoutMS < 0 || r.TimeoutMS > 300000 || r.RateLimit < 0 || r.RateLimit > 1000000 || r.RateLimitWindowMS < 0 || r.RateLimitWindowMS > 86400000 || r.MaxRetryCount < 0 || r.MaxRetryCount > 100 {
		return invalid("configuration exceeds allowed range")
	}
	for k, v := range r.Headers {
		if k == "" || strings.ContainsAny(v, "\r\n") {
			return invalid("invalid header")
		}
		for _, c := range k {
			if !strings.ContainsRune("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", c) {
				return invalid("invalid header name")
			}
		}
	}
	for _, code := range r.RetryHTTPStatuses {
		if code < 400 || code > 599 {
			return invalid("retry HTTP codes must be between 400 and 599")
		}
	}
	return nil
}
