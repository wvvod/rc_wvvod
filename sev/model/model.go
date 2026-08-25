package model

import "time"

type ContentType string

const (
	ContentTypeJSON ContentType = "JSON"
	ContentTypeForm ContentType = "FORM"
)

func (c ContentType) HeaderValue() string {
	switch c {
	case ContentTypeForm:
		return "application/x-www-form-urlencoded"
	default:
		return "application/json"
	}
}

func (c ContentType) Valid() bool {
	switch c {
	case ContentTypeJSON, ContentTypeForm:
		return true
	default:
		return false
	}
}

type BodyMode string

const (
	BodyModePassThrough BodyMode = "PASS_THROUGH"
	BodyModeTemplate    BodyMode = "TEMPLATE"
)

func (m BodyMode) Valid() bool {
	switch m {
	case BodyModePassThrough, BodyModeTemplate:
		return true
	default:
		return false
	}
}

type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusProcessing JobStatus = "PROCESSING"
	JobStatusRetryWait  JobStatus = "RETRY_WAIT"
	JobStatusSuccess    JobStatus = "SUCCESS"
	JobStatusFailed     JobStatus = "FAILED"
)

func (s JobStatus) Terminal() bool {
	switch s {
	case JobStatusSuccess, JobStatusFailed:
		return true
	default:
		return false
	}
}

type AttemptResult string

const (
	AttemptResultSuccess        AttemptResult = "SUCCESS"
	AttemptResultFailed         AttemptResult = "FAILED"
	AttemptResultRateLimited    AttemptResult = "RATE_LIMITED"
	AttemptResultRateLimitError AttemptResult = "RATE_LIMIT_ERROR"
)

type FailReason string

const (
	FailReasonHTTP4xx           FailReason = "HTTP_4XX"
	FailReasonHTTP5xx           FailReason = "HTTP_5XX"
	FailReasonHTTP429           FailReason = "HTTP_429"
	FailReasonNetworkError      FailReason = "NETWORK_ERROR"
	FailReasonConnectTimeout    FailReason = "CONNECT_TIMEOUT"
	FailReasonRequestTimeout    FailReason = "REQUEST_TIMEOUT"
	FailReasonDNSError          FailReason = "DNS_ERROR"
	FailReasonConnectionRefused FailReason = "CONNECTION_REFUSED"
	FailReasonRateLimited       FailReason = "RATE_LIMITED"
	FailReasonRateLimitError    FailReason = "RATE_LIMIT_ERROR"
	FailReasonInvalidConfig     FailReason = "INVALID_CONFIG"
	FailReasonInvalidResponse   FailReason = "INVALID_RESPONSE"
	FailReasonMaxRetryExceeded  FailReason = "MAX_RETRY_EXCEEDED"
	FailReasonHTTPStatus        FailReason = "HTTP_STATUS"
)

type Endpoint struct {
	ID                uint64            `gorm:"primaryKey;autoIncrement" json:"-"`
	Code              string            `gorm:"column:endpoint_code;size:128;not null;uniqueIndex:uk_endpoint_code" json:"endpoint_code"`
	Name              string            `gorm:"column:name;size:255;not null" json:"name"`
	URL               string            `gorm:"column:url;size:2048;not null" json:"url"`
	Method            string            `gorm:"column:method;size:16;not null" json:"method"`
	ContentType       ContentType       `gorm:"column:content_type;size:32;not null" json:"content_type"`
	BodyMode          BodyMode          `gorm:"column:body_mode;size:32;not null" json:"body_mode"`
	Headers           map[string]string `gorm:"column:header_config;type:json;serializer:json" json:"headers,omitempty"`
	BodyTemplate      map[string]any    `gorm:"column:body_template;type:json;serializer:json" json:"body_template,omitempty"`
	Secrets           map[string]string `gorm:"column:secret_config;type:json;serializer:json" json:"secrets,omitempty"`
	Timeout           MillisDuration    `gorm:"column:timeout_ms;type:int;not null" json:"timeout_ms"`
	RateLimit         int               `gorm:"column:rate_limit;not null" json:"rate_limit"`
	RateLimitWindow   MillisDuration    `gorm:"column:rate_limit_window_ms;type:int;not null" json:"rate_limit_window_ms"`
	Enabled           bool              `gorm:"column:enabled;not null;default:true" json:"enabled"`
	MaxRetryCount     int               `gorm:"column:max_retry_count;not null;default:10" json:"max_retry_count"`
	RetryHTTPStatuses map[int]struct{}  `gorm:"column:retry_http_statuses;type:json;serializer:json" json:"retry_http_codes,omitempty"`
	CreatedAt         time.Time         `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time         `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Endpoint) TableName() string {
	return "notification_endpoint"
}

type NotificationJob struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement" json:"-"`
	NotificationID string `gorm:"column:notification_id;size:64;not null;uniqueIndex:uk_notification_id" json:"notification_id"`
	RequestID      string `gorm:"column:request_id;size:128;not null;uniqueIndex:uk_business_request,priority:2" json:"request_id"`
	Business       string `gorm:"column:business;size:64;not null;uniqueIndex:uk_business_request,priority:1" json:"business"`
	EndpointCode   string `gorm:"column:endpoint_code;size:128;not null;index" json:"endpoint_code"`

	Payload map[string]any `gorm:"column:payload;type:json;serializer:json" json:"payload"`

	RequestURL     string            `gorm:"column:request_url;size:2048;not null" json:"request_url"`
	RequestMethod  string            `gorm:"column:request_method;size:16;not null" json:"request_method"`
	RequestHeaders map[string]string `gorm:"column:request_headers;type:json;serializer:json" json:"request_headers,omitempty"`
	RequestBody    string            `gorm:"column:request_body;type:mediumtext" json:"request_body,omitempty"`
	ContentType    ContentType       `gorm:"column:content_type;size:32;not null" json:"content_type"`
	Timeout        MillisDuration    `gorm:"column:timeout_ms;type:int;not null" json:"timeout_ms"`

	Status        JobStatus  `gorm:"column:status;size:32;not null;index:idx_retry,priority:1;index:idx_processing,priority:1" json:"status"`
	FailReason    FailReason `gorm:"column:fail_reason;size:64" json:"fail_reason,omitempty"`
	RetryCount    int        `gorm:"column:retry_count;not null;default:0" json:"retry_count"`
	MaxRetryCount int        `gorm:"column:max_retry_count;not null;default:10" json:"max_retry_count"`
	NextRetryAt   *time.Time `gorm:"column:next_retry_at;index:idx_retry,priority:2" json:"next_retry_at,omitempty"`
	LockedUntil   *time.Time `gorm:"column:locked_until;index:idx_processing,priority:2" json:"locked_until,omitempty"`
	HTTPStatus    *int       `gorm:"column:http_status" json:"http_status,omitempty"`
	LastError     string     `gorm:"column:last_error;size:2048" json:"last_error,omitempty"`

	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (NotificationJob) TableName() string {
	return "notification_job"
}

type NotificationAttempt struct {
	ID             uint64        `gorm:"primaryKey;autoIncrement" json:"-"`
	NotificationID string        `gorm:"column:notification_id;size:64;not null;index:idx_notification_attempt,priority:1;index" json:"notification_id"`
	AttemptNo      int           `gorm:"column:attempt_no;not null;index:idx_notification_attempt,priority:2" json:"attempt_no"`
	Result         AttemptResult `gorm:"column:result;size:32;not null" json:"result"`
	FailReason     FailReason    `gorm:"column:fail_reason;size:64" json:"fail_reason,omitempty"`
	HTTPStatus     *int          `gorm:"column:http_status" json:"http_status,omitempty"`
	DurationMS     int           `gorm:"column:duration_ms" json:"duration_ms"`
	ErrorMessage   string        `gorm:"column:error_message;size:2048" json:"error_message,omitempty"`
	CreatedAt      time.Time     `gorm:"column:created_at;autoCreateTime" json:"created_at"`
}

func (NotificationAttempt) TableName() string {
	return "notification_attempt"
}

type CreateNotificationRequest struct {
	RequestID    string         `json:"request_id"`
	Business     string         `json:"business"`
	EndpointCode string         `json:"endpoint_code"`
	Payload      map[string]any `json:"payload"`
}

type CreateEndpointRequest struct {
	EndpointCode      string            `json:"endpoint_code"`
	Name              string            `json:"name"`
	URL               string            `json:"url"`
	Method            string            `json:"method"`
	ContentType       ContentType       `json:"content_type"`
	BodyMode          BodyMode          `json:"body_mode"`
	Headers           map[string]string `json:"headers"`
	BodyTemplate      map[string]any    `json:"body_template"`
	Secrets           map[string]string `json:"secrets"`
	TimeoutMS         int               `json:"timeout_ms"`
	RateLimit         int               `json:"rate_limit"`
	RateLimitWindowMS int               `json:"rate_limit_window_ms"`
	Enabled           *bool             `json:"enabled"`
	MaxRetryCount     int               `json:"max_retry_count"`
	RetryHTTPStatuses []int             `json:"retry_http_codes"`
}

type CreateNotificationResponse struct {
	NotificationID string `json:"notification_id"`
	Status         string `json:"status"`
}

type NotificationStatusResponse struct {
	NotificationID string     `json:"notification_id"`
	EndpointCode   string     `json:"endpoint_code"`
	Status         JobStatus  `json:"status"`
	RetryCount     int        `json:"retry_count"`
	NextRetryAt    *time.Time `json:"next_retry_at,omitempty"`
	FailReason     FailReason `json:"fail_reason,omitempty"`
	HTTPStatus     *int       `json:"http_status,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
}

type NotificationAttemptResponse struct {
	Attempt      int           `json:"attempt"`
	Result       AttemptResult `json:"result"`
	FailReason   FailReason    `json:"fail_reason,omitempty"`
	HTTPStatus   *int          `json:"http_status,omitempty"`
	DurationMS   int           `json:"duration_ms"`
	ErrorMessage string        `json:"error_message,omitempty"`
	CreatedAt    time.Time     `json:"created_at"`
}
