package delivery

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"awesomeProject1/sev/model"
)

type RequestSnapshot struct {
	URL         string
	Method      string
	Headers     map[string]string
	Body        string
	ContentType model.ContentType
	Timeout     time.Duration
}

type RequestBuilder interface {
	Build(endpoint model.Endpoint, payload map[string]any) (RequestSnapshot, error)
}

type TemplateBuilder struct{}

func NewTemplateBuilder() *TemplateBuilder {
	return &TemplateBuilder{}
}

func (b *TemplateBuilder) Build(endpoint model.Endpoint, payload map[string]any) (RequestSnapshot, error) {
	if strings.TrimSpace(endpoint.Code) == "" {
		return RequestSnapshot{}, errors.New("endpoint code is required")
	}
	if !endpoint.Enabled {
		return RequestSnapshot{}, fmt.Errorf("endpoint %s is disabled", endpoint.Code)
	}
	if !endpoint.ContentType.Valid() {
		return RequestSnapshot{}, fmt.Errorf("invalid content type %q", endpoint.ContentType)
	}
	if !endpoint.BodyMode.Valid() {
		return RequestSnapshot{}, fmt.Errorf("invalid body mode %q", endpoint.BodyMode)
	}
	if strings.TrimSpace(endpoint.URL) == "" {
		return RequestSnapshot{}, errors.New("endpoint url is required")
	}
	if strings.TrimSpace(endpoint.Method) == "" {
		return RequestSnapshot{}, errors.New("endpoint method is required")
	}

	headers := make(map[string]string, len(endpoint.Headers)+1)
	for key, value := range endpoint.Headers {
		rendered, err := renderString(value, payload, endpoint.Secrets)
		if err != nil {
			return RequestSnapshot{}, fmt.Errorf("render header %s: %w", key, err)
		}
		headers[key] = rendered
	}
	headers["Content-Type"] = endpoint.ContentType.HeaderValue()

	body, err := buildBody(endpoint, payload)
	if err != nil {
		return RequestSnapshot{}, err
	}

	timeout := endpoint.Timeout.Duration()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return RequestSnapshot{
		URL:         endpoint.URL,
		Method:      strings.ToUpper(strings.TrimSpace(endpoint.Method)),
		Headers:     headers,
		Body:        body,
		ContentType: endpoint.ContentType,
		Timeout:     timeout,
	}, nil
}

func buildBody(endpoint model.Endpoint, payload map[string]any) (string, error) {
	switch endpoint.BodyMode {
	case model.BodyModePassThrough:
		if endpoint.ContentType == model.ContentTypeForm {
			values := url.Values{}
			flattenFormValues(values, "", payload)
			return values.Encode(), nil
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("marshal payload: %w", err)
		}
		return string(raw), nil
	case model.BodyModeTemplate:
		rendered, err := renderAny(endpoint.BodyTemplate, payload, endpoint.Secrets)
		if err != nil {
			return "", err
		}
		if endpoint.ContentType == model.ContentTypeForm {
			values, err := renderFormBody(rendered)
			if err != nil {
				return "", err
			}
			return values.Encode(), nil
		}
		raw, err := json.Marshal(rendered)
		if err != nil {
			return "", fmt.Errorf("marshal template body: %w", err)
		}
		return string(raw), nil
	default:
		return "", fmt.Errorf("unsupported body mode %q", endpoint.BodyMode)
	}
}

func renderFormBody(rendered any) (url.Values, error) {
	values := url.Values{}
	switch v := rendered.(type) {
	case map[string]any:
		flattenFormValues(values, "", v)
	case map[string]string:
		for key, value := range v {
			values.Set(key, value)
		}
	default:
		return nil, fmt.Errorf("form body template must render to map, got %T", rendered)
	}
	return values, nil
}

func flattenFormValues(values url.Values, prefix string, value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			flattenFormValues(values, next, item)
		}
	case map[string]string:
		for key, item := range v {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			values.Set(next, item)
		}
	case []any:
		for idx, item := range v {
			next := prefix
			if next == "" {
				next = fmt.Sprintf("%d", idx)
			}
			flattenFormValues(values, next, item)
		}
	case []string:
		for _, item := range v {
			values.Add(prefix, item)
		}
	case nil:
		if prefix != "" {
			values.Set(prefix, "")
		}
	default:
		if prefix != "" {
			values.Set(prefix, fmt.Sprint(v))
		}
	}
}

func renderAny(value any, payload map[string]any, secrets map[string]string) (any, error) {
	switch v := value.(type) {
	case string:
		return renderValue(v, payload, secrets)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			rendered, err := renderAny(item, payload, secrets)
			if err != nil {
				return nil, err
			}
			out[key] = rendered
		}
		return out, nil
	case map[string]string:
		out := make(map[string]any, len(v))
		for key, item := range v {
			rendered, err := renderValue(item, payload, secrets)
			if err != nil {
				return nil, err
			}
			out[key] = rendered
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i := range v {
			rendered, err := renderAny(v[i], payload, secrets)
			if err != nil {
				return nil, err
			}
			out[i] = rendered
		}
		return out, nil
	default:
		return v, nil
	}
}

func renderString(template string, payload map[string]any, secrets map[string]string) (string, error) {
	rendered, err := renderValue(template, payload, secrets)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(rendered), nil
}

func renderValue(template string, payload map[string]any, secrets map[string]string) (any, error) {
	if token, ok := singleToken(template); ok {
		return resolveToken(token, payload, secrets)
	}

	var buf bytes.Buffer
	segments, err := splitTemplate(template)
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		if segment.literal != nil {
			buf.WriteString(*segment.literal)
			continue
		}
		value, err := resolveToken(*segment.token, payload, secrets)
		if err != nil {
			return nil, err
		}
		buf.WriteString(fmt.Sprint(value))
	}
	return buf.String(), nil
}
