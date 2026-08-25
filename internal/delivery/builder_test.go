package delivery

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"awesomeProject1/internal/model"
)

func TestTemplateBuilderBuildJSONTemplatePreservesTypes(t *testing.T) {
	builder := NewTemplateBuilder()
	endpoint := model.Endpoint{
		Code:        "crm_payment_success",
		URL:         "https://crm.example.com/api/contact",
		Method:      "post",
		ContentType: model.ContentTypeJSON,
		BodyMode:    model.BodyModeTemplate,
		Headers: map[string]string{
			"Authorization": "Bearer {{secret.crm_token}}",
			"X-App-Id":      "notification-service",
		},
		BodyTemplate: map[string]any{
			"contact_id":     "{{payload.user_id}}",
			"contact_status": "PAID",
			"amount":         "{{payload.amount}}",
		},
		Secrets: map[string]string{
			"crm_token": "abc123",
		},
		Enabled: true,
		Timeout: model.NewMillisDuration(3 * time.Second),
	}

	snapshot, err := builder.Build(endpoint, map[string]any{
		"user_id": 100,
		"amount":  199,
	})
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	if snapshot.Method != "POST" {
		t.Fatalf("expected POST, got %s", snapshot.Method)
	}
	if got := snapshot.Headers["Authorization"]; got != "Bearer abc123" {
		t.Fatalf("expected rendered auth header, got %q", got)
	}
	if got := snapshot.Headers["Content-Type"]; got != "application/json" {
		t.Fatalf("expected json content type, got %q", got)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(snapshot.Body), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}

	if got := body["contact_status"]; got != "PAID" {
		t.Fatalf("expected PAID, got %#v", got)
	}
	if got := body["contact_id"]; got != float64(100) {
		t.Fatalf("expected numeric contact_id, got %#v", got)
	}
	if got := body["amount"]; got != float64(199) {
		t.Fatalf("expected numeric amount, got %#v", got)
	}
}

func TestTemplateBuilderBuildFormPassThrough(t *testing.T) {
	builder := NewTemplateBuilder()
	endpoint := model.Endpoint{
		Code:        "inventory_change",
		URL:         "https://inventory.example.com/api/stock",
		Method:      "POST",
		ContentType: model.ContentTypeForm,
		BodyMode:    model.BodyModePassThrough,
		Enabled:     true,
	}

	snapshot, err := builder.Build(endpoint, map[string]any{
		"user_id": "u10001",
		"amount":  199,
	})
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	values, err := url.ParseQuery(snapshot.Body)
	if err != nil {
		t.Fatalf("parse form body: %v", err)
	}

	if got := values.Get("user_id"); got != "u10001" {
		t.Fatalf("expected user_id, got %q", got)
	}
	if got := values.Get("amount"); got != "199" {
		t.Fatalf("expected amount, got %q", got)
	}
	if got := snapshot.Headers["Content-Type"]; got != "application/x-www-form-urlencoded" {
		t.Fatalf("expected form content type, got %q", got)
	}
}

func TestTemplateBuilderBuildReturnsErrorForMissingPayloadPath(t *testing.T) {
	builder := NewTemplateBuilder()
	endpoint := model.Endpoint{
		Code:        "crm_payment_success",
		URL:         "https://crm.example.com/api/contact",
		Method:      "POST",
		ContentType: model.ContentTypeJSON,
		BodyMode:    model.BodyModeTemplate,
		BodyTemplate: map[string]any{
			"contact_id": "{{payload.missing}}",
		},
		Enabled: true,
	}

	_, err := builder.Build(endpoint, map[string]any{
		"user_id": 100,
	})
	if err == nil {
		t.Fatalf("expected error for missing path")
	}
}
