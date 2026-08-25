package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMillisDurationJSONRoundTrip(t *testing.T) {
	d := NewMillisDuration(1500 * time.Millisecond)

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != "1500" {
		t.Fatalf("expected millisecond json, got %s", string(data))
	}

	var decoded MillisDuration
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Duration() != 1500*time.Millisecond {
		t.Fatalf("unexpected decoded duration: %v", decoded.Duration())
	}
}

func TestMillisDurationScan(t *testing.T) {
	var decoded MillisDuration
	if err := decoded.Scan(int64(2500)); err != nil {
		t.Fatalf("scan int64: %v", err)
	}
	if decoded.Duration() != 2500*time.Millisecond {
		t.Fatalf("unexpected scanned duration: %v", decoded.Duration())
	}
}
