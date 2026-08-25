package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type MillisDuration time.Duration

func NewMillisDuration(d time.Duration) MillisDuration {
	return MillisDuration(d)
}

func (MillisDuration) GormDataType() string {
	return "int"
}

func (d MillisDuration) Duration() time.Duration {
	return time.Duration(d)
}

func (d MillisDuration) Milliseconds() int64 {
	return int64(time.Duration(d) / time.Millisecond)
}

func (d MillisDuration) Value() (driver.Value, error) {
	return d.Milliseconds(), nil
}

func (d *MillisDuration) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*d = 0
		return nil
	case int64:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case int32:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case int:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case uint64:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case uint32:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case uint:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case float64:
		*d = MillisDuration(time.Duration(v) * time.Millisecond)
		return nil
	case []byte:
		return d.scanString(string(v))
	case string:
		return d.scanString(v)
	default:
		return fmt.Errorf("unsupported millis duration scan type %T", value)
	}
}

func (d *MillisDuration) scanString(value string) error {
	if value == "" {
		*d = 0
		return nil
	}
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	*d = MillisDuration(time.Duration(ms) * time.Millisecond)
	return nil
}

func (d MillisDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Milliseconds())
}

func (d *MillisDuration) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = 0
		return nil
	}
	var ms int64
	if err := json.Unmarshal(data, &ms); err == nil {
		*d = MillisDuration(time.Duration(ms) * time.Millisecond)
		return nil
	}
	var raw string
	if err := json.Unmarshal(data, &raw); err == nil {
		if raw == "" {
			*d = 0
			return nil
		}
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		*d = MillisDuration(time.Duration(ms) * time.Millisecond)
		return nil
	}
	return fmt.Errorf("invalid millis duration json: %s", string(data))
}
