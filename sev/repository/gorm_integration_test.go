package repository

import (
	"awesomeProject1/sev/model"
	"context"
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

func TestMySQLLeaseAndAtomicCompletion(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN is not set")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	r := NewGormRepository(db)
	ctx := context.Background()
	if err := r.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("it_%d", time.Now().UnixNano())
	now := time.Now().UTC().Truncate(time.Second)
	defer db.Where("notification_id = ?", id).Delete(&model.NotificationAttempt{})
	defer db.Where("notification_id = ?", id).Delete(&model.NotificationJob{})
	_, _, err = r.CreateJob(ctx, model.NotificationJob{NotificationID: id, RequestID: id, Business: "integration", EndpointCode: "test", Payload: map[string]any{}, RequestURL: "https://example.com", RequestMethod: "POST", ContentType: model.ContentTypeJSON, Status: model.JobStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	old, ok, err := r.ClaimJob(ctx, id, now, time.Second)
	if err != nil || !ok {
		t.Fatal("claim", err)
	}
	fresh, ok, err := r.ClaimJob(ctx, id, now.Add(2*time.Second), time.Second)
	if err != nil || !ok {
		t.Fatal("reclaim", err)
	}
	old.Status = model.JobStatusSuccess
	if err := r.FinishAttempt(ctx, old, model.NotificationAttempt{NotificationID: id}); !errors.Is(err, ErrConflict) {
		t.Fatal("stale write", err)
	}
	fresh.Status = model.JobStatusSuccess
	// Force an insert error: the state update must roll back with it.
	bad := model.NotificationAttempt{NotificationID: id, ErrorMessage: string(make([]byte, 3000))}
	if err := r.FinishAttempt(ctx, fresh, bad); err == nil {
		t.Fatal("expected strict-mode insert failure")
	}
	stored, _, err := r.GetJob(ctx, id)
	if err != nil || stored.Status != model.JobStatusProcessing {
		t.Fatal("transaction did not roll back", stored, err)
	}
	if err := r.FinishAttempt(ctx, fresh, model.NotificationAttempt{NotificationID: id, Result: model.AttemptResultSuccess}); err != nil {
		t.Fatal(err)
	}
	attempts, err := r.ListAttempts(ctx, id)
	if err != nil || len(attempts) != 1 {
		t.Fatal(attempts, err)
	}
}
