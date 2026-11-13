package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"awesomeProject1/sev/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GormRepository struct {
	db *gorm.DB
}

func NewGormRepository(db *gorm.DB) *GormRepository {
	return &GormRepository{db: db}
}

func (r *GormRepository) Migrate(ctx context.Context) error {
	return newMigrationRunner(r.db).Run(ctx)
}

func (r *GormRepository) UpsertEndpoint(ctx context.Context, endpoint model.Endpoint) error {
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

	endpoint.UpdatedAt = time.Now().UTC()

	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "endpoint_code"}},
		UpdateAll: true,
	}).Create(&endpoint)
	return result.Error
}

func (r *GormRepository) GetEndpoint(ctx context.Context, code string) (model.Endpoint, bool, error) {
	var endpoint model.Endpoint
	err := r.db.WithContext(ctx).Where("endpoint_code = ?", code).First(&endpoint).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Endpoint{}, false, nil
		}
		return model.Endpoint{}, false, err
	}
	return endpoint, true, nil
}

func (r *GormRepository) ListEndpoints(ctx context.Context) ([]model.Endpoint, error) {
	var endpoints []model.Endpoint
	if err := r.db.WithContext(ctx).Order("endpoint_code ASC").Find(&endpoints).Error; err != nil {
		return nil, err
	}
	return endpoints, nil
}

func (r *GormRepository) CreateJob(ctx context.Context, job model.NotificationJob) (model.NotificationJob, bool, error) {
	if job.Timeout <= 0 {
		job.Timeout = model.NewMillisDuration(5 * time.Second)
	}
	if job.MaxRetryCount <= 0 {
		job.MaxRetryCount = 10
	}
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = job.CreatedAt
	}
	if job.Status == "" {
		job.Status = model.JobStatusPending
	}

	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "business"}, {Name: "request_id"}},
		DoNothing: true,
	}).Create(&job)
	if result.Error != nil {
		return model.NotificationJob{}, false, result.Error
	}

	if result.RowsAffected == 0 {
		existing, found, err := r.GetJobByBusinessRequest(ctx, job.Business, job.RequestID)
		if err != nil {
			return model.NotificationJob{}, false, err
		}
		if !found {
			return model.NotificationJob{}, false, ErrNotFound
		}
		return existing, true, nil
	}

	return job, false, nil
}

func (r *GormRepository) GetJob(ctx context.Context, notificationID string) (model.NotificationJob, bool, error) {
	var job model.NotificationJob
	err := r.db.WithContext(ctx).Where("notification_id = ?", notificationID).First(&job).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.NotificationJob{}, false, nil
		}
		return model.NotificationJob{}, false, err
	}
	return job, true, nil
}

func (r *GormRepository) GetJobByBusinessRequest(ctx context.Context, business, requestID string) (model.NotificationJob, bool, error) {
	var job model.NotificationJob
	err := r.db.WithContext(ctx).Where("business = ? AND request_id = ?", business, requestID).First(&job).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.NotificationJob{}, false, nil
		}
		return model.NotificationJob{}, false, err
	}
	return job, true, nil
}

func (r *GormRepository) UpdateJob(ctx context.Context, job model.NotificationJob) error {
	if strings.TrimSpace(job.NotificationID) == "" {
		return fmt.Errorf("notification id is required")
	}
	return r.db.WithContext(ctx).Model(&model.NotificationJob{}).
		Where("notification_id = ?", job.NotificationID).
		Select("Status", "FailReason", "RetryCount", "NextRetryAt", "LockedUntil", "HTTPStatus", "LastError", "UpdatedAt").
		Updates(job).Error
}

func (r *GormRepository) ClaimJob(ctx context.Context, notificationID string, now time.Time, lockFor time.Duration) (model.NotificationJob, bool, error) {
	var (
		job     model.NotificationJob
		found   bool
		claimed bool
	)

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var selected model.NotificationJob
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("notification_id = ?", notificationID).
			First(&selected).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}

		found = true
		if !claimable(selected, now) {
			job = selected
			return nil
		}

		lockUntil := now.Add(lockFor)
		updates := map[string]any{
			"status":        model.JobStatusProcessing,
			"locked_until":  lockUntil,
			"updated_at":    now.UTC(),
			"lease_version": selected.LeaseVersion + 1,
		}
		if err := tx.Model(&model.NotificationJob{}).
			Where("notification_id = ?", notificationID).
			Updates(updates).Error; err != nil {
			return err
		}

		selected.LeaseVersion++
		selected.Status = model.JobStatusProcessing
		selected.LockedUntil = &lockUntil
		selected.UpdatedAt = now.UTC()
		job = selected
		claimed = true
		return nil
	})
	if err != nil {
		return model.NotificationJob{}, false, err
	}
	if !found {
		return model.NotificationJob{}, false, nil
	}
	if !claimed {
		return job, false, nil
	}
	return job, true, nil
}

func (r *GormRepository) ListRunnableJobs(ctx context.Context, now time.Time, limit int) ([]model.NotificationJob, error) {
	if limit <= 0 {
		limit = 100
	}
	var jobs []model.NotificationJob
	err := r.db.WithContext(ctx).Where(
		"status = ? OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?)) OR (status = ? AND locked_until IS NOT NULL AND locked_until <= ?)",
		model.JobStatusPending,
		model.JobStatusRetryWait,
		now.UTC(),
		model.JobStatusProcessing,
		now.UTC(),
	).Order("COALESCE(next_retry_at, locked_until, created_at) ASC, notification_id ASC").
		Limit(limit).
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}

	filtered := jobs[:0]
	for _, job := range jobs {
		if claimable(job, now) {
			filtered = append(filtered, job)
		}
	}
	return filtered, nil
}

func (r *GormRepository) AppendAttempt(ctx context.Context, attempt model.NotificationAttempt) error {
	return r.db.WithContext(ctx).Create(&attempt).Error
}

func (r *GormRepository) ListAttempts(ctx context.Context, notificationID string) ([]model.NotificationAttempt, error) {
	var attempts []model.NotificationAttempt
	if err := r.db.WithContext(ctx).Where("notification_id = ?", notificationID).
		Order("attempt_no ASC, created_at ASC").
		Find(&attempts).Error; err != nil {
		return nil, err
	}
	return attempts, nil
}

func (r *GormRepository) FinishAttempt(ctx context.Context, job model.NotificationJob, attempt model.NotificationAttempt) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current model.NotificationJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("notification_id = ?", job.NotificationID).First(&current).Error; err != nil {
			return err
		}
		if current.Status != model.JobStatusProcessing || current.LeaseVersion != job.LeaseVersion {
			return ErrConflict
		}
		attempt.AttemptNo = current.AttemptCount + 1
		updates := map[string]any{"status": job.Status, "fail_reason": job.FailReason, "retry_count": job.RetryCount, "next_retry_at": job.NextRetryAt, "locked_until": job.LockedUntil, "http_status": job.HTTPStatus, "last_error": job.LastError, "updated_at": job.UpdatedAt, "attempt_count": attempt.AttemptNo}
		if err := tx.Model(&model.NotificationJob{}).Where("notification_id = ?", job.NotificationID).Updates(updates).Error; err != nil {
			return err
		}
		return tx.Create(&attempt).Error
	})
}
func (r *GormRepository) RetryFailed(ctx context.Context, id string, now time.Time) (model.NotificationJob, error) {
	var job model.NotificationJob
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("notification_id = ?", id).First(&job).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if job.Status != model.JobStatusFailed {
			return ErrConflict
		}
		job.Status = model.JobStatusRetryWait
		job.RetryCount = 0
		job.NextRetryAt = &now
		job.LockedUntil = nil
		job.FailReason = ""
		job.LastError = ""
		job.HTTPStatus = nil
		job.UpdatedAt = now
		return (&GormRepository{db: tx}).UpdateJob(ctx, job)
	})
	return job, err
}

func (r *GormRepository) StatusCounts(ctx context.Context) (map[string]int64, error) {
	var rows []struct {
		Status string
		Count  int64
	}
	if err := r.db.WithContext(ctx).Model(&model.NotificationJob{}).Select("status, COUNT(*) AS count").Group("status").Scan(&rows).Error; err != nil {
		return nil, err
	}
	result := map[string]int64{}
	for _, row := range rows {
		result[row.Status] = row.Count
	}
	return result, nil
}
