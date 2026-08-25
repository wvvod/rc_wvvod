package service

import (
	"context"
	"sync"
	"time"

	"awesomeProject1/internal/repository"
)

type RetryScheduler struct {
	repo        repository.Repository
	delivery    *DeliveryService
	clock       Clock
	batchSize   int
	workerCount int
	interval    time.Duration
}

func NewRetryScheduler(repo repository.Repository, delivery *DeliveryService, clock Clock, batchSize, workerCount int, interval time.Duration) *RetryScheduler {
	if clock == nil {
		clock = time.Now
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if workerCount <= 0 {
		workerCount = 10
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &RetryScheduler{
		repo:        repo,
		delivery:    delivery,
		clock:       clock,
		batchSize:   batchSize,
		workerCount: workerCount,
		interval:    interval,
	}
}

func (s *RetryScheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		if _, err := s.RunOnce(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *RetryScheduler) RunOnce(ctx context.Context) (int, error) {
	jobs, err := s.repo.ListRunnableJobs(ctx, s.clock().UTC(), s.batchSize)
	if err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	jobCh := make(chan string)
	var wg sync.WaitGroup

	workerCount := s.workerCount
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for notificationID := range jobCh {
				_ = s.delivery.Deliver(ctx, notificationID)
			}
		}()
	}

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			close(jobCh)
			wg.Wait()
			return 0, ctx.Err()
		case jobCh <- job.NotificationID:
		}
	}
	close(jobCh)
	wg.Wait()

	return len(jobs), nil
}
