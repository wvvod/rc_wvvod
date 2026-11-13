package service

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"awesomeProject1/sev/repository"
)

type RetryScheduler struct {
	repo        repository.Repository
	delivery    *DeliveryService
	clock       Clock
	batchSize   int
	workerCount int
	interval    time.Duration
	wake        chan struct{}
	healthy     atomic.Bool
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
		wake:        make(chan struct{}, 1),
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

	backoff := s.interval
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := s.RunOnce(ctx); err != nil {
			s.healthy.Store(false)
			log.Printf("scheduler scan failed: %v", err)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		} else {
			s.healthy.Store(true)
			backoff = s.interval
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
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
				attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 310*time.Second)
				if err := s.delivery.Deliver(attemptCtx, notificationID); err != nil {
					log.Printf("delivery %s failed: %v", notificationID, err)
				}
				cancel()
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

func (s *RetryScheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *RetryScheduler) Healthy() bool { return s.healthy.Load() }
