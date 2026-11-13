package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"awesomeProject1/sev/api"
	"awesomeProject1/sev/delivery"
	"awesomeProject1/sev/ratelimit"
	"awesomeProject1/sev/repository"
	"awesomeProject1/sev/service"

	mysqlDriver "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gormDB, err := openGormDB(ctx, os.Getenv("MYSQL_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	sqlDB, err := gormDB.DB()
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	gormRepo := repository.NewGormRepository(gormDB)
	if err := gormRepo.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	builder := delivery.NewTemplateBuilder()
	limiter := ratelimit.NewMemoryLimiter(time.Now)
	client := delivery.NewHTTPClient()

	deliveryService := service.NewDeliveryService(gormRepo, limiter, client, time.Now, rand.Float64)
	notificationService := service.NewNotificationService(gormRepo, builder, time.Now, nil)
	scheduler := service.NewRetryScheduler(gormRepo, deliveryService, time.Now, 100, 10, time.Second)
	handler := api.NewHandler(notificationService, deliveryService)
	handler.SetWake(scheduler.Wake)

	mux := http.NewServeMux()
	mux.Handle("/api/", handler)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		counts, err := gormRepo.StatusCounts(checkCtx)
		if err != nil {
			http.Error(w, "metrics unavailable", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"delivery": deliveryService.Metrics(), "jobs": counts})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if !scheduler.Healthy() || sqlDB.PingContext(checkCtx) != nil {
			http.Error(w, "not ready", 503)
			return
		}
		w.WriteHeader(200)
	})
	var workers sync.WaitGroup
	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := scheduler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("scheduler stopped: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Println("notification service started on :8080")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	stop()
	workers.Wait()
}

func openGormDB(ctx context.Context, dsn string) (*gorm.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("MYSQL_DSN is required")
	}

	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	if cfg.Loc == nil {
		cfg.Loc = time.UTC
	}

	db, err := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open gorm db: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql db: %w", err)
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql db: %w", err)
	}
	return db, nil
}
