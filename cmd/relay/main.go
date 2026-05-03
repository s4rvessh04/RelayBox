package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"outbox-relay/admin"
	"outbox-relay/internal/poller"
	"outbox-relay/internal/publisher"
	"outbox-relay/internal/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration with production safety
	relayEnv := os.Getenv("RELAY_ENV")

	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		if relayEnv == "production" {
			log.Fatal("DB_URL is required in production")
		}
		dbURL = "postgres://postgres:password@localhost:5432/relay_db"
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		if relayEnv == "production" {
			log.Fatal("KAFKA_BROKERS is required in production")
		}
		kafkaBrokers = "localhost:29092"
	}
	adminToken := os.Getenv("ADMIN_TOKEN")

	// 1. Initialize Stores
	pgStore, err := store.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer pgStore.Close()

	// 2. Initialize Kafka Publisher
	pub, err := publisher.NewKafkaPublisher(kafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka publisher: %v", err)
	}
	defer pub.Close()

	// 3. Initialize Poller
	p := poller.NewPoller(pgStore, pub, 10000, 100*time.Millisecond, "outbox_events")
	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("Poller exited with error: %v", err)
		}
	}()

	// 4. Admin API
	mux := http.NewServeMux()
	adminHandler := admin.NewAdminHandler(pgStore, adminToken)
	adminHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Admin API listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// 5. Metrics Server (dedicated port for Prometheus scraping)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:         ":8081",
		Handler:      metricsMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Metrics server listening on :8081")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics listen: %s\n", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Cancel context first to stop the poller
	cancel()

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
	if err := metricsSrv.Shutdown(ctxShutdown); err != nil {
		log.Fatal("Metrics server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
