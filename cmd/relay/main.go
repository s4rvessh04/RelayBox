package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"outbox-relay/admin"
	"outbox-relay/internal/poller"
	"outbox-relay/internal/publisher"
	"outbox-relay/internal/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/relay_db"
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	// 1. Initialize Stores
	pgStore, err := store.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer pgStore.Close()

	redisStore, err := store.NewRedisStore(ctx, redisURL)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// 2. Initialize Kafka Publisher
	pub, err := publisher.NewKafkaPublisher(kafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka publisher: %v", err)
	}
	defer pub.Close()

	// 3. Initialize Poller
	p := poller.NewPoller(pgStore, pub, 100, 100*time.Millisecond, "outbox_events")
	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("Poller exited with error: %v", err)
		}
	}()

	// 4. Admin API
	mux := http.NewServeMux()
	adminHandler := admin.NewAdminHandler(pgStore)
	adminHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Printf("Admin API listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
