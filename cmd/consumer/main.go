package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"outbox-relay/internal/consumer"
	"outbox-relay/internal/dlq"
	"outbox-relay/internal/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
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
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	if kafkaGroupID == "" {
		kafkaGroupID = "relay-consumer-group"
	}
	kafkaTopicsEnv := os.Getenv("KAFKA_TOPICS")
	if kafkaTopicsEnv == "" {
		kafkaTopicsEnv = "outbox_events"
	}
	kafkaTopics := strings.Split(kafkaTopicsEnv, ",")

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		if relayEnv == "production" {
			log.Fatal("REDIS_URL is required in production")
		}
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

	// 2. Initialize DLQ Manager
	dlqMgr := dlq.NewManager(pgStore)

	// 3. Define business logic handler
	// In a real application, this is where you would update your local database,
	// send an email, or trigger another workflow.
	sampleHandler := func(ctx context.Context, event *consumer.Event) error {
		log.Printf("Processing event %s (IdempotencyKey: %s)", event.ID, event.IdempotencyKey)
		fmt.Printf("Payload: %s\n", string(event.Payload))

		// Simulate some work
		return nil
	}

	// 4. Initialize Idempotent Consumer helper
	idempotentConsumer := consumer.NewIdempotentConsumer(redisStore, dlqMgr, sampleHandler, 24*time.Hour)

	// 5. Initialize Kafka Consumer
	kafkaConsumer, err := consumer.NewKafkaConsumer(kafkaBrokers, kafkaGroupID, kafkaTopics)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// 6. Start consuming with WaitGroup for clean shutdown
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Consumer started. Group: %s, Topics: %v", kafkaGroupID, kafkaTopics)
		// We wrap the business handler with the idempotent check
		err := kafkaConsumer.Start(ctx, idempotentConsumer.Process)
		if err != nil && err != context.Canceled {
			log.Printf("Consumer exited with error: %v", err)
		}
	}()

	// 7. Metrics Server
	metricsSrv := &http.Server{
		Addr:         ":8081",
		Handler:      promhttp.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Printf("Consumer metrics listening on :8081")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down consumer...")

	// Cancel context to stop consumer goroutine
	cancel()

	// Wait for consumer to finish processing current message
	wg.Wait()

	// Shutdown metrics server
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := metricsSrv.Shutdown(ctxShutdown); err != nil {
		log.Printf("Metrics server forced to shutdown: %v", err)
	}

	log.Println("Consumer exiting")
}
