package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"outbox-relay/internal/consumer"
	"outbox-relay/internal/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
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
		redisURL = "localhost:6379"
	}

	// 1. Initialize Redis Store for idempotency
	redisStore, err := store.NewRedisStore(ctx, redisURL)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// 2. Define business logic handler
	// In a real application, this is where you would update your local database,
	// send an email, or trigger another workflow.
	sampleHandler := func(ctx context.Context, event *consumer.Event) error {
		log.Printf("Processing event %s (IdempotencyKey: %s)", event.ID, event.IdempotencyKey)
		fmt.Printf("Payload: %s\n", string(event.Payload))

		// Simulate some work
		return nil
	}

	// 3. Initialize Idempotent Consumer helper
	idempotentConsumer := consumer.NewIdempotentConsumer(redisStore, sampleHandler, 24*time.Hour)

	// 4. Initialize Kafka Consumer
	kafkaConsumer, err := consumer.NewKafkaConsumer(kafkaBrokers, kafkaGroupID, kafkaTopics)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// 5. Start consuming
	go func() {
		log.Printf("Consumer started. Group: %s, Topics: %v", kafkaGroupID, kafkaTopics)
		// We wrap the business handler with the idempotent check
		err := kafkaConsumer.Start(ctx, idempotentConsumer.Process)
		if err != nil && err != context.Canceled {
			log.Printf("Consumer exited with error: %v", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// 6. Metrics Server
	go func() {
		log.Printf("Consumer metrics listening on :8081")
		if err := http.ListenAndServe(":8081", promhttp.Handler()); err != nil {
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down consumer...")
}
