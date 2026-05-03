package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	connString := os.Getenv("DB_URL")
	if connString == "" {
		connString = "postgres://postgres:password@localhost:5432/relay_db"
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	count := 1000000 // 1M events
	batchSize := 10000
	log.Printf("Generating %d events using CopyFrom...", count)

	for i := 0; i < count; i += batchSize {
		rows := make([][]any, 0, batchSize)
		for j := 0; j < batchSize && i+j < count; j++ {
			rows = append(rows, []any{
				"User", fmt.Sprintf("id-%d", i+j), "UserCreated", `{"name": "test"}`, "PENDING", uuid.New(),
			})
		}

		copyCount, err := pool.CopyFrom(
			ctx,
			pgx.Identifier{"outbox_events"},
			[]string{"aggregate_type", "aggregate_id", "event_type", "payload", "status", "idempotency_key"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			log.Printf("CopyFrom error at offset %d: %v", i, err)
		}
		log.Printf("Inserted %d rows. Total: %d/%d", copyCount, i+len(rows), count)
	}
	log.Println("Load generation complete.")
}
