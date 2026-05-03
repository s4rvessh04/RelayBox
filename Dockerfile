# -------- Build stage --------
FROM golang:1.26-bookworm AS builder

# Install build deps for CGO + librdkafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache deps
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build target is configurable via build arg (defaults to relay)
ARG BUILD_TARGET=relay
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /app/service ./cmd/${BUILD_TARGET}/main.go


# -------- Runtime stage --------
FROM debian:bookworm-slim

# Only runtime lib (no -dev)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      librdkafka1 && \
    rm -rf /var/lib/apt/lists/*

# Non-root user
RUN useradd -r -u 10001 -g root appuser

WORKDIR /app

# Copy binary
COPY --from=builder /app/service /app/service

# Permissions
RUN chown -R appuser:root /app
USER appuser

EXPOSE 8080 8081

HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:8080/health || exit 1

CMD ["./service"]
