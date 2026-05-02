FROM golang:1.23-alpine as builder
RUN apk add --no-cache build-base gcc librdkafka-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o relay ./cmd/relay/main.go

FROM alpine:latest
RUN apk add --no-cache librdkafka
WORKDIR /root/
COPY --from=builder /app/relay .
CMD ["./relay"]
