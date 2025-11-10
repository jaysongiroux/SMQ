# Stage 1: Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
# CGO_ENABLED=0 for static binary
# -ldflags="-w -s" to strip debug info and reduce binary size
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o smq \
    main.go

# Stage 2: Runtime stage
FROM alpine:3.19

# Install ca-certificates for HTTPS and tzdata for timezone support
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user and group
RUN addgroup -g 1000 smq && \
    adduser -D -u 1000 -G smq smq

# Create necessary directories with proper permissions
RUN mkdir -p /app /data /config && \
    chown -R smq:smq /app /data /config

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/smq /app/smq

# Copy default config (can be overridden by volume mount)
COPY --chown=smq:smq config.json /config/config.json.example

# Switch to non-root user
USER smq

# Expose ports (defaults, can be overridden via environment)
# Producer, Consumer, Health
EXPOSE 8081 8082 8083

# Set environment variables with sensible defaults
ENV SMQ_CONFIG_PATH=/config/config.json \
    buffer_wal_path=/data/smq_wal.log

# Add health check
# Checks the health endpoint every 30 seconds
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8083/v1/health || exit 1

# Run the application
ENTRYPOINT ["/app/smq"]

