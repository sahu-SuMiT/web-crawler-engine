# ==========================================
# STAGE 1: Build executable binary
# ==========================================
FROM golang:1.22-alpine AS builder

# Install CA certificates for HTTPS/SSL support
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go module files first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy source code into container
COPY . .

# Build statically compiled binary (-w -s strips debugging symbols for smaller binary size)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o crawler cmd/crawler/main.go

# ==========================================
# STAGE 2: Lightweight runtime environment
# ==========================================
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy compiled binary from builder stage
COPY --from=builder /app/crawler .

# Create directory structures for local disk data
RUN mkdir -p /app/data/pebble /app/data/warc

# Expose Web UI dashboard port
EXPOSE 8080

# Set environment defaults
ENV PORT=8080
ENV DATA_DIR=/app/data/pebble
ENV WARC_DIR=/app/data/warc

# Command to execute on container launch
ENTRYPOINT ["./crawler"]
