FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o crawler cmd/crawler/main.go

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/crawler .

RUN mkdir -p /app/data/pebble /app/data/warc

EXPOSE 8080

ENV PORT=8080
ENV DATA_DIR=/app/data/pebble
ENV WARC_DIR=/app/data/warc

ENTRYPOINT ["./crawler"]
