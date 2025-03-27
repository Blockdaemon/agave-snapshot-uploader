FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /snapshot-monitor ./cmd/snapshot-monitor

# Create a minimal image
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /snapshot-monitor /app/

# Create directory for snapshots
RUN mkdir -p /snapshots

# Set the entrypoint
ENTRYPOINT ["/app/snapshot-monitor"] cngoh0sfd7unq8o7np40
