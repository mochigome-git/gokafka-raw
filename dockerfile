# --- Stage 1: Build ---
FROM golang:1.25.3-alpine AS builder

# Accept platform args from buildx
ARG TARGETOS
ARG TARGETARCH

# Configure Go environment for static build
ENV CGO_ENABLED=0 \
    GOOS=$TARGETOS \
    GOARCH=$TARGETARCH

WORKDIR /app

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary (adjust the main package path if needed)
RUN go build -o main ./cmd/

# --- Stage 2: Runtime ---
FROM gcr.io/distroless/base-debian11

# Copy built binary
COPY --from=builder /app/main /app/main

# Working directory and port
WORKDIR /app
EXPOSE 8080

# Run the app
ENTRYPOINT ["/app/main"]
