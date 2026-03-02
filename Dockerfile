FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the server application (includes consumer logic in same process)
RUN CGO_ENABLED=0 GOOS=linux go build -o /bidsrv ./cmd/server

# Final stage
FROM alpine:latest

WORKDIR /

# Copy the pre-built server binary
COPY --from=builder /bidsrv /bidsrv

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["/bidsrv"]
