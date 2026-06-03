# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Download dependencies trước (cache layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary — ARG SERVICE chỉ định service cần build (api/worker/persistence/migrate)
ARG SERVICE=api
RUN go build -o /bin/service ./cmd/${SERVICE}/


# Stage 2: Runtime — image nhỏ gọn, không có Go toolchain
FROM alpine:3.19

# ca-certificates: cần cho HTTPS calls
# tzdata: timezone support
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Chỉ copy binary từ stage build
COPY --from=builder /bin/service /app/service

CMD ["/app/service"]
