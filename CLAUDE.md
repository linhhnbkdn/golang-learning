# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is an event-driven LLM streaming architecture — not a toy. It exists to explore high-throughput chat streaming patterns in Go: Kafka for async dispatch, gRPC client-streaming for token delivery, Redis for session ownership and history, and PostgreSQL for durable persistence.

## Commands

```bash
# Dev: start infrastructure (Kafka, Redis, Postgres)
make up / make down

# Run individual services locally
make api                  # HTTP + gRPC server
make worker               # LLM-request Kafka consumer
make persistence          # Persistence Kafka consumer
make streaming-worker     # Token streaming Kafka consumer
make migrate              # Run DB migrations

# Build all binaries
make build

# Test
go test ./...
go test -run TestFunctionName ./internal/usecase/
go test -v ./internal/adapter/controller/consumer/

# Load testing (full prod stack + Locust at http://localhost:8089)
make benchmark

# Production stack only (docker-compose.prod.yml)
make prod-up / make prod-down / make prod-migrate

# Dev utilities
make token                 # Generate JWT for user "li"
make chat MSG="hello"      # POST /chat/:session_id with JWT
make history               # GET history from Redis
make history-db            # GET history from PostgreSQL

# Proto regeneration
protoc --go_out=proto/gen --go-grpc_out=proto/gen proto/token.proto
```

## Architecture

### Request flow (current)

```
POST /chat/:session_id
  → API: JWT auth → ClaimOwner (Redis Lua) → register TokenHub channel → publish chat.requests (Kafka)
  → API: hold HTTP connection open, stream NDJSON

chat.requests (Kafka)
  → LLM-Request-Worker: call mock/real LLM → gRPC stream each token back to API's TokenHub
  → API: TokenHub.Deliver → write delta to HTTP response

LLM-Request-Worker (on done):
  → publish chat.completed (Kafka)

chat.completed (Kafka)
  → Persistence: Redis GetHistory → PostgreSQL BulkUpsertMessages
```

`TokenHub` is an in-process `sync.Map` keyed by `requestID`. The worker dials gRPC directly to the API instance that owns the request (callback address embedded in `ChatRequest`).

### Clean / hexagonal layers

```
internal/
  entity/         Domain types: Message, Session
  usecase/        Business logic + port interfaces (boundary.go)
  adapter/
    controller/   Inbound: HTTP handlers (Gin), Kafka consumers, gRPC server
    gateway/      Outbound: Redis, PostgreSQL, Kafka, in-process hub implementations
    presenter/    Response formatting
  framework/      Infrastructure wiring: DB, Redis, LLM connections
  module/         Logger factory
config/           Env var loading
shared/           Kafka message schemas: ChatRequest, ChatCompleted, TokenEvent
proto/            gRPC protobuf + generated code
cmd/              Entry points (one per service)
```

All dependency injection is done via **Uber fx** in each `cmd/*/main.go`. Interfaces are declared in `usecase/boundary.go`; concrete implementations are in `adapter/gateway/`. The `cmd/api/main.go` wires everything with `asXxx()` adapter functions that cast concretes to their interfaces.

### Key design decisions

- **Redis Lua (`ClaimOwner`)** — atomic session ownership: first user to claim a session owns it for its TTL; subsequent users on the same session are rejected.
- **TokenHub buffer (1000)** — `Register` returns a buffered channel sized well above the max tokens per request; `Deliver` uses a non-blocking send so a slow HTTP writer cannot stall the gRPC stream.
- **Callback address in ChatRequest** — each `ChatRequest` carries the originating API instance's gRPC address so the worker can route tokens back to exactly the right instance, enabling horizontal scaling.
- **Bulk upsert idempotency** — `ON CONFLICT (request_id, role) DO UPDATE SET content = EXCLUDED.content` so Kafka replays are safe.

## Go Conventions

- Error handling: always check and handle errors explicitly; do not use `_` for errors unless intentional
- Prefer table-driven tests using `t.Run`
- Keep interfaces small (1–3 methods); interfaces live in `usecase/boundary.go`, not in the implementing package
