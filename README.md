# golang-learning

A Go learning project culminating in a production-style event-driven LLM streaming chat API.

## Architecture

### Data Flow

```
POST /chat ──► Kafka: chat.requested ──► Worker (LLM call) ──► Kafka: chat.completed
                                                  │
GET /chat/stream/:id ◄── Redis SSE buffer ◄───────┘
                                                  │
                                      Persistence Worker ──► PostgreSQL
```

### Sequence Diagram

```mermaid
sequenceDiagram
    actor Client
    participant API
    participant Redis
    participant Kafka
    participant Worker
    participant Persistence
    participant PostgreSQL

    rect rgb(230, 240, 255)
        note over Client,PostgreSQL: POST /chat
        Client->>API: POST /chat {session_id, content}
        API->>Redis: ClaimOwner SetNX(session_id, userID)
        Redis-->>API: owned=true
        API->>Kafka: publish chat.requests
        API->>Redis: SetRequestOwner(request_id, userID)
        API-->>Client: {request_id}
    end

    rect rgb(230, 255, 230)
        note over Client,PostgreSQL: GET /chat/stream/:request_id
        Client->>API: GET /chat/stream/:request_id
        API->>Redis: GetRequestOwner(request_id)
        Redis-->>API: userID
        API->>API: Register SSE channel
        Kafka->>Worker: consume chat.requests
        loop each token
            Worker->>Kafka: publish token → chat.responses
            Kafka->>API: consume token
            API-->>Client: SSE token
        end
        API-->>Client: SSE [DONE]
    end

    rect rgb(255, 240, 230)
        note over Client,PostgreSQL: Lưu vào DB
        Worker->>Redis: SaveMessage(user msg + full reply)
        Worker->>Kafka: publish chat.completed
        Kafka->>Persistence: consume chat.completed
        Persistence->>Redis: GetHistory(session_id, filter by request_id)
        Redis-->>Persistence: messages
        Persistence->>PostgreSQL: SaveMessage (INSERT)
    end
```

### Clean Architecture Rings

```
┌──────────────────────────────────────────────────────────────┐
│  Frameworks & Drivers                                        │
│  (framework/postgres, framework/redis, framework/llm)        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Interface Adapters                                    │  │
│  │  (adapter/controller, adapter/presenter, adapter/gateway)  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │  Use Cases                                       │  │  │
│  │  │  (usecase/ + port interfaces)                    │  │  │
│  │  │  ┌────────────────────────────────────────────┐  │  │  │
│  │  │  │  Entities                                  │  │  │  │
│  │  │  │  (entity/)                                 │  │  │  │
│  │  │  └────────────────────────────────────────────┘  │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**Services:**
- **API** — Gin HTTP server, JWT auth, SSE streaming
- **Worker** — Kafka consumer, calls LLM, publishes responses to Redis + Kafka
- **Persistence** — Kafka consumer on `chat.completed`, writes to PostgreSQL

## Tech Stack

| Layer | Technology |
|---|---|
| HTTP | Gin |
| Event bus | Kafka |
| Cache / SSE state | Redis |
| Database | PostgreSQL + GORM |
| Auth | JWT (golang-jwt/v5) |
| DI | Uber fx |
| Logging | Uber zap |
| LLM | OpenAI-compatible (mock default) |

## Prerequisites

- Go 1.21+
- Docker & Docker Compose

## Setup

```bash
# 1. Copy env config
cp .env.example .env

# 2. Start infrastructure (Kafka, Zookeeper, Redis, PostgreSQL)
make up

# 3. Run database migrations
make migrate
```

## Running

Open three terminals:

```bash
make api          # HTTP server on :8000
make worker       # LLM processing consumer
make persistence  # Database persistence consumer
```

## Usage

```bash
# Generate a JWT token for user "li"
make token USER=li

# Send a chat message and stream the response
make chat SESSION=my-session MSG="Hello, world!"

# View chat history (Redis cache)
make history SESSION=my-session

# View chat history (PostgreSQL)
make history-db SESSION=my-session
```

## API Endpoints

All endpoints require `Authorization: Bearer <token>`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/chat` | Submit a chat message |
| `GET` | `/chat/stream/:request_id` | Stream the LLM response (SSE) |
| `GET` | `/history/:session_id` | Get session history from Redis |
| `GET` | `/history/:session_id/db` | Get session history from PostgreSQL |

### POST /chat

```json
{
  "session_id": "my-session",
  "content": "Tell me about Go channels"
}
```

Returns `request_id` — use it with the stream endpoint.

## Security

- **Session ownership** — first user to open a session claims it via Redis `SetNX`. Subsequent requests from other users are rejected with `403 Forbidden`.
- **Request ownership** — each `request_id` is bound to the user who created it. The SSE stream endpoint rejects connections from users who don't own the request (IDOR prevention).
- **JWT auth** — all endpoints require a signed JWT token.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `DATABASE_URL` | `postgresql://app:app@localhost:5432/chatdb` | PostgreSQL connection URL |
| `REDIS_TTL` | `86400` | Session TTL in seconds |
| `LLM_PROVIDER` | `mock` | LLM provider (`mock` or `openai`) |
| `OPENAI_API_KEY` | — | Required when `LLM_PROVIDER=openai` |
| `JWT_SECRET` | — | Secret key for signing JWTs |
| `PORT` | `8000` | HTTP server port |

## Project Structure

```
cmd/
  api/              # HTTP server entry point (Gin + fx wiring)
  worker/           # LLM consumer entry point
  persistence/      # DB persistence consumer entry point
  migrate/          # Database migration entry point (GORM AutoMigrate)
  gentoken/         # JWT token generator CLI

internal/
  entity/           # Entities ring — pure business types, no framework tags
    message.go      # Message, MessageRole
    session.go      # Session

  usecase/          # Use Cases ring — business logic + port interfaces
    port.go         # Input/Output port interfaces
    send_message.go
    get_history.go
    process_chat_request.go
    persist_session.go

  adapter/          # Interface Adapters ring
    controller/     # Inbound — parse input, call use cases
      http/
        handler/    # Gin HTTP handlers (ChatHandler)
        middleware/ # JWT auth middleware
        state/      # SSEState: in-memory request→channel router
      consumer/     # Kafka consumers (SSE fan-out, worker, persistence)
    presenter/      # Format use case output → HTTP/gRPC response
      http/         # JSON view models (MessageView, SendMessagePresenter)
    gateway/        # Outbound — implement port interfaces
      store/        # PostgreSQL: MessageStore + GORM models
      cache/        # Redis: ConversationCache, SessionOwnerStore, RequestOwnerStore
      broker/       # Kafka: EventPublisher

  framework/        # Frameworks & Drivers ring — infrastructure setup only
    postgres/       # GORM *gorm.DB connection factory
    redis/          # go-redis client factory
    llm/            # Mock LLM token generator

  module/
    logger/         # Zap logger factory

config/             # Config loading from environment variables
shared/             # Shared Kafka message schemas
```

## Extending to gRPC

The Clean Architecture structure makes adding gRPC straightforward — use cases are untouched:

```
adapter/
  controller/
    http/           # existing
    grpc/           # add new gRPC handlers here
  presenter/
    http/           # existing
    grpc/           # add new protobuf formatters here
```

## Build

```bash
make build      # Compiles all binaries to bin/
```
