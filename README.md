# golang-learning

A Go learning project culminating in a production-style event-driven LLM streaming chat API.

## Architecture

### Data Flow

```
GET /history (HTTP)  ──────────────────────────────────────────► Redis / PostgreSQL
POST /chat/:session_id (HTTP chunked stream)
  │
  ├─ send msg ──► Kafka: chat.requests ──► Worker (LLM call) ──► Redis Pub/Sub publish
  │                                                 │
  └─ recv tokens ◄── Redis Pub/Sub subscribe ◄──────┘
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

    rect rgb(230, 255, 230)
        note over Client,PostgreSQL: Send message & stream tokens
        Client->>API: POST /chat/:session_id {"content": "..."}
        API->>API: ParseJWT(Authorization header)
        API->>Redis: ClaimOwner SetNX(session_id, userID)
        Redis-->>API: owned=true
        API->>Redis: Subscribe(pubsub:session_id)
        API->>Kafka: publish chat.requests
        note over API,Client: HTTP 200 — Transfer-Encoding: chunked (connection stays open)
        Kafka->>Worker: consume chat.requests
        loop each token
            Worker->>Redis: Publish(pubsub:session_id, token)
            Redis-->>API: token via subscription
            API-->>Client: chunk {request_id, delta, done:false}
        end
        API-->>Client: chunk {request_id, delta:"", done:true}
        API-->>Client: EOF (response closed)
    end

    rect rgb(255, 240, 230)
        note over Client,PostgreSQL: Persist to DB
        Worker->>Redis: SaveMessage(user msg + full reply)
        Worker->>Kafka: publish chat.completed
        Kafka->>Persistence: consume chat.completed
        Persistence->>Redis: GetHistory(session_id)
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
- **API** — Gin HTTP server, JWT auth, chunked HTTP streaming via Redis Pub/Sub
- **Worker** — Kafka consumer, calls LLM, publishes tokens to Redis Pub/Sub + Kafka
- **Persistence** — Kafka consumer on `chat.completed`, writes to PostgreSQL

## Tech Stack

| Layer | Technology |
|---|---|
| HTTP streaming | Gin (chunked transfer) |
| Event bus | Kafka |
| Token streaming | Redis Pub/Sub |
| Cache / session state | Redis |
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

# 2. Start all services
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

# Send a message and stream the response
TOKEN=$(make token USER=li)
curl -N -X POST http://localhost:8000/chat/my-session \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"Tell me about Go channels"}'

# View chat history (Redis cache)
make history SESSION=my-session

# View chat history (PostgreSQL)
make history-db SESSION=my-session
```

## API Endpoints

### POST `/chat/:session_id` — send message & stream response

Auth via `Authorization: Bearer <JWT>` header.

Send a message, receive a chunked stream of token frames. Connection closes (EOF) when streaming finishes.

**Request body:**
```json
{"content": "Tell me about Go channels"}
```

**Response — one JSON object per chunk:**
```json
{"request_id": "abc123", "delta": "Go ", "done": false}
{"request_id": "abc123", "delta": "channels", "done": false}
{"request_id": "abc123", "delta": "", "done": true}
```

### Other HTTP endpoints

All require `Authorization: Bearer <token>`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/history/:session_id` | Session history from Redis |
| `GET` | `/history/:session_id/db` | Session history from PostgreSQL |

## Security

- **Session ownership** — first user to POST to a session claims it via Redis `SetNX`. Other users receive `403 Forbidden`.
- **JWT auth** — all endpoints require a valid signed JWT in the `Authorization: Bearer` header.

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
  api/              # HTTP + WebSocket server entry point (Gin + fx wiring)
  worker/           # LLM consumer entry point
  persistence/      # DB persistence consumer entry point
  migrate/          # Database migration entry point (GORM AutoMigrate)
  gentoken/         # JWT token generator CLI

internal/
  entity/           # Entities ring — pure business types, no framework tags
    message.go      # Message, MessageRole
    session.go      # Session

  usecase/          # Use Cases ring — business logic + port interfaces
    boundary.go     # Port interfaces (IPubSubStream, ISessionOwnerStore, ...)
    send_message.go
    get_history.go
    process_chat_request.go
    persist_session.go

  adapter/          # Interface Adapters ring
    controller/     # Inbound — parse input, call use cases
      http/
        handler/    # Gin HTTP handlers (chat stream + history endpoints)
        middleware/ # JWT auth + ParseJWT helper
      consumer/     # Kafka consumers (worker, persistence)
    presenter/      # Format use case output → HTTP response
      http/         # JSON view models (MessageView, SendMessagePresenter)
    gateway/        # Outbound — implement port interfaces
      store/        # PostgreSQL: MessageStore + GORM models
      cache/        # Redis: ConversationCache, SessionOwnerStore, PubSubStreamImpl
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
    ws/             # existing
    grpc/           # add new gRPC handlers here
  presenter/
    http/           # existing
    grpc/           # add new protobuf formatters here
```

## Build

```bash
make build      # Compiles all binaries to bin/
```
