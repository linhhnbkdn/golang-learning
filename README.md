# golang-learning

A Go learning project culminating in a production-style event-driven LLM streaming chat API.

## Architecture

### Data Flow

```
GET /history (HTTP)  ──────────────────────────────────────────► Redis / PostgreSQL
WS  /ws/chat/:session_id?token=JWT
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

    rect rgb(230, 240, 255)
        note over Client,PostgreSQL: WebSocket connect
        Client->>API: GET /ws/chat/:session_id?token=JWT (Upgrade)
        API->>API: ParseJWT(token)
        API->>Redis: ClaimOwner SetNX(session_id, userID)
        Redis-->>API: owned=true
        API->>Redis: Subscribe(pubsub:session_id)
        API-->>Client: 101 Switching Protocols
    end

    rect rgb(230, 255, 230)
        note over Client,PostgreSQL: Send message & stream tokens
        Client->>API: WS {"content": "..."}
        API->>Kafka: publish chat.requests
        Kafka->>Worker: consume chat.requests
        loop each token
            Worker->>Redis: Publish(pubsub:session_id, token)
            Redis-->>API: token via subscription
            API-->>Client: WS {request_id, delta, done:false}
        end
        API-->>Client: WS {request_id, delta:"", done:true}
        API-->>Client: WS CloseNormalClosure (1000)
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
- **API** — Gin HTTP server, JWT auth, WebSocket streaming via Redis Pub/Sub
- **Worker** — Kafka consumer, calls LLM, publishes tokens to Redis Pub/Sub + Kafka
- **Persistence** — Kafka consumer on `chat.completed`, writes to PostgreSQL

## Tech Stack

| Layer | Technology |
|---|---|
| HTTP / WebSocket | Gin + gorilla/websocket |
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
make api          # HTTP + WebSocket server on :8000
make worker       # LLM processing consumer
make persistence  # Database persistence consumer
```

## Usage

```bash
# Generate a JWT token for user "li"
make token USER=li

# Connect and chat via WebSocket (requires wscat)
TOKEN=$(make token USER=li)
wscat -c "ws://localhost:8000/ws/chat/my-session?token=$TOKEN"
# then type: {"content":"Tell me about Go channels"}

# View chat history (Redis cache)
make history SESSION=my-session

# View chat history (PostgreSQL)
make history-db SESSION=my-session
```

## API Endpoints

### WebSocket — `/ws/chat/:session_id`

Auth via query param: `?token=<JWT>`

Connect once per session. Send a message, receive a stream of token frames. The server sends `done:true` and closes with `1000 Normal Closure` when streaming finishes.

**Client → Server:**
```json
{"content": "Tell me about Go channels"}
```

**Server → Client (per token):**
```json
{"request_id": "abc123", "delta": "Go ", "done": false}
{"request_id": "abc123", "delta": "channels", "done": false}
{"request_id": "abc123", "delta": "", "done": true}
```

**Error frame:**
```json
{"error": "stream in progress"}
```

### HTTP endpoints

All require `Authorization: Bearer <token>`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/history/:session_id` | Session history from Redis |
| `GET` | `/history/:session_id/db` | Session history from PostgreSQL |

## Security

- **Session ownership** — first user to open a WebSocket for a session claims it via Redis `SetNX`. Other users receive `{"error":"forbidden"}` and the connection is closed.
- **JWT auth** — WebSocket upgrade requires a valid signed JWT passed as `?token=`.

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
        handler/    # Gin HTTP handlers (history endpoints)
        middleware/ # JWT auth + ParseJWT helper
      ws/
        handler/    # WebSocket handler (ChatWsHandler)
      consumer/     # Kafka consumers (worker, persistence)
    presenter/      # Format use case output → HTTP/WS response
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
