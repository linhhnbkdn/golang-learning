# golang-learning

A Go learning project culminating in a production-style event-driven LLM streaming chat API.

## Architecture

Clean (Hexagonal) Architecture with event-driven async processing:

```
POST /chat ──► Kafka topic: chat.requested ──► Worker (LLM call) ──► Kafka topic: chat.completed
                                                        │
GET /chat/stream/:id ◄── Redis SSE buffer ◄────────────┘
                                                        │
                                            Persistence Worker ──► PostgreSQL
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
| Database | PostgreSQL |
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
  api/          # HTTP server entry point
  worker/       # LLM consumer entry point
  persistence/  # DB persistence consumer entry point
  migrate/      # Database migration entry point
  gentoken/     # JWT token generator CLI
internal/
  api/          # Handlers, middleware, SSE state
  application/  # Use cases and port interfaces
  domain/       # Core domain types
  infrastructure/ # Kafka, Redis, PostgreSQL, LLM adapters
  consumer/     # Kafka consumer logic
  logger/       # Zap logger factory
config/         # Config loading from env
shared/         # Shared Kafka message schemas
```

## Build

```bash
make build      # Compiles all binaries to bin/
```
