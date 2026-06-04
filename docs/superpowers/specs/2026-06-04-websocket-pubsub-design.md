# WebSocket + Redis Pub/Sub — Replace SSE

## Goal

Replace 2-round-trip SSE flow (POST /chat + GET /stream/:id) with a single WebSocket connection. Replace Redis Streams with Redis Pub/Sub to eliminate memory accumulation.

## Requirements

- 1 WS connection per message: connect → send → receive stream → DONE → close
- Auth via query param: `?token=JWT`
- Fan-out by session_id: multiple tabs with same session receive same tokens
- Block new message if stream already in progress for that session
- WS connects on user action (not on page load)
- Same session_id reused as Pub/Sub channel key across messages (stateless, no cleanup needed)

## Sequence Diagram

```
Browser Tab 1          API Service          Kafka         Worker Service        Redis Pub/Sub
     │                     │                  │                 │                     │
     │──WS /ws/chat/       │                  │                 │                     │
     │  sess_A?token=JWT──>│                  │                 │                     │
     │                     │─verify JWT       │                 │                     │
     │                     │─ClaimOwner(sess) │                 │                     │
     │                     │─SUBSCRIBE ───────────────────────────────────────────────>│
     │<──WS connected───── │                  │                 │                     │
     │                     │                  │                 │                     │
     │──{"content":        │                  │                 │                     │
     │   "xin chào"}──────>│                  │                 │                     │
     │                     │──chat.requests──>│                 │                     │
     │                     │                  │──consume───────>│                     │
     │                     │                  │                 │──call LLM           │
     │                     │                  │                 │<──"Xin"             │
     │                     │                  │                 │──PUBLISH ───────────>│
     │<──WS: "Xin"─────────│<────────────────────────────────────────────────────────│
     │                     │                  │                 │<──[DONE]            │
     │                     │                  │                 │──PUBLISH done ──────>│
     │<──WS: [DONE]────────│<────────────────────────────────────────────────────────│
     │──WS closed──────────│                  │                 │                     │
```

Tab 2 (same session, concurrent): connects independently, subscribes to same Pub/Sub channel, receives same tokens automatically.

## Pub/Sub Channel

```
Key:     pubsub:session:{session_id}
Created: implicitly on first SUBSCRIBE or PUBLISH
Removed: implicitly when no subscribers remain
Memory:  zero — no data stored, fire and forget
```

## Message Formats

**Client → Server (WS message):**
```json
{"content": "xin chào"}
```

**Server → Client (WS token):**
```json
{"request_id": "uuid", "delta": "Xin", "done": false}
```

**Server → Client (WS done):**
```json
{"request_id": "uuid", "delta": "", "done": true}
```

**Server → Client (error):**
```json
{"error": "stream in progress"}
```

## Components

### Add

| File | Purpose |
|------|---------|
| `internal/adapter/controller/ws/handler/chat.go` | WS handler — auth, ownership, read/write loops |
| `internal/adapter/gateway/cache/pubsub_stream.go` | Redis Pub/Sub implementation |
| `usecase.IPubSubStream` in `boundary.go` | Port interface for publish/subscribe |

### Remove

| File / Symbol | Reason |
|--------------|--------|
| `cache/sse_stream.go` | Replaced by pubsub_stream.go |
| `cache/request_owner.go` | No request_id round trip needed |
| `usecase.ISSEStream` | Replaced by IPubSubStream |
| `usecase.IRequestOwnerStore` | No longer needed |
| `usecase.SSEToken` | Replaced by new token struct |
| Route `POST /chat` | Merged into WS handler |
| Route `GET /chat/stream/:id` | Replaced by WS |

### Modify

| File | Change |
|------|--------|
| `boundary.go` | Add IPubSubStream, remove ISSEStream + IRequestOwnerStore |
| `process_chat_request.go` | Publish tokens via IPubSubStream.Publish instead of ISSEStream |
| `cmd/api/main.go` | Wire WS handler, remove SSE wiring |
| `handler/chat.go` | Remove PostChat + StreamResponse, keep GetHistory + GetHistoryDB |

## IPubSubStream Interface

```go
type IPubSubStream interface {
    Publish(ctx context.Context, sessionID, requestID, delta string, done bool) error
    Subscribe(ctx context.Context, sessionID string) (<-chan PubSubToken, func(), error)
}

type PubSubToken struct {
    RequestID string
    Delta     string
    Done      bool
}
```

## WS Handler Logic

```
OnConnect:
  1. Parse JWT from query param ?token=
  2. ClaimOwner(session_id, user_id) → 403 if fails
  3. Subscribe(session_id) → get token channel
  4. Start write goroutine: read from token channel → push WS messages
  5. Start read loop: read WS messages from client

OnClientMessage:
  1. If streaming in progress → send {"error": "stream in progress"}
  2. Set streaming = true
  3. Publish to Kafka chat.requests

OnToken (from Pub/Sub):
  1. Push token to client via WS
  2. If done == true → set streaming = false → close WS

OnDisconnect:
  1. Unsubscribe from Pub/Sub
  2. Cancel context
```

## Library

`github.com/gorilla/websocket` — standard Go WebSocket library.

## Out of Scope

- Tab 2 sync via getHistory + compare (deferred)
- Reconnect logic on WS drop
