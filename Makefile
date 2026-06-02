SESSION ?= demo-001
MSG     ?= xin chào
PORT    ?= 8000

.PHONY: up down migrate api worker persistence chat history build

up:
	docker compose up -d

down:
	docker compose down

migrate:
	go run ./cmd/migrate/

api:
	go run ./cmd/api/

worker:
	go run ./cmd/worker/

persistence:
	go run ./cmd/persistence/

build:
	go build -o bin/api         ./cmd/api/
	go build -o bin/worker      ./cmd/worker/
	go build -o bin/persistence ./cmd/persistence/
	go build -o bin/migrate     ./cmd/migrate/

chat:
	@curl -s -X POST http://localhost:$(PORT)/chat \
		-H "Content-Type: application/json" \
		-d '{"session_id":"$(SESSION)","content":"$(MSG)"}' | tee /tmp/chat_resp.json
	@echo ""
	@REQUEST_ID=$$(cat /tmp/chat_resp.json | grep -o '"request_id":"[^"]*"' | cut -d'"' -f4); \
		echo "Streaming request_id: $$REQUEST_ID"; \
		curl -s "http://localhost:$(PORT)/chat/stream/$$REQUEST_ID"

history:
	@curl -s http://localhost:$(PORT)/history/$(SESSION) | python3 -m json.tool

history-db:
	@curl -s http://localhost:$(PORT)/history/$(SESSION)/db | python3 -m json.tool
