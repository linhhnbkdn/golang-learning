SESSION    ?= demo-001
MSG        ?= xin chào
USER       ?= li
PORT       ?= 8000
DOCKER_HOST ?= unix:///run/user/1000/docker.sock

.PHONY: up down migrate api worker persistence chat history history-db build token \
        run-api run-worker run-persistence \
        prod-up prod-down prod-migrate prod-chat \
        benchmark

# ── Dev ──────────────────────────────────────────────────────────────────────

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
	go build -o ./api         ./cmd/api/
	go build -o ./worker      ./cmd/worker/
	go build -o ./persistence ./cmd/persistence/
	go build -o ./migrate     ./cmd/migrate/

run-api: build
	./api

run-worker: build
	./worker

run-persistence: build
	./persistence

token:
	@go run ./cmd/gentoken/ $(USER)

chat:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@echo "Connecting to ws://localhost:$(PORT)/ws/chat/$(SESSION)"
	@echo 'Send: {"content":"your message"}  |  Ctrl+C to exit'
	@wscat -c "ws://localhost:$(PORT)/ws/chat/$(SESSION)?token=$(T)"

history:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@curl -s http://localhost:$(PORT)/history/$(SESSION) \
		-H "Authorization: Bearer $(T)" | python3 -m json.tool

history-db:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@curl -s http://localhost:$(PORT)/history/$(SESSION)/db \
		-H "Authorization: Bearer $(T)" | python3 -m json.tool

# ── Production ───────────────────────────────────────────────────────────────

prod-up:
	DOCKER_HOST=$(DOCKER_HOST) docker compose -f docker-compose.prod.yml up -d --build

prod-down:
	DOCKER_HOST=$(DOCKER_HOST) docker compose -f docker-compose.prod.yml down

prod-migrate:
	DOCKER_HOST=$(DOCKER_HOST) docker compose -f docker-compose.prod.yml run --rm \
		-e SERVICE=migrate api /app/service

benchmark:
	docker compose -f docker-compose.prod.yml --profile benchmark up -d locust
	@echo "Locust UI: http://localhost:8089"

prod-chat:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@echo "Connecting to ws://localhost:$(PORT)/ws/chat/$(SESSION)"
	@echo 'Send: {"content":"your message"}  |  Ctrl+C to exit'
	@wscat -c "ws://localhost:$(PORT)/ws/chat/$(SESSION)?token=$(T)"
