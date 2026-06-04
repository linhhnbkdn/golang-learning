SESSION ?= demo-001
MSG     ?= xin chào
USER    ?= li
PORT    ?= 8000

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
	@curl -N -s -X POST http://localhost:$(PORT)/chat/$(SESSION) \
		-H "Authorization: Bearer $(T)" \
		-H "Content-Type: application/json" \
		-d "{\"content\":\"$(MSG)\"}"

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
	docker compose -f docker-compose.prod.yml up -d --build

prod-down:
	docker compose -f docker-compose.prod.yml down

prod-migrate:
	docker compose -f docker-compose.prod.yml run --rm \
		-e SERVICE=migrate api /app/service

benchmark:
	docker compose -f docker-compose.prod.yml --profile benchmark up -d locust
	@echo "Locust UI: http://localhost:8089"

prod-chat:
	$(eval T := $(shell go run ./cmd/gentoken/ $(USER)))
	@curl -N -s -X POST http://localhost:$(PORT)/chat/$(SESSION) \
		-H "Authorization: Bearer $(T)" \
		-H "Content-Type: application/json" \
		-d "{\"content\":\"$(MSG)\"}"
