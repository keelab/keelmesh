.PHONY: help run run-http run-grpc build build-http build-grpc test test-race fmt vet lint check tidy generate migrate-up migrate-down

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_0-9-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

run: run-http ## Run the default HTTP process.

run-http: ## Run the Keelith HTTP process.
	go run ./cmd/http-demo

run-grpc: ## Run the Keelith gRPC process.
	go run ./cmd/grpc-demo

build: build-http build-grpc ## Build both process binaries.

build-http: ## Build the HTTP binary.
	mkdir -p bin
	go build -trimpath -o bin/http-demo ./cmd/http-demo

build-grpc: ## Build the gRPC binary.
	mkdir -p bin
	go build -trimpath -o bin/grpc-demo ./cmd/grpc-demo

test: ## Run unit tests.
	go test ./...

test-race: ## Run tests with the race detector.
	go test -race ./...

fmt: ## Verify that Go source is formatted.
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet: ## Run go vet.
	go vet ./...

lint: ## Run golangci-lint.
	golangci-lint run

check: fmt vet test ## Run the standard local quality gate.

tidy: ## Normalize module dependencies.
	go mod tidy

generate: ## Generate API code.
	./scripts/generate.sh

migrate-up: ## Apply all database migrations (requires DATABASE_URL).
	./scripts/migrate.sh up

migrate-down: ## Roll back one database migration (requires DATABASE_URL).
	./scripts/migrate.sh down 1
