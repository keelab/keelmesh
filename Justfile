set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

run:
    go run ./cmd/http-demo

run-http:
    go run ./cmd/http-demo

run-grpc:
    go run ./cmd/grpc-demo

build:
    mkdir -p bin
    go build -trimpath -o bin/http-demo ./cmd/http-demo
    go build -trimpath -o bin/grpc-demo ./cmd/grpc-demo

test:
    go test ./...

test-race:
    go test -race ./...

fmt:
    test -z "$(gofmt -l .)"

vet:
    go vet ./...

lint:
    golangci-lint run

check: fmt vet test

tidy:
    go mod tidy
