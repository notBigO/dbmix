GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LINT_VERSION := v2.14.0

.PHONY: build test race integration-up integration-down test-integration lint fmt run demo bench clean
build:
	$(GO) build -trimpath -ldflags '-X main.version=$(VERSION)' -o bin/dbmix ./cmd/dbmix

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

integration-up:
	docker compose up -d --wait

integration-down:
	docker compose down

test-integration: integration-up
	$(GO) test -race -tags=integration ./integration -count=1 -timeout=10m

bin/golangci-lint:
	GOBIN=$(CURDIR)/bin $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VERSION)

lint: bin/golangci-lint
	./bin/golangci-lint run ./...
	./bin/golangci-lint run --build-tags=integration ./integration/...
	@test -z "$$($(GO) fmt ./...)" || (echo 'Formatting changed; commit formatted files'; exit 1)

fmt:
	$(GO) fmt ./...

run:
	$(GO) run ./cmd/dbmix $(ARGS)

demo: build integration-up
	./scripts/demo.sh

bench: build integration-up
	./scripts/benchmark.sh

clean:
	rm -rf bin
