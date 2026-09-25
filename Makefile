# EventFlow developer commands.
#
# There is no Go toolchain required on the host - build/test run inside the golang
# Docker image so the only prerequisite is Docker. The GOCACHE/GOMODCACHE bind
# mounts make repeated runs fast.

GO_IMAGE := golang:1.23
GO_RUN := docker run --rm -v "$(PWD)":/app -w /app \
	-v "$(PWD)/.gocache":/root/.cache/go-build \
	-v "$(PWD)/.gomodcache":/go/pkg/mod \
	$(GO_IMAGE)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: tidy
tidy: ## go mod tidy
	$(GO_RUN) sh -c "GOFLAGS=-mod=mod go mod tidy"

.PHONY: build
build: ## Compile all packages
	$(GO_RUN) go build -buildvcs=false ./...

.PHONY: vet
vet: ## go vet
	$(GO_RUN) go vet -buildvcs=false ./...

.PHONY: test
test: ## Run unit tests
	$(GO_RUN) go test -buildvcs=false ./...

.PHONY: up
up: ## Build images and start the whole stack
	docker compose up --build -d

.PHONY: down
down: ## Stop the stack and remove volumes
	docker compose down -v

.PHONY: ps
ps: ## Show service status
	docker compose ps

.PHONY: logs
logs: ## Tail logs from all services
	docker compose logs -f

.PHONY: smoke
smoke: ## Run the end-to-end smoke test against a running stack
	./scripts/smoke.sh
