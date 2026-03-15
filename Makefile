.PHONY: build check test test-unit test-integration test-k8s lint fmt fmt-check \
       docker-build build-images docker-up docker-down e2e clean ci help

CARGO := cargo
DOCKER_COMPOSE := docker compose -f deploy/docker-compose.yml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all workspace crates
	$(CARGO) build --workspace

check: ## Type-check all workspace crates
	$(CARGO) check --workspace

test: test-unit ## Run all unit tests

test-unit: ## Run unit tests only (no external deps)
	$(CARGO) test --workspace --lib

test-integration: ## Run integration tests (requires Postgres via Docker)
	$(DOCKER_COMPOSE) up -d source-postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 5
	$(CARGO) test --workspace --test '*' -- --test-threads=1
	$(DOCKER_COMPOSE) stop source-postgres

lint: ## Run clippy lints
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt: ## Format all code
	$(CARGO) fmt --all

fmt-check: ## Check code formatting
	$(CARGO) fmt --all -- --check

docker-build: ## Build Docker images for both services
	docker build -f deploy/Dockerfile --target wal-capture -t pgiceberg-wal-capture:latest .
	docker build -f deploy/Dockerfile --target iceberg-writer -t pgiceberg-iceberg-writer:latest .

docker-up: ## Start all services via Docker Compose
	$(DOCKER_COMPOSE) up -d

docker-down: ## Stop and remove all Docker Compose services and volumes
	$(DOCKER_COMPOSE) down -v

e2e: ## Run full end-to-end test
	bash deploy/tests/e2e_local.sh

build-images: ## Build Docker images tagged :test for K8s testing
	docker build -f deploy/Dockerfile --target wal-capture -t walrus/wal-capture:test .
	docker build -f deploy/Dockerfile --target iceberg-writer -t walrus/iceberg-writer:test .

test-k8s: build-images ## Run K8s integration test (requires Docker, kind)
	bash deploy/tests/e2e_k8s.sh

ci: fmt-check lint test-unit build ## Run all CI checks locally

clean: ## Clean build artifacts and Docker resources
	$(CARGO) clean
	$(DOCKER_COMPOSE) down -v 2>/dev/null || true
