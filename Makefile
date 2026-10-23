.PHONY: help install up down logs psql topics check lint format test clean

help:  ## Show available commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:  ## Install the package with dev dependencies
	pip install -e ".[dev]"

up:  ## Start PostgreSQL and Redpanda
	docker compose up -d

down:  ## Stop containers
	docker compose down

logs:  ## Follow container logs
	docker compose logs -f

psql:  ## Open a psql shell in the database container
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

topics:  ## List Redpanda topics
	docker compose exec redpanda rpk topic list

check:  ## Verify database and stream connections from Python
	python -m transit_platform.health

lint:  ## Run linter
	ruff check .

format:  ## Format code
	ruff format .

test:  ## Run tests
	pytest

clean:  ## Remove caches
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
