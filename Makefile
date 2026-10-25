.PHONY: help install duckdb-extensions up down logs psql topics check migrate migrate-down \
	migrate-status gtfs-load produce record replay process aggregate archive \
	lint format test test-unit test-db clean

help:  ## Show available commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:  ## Install the package with dev dependencies at the pinned versions
	pip install -c constraints.txt -e ".[dev]"
	$(MAKE) duckdb-extensions

duckdb-extensions:  ## Install DuckDB's PostgreSQL extension once, so nothing downloads at run time
	python -c "import duckdb; duckdb.connect().install_extension('postgres')"

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

migrate:  ## Apply all database migrations
	alembic upgrade head

migrate-down:  ## Revert all database migrations
	alembic downgrade base

migrate-status:  ## Show the current migration and the history
	alembic current
	alembic history

gtfs-load:  ## Download the GTFS timetable and load it (FILE=path.zip loads a local zip)
	python -m gtfs.load $(if $(FILE),--file $(FILE))

produce:  ## Forward the live HSL feed to the raw topic
	python -m producer.produce

record:  ## Record the live HSL feed to hourly files in data/recordings
	python -m producer.record

replay:  ## Replay a recording (FILE=file or directory, SPEED=1, 10 or 0 for no waiting)
	python -m producer.replay $(FILE) --speed $(or $(SPEED),1)

process:  ## Store the raw topic in PostgreSQL (consumer group sink)
	python -m processor.sink

aggregate:  ## Per-minute event-time statistics from the raw topic (consumer group aggregator)
	python -m processor.aggregate

archive:  ## Move old day partitions and replaced timetable versions to Parquet
	python -m archive.lifecycle

lint:  ## Run linter
	ruff check .

format:  ## Format code
	ruff format .

test:  ## Run all tests (database tests are skipped if PostgreSQL is not running)
	pytest

test-unit:  ## Run tests that do not need a database
	pytest -m "not db"

test-db:  ## Run database tests
	pytest -m db

clean:  ## Remove caches
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
