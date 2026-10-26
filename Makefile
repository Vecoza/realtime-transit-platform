.PHONY: help install duckdb-extensions up platform demo down logs psql topics check migrate migrate-down \
	migrate-status gtfs-load produce record replay process aggregate archive marts scheduler \
	reader-role grafana lint format test test-unit test-db clean

# dbt reads its connection from the environment; Python reads .env itself.
-include .env
export

help:  ## Show available commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:  ## Install the package with dev dependencies at the pinned versions
	pip install -c constraints.txt -e ".[dev]"
	$(MAKE) duckdb-extensions

duckdb-extensions:  ## Install DuckDB's PostgreSQL extension once, so nothing downloads at run time
	python -c "import duckdb; duckdb.connect().install_extension('postgres')"

up:  ## Start PostgreSQL and Redpanda (for running the services on the host)
	docker compose up -d postgres redpanda

platform:  ## Build and start the whole platform in containers, as on a server
	docker compose up -d --build

# The demo runs in containers on the committed fixtures: no live feed, no download.
# FULL=1 loads HSL's current timetable instead and replays the recordings in RECORDING.
DEMO_FIXTURES := $(CURDIR)/tests/fixtures
RECORDING ?= data/recordings
DEMO_TIMETABLE = $(if $(FULL),,--file /app/fixtures/gtfs_saturday_routes)
DEMO_REPLAY = $(if $(FULL),$(abspath $(RECORDING)),$(DEMO_FIXTURES)/hfp_saturday_routes.jsonl)
# Mounted under its own name: the replayer recognises gzip files by their extension.
DEMO_REPLAY_PATH = /app/replay/$(notdir $(DEMO_REPLAY))

demo:  ## Run the platform offline on the recorded fixtures (FULL=1: real timetable and RECORDING)
	@test -f .env || { cp .env.example .env; echo "created .env from .env.example"; }
	docker compose build init
	docker compose up -d --wait postgres redpanda
	docker compose run --rm --no-deps -v "$(DEMO_FIXTURES):/app/fixtures:ro" init sh -c \
		"alembic upgrade head && python -m transit_platform.reader_role \
		&& python -m gtfs.load $(DEMO_TIMETABLE) && python -m archive.lifecycle"
	docker compose up -d sink aggregator grafana
	docker compose run --rm --no-deps -v "$(DEMO_REPLAY):$(DEMO_REPLAY_PATH):ro" producer \
		python -m producer.replay $(DEMO_REPLAY_PATH) --speed 0
	docker compose run --rm --no-deps sink python -m processor.wait
	docker compose run --rm --no-deps scheduler sh -c \
		"dbt build --project-dir dbt --profiles-dir dbt; python -m analytics.publish"
	@echo "Grafana: http://localhost:$(or $(GRAFANA_PORT),3000)/d/punctuality$(if $(FULL),,?var-day=2026-10-24)"

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

marts:  ## Build and test the dbt models in DuckDB, then publish the marts to PostgreSQL
	rm -f dbt/target/run_results.json
	dbt build --project-dir dbt --profiles-dir dbt; status=$$?; python -m analytics.publish; exit $$status

scheduler:  ## Build and start the scheduler container (GTFS refresh, archive, dbt)
	docker compose up -d --build scheduler

reader-role:  ## Set the password of the dashboards' read-only role (grafana_reader) from .env
	python -m transit_platform.reader_role

grafana:  ## Start Grafana with its provisioned data source, dashboards and alert rules
	docker compose up -d grafana

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
