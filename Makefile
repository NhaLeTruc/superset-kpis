# GoodNote Analytics Platform - Makefile
# All commands run via Docker containers (no virtual environment required)

.PHONY: help quickstart setup test clean status logs lint format check fix typecheck security quality install-hooks pre-commit-all update-hooks

# Docker container names
SPARK_DEV := goodnote-spark-dev
SPARK_MASTER := goodnote-spark-master
POSTGRES := goodnote-postgres

# Default target - show help
help:
	@echo "GoodNote Analytics Platform - Available Commands"
	@echo "================================================="
	@echo ""
	@echo "Quick Start:"
	@echo "  make quickstart     - Start everything (Docker + generate data + run jobs)"
	@echo "  make setup          - Initial setup (start Docker services only)"
	@echo ""
	@echo "Testing (via Docker):"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make test-file F=<file> - Run specific test file"
	@echo ""
	@echo "Code Quality (via Docker):"
	@echo "  make lint           - Run Ruff linter"
	@echo "  make format         - Format code with Ruff"
	@echo "  make fix            - Auto-fix all code issues"
	@echo "  make check          - Check formatting and linting"
	@echo "  make typecheck      - Run mypy type checker"
	@echo "  make security       - Run Bandit security scan"
	@echo "  make quality        - Run all quality checks"
	@echo "  make pre-commit     - Run pre-commit checks (via Docker)"
	@echo ""
	@echo "Data & Jobs:"
	@echo "  make generate-data  - Generate sample data (medium size)"
	@echo "  make run-jobs       - Run all Spark ETL jobs"
	@echo "  make run-job-1      - Run Data Processing job only"
	@echo "  make run-job-2      - Run User Engagement job only"
	@echo "  make run-job-3      - Run Performance Metrics job only"
	@echo "  make run-job-4      - Run Session Analysis job only"
	@echo ""
	@echo "Docker Management:"
	@echo "  make up             - Start Docker services"
	@echo "  make down           - Stop Docker services"
	@echo "  make restart        - Restart all services"
	@echo "  make status         - Show service status"
	@echo "  make logs           - View all service logs"
	@echo ""
	@echo "Database:"
	@echo "  make db-init        - Initialize PostgreSQL database"
	@echo "  make db-connect     - Connect to PostgreSQL database"
	@echo "  make db-tables      - List all database tables"
	@echo ""
	@echo "Development:"
	@echo "  make shell          - Open Spark dev container shell"
	@echo "  make shell-master   - Open Spark master shell"
	@echo "  make superset       - Show Superset URL"
	@echo "  make spark-ui       - Show Spark UI URLs"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean          - Stop services and clean volumes"
	@echo "  make clean-all      - Full cleanup (WARNING: deletes all data)"
	@echo ""

# ============================================================================
# Quick Start Commands
# ============================================================================

quickstart: setup generate-data run-jobs
	@echo ""
	@echo "Quickstart Complete!"
	@echo ""
	@echo "Access Points:"
	@echo "  - Spark Master UI:  http://localhost:8080"
	@echo "  - Spark App UI:     http://localhost:4040"
	@echo "  - Apache Superset:  http://localhost:8088 (admin/admin)"
	@echo ""
	@echo "Next Steps:"
	@echo "  1. Run 'make test' to verify all tests pass"
	@echo "  2. Access Superset to create dashboards"
	@echo "  3. See docs/SETUP_INSTRUCTIONS.md for details"
	@echo ""

setup: up
	@echo "Waiting for services to be ready (30 seconds)..."
	@sleep 30
	@echo "Setup complete! All services are running."
	@make status

# ============================================================================
# Docker Management
# ============================================================================

up:
	@echo "Starting Docker services..."
	docker compose up -d
	@echo "Docker services started"

down:
	@echo "Stopping Docker services..."
	docker compose down -v --remove-orphans
	@echo "Docker services stopped"

restart: down up
	@echo "Services restarted"

status:
	@echo "Service Status:"
	@docker compose ps

logs:
	@echo "Service Logs (Ctrl+C to exit):"
	docker compose logs -f

logs-spark:
	@echo "Spark Master Logs:"
	docker compose logs -f spark-master

logs-superset:
	@echo "Superset Logs:"
	docker compose logs -f superset

logs-dev:
	@echo "Spark Dev Container Logs:"
	docker compose logs -f spark-dev

logs-postgres:
	@echo "PostgreSQL Logs:"
	docker compose logs -f postgres

# Build/rebuild Docker images
build:
	@echo "Building Docker images..."
	docker compose build
	@echo "Build complete"

build-no-cache:
	@echo "Building Docker images (no cache)..."
	docker compose build --no-cache
	@echo "Build complete"

# ============================================================================
# Testing (via Docker)
# ============================================================================

test: test-unit test-integration
	@echo "All tests completed"

test-unit:
	@echo "Running unit tests..."
	docker exec $(SPARK_DEV) pytest tests/unit -v --tb=short
	@echo "Unit tests completed"

test-integration:
	@echo "Running integration tests..."
	docker exec $(SPARK_DEV) pytest tests/integration -v --tb=short
	@echo "Integration tests completed"

test-coverage:
	@echo "Running tests with coverage report..."
	docker exec $(SPARK_DEV) pytest tests/unit \
		--cov=src \
		--cov-report=html \
		--cov-report=term \
		-v
	@echo "Coverage report generated at htmlcov/index.html"

test-file:
	@if [ -z "$(F)" ]; then \
		echo "Usage: make test-file F=tests/unit/test_file.py"; \
		echo "Example: make test-file F=tests/unit/test_join_transforms.py"; \
		exit 1; \
	fi
	@echo "Running tests in $(F)..."
	docker exec $(SPARK_DEV) pytest $(F) -v --tb=short

test-function:
	@if [ -z "$(T)" ]; then \
		echo "Usage: make test-function T=tests/unit/test_file.py::test_function_name"; \
		exit 1; \
	fi
	@echo "Running test $(T)..."
	docker exec $(SPARK_DEV) pytest $(T) -v --tb=long

# ============================================================================
# Data Generation & Jobs
# ============================================================================

generate-data:
	@echo "Generating sample data (medium size)..."
	docker exec $(SPARK_MASTER) python3 /opt/spark-apps/scripts/generate_sample_data.py \
		--medium \
		--seed 42
	@echo "Sample data generated"

generate-data-small:
	@echo "Generating small sample data (for quick testing)..."
	docker exec $(SPARK_MASTER) python3 /opt/spark-apps/scripts/generate_sample_data.py \
		--small \
		--seed 42
	@echo "Small sample data generated"

generate-data-large:
	@echo "Generating large sample data (WARNING: may take several minutes)..."
	docker exec $(SPARK_MASTER) python3 /opt/spark-apps/scripts/generate_sample_data.py \
		--large \
		--seed 42
	@echo "Large sample data generated"

run-jobs:
	@echo "Running all Spark ETL jobs..."
	docker exec $(SPARK_MASTER) bash /opt/spark-apps/src/jobs/run_all_jobs.sh
	@echo "All jobs completed"

run-job-1:
	@echo "Running Job 1: Data Processing..."
	docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/01_data_processing.py \
        --interactions-path /app/data/raw/user_interactions.csv \
        --metadata-path /app/data/raw/user_metadata.csv \
        --output-path /app/data/processed/enriched_interactions.parquet'
	@echo "Data Processing job completed"

run-job-2:
	@echo "Running Job 2: User Engagement..."
	docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/02_user_engagement.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
	@echo "User Engagement job completed"

run-job-3:
	@echo "Running Job 3: Performance Metrics..."
	docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/03_performance_metrics.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
	@echo "Performance Metrics job completed"

run-job-4:
	@echo "Running Job 4: Session Analysis..."
	docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "local[*]" \
        /opt/spark-apps/src/jobs/04_session_analysis.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --write-to-db'
	@echo "Session Analysis job completed"

# ============================================================================
# Database Management
# ============================================================================

# Database credentials (from docker-compose environment)
DB_USER := analytics_user
DB_NAME := analytics

db-init:
	@echo "Initializing PostgreSQL database..."
	docker exec $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME) -f /docker-entrypoint-initdb.d/01_schema.sql
	docker exec $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME) -f /docker-entrypoint-initdb.d/02_indexes.sql
	@echo "Database initialized"

db-connect:
	@echo "Connecting to PostgreSQL database..."
	docker exec -it $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME)

db-tables:
	@echo "Listing all database tables..."
	docker exec $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME) -c "\dt"

db-table-counts:
	@echo "Showing row counts for all tables..."
	docker exec $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME) -c "\
		SELECT \
			schemaname, \
			tablename, \
			n_live_tup as row_count \
		FROM pg_stat_user_tables \
		ORDER BY n_live_tup DESC;"

db-query:
	@if [ -z "$(Q)" ]; then \
		echo "Usage: make db-query Q=\"SELECT * FROM table_name LIMIT 10\""; \
		exit 1; \
	fi
	docker exec $(POSTGRES) psql -U $(DB_USER) -d $(DB_NAME) -c "$(Q)"

# ============================================================================
# Development Tools
# ============================================================================

# Open dev container shell (recommended for development)
shell:
	@echo "Opening Spark dev container shell..."
	docker exec -it $(SPARK_DEV) bash

# Open Spark master shell
shell-master:
	@echo "Opening Spark master shell..."
	docker exec -it $(SPARK_MASTER) bash

# Open Python REPL in dev container
python:
	@echo "Opening Python REPL in dev container..."
	docker exec -it $(SPARK_DEV) python3

# Open PySpark shell
pyspark:
	@echo "Opening PySpark shell..."
	docker exec -it $(SPARK_MASTER) /opt/spark/bin/pyspark

superset:
	@echo "Apache Superset URL:"
	@echo "  http://localhost:8088"
	@echo "  Username: admin"
	@echo "  Password: admin"
	@echo ""
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8088 || \
	command -v open >/dev/null 2>&1 && open http://localhost:8088 || \
	echo "Please open http://localhost:8088 in your browser"

spark-ui:
	@echo "Spark UI URLs:"
	@echo "  - Spark Master:     http://localhost:8080"
	@echo "  - Spark App:        http://localhost:4040"
	@echo "  - Spark History:    http://localhost:18080"
	@echo ""
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8080 || \
	command -v open >/dev/null 2>&1 && open http://localhost:8080 || \
	echo "Please open http://localhost:8080 in your browser"

# ============================================================================
# Optimization & Analysis
# ============================================================================

run-optimization-analysis:
	@echo "Running Spark UI optimization analysis..."
	@echo "Note: This will take 10-15 minutes to complete"
	docker exec $(SPARK_MASTER) bash /opt/spark-apps/scripts/run_optimization_analysis.sh \
		--size medium \
		--iterations 2
	@echo "Optimization analysis complete"
	@echo "Review results in Spark UI at http://localhost:4040"

# ============================================================================
# Cleanup
# ============================================================================

clean-all:
	@echo "WARNING: This will delete ALL data including Docker images!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	@echo "Performing full cleanup..."
	docker compose down -v --remove-orphans
	docker system prune -af --volumes
	@echo "Full cleanup complete"

clean-data:
	@echo "Cleaning generated data..."
	docker exec $(SPARK_MASTER) rm -rf /opt/spark-apps/data/raw/*
	docker exec $(SPARK_MASTER) rm -rf /opt/spark-apps/data/processed/*
	@echo "Data cleaned"

# ============================================================================
# Code Quality (via Docker)
# ============================================================================

.PHONY: lint format check fix typecheck security quality pre-commit

# Run Ruff linter (no fixes)
lint:
	@echo "Running Ruff linter..."
	docker exec $(SPARK_DEV) ruff check src tests scripts
	@echo "Linting complete"

# Format code with Ruff
format:
	@echo "Formatting code with Ruff..."
	docker exec $(SPARK_DEV) ruff format src tests scripts
	@echo "Formatting complete"

# Check formatting and linting without changes
check:
	@echo "Checking code formatting and linting..."
	docker exec $(SPARK_DEV) ruff format --check src tests scripts
	docker exec $(SPARK_DEV) ruff check src tests scripts
	@echo "All checks passed"

# Auto-fix all issues
fix:
	@echo "Auto-fixing code issues..."
	docker exec $(SPARK_DEV) ruff check --fix src tests scripts
	docker exec $(SPARK_DEV) ruff format src tests scripts
	@echo "Auto-fix complete"

# Run type checking with mypy
typecheck:
	@echo "Running mypy type checker..."
	docker exec $(SPARK_DEV) mypy src --ignore-missing-imports
	@echo "Type checking complete"

# Security scan with Bandit
security:
	@echo "Running security scan with Bandit..."
	docker exec $(SPARK_DEV) bandit -r src -c pyproject.toml
	@echo "Security scan complete"

# Full quality check (all tools)
quality: lint typecheck security
	@echo ""
	@echo "All quality checks passed!"

# Run pre-commit checks via Docker
pre-commit:
	@echo "Running pre-commit checks via Docker..."
	docker exec $(SPARK_DEV) pre-commit run --all-files
	@echo "Pre-commit complete"

# Install pre-commit hooks (local - required for git hooks)
install-hooks:
	@echo "Installing pre-commit hooks locally..."
	@echo "Note: This requires pre-commit installed locally for git hooks"
	pip install pre-commit
	pre-commit install
	@echo "Pre-commit hooks installed"

# ============================================================================
# TDD Compliance
# ============================================================================

check-tdd:
	@echo "Checking TDD compliance..."
	@if [ -f "./scripts/check-tdd.sh" ]; then \
		bash ./scripts/check-tdd.sh; \
	else \
		echo "scripts/check-tdd.sh not found"; \
	fi

# ============================================================================
# Development Shortcuts
# ============================================================================

dev: setup
	@echo "Development environment ready!"
	@echo ""
	@echo "Quick commands:"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Run linter"
	@echo "  make fix            - Auto-fix code issues"
	@echo "  make generate-data  - Generate sample data"
	@echo "  make run-jobs       - Run Spark jobs"
	@echo "  make shell          - Open dev container shell"
	@echo ""

# Show current project status
project-status:
	@echo "GoodNote Analytics Platform - Project Status"
	@echo "=============================================="
	@echo ""
	@echo "Docker Services:"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"
	@echo ""
	@echo "Available Commands: make help"
	@echo ""
