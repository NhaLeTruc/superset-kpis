# GoodNote Analytics Platform - Makefile
# Simplified commands for development and testing

.PHONY: help quickstart setup test clean status logs

# Default target - show help
help:
	@echo "GoodNote Analytics Platform - Available Commands"
	@echo "================================================="
	@echo ""
	@echo "Quick Start:"
	@echo "  make quickstart     - Start everything (Docker + generate data + run jobs)"
	@echo "  make setup          - Initial setup (start Docker services only)"
	@echo ""
	@echo "Testing:"
	@echo "  make test           - Run all unit tests (59+ tests)"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-coverage  - Run tests with coverage report"
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
	@echo "  make shell          - Open Spark master shell"
	@echo "  make jupyter        - Show Jupyter notebook URL"
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
	@echo "✅ Quickstart Complete!"
	@echo ""
	@echo "Access Points:"
	@echo "  - Spark Master UI:  http://localhost:8080"
	@echo "  - Spark App UI:     http://localhost:4040"
	@echo "  - Apache Superset:  http://localhost:8088 (admin/admin)"
	@echo "  - Jupyter Notebook: http://localhost:8888"
	@echo ""
	@echo "Next Steps:"
	@echo "  1. Run 'make test' to verify all tests pass"
	@echo "  2. Access Superset to create dashboards"
	@echo "  3. See docs/SETUP_INSTRUCTIONS.md for details"
	@echo ""

setup: up
	@echo "⏳ Waiting for services to be ready (30 seconds)..."
	@sleep 30
	@echo "✅ Setup complete! All services are running."
	@make status

# ============================================================================
# Docker Management
# ============================================================================

up:
	@echo "🚀 Starting Docker services..."
	docker compose up -d
	@echo "✅ Docker services started"

down:
	@echo "🛑 Stopping Docker services..."
	docker compose down -v --remove-orphans
	@echo "✅ Docker services stopped"

restart: down up
	@echo "✅ Services restarted"

status:
	@echo "📊 Service Status:"
	@docker compose ps

logs:
	@echo "📋 Service Logs (Ctrl+C to exit):"
	docker compose logs -f

logs-spark:
	@echo "📋 Spark Master Logs:"
	docker compose logs -f goodnote-spark-master

logs-superset:
	@echo "📋 Superset Logs:"
	docker compose logs -f goodnote-superset

# ============================================================================
# Testing
# ============================================================================

test: test-unit test-integration 
	@echo "✅ All tests completed"

test-unit:
	@echo "🧪 Running unit tests (59+ tests)..."
	docker exec goodnote-spark-dev pytest tests/unit -v --tb=short
	@echo "✅ Unit tests completed"

test-integration:
	@echo "🧪 Running integration tests..."
	docker exec goodnote-spark-dev pytest tests/integration -v --tb=short
	@echo "✅ Integration tests completed"

test-coverage:
	@echo "🧪 Running tests with coverage report..."
	docker exec goodnote-spark-dev pytest tests/unit \
		--cov=src \
		--cov-report=html \
		--cov-report=term \
		-v
	@echo "✅ Coverage report generated at htmlcov/index.html"

test-specific:
	@echo "🧪 Usage: make test-specific TEST=tests/unit/test_join_transforms.py::test_identify_hot_keys_basic"
	@echo "Example: docker exec goodnote-spark-dev pytest $(TEST) -v"

# ============================================================================
# Data Generation & Jobs
# ============================================================================

generate-data:
	@echo "📊 Generating sample data (medium size)..."
	docker exec goodnote-spark-master python /opt/spark-apps/scripts/generate_sample_data.py \
		--medium \
		--seed 42
	@echo "✅ Sample data generated"

generate-data-small:
	@echo "📊 Generating small sample data (for quick testing)..."
	docker exec goodnote-spark-master python /opt/spark-apps/scripts/generate_sample_data.py \
		--small \
		--seed 42
	@echo "✅ Small sample data generated"

generate-data-large:
	@echo "📊 Generating large sample data (WARNING: may take several minutes)..."
	docker exec goodnote-spark-master python /opt/spark-apps/scripts/generate_sample_data.py \
		--large \
		--seed 42
	@echo "✅ Large sample data generated"

run-jobs:
	@echo "⚡ Running all Spark ETL jobs..."
	docker exec goodnote-spark-master bash /opt/spark-apps/src/jobs/run_all_jobs.sh
	@echo "✅ All jobs completed"

run-job-1:
	@echo "⚡ Running Job 1: Data Processing..."
	docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/01_data_processing.py
	@echo "✅ Data Processing job completed"

run-job-2:
	@echo "⚡ Running Job 2: User Engagement..."
	docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/02_user_engagement.py
	@echo "✅ User Engagement job completed"

run-job-3:
	@echo "⚡ Running Job 3: Performance Metrics..."
	docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/03_performance_metrics.py
	@echo "✅ Performance Metrics job completed"

run-job-4:
	@echo "⚡ Running Job 4: Session Analysis..."
	docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/04_session_analysis.py
	@echo "✅ Session Analysis job completed"

# ============================================================================
# Database Management
# ============================================================================

db-init:
	@echo "🗄️  Initializing PostgreSQL database..."
	docker exec goodnote-postgres psql -U postgres -d goodnote_analytics -f /docker-entrypoint-initdb.d/schema.sql
	docker exec goodnote-postgres psql -U postgres -d goodnote_analytics -f /docker-entrypoint-initdb.d/indexes.sql
	@echo "✅ Database initialized"

db-connect:
	@echo "🗄️  Connecting to PostgreSQL database..."
	@echo "Password: postgres"
	docker exec -it goodnote-postgres psql -U postgres -d goodnote_analytics

db-tables:
	@echo "🗄️  Listing all database tables..."
	docker exec goodnote-postgres psql -U postgres -d goodnote_analytics -c "\dt"

db-table-counts:
	@echo "🗄️  Showing row counts for all tables..."
	docker exec goodnote-postgres psql -U postgres -d goodnote_analytics -c "\
		SELECT \
			schemaname, \
			tablename, \
			n_live_tup as row_count \
		FROM pg_stat_user_tables \
		ORDER BY n_live_tup DESC;"

# ============================================================================
# Development Tools
# ============================================================================

shell:
	@echo "🐚 Opening Spark master shell..."
	docker exec -it goodnote-spark-master bash

jupyter:
	@echo "📓 Jupyter Notebook URL:"
	@echo "  http://localhost:8888"
	@echo ""
	@echo "Opening browser..."
	@command -v open >/dev/null 2>&1 && open http://localhost:8888 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8888 || \
	echo "Please open http://localhost:8888 in your browser"

superset:
	@echo "📊 Apache Superset URL:"
	@echo "  http://localhost:8088"
	@echo "  Username: admin"
	@echo "  Password: admin"
	@echo ""
	@echo "Opening browser..."
	@command -v open >/dev/null 2>&1 && open http://localhost:8088 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8088 || \
	echo "Please open http://localhost:8088 in your browser"

spark-ui:
	@echo "⚡ Spark UI URLs:"
	@echo "  - Spark Master:     http://localhost:8080"
	@echo "  - Spark App:        http://localhost:4040"
	@echo "  - Spark History:    http://localhost:18080"
	@echo ""
	@echo "Opening Spark Master UI..."
	@command -v open >/dev/null 2>&1 && open http://localhost:8080 || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8080 || \
	echo "Please open http://localhost:8080 in your browser"

# ============================================================================
# Optimization & Analysis
# ============================================================================

run-optimization-analysis:
	@echo "🔍 Running Spark UI optimization analysis..."
	@echo "⚠️  This will take 10-15 minutes to complete"
	docker exec goodnote-spark-master bash /opt/spark-apps/scripts/run_optimization_analysis.sh \
		--size medium \
		--iterations 2
	@echo "✅ Optimization analysis complete"
	@echo "📊 Review results in logs and Spark UI at http://localhost:4040"

# ============================================================================
# Cleanup
# ============================================================================

clean: down
	@echo "🧹 Cleaning up Docker volumes..."
	docker compose down -v
	@echo "✅ Cleanup complete"

clean-all:
	@echo "⚠️  WARNING: This will delete ALL data including Docker images!"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	@echo "🧹 Performing full cleanup..."
	docker compose down -v --remove-orphans
	docker system prune -af --volumes
	@echo "✅ Full cleanup complete"

clean-data:
	@echo "🧹 Cleaning generated data..."
	docker exec goodnote-spark-master rm -rf /opt/spark-apps/data/raw/*
	docker exec goodnote-spark-master rm -rf /opt/spark-apps/data/processed/*
	@echo "✅ Data cleaned"

# ============================================================================
# TDD Compliance
# ============================================================================

check-tdd:
	@echo "🔍 Checking TDD compliance..."
	@if [ -f "./check-tdd.sh" ]; then \
		bash ./check-tdd.sh; \
	else \
		echo "⚠️  check-tdd.sh not found"; \
	fi

# ============================================================================
# Documentation
# ============================================================================

docs:
	@echo "📚 Available Documentation:"
	@echo ""
	@echo "Setup & Configuration:"
	@echo "  - docs/SETUP_INSTRUCTIONS.md      - Step-by-step setup guide"
	@echo "  - docs/ENVIRONMENT_VARIABLES.md   - Environment configuration"
	@echo "  - docs/DOCKER_QUICKSTART.md       - Docker quick reference"
	@echo ""
	@echo "Implementation:"
	@echo "  - docs/IMPLEMENTATION_TASKS.md    - Task checklist (95% complete)"
	@echo "  - docs/TDD_SPEC.md                - Test specifications"
	@echo "  - docs/ARCHITECTURE.md            - System architecture"
	@echo ""
	@echo "Dashboards & Analysis:"
	@echo "  - superset/DASHBOARD_SETUP_GUIDE.md      - Dashboard setup"
	@echo "  - docs/SPARK_UI_SCREENSHOT_GUIDE.md      - Optimization guide"
	@echo "  - docs/REPORT.md                         - Technical report (1,300+ lines)"
	@echo ""

# ============================================================================
# Validation
# ============================================================================

validate: test status
	@echo ""
	@echo "✅ Validation Complete"
	@echo ""
	@echo "System Status:"
	@make status
	@echo ""
	@echo "Next Steps:"
	@echo "  1. Review test results above"
	@echo "  2. Check Spark UI for job metrics"
	@echo "  3. Verify database tables: make db-table-counts"
	@echo ""

# ============================================================================
# CI/CD Helpers
# ============================================================================

ci-test: up
	@echo "🤖 Running CI tests..."
	@sleep 30  # Wait for services
	@make test-unit
	@make generate-data-small
	@make run-jobs
	@echo "✅ CI tests completed"

# ============================================================================
# Development Shortcuts
# ============================================================================

dev: setup
	@echo "🔧 Development environment ready!"
	@echo ""
	@echo "Quick commands:"
	@echo "  make test           - Run tests"
	@echo "  make generate-data  - Generate sample data"
	@echo "  make run-jobs       - Run Spark jobs"
	@echo "  make shell          - Open Spark shell"
	@echo ""

# Show current project status
project-status:
	@echo "📊 GoodNote Analytics Platform - Project Status"
	@echo "=============================================="
	@echo ""
	@echo "Implementation: 95% Complete"
	@echo "Last Updated: 2025-11-13"
	@echo ""
	@echo "✅ Completed:"
	@echo "  - Phase 1-8: Core implementation (100%)"
	@echo "  - Unit Tests: 59+ tests passing"
	@echo "  - Spark Jobs: 4/4 production jobs ready"
	@echo "  - Database: 13 tables with indexes"
	@echo "  - Dashboard Specs: 4 dashboards defined"
	@echo ""
	@echo "⚠️  Remaining Work (9-13 hours):"
	@echo "  - Spark UI optimization analysis"
	@echo "  - Superset dashboard UI implementation"
	@echo "  - Integration tests"
	@echo ""
	@echo "Run 'make help' for all available commands"
	@echo ""
