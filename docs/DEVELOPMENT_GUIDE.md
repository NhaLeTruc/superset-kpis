# Development Guide - GoodNote Analytics Platform

Complete developer workflow documentation for contributing to the GoodNote Analytics Platform.

---

## Overview

This platform follows **Test-Driven Development (TDD)** principles with strict enforcement via git hooks. All development happens inside Docker containers for consistency across environments.

**Key Principles:**
- 🐳 **Docker-First:** All code runs in containers
- 🧪 **TDD Enforcement:** Tests before code, enforced by hooks
- 📦 **Makefile Automation:** Simplified commands for all operations
- 🔧 **Local Development:** Full stack runs on your laptop

---

## Prerequisites

### System Requirements

**Minimum:**
- **RAM:** 8GB (16GB recommended for large datasets)
- **Disk:** 100GB free space
- **CPU:** 4+ cores recommended
- **OS:** Linux, macOS, or Windows with WSL2

### Required Software

**Docker & Docker Compose:**
```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
sudo apt install docker-compose-plugin

# macOS (via Homebrew)
brew install --cask docker

# Windows (WSL2)
# Install Docker Desktop with WSL2 backend enabled
```

**Make:**
```bash
# Ubuntu/Debian
sudo apt install make

# macOS (via Xcode Command Line Tools)
xcode-select --install

# Windows (via WSL)
# Usually pre-installed in WSL Ubuntu
```

**Python 3.9+ (for local scripts):**
```bash
# Ubuntu/Debian
sudo apt install python3.9 python3-pip

# macOS
brew install python@3.9
```

---

## Quick Setup (5 Minutes)

```bash
# 1. Clone repository
git clone <repo-url>
cd superset-kpis

# 2. Start all services (one command!)
make quickstart

# What it does:
# - Starts Docker services (Spark, PostgreSQL, Superset, Jupyter)
# - Waits 30 seconds for readiness
# - Generates sample data (medium size)
# - Runs all 4 Spark ETL jobs
# - Shows access URLs
```

**Access Points After Setup:**
- Spark Master UI: http://localhost:8080
- Spark App UI: http://localhost:4040 (during job execution)
- Apache Superset: http://localhost:8088 (admin/admin)
- Jupyter Notebook: http://localhost:8888
- PostgreSQL: localhost:5432 (postgres/postgres)

---

## Development Workflow

### 1. Make Code Changes

**Edit code on your host machine:**
```bash
# Files are mounted via Docker volumes
# Changes to src/ are immediately reflected in containers

# Example: Edit a transform function
code src/transforms/engagement_transforms.py  # or vim, nano, etc.
```

### 2. Write Tests First (TDD)

```bash
# ALWAYS write tests BEFORE implementation
# Pre-commit hooks will block untested code

# Example workflow:
# Step 1: Write failing test
code tests/unit/test_engagement_transforms.py

# Step 2: Run test (should fail)
make test

# Step 3: Implement function
code src/transforms/engagement_transforms.py

# Step 4: Run test again (should pass)
make test

# Step 5: Refactor and verify
make test
```

### 3. Run Tests Frequently

```bash
# Run all tests
make test

# Run specific test file
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py -v

# Run specific test function
docker exec goodnote-spark-master pytest \
    tests/unit/test_join_transforms.py::test_identify_hot_keys_basic -v

# Watch mode (requires entr or similar)
# Not included by default - install if needed
```

### 4. Check Coverage

```bash
# Generate coverage report
make test-coverage

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux

# Target: >80% coverage for all modules
```

### 5. Test with Sample Data

```bash
# Generate small dataset (quick testing)
make generate-data-small

# Generate medium dataset (realistic testing)
make generate-data

# Generate large dataset (performance testing - takes time!)
make generate-data-large
```

### 6. Run ETL Jobs

```bash
# Run all 4 jobs
make run-jobs

# Run individual jobs
make run-job-1  # Data Processing
make run-job-2  # User Engagement
make run-job-3  # Performance Metrics
make run-job-4  # Session Analysis
```

### 7. Verify Database Output

```bash
# Connect to PostgreSQL
make db-connect

# Then run SQL queries:
SELECT COUNT(*) FROM goodnote_analytics.daily_active_users;
SELECT * FROM goodnote_analytics.power_users LIMIT 10;
\q  # Exit
```

### 8. Check TDD Compliance

```bash
# Before committing
make check-tdd

# Or use git hook (automatic)
git add .
git commit -m "Add new feature"
# Hook runs automatically and blocks if TDD violated
```

### 9. Commit Changes

```bash
# Stage changes
git add src/transforms/engagement_transforms.py
git add tests/unit/test_engagement_transforms.py

# Commit (hook runs automatically)
git commit -m "Add cohort retention calculation

- Implement calculate_cohort_retention() function
- Add 5 unit tests covering edge cases
- Update documentation"

# Push to remote
git push origin your-branch-name
```

---

## Makefile Command Reference

### Most Common Commands

```bash
make help           # Show all available commands
make quickstart     # Complete setup (Docker + data + jobs)
make test           # Run all unit tests
make test-coverage  # Run tests with coverage report
make generate-data  # Create sample data (medium size)
make run-jobs       # Execute all 4 Spark ETL jobs
make status         # Show service status
make logs           # View all service logs
make shell          # Open Spark master shell
make db-connect     # Connect to PostgreSQL
make clean          # Stop services and remove volumes
```

### Full Command List

See `make help` for complete list of 50+ commands organized into:
- Quick Start (quickstart, setup)
- Testing (test, test-unit, test-integration, test-coverage)
- Data & Jobs (generate-data, run-jobs, run-job-1/2/3/4)
- Docker Management (up, down, restart, status, logs)
- Database (db-init, db-connect, db-tables, db-table-counts)
- Development (shell, jupyter, superset, spark-ui)
- Optimization (run-optimization-analysis)
- Cleanup (clean, clean-all, clean-data)

---

## Docker Workflow

### Container Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Docker Compose Stack                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │ Spark Master    │  │ PostgreSQL   │  │ Superset  │ │
│  │ - PySpark 3.5   │  │ - Port 5432  │  │ - Redis   │ │
│  │ - Ports: 8080   │  │ - Analytics  │  │ - Port    │ │
│  │   4040, 18080   │  │   Database   │  │   8088    │ │
│  └─────────────────┘  └──────────────┘  └───────────┘ │
│                                                         │
│  ┌─────────────────┐                                   │
│  │ Jupyter         │  Volumes (mounted from host):     │
│  │ - Port 8888     │  - ./src → /opt/spark-apps/src   │
│  │ - Notebooks     │  - ./tests → /opt/spark-apps/tests│
│  └─────────────────┘  - ./data → /opt/spark-apps/data │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Managing Services

```bash
# Start all services
make up
# Or: docker compose up -d

# Stop all services (preserves data)
make down
# Or: docker compose down

# Restart services
make restart

# Check service status
make status
# Or: docker compose ps

# View logs
make logs                    # All services
make logs-spark              # Spark only
make logs-superset           # Superset only
docker compose logs -f goodnote-postgres  # PostgreSQL
```

### Accessing Containers

```bash
# Open shell in Spark container
make shell
# Or: docker exec -it goodnote-spark-master bash

# Run commands in containers
docker exec goodnote-spark-master python --version
docker exec goodnote-postgres pg_isready -U postgres

# Copy files from container
docker cp goodnote-spark-master:/opt/spark-apps/data/output.csv ./
```

### Rebuilding Containers

```bash
# Rebuild after Dockerfile changes
docker compose build --no-cache
make up

# Full cleanup and rebuild
make clean-all  # WARNING: Deletes ALL data
docker compose build --no-cache
make setup
```

---

## Common Development Tasks

### Task 1: Add a New Transform Function

**Scenario:** Implement `calculate_weekly_active_users()` function

```bash
# 1. Write test first
code tests/unit/test_engagement_transforms.py

# Add:
def test_calculate_wau_basic(spark):
    """Test WAU calculation."""
    data = [...]
    df = spark.createDataFrame(data, schema)

    wau_df = calculate_wau(df)

    assert wau_df.count() == expected_count

# 2. Run test (should fail)
make test

# 3. Implement function
code src/transforms/engagement_transforms.py

# Add:
def calculate_wau(df):
    """Calculate Weekly Active Users."""
    # Implementation...
    return wau_df

# 4. Run test (should pass)
make test

# 5. Test with real data
make generate-data
make run-job-2  # If it's in engagement job

# 6. Commit
git add tests/unit/test_engagement_transforms.py
git add src/transforms/engagement_transforms.py
git commit -m "Add WAU calculation"
```

### Task 2: Debug a Failing Job

```bash
# 1. Check job logs
docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/02_user_engagement.py

# 2. Access Spark UI for detailed execution info
open http://localhost:4040

# 3. Debug in Jupyter (interactive)
make jupyter  # Opens http://localhost:8888

# In notebook:
from src.transforms.engagement_transforms import calculate_dau
df = spark.read.parquet("/opt/spark-apps/data/processed/enriched_interactions")
result = calculate_dau(df)
result.show()

# 4. Add print debugging
# Edit src/jobs/02_user_engagement.py
print(f"📊 Loaded {enriched_df.count()} interactions")
df.printSchema()
df.show(5)

# 5. Re-run job
make run-job-2
```

### Task 3: Test Performance with Large Dataset

```bash
# 1. Generate large dataset (WARNING: takes 30+ minutes)
make generate-data-large

# 2. Run single job with timing
time docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/01_data_processing.py

# 3. Monitor Spark UI during execution
open http://localhost:4040

# Check:
# - Stage duration
# - Task distribution (max vs median)
# - Shuffle read/write volumes
# - GC time percentage

# 4. Run optimization analysis
make run-optimization-analysis

# 5. Review results
# See docs/SPARK_UI_SCREENSHOT_GUIDE.md for what to capture
```

### Task 4: Update Database Schema

```bash
# 1. Edit schema
code database/schema/01_tables.sql

# 2. Rebuild database
make clean  # Stops and removes volumes
make up
sleep 30  # Wait for PostgreSQL to start

# 3. Verify schema
make db-connect
\d goodnote_analytics.new_table
\q

# 4. Test with jobs
make run-jobs
```

### Task 5: Add a New Spark Configuration

```bash
# 1. Edit config
code src/config/spark_config.py

# Add:
SPARK_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.custom.new.config": "value",  # New config
}

# 2. Test in job
code src/jobs/01_data_processing.py

# Verify config is applied:
spark = create_spark_session("data-processing")
print(spark.conf.get("spark.custom.new.config"))

# 3. Run job
make run-job-1

# 4. Verify in Spark UI
open http://localhost:4040
# Environment tab → Spark Properties
```

---

## Debugging Tips

### 1. Check Container Logs

```bash
# Real-time logs for all services
make logs

# Specific service
docker logs goodnote-spark-master
docker logs goodnote-postgres
docker logs goodnote-superset

# Follow logs (Ctrl+C to exit)
docker logs -f goodnote-spark-master
```

### 2. Verify Services Are Running

```bash
# Overall status
make status

# Individual health checks
docker exec goodnote-postgres pg_isready -U postgres
curl http://localhost:8080  # Spark Master UI
curl http://localhost:8088/health  # Superset
```

### 3. Debug Inside Container

```bash
# Open shell
make shell

# Check environment
env | grep SPARK
env | grep PYTHON
echo $PYTHONPATH

# Test imports
python3 -c "import pyspark; print(pyspark.__version__)"
python3 -c "from src.transforms import join_transforms"

# Run Python interactively
python3
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.master("local[2]").getOrCreate()
>>> df = spark.read.csv("/opt/spark-apps/data/raw/interactions.csv", header=True)
>>> df.show(5)
```

### 4. Memory Issues

```bash
# Check Docker resource allocation
docker stats

# Increase Docker memory:
# Docker Desktop → Settings → Resources → Memory: 16GB

# Reduce dataset size for testing
make generate-data-small

# Adjust Spark memory in src/config/spark_config.py
SPARK_EXECUTOR_MEMORY = "8g"  # Reduce from 16g
```

### 5. Port Conflicts

```bash
# Check if ports are in use
lsof -i :8080  # Spark Master
lsof -i :4040  # Spark App
lsof -i :5432  # PostgreSQL
lsof -i :8088  # Superset

# Kill conflicting process
kill -9 <PID>

# Or change ports in docker-compose.yml
```

---

## Git Workflow

### Branch Strategy

```bash
# Main branch: main
# Feature branches: feature/<name>
# Bug fixes: bugfix/<name>

# Create feature branch
git checkout -b feature/add-wau-calculation

# Make changes, test, commit
git add .
git commit -m "Add WAU calculation with tests"

# Push to remote
git push origin feature/add-wau-calculation

# Create pull request (use GitHub UI)
```

### Pre-Commit Hook

**Hook enforces:**
1. All new Python files have corresponding tests
2. All tests pass before commit
3. TDD compliance (tests before code)

**Bypass (use sparingly):**
```bash
# For documentation-only commits
git commit --no-verify -m "Update documentation"

# Only use for:
# - .md files
# - Configuration files
# - Non-code changes
```

### Commit Message Format

```bash
# Good commit messages:
git commit -m "Add cohort retention calculation

- Implement calculate_cohort_retention() function
- Add 5 unit tests with realistic data
- Update IMPLEMENTATION_TASKS.md with completion status
- Document function in TESTING_GUIDE.md"

# Bad commit messages:
git commit -m "update"
git commit -m "fix bug"
git commit -m "wip"
```

---

## Performance Profiling

### Spark UI Analysis

```bash
# Run job
make run-job-1

# Access Spark UI during execution
open http://localhost:4040

# Key metrics to check:
# 1. Jobs tab - Overall duration
# 2. Stages tab - Task distribution, shuffle volumes
# 3. Storage tab - Cached RDD sizes
# 4. Executors tab - GC time, memory usage
# 5. SQL tab - Query plans, adaptive changes
```

### Automated Analysis

```bash
# Run optimization comparison
make run-optimization-analysis

# What it does:
# 1. Generates sample data with configurable skew
# 2. Runs baseline (unoptimized) jobs
# 3. Runs optimized jobs
# 4. Compares metrics (runtime, shuffle, GC time)
# 5. Outputs performance report

# Review results in logs
```

### Manual Benchmarking

```bash
# Benchmark single job
time docker exec goodnote-spark-master python /opt/spark-apps/src/jobs/01_data_processing.py

# Benchmark all jobs
time make run-jobs

# Compare with different configurations
# Edit src/config/spark_config.py
# Re-run and compare times
```

---

## Troubleshooting

For common issues and solutions, see **[SETUP_GUIDE.md](./SETUP_GUIDE.md)** which covers:
- Docker containers won't start
- Spark job OOM errors
- PostgreSQL connection refused
- Module not found errors
- Superset "No data" issues
- Permission denied errors
- And 4 more common issues

---

## Best Practices

### 1. Always Use Makefile Commands

```bash
# ✅ Good
make test
make run-jobs
make db-connect

# ❌ Avoid (error-prone)
docker exec goodnote-spark-master pytest tests/unit -v --tb=short --cov=src...
```

### 2. Test Changes in Isolation

```bash
# Test specific module after changes
make test  # Run all tests
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py -v

# Don't assume everything works
```

### 3. Keep Containers Running

```bash
# Don't restart unnecessarily
# Volumes are mounted - changes reflect immediately

# Only restart if:
# - Dockerfile changes
# - docker-compose.yml changes
# - Container crashes
```

### 4. Clean Up Regularly

```bash
# Remove old data
make clean-data

# Full cleanup (WARNING: deletes all data)
make clean

# Docker system cleanup
docker system prune -f
```

### 5. Monitor Resource Usage

```bash
# Check container resource usage
docker stats

# If sluggish:
# - Increase Docker memory/CPU
# - Use smaller datasets
# - Close unused containers
```

---

## Contributing

### Pull Request Checklist

Before submitting a PR, ensure:

- [ ] All tests pass (`make test`)
- [ ] Coverage >80% (`make test-coverage`)
- [ ] TDD compliant (`make check-tdd`)
- [ ] Jobs run successfully (`make run-jobs`)
- [ ] Database tables populated correctly
- [ ] Documentation updated (if applicable)
- [ ] Commit messages are descriptive
- [ ] No debugging print statements left in code

### Code Review Guidelines

**What reviewers check:**
- Test quality and coverage
- Code follows existing patterns
- No performance regressions
- Documentation accuracy
- Error handling
- Edge case coverage

---

## References

- **Setup Guide:** [docs/SETUP_GUIDE.md](./SETUP_GUIDE.md)
- **Testing Guide:** [docs/TESTING_GUIDE.md](./TESTING_GUIDE.md)
- **Optimization Guide:** [docs/OPTIMIZATION_GUIDE.md](./OPTIMIZATION_GUIDE.md)
- **Implementation Tasks:** [docs/IMPLEMENTATION_TASKS.md](./IMPLEMENTATION_TASKS.md)
- **Makefile:** Run `make help` for all commands

---

**Document Version:** 1.0
**Last Updated:** 2025-11-14
**Status:** Complete development workflow documentation
