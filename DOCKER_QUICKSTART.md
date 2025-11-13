# Docker Quick Start Guide

## Prerequisites

- Docker Desktop installed and running
- 8GB+ RAM allocated to Docker
- 20GB+ free disk space

## Quick Start (30 seconds)

```bash
# 1. Start the development environment
./docker/start-dev.sh

# 2. Run tests
./docker/run-tests.sh

# 3. Enter the Spark container (optional)
docker exec -it goodnote-spark-dev bash
```

## What Gets Installed

### Docker Services

| Service | Container Name | Purpose |
|---------|---------------|---------|
| **spark-dev** | goodnote-spark-dev | Spark 3.5.0 + Python 3 + Testing tools |
| **postgres** | goodnote-postgres | PostgreSQL 15 for analytics |

### Python Packages (in spark-dev)

- `pyspark==3.5.0` - Apache Spark
- `pytest==9.0.1` - Testing framework
- `pytest-cov==7.0.0` - Coverage reporting
- `chispa==0.9.4` - PySpark test utilities
- `psycopg2-binary` - PostgreSQL adapter

## Running Tests

### All Tests
```bash
./docker/run-tests.sh
```

### Specific Test File
```bash
./docker/run-tests.sh tests/unit/test_data_quality.py
```

### Specific Test Function
```bash
./docker/run-tests.sh tests/unit/test_data_quality.py::TestValidateSchema::test_validate_schema_valid
```

### With Coverage Report
```bash
docker exec goodnote-spark-dev pytest tests/ --cov=src --cov-report=html
# View coverage: open htmlcov/index.html
```

## Development Workflow

### 1. Code in Your IDE
Edit files normally in `src/` and `tests/`. Changes are automatically synced to the container via Docker volumes.

### 2. Run Tests in Docker
```bash
./docker/run-tests.sh
```

### 3. TDD Cycle
```bash
# RED: Write test (will fail)
vim tests/unit/test_new_function.py

# Run test (verify RED)
./docker/run-tests.sh tests/unit/test_new_function.py

# GREEN: Implement function
vim src/utils/new_module.py

# Run test (verify GREEN)
./docker/run-tests.sh tests/unit/test_new_function.py
```

## Interactive Development

### Enter Spark Container
```bash
docker exec -it goodnote-spark-dev bash

# Now you're inside the container
spark@container:/app$ python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.master("local[2]").getOrCreate()
>>> # Do Spark things...
```

### Run Python Scripts
```bash
docker exec goodnote-spark-dev python src/jobs/01_data_processing.py
```

### Access PostgreSQL
```bash
# From host machine
psql -h localhost -U analytics_user -d analytics

# From inside container
docker exec -it goodnote-postgres psql -U analytics_user -d analytics
```

## Troubleshooting

### Container Won't Start
```bash
# Check Docker is running
docker ps

# View logs
docker-compose logs spark-dev

# Rebuild from scratch
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Tests Not Found
```bash
# Make sure you're in project root
pwd  # Should show .../claude-superset-demo

# Check PYTHONPATH inside container
docker exec goodnote-spark-dev env | grep PYTHONPATH
# Should show: PYTHONPATH=/app
```

### Permission Issues
```bash
# Fix file ownership (if needed)
sudo chown -R $USER:$USER .
```

### Out of Memory
```bash
# Increase Docker memory
# Docker Desktop → Settings → Resources → Memory: 8GB+
```

## Useful Commands

```bash
# Start environment
./docker/start-dev.sh

# Stop environment
docker-compose down

# Stop and remove volumes
docker-compose down -v

# View logs
docker-compose logs -f spark-dev

# Rebuild after dependency changes
docker-compose build spark-dev

# Run tests
./docker/run-tests.sh

# Check test coverage
./docker/run-tests.sh --cov-report=term-missing

# Enter container
docker exec -it goodnote-spark-dev bash

# Run specific pytest markers
docker exec goodnote-spark-dev pytest -m unit tests/
```

## Project Structure in Container

```
/app/
├── src/           → Your source code (mounted from host)
├── tests/         → Your tests (mounted from host)
├── data/          → Data files (mounted from host)
├── pytest.ini     → Pytest configuration
└── requirements.txt
```

## Next Steps

1. **Read TDD Spec**: `docs/TDD_SPEC.md`
2. **Follow Task List**: `IMPLEMENTATION_TASKS.md`
3. **Run Tests**: `./docker/run-tests.sh`
4. **Implement Functions**: Follow RED → GREEN → REFACTOR cycle

## Advanced Usage

### Add New Python Dependency

1. Edit `requirements.txt`
2. Rebuild container:
   ```bash
   docker-compose build spark-dev
   docker-compose up -d spark-dev
   ```

### Run Spark Shell
```bash
docker exec -it goodnote-spark-dev spark-shell
```

### Run PySpark
```bash
docker exec -it goodnote-spark-dev pyspark
```

### Debug with iPython
```bash
docker exec -it goodnote-spark-dev ipython
```

---

**Ready to code!** Start with `./docker/start-dev.sh` then `./docker/run-tests.sh` 🚀
