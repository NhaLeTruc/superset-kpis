# Setup Guide - GoodNote Analytics Platform

Quick reference for setting up and troubleshooting the GoodNote Analytics Platform.

---

## Quick Start (5 Minutes)

For detailed quickstart, see the main [README.md](../README.md#quick-start)

```bash
# Using Makefile (recommended)
make quickstart

# Or manually
git clone <repo-url>
cd superset-kpis
docker compose up -d
make generate-data
make run-jobs
```

**Access Points:**
- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040
- Apache Superset: http://localhost:8088 (admin/admin)
- Jupyter Notebook: http://localhost:8888
- PostgreSQL: localhost:5432

---

## Prerequisites

### Minimum Requirements
- **Docker** 24.x+ and **Docker Compose** 2.x+
- **Python** 3.9+
- **RAM:** 8GB+ (16GB recommended)
- **Storage:** 100GB+ free space
- **OS:** Linux, macOS, or Windows with WSL2

### Installation

**Ubuntu/Debian:**
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo apt install docker-compose-plugin

# Logout/login for docker group to take effect
```

**macOS:**
```bash
# Install Docker Desktop
brew install --cask docker

# Start Docker Desktop from Applications
```

**Windows (WSL2):**
```powershell
# Install WSL2 (PowerShell as Administrator)
wsl --install

# Install Docker Desktop for Windows
# Enable WSL2 backend in Docker Desktop settings
```

---

## Common Makefile Commands

```bash
# Setup and quickstart
make help           # Show all available commands
make quickstart     # Complete setup (Docker + data + jobs)
make setup          # Start Docker services only

# Testing
make test           # Run all unit tests
make test-coverage  # Generate coverage report

# Data and jobs
make generate-data  # Create sample data (medium size)
make run-jobs       # Execute all 4 Spark ETL jobs

# Docker management
make up             # Start services
make down           # Stop services
make status         # Show service status
make logs           # View all logs

# Database
make db-connect     # Connect to PostgreSQL
make db-tables      # List all tables

# Development
make shell          # Open Spark master shell
make spark-ui       # Open Spark UI in browser
make superset       # Open Superset in browser

# Cleanup
make clean          # Stop and remove volumes
```

---

## Troubleshooting

### Issue 1: Docker Containers Won't Start

**Symptoms:**
```bash
docker compose ps
# Shows containers as "Restarting" or "Exited"
```

**Solutions:**

1. **Check Docker is running:**
   ```bash
   docker ps
   # If this fails, start Docker Desktop
   ```

2. **Increase Docker memory:**
   - Docker Desktop → Settings → Resources → Memory: 8GB minimum, 16GB recommended

3. **Check port conflicts:**
   ```bash
   # Check if ports are already in use
   lsof -i :8080  # Spark Master
   lsof -i :5432  # PostgreSQL
   lsof -i :8088  # Superset

   # Kill conflicting processes or change ports in docker-compose.yml
   ```

4. **View container logs:**
   ```bash
   docker compose logs goodnote-spark-master
   docker compose logs goodnote-postgres
   docker compose logs goodnote-superset
   ```

5. **Rebuild containers:**
   ```bash
   make clean
   docker compose build --no-cache
   make up
   ```

---

### Issue 2: Spark Job Fails with Out of Memory (OOM)

**Symptoms:**
```
ERROR Executor: Exception in task 1.0 in stage 3.0
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

1. **Increase Docker memory allocation** (see Issue 1)

2. **Use smaller dataset for testing:**
   ```bash
   make generate-data-small  # Generate small dataset
   ```

3. **Adjust Spark configuration:**
   Edit `src/config/spark_config.py` and reduce executor memory:
   ```python
   SPARK_EXECUTOR_MEMORY = "8g"  # Reduce from 16g
   ```

4. **Check available memory:**
   ```bash
   docker stats
   # Monitor memory usage of containers
   ```

---

### Issue 3: PostgreSQL Connection Refused

**Symptoms:**
```
psycopg2.OperationalError: could not connect to server: Connection refused
```

**Solutions:**

1. **Check PostgreSQL is running:**
   ```bash
   docker exec goodnote-postgres pg_isready -U postgres
   # Expected: postgres:5432 - accepting connections
   ```

2. **Wait for PostgreSQL to be ready:**
   ```bash
   # PostgreSQL takes 10-30 seconds to start
   sleep 30
   make db-connect  # Try again
   ```

3. **Check PostgreSQL logs:**
   ```bash
   docker logs goodnote-postgres
   ```

4. **Restart PostgreSQL:**
   ```bash
   docker compose restart goodnote-postgres
   sleep 10
   docker exec goodnote-postgres pg_isready -U postgres
   ```

5. **Verify connection settings:**
   Check `src/config/db_config.py` for correct credentials

---

### Issue 4: Tests Fail with "Module Not Found"

**Symptoms:**
```
ModuleNotFoundError: No module named 'pyspark'
ModuleNotFoundError: No module named 'src'
```

**Solutions:**

1. **Verify you're running tests inside Docker:**
   ```bash
   # Correct way
   make test
   # Or
   docker exec goodnote-spark-master pytest tests/unit -v

   # Wrong way (don't do this)
   pytest tests/unit  # This runs on host, not in Docker
   ```

2. **Rebuild Docker image:**
   ```bash
   docker compose build --no-cache goodnote-spark-master
   docker compose up -d
   ```

3. **Check PYTHONPATH inside container:**
   ```bash
   docker exec goodnote-spark-master env | grep PYTHONPATH
   # Should show: PYTHONPATH=/opt/spark-apps
   ```

---

### Issue 5: Superset Dashboard Shows "No Data"

**Symptoms:**
- Charts show "No data"
- SQL Lab queries return empty results

**Solutions:**

1. **Verify data exists in PostgreSQL:**
   ```bash
   make db-connect
   # Then run:
   SELECT COUNT(*) FROM goodnote_analytics.daily_active_users;
   \q
   ```

2. **Re-run Spark jobs:**
   ```bash
   make run-jobs
   ```

3. **Clear Superset cache:**
   ```bash
   docker exec goodnote-superset superset cache-clear
   # Refresh browser
   ```

4. **Check database connection in Superset:**
   - Login to Superset: http://localhost:8088
   - Settings → Database Connections
   - Test connection

---

### Issue 6: Spark UI Not Accessible

**Symptoms:**
- Browser shows "Connection refused" at http://localhost:8080

**Solutions:**

1. **Check Spark Master is running:**
   ```bash
   docker ps | grep spark-master
   ```

2. **Check port mapping:**
   ```bash
   docker port goodnote-spark-master
   # Should show: 8080/tcp -> 0.0.0.0:8080
   ```

3. **Wait for Spark to start:**
   ```bash
   # Spark takes 15-30 seconds to start
   sleep 20
   curl http://localhost:8080
   ```

4. **Check firewall (Linux):**
   ```bash
   sudo ufw allow 8080/tcp
   ```

---

### Issue 7: Permission Denied Errors

**Symptoms:**
```
Permission denied: '/opt/spark-apps/data'
```

**Solutions:**

1. **Fix ownership (Linux/Mac):**
   ```bash
   sudo chown -R $USER:$USER .
   ```

2. **Check Docker user:**
   ```bash
   docker exec goodnote-spark-master whoami
   # Should match or be compatible with host user
   ```

---

### Issue 8: Makefile Commands Not Working

**Symptoms:**
```
make: *** No rule to make target 'quickstart'. Stop.
```

**Solutions:**

1. **Ensure you're in project root:**
   ```bash
   pwd  # Should show .../superset-kpis
   ls Makefile  # Should exist
   ```

2. **Check Make is installed:**
   ```bash
   make --version

   # If not installed:
   # Ubuntu/Debian: sudo apt install make
   # macOS: xcode-select --install
   ```

---

### Issue 9: Data Generation Takes Too Long

**Symptoms:**
- `make generate-data` runs for hours

**Solutions:**

1. **Use smaller dataset:**
   ```bash
   make generate-data-small  # Quick testing dataset
   ```

2. **Check available CPU/memory:**
   ```bash
   docker stats
   ```

3. **Increase Docker resources:**
   - Docker Desktop → Resources → CPUs: 4+
   - Docker Desktop → Resources → Memory: 16GB+

---

### Issue 10: Git Hooks Blocking Commits

**Symptoms:**
```
pre-commit hook failed
TDD violation detected
```

**Solutions:**

1. **Ensure tests exist for your code:**
   ```bash
   # Check test coverage
   make test-coverage
   ```

2. **Verify TDD compliance:**
   ```bash
   ./check-tdd.sh
   ```

3. **If intentional (documentation, config files):**
   ```bash
   git commit --no-verify -m "Update documentation"
   # Use sparingly!
   ```

---

## Getting Help

### Check Logs

```bash
# All services
make logs

# Specific service
docker compose logs goodnote-spark-master
docker compose logs goodnote-postgres
docker compose logs goodnote-superset
```

### Service Health Checks

```bash
# Overall status
make status

# Individual checks
docker exec goodnote-postgres pg_isready
curl http://localhost:8080  # Spark UI
curl http://localhost:8088/health  # Superset
```

### Debug Inside Container

```bash
# Open shell in Spark container
make shell

# Or manually
docker exec -it goodnote-spark-master bash

# Check environment
env | grep SPARK
python --version
pytest --version
```

---

## Additional Resources

- **Main Documentation:** [README.md](../README.md)
- **Makefile Commands:** Run `make help`
- **Implementation Tasks:** [IMPLEMENTATION_TASKS.md](./IMPLEMENTATION_TASKS.md)
- **Architecture:** [ARCHITECTURE.md](./ARCHITECTURE.md)
- **Project Structure:** [PROJECT_STRUCTURE.md](./PROJECT_STRUCTURE.md)

---

**Document Version:** 2.0
**Last Updated:** 2025-11-14
**Focus:** Troubleshooting & Quick Reference
