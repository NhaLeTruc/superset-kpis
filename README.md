# SuperSet Pipeline Demo

**A production-grade Apache Spark analytics platform for processing user interaction data.**

**Stack:** Spark 3.5, PostgreSQL 15, Superset 3.0 | **Tests:** 167 tests (26 test files) | **Optimizations:** 7 techniques implemented

---

## 🚀 Quick Start

```bash
# Prerequisites: Docker 24.x+, 8GB+ RAM, 100GB+ disk

# Clone and start (one command)
git clone <repo-url> && cd superset-kpis
cp .env.example .env
make quickstart

# Verify
make test              # Run 167 tests
make status            # Check services
make db-tables         # View database tables

# Common Commands
make down              # Complete setup
make test-coverage     # Generate coverage report
make generate-data     # Create sample data
make run-jobs          # Execute all ETL jobs
make db-connect        # Connect to PostgreSQL
make logs              # View service logs
make help              # Show all 50+ commands
```

**Access Points:**

- Spark Master UI: <http://localhost:8080>
- Spark App UI: <http://localhost:4040>
- Superset: <http://localhost:8088> (see credentials in `.env` file)
- Jupyter: <http://localhost:8888>
- PostgreSQL: localhost:5432 (see credentials in `.env` file)

**See:** [DEVELOPMENT_GUIDE.md](./docs/DEVELOPMENT_GUIDE.md) for detailed setup

**See:** [TESTING_GUIDE.md](./docs/TESTING_GUIDE.md) for detailed testing strategy

---

## 📁 Project Structure

```txt
superset-kpis/
├── src/                    # Source code (43 functions, 4,500+ lines)
├── tests/                  # Test suite (167 tests, 26 test files)
├── scripts/                # Utility scripts (data generation, TDD tools)
├── database/               # PostgreSQL schemas (13 tables, 40+ indexes)
├── docker/                 # Dockerfiles and container scripts
├── docs/                   # Documentation (12 comprehensive guides)
├── Makefile                # 54 simplified commands
└── docker-compose.yml      # Multi-container orchestration
```

**See:** [PROJECT_STRUCTURE.md](./docs/PROJECT_STRUCTURE.md) for complete tree

---

## 🛠️ Scripts

Utility scripts in the `scripts/` directory:

| Script | Description |
|--------|-------------|
| `generate_sample_data.py` | Generate synthetic user interaction data for testing and development |
| `run_spark_job.sh` | Execute Spark jobs with proper configuration and classpath |
| `setup-tdd.sh` | Install TDD enforcement tools (git hooks, pytest config) |
| `check-tdd.sh` | Verify TDD compliance (test coverage, file mappings, hooks) |

**Usage:**

```bash
# Generate sample data
python3 scripts/generate_sample_data.py

# Run a specific Spark job
./scripts/run_spark_job.sh <job_name>

# Check TDD compliance
./scripts/check-tdd.sh
```

---

## 📖 Documentation

**Quick Reference:**

- **[Makefile](./Makefile)** - Run `make help` for all commands
- **[SETUP_GUIDE.md](./docs/SETUP_GUIDE.md)** - Installation & troubleshooting
- **[DEVELOPMENT_GUIDE.md](./docs/DEVELOPMENT_GUIDE.md)** - Developer workflow
- **[TESTING_GUIDE.md](./docs/TESTING_GUIDE.md)** - Testing strategy

**Implementation:**

- **[OPTIMIZATION_GUIDE.md](./docs/OPTIMIZATION_GUIDE.md)** - 7 Spark optimizations
- **[ENVIRONMENT_VARIABLES.md](./docs/ENVIRONMENT_VARIABLES.md)** - Configuration reference
- **[DAU_MAU.md](./docs/DAU_MAU.md)** - User engagement metrics

**Architecture:**

- **[ARCHITECTURE.md](./docs/ARCHITECTURE.md)** - System diagrams & design
- **[SUPERSET_DASHBOARDS.md](./docs/SUPERSET_DASHBOARDS.md)** - Dashboard specs
- **[TheChallenge.md](./docs/challenge/TheChallenge.md)** - Original requirements

---

## ⚡ Optimizations

7 major techniques implemented for 30-60% performance improvement:

- Broadcast joins
- Salting for skew
- AQE
- Predicate pushdown
- Column pruning
- Optimal partitioning
- Efficient caching

**See:** [OPTIMIZATION_GUIDE.md](./docs/OPTIMIZATION_GUIDE.md) for implementation details

---

## BI Dashboards examples

![](.\docs\Screenshot%202026-02-01%20161707_result.jpg)
![](.\docs\Screenshot%202026-02-01%20161742_result.jpg)
![](.\docs\Screenshot%202026-02-01%20161806_result.jpg)

---

## Spark Jobs run history

![](.\docs\Screenshot%202026-02-01%20162305_result.jpg)

---

## Spark Jobs Runs UI

**See:** *.jpg files in `./docs/SparkUI` for detailed screenshots

---

## 🏗️ Technology Stack

- **Processing:** Apache Spark 3.5 (PySpark), Python 3.9+
- **Storage:** PostgreSQL 15, Parquet (columnar)
- **Visualization:** Apache Superset 3.0, Redis 7
- **Development:** Docker Compose, Jupyter, pytest + chispa
- **Monitoring:** Spark UI (ports 8080, 4040, 18080)

**See:** [ARCHITECTURE.md](./docs/ARCHITECTURE.md) for detailed diagrams

---

## ⚙️ Configuration Management

All services are fully configurable through the `.env` file. No hardcoded values in docker-compose or configuration files.

### Default Credentials

**Superset Login:**

- Username: `admin`
- Password: `change_this_password_in_production`
- Email: `admin@example.com`

**PostgreSQL:**

- Host: `localhost:5432`
- Database: `analytics`
- User: `analytics_user`
- Password: `change_this_password_in_production`

### Changing Credentials

**To update Superset login credentials:**

1. Edit the `.env` file and update the values:

   ```bash
   SUPERSET_ADMIN_USERNAME=your_username
   SUPERSET_ADMIN_PASSWORD=your_secure_password
   SUPERSET_ADMIN_EMAIL=your_email@example.com
   SUPERSET_ADMIN_FIRSTNAME=Your_Firstname
   SUPERSET_ADMIN_LASTNAME=Your_Lastname
   ```

2. Reset the Superset database and restart:

   ```bash
   docker compose stop superset
   rm superset/superset.db
   docker compose up -d superset
   ```

3. Wait ~30 seconds for initialization, then log in with your new credentials

**Note:** The `superset fab create-admin` command doesn't update existing users. You must delete superset's database to apply new credentials.

### Other Configurable Parameters

All aspects of the environment can be controlled via `.env`:

- **Docker Images**: Spark, PostgreSQL, Redis, Superset versions
- **Resource Limits**: Memory, CPU cores for workers
- **Network & Ports**: Custom ports for all services
- **Cache Settings**: Redis DB numbers, timeout values
- **Application Settings**: Row limits, timeouts, security options

**See:** `.env` file for the complete list of 80+ configurable parameters

---

## 🚨 Troubleshooting

- **Containers restarting?** → Increase Docker memory to 16GB
- **Tests fail?** → Run `make test` (inside Docker), not `pytest` on host
- **Superset shows "No Data"?** → Run `make run-jobs` to populate database
- **Superset login fails?** → Check credentials in `.env`, see Configuration Management above

**See:** [SETUP_GUIDE.md](./docs/SETUP_GUIDE.md#troubleshooting) for 10+ common issues

---
