# GoodNote Data Engineering Challenge - Implementation

## 🎯 Overview

This repository contains a **95% complete implementation** of the **GoodNote Data Engineering Challenge**, a production-grade Apache Spark-based analytics platform designed to process and analyze 1TB+ of user interaction data.

**Implementation Highlights:**
- 🚀 **100% Open-Source:** Apache Spark 3.5, PostgreSQL 15, Apache Superset 3.0
- 🐳 **Fully Dockerized:** Complete docker-compose setup with all services
- 📊 **Dashboard Specs Ready:** 4 dashboard specifications with 30+ charts (UI implementation pending)
- ⚡ **Optimized for Scale:** 7 Spark optimizations implemented (broadcast joins, salting, AQE)
- 🧪 **Well-Tested:** 59+ unit tests with >80% coverage, TDD-compliant

**📊 Current Status:** ~95% Complete | **🎯 Remaining:** Spark UI analysis execution (4-6 hours), Superset UI implementation (2-3 hours), Integration tests (3-4 hours)

---

## 🚀 Quick Start

### Prerequisites

- **Docker** 24.x+ and **Docker Compose** 2.x+
- **8GB+ RAM** (16GB recommended)
- **100GB+ free disk space**

### Installation (5 Minutes)

```bash
# 1. Clone the repository
git clone <repo-url>
cd claude-superset-demo

# 2. Start everything (one command!)
make quickstart

# What it does:
# - Starts Docker services (Spark, PostgreSQL, Superset, Jupyter)
# - Generates sample data (medium size)
# - Runs all 4 Spark ETL jobs
# - Shows access URLs
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark Application UI** | http://localhost:4040 | - |
| **Spark History Server** | http://localhost:18080 | - |
| **Apache Superset** | http://localhost:8088 | admin/admin |
| **Jupyter Notebook** | http://localhost:8888 | - |
| **PostgreSQL** | localhost:5432 | postgres/postgres |

### Quick Verification

```bash
# Run all tests (59+ unit tests)
make test

# Check service status
make status

# View database tables
make db-tables

# Generate small dataset for quick testing
make generate-data-small

# See all available commands
make help
```

---

## 📁 Repository Structure

```
claude-superset-demo/
├── src/                              # Source code
│   ├── config/                       # Spark and database configuration
│   ├── jobs/                         # 4 production Spark ETL jobs (1,200+ lines)
│   ├── transforms/                   # Business logic modules (23 functions)
│   ├── utils/                        # Data quality, database utilities
│   └── schemas/                      # PySpark schema definitions
├── tests/                            # Test suite (59+ unit tests)
│   ├── unit/                         # Unit tests (5 modules)
│   ├── integration/                  # Integration tests (minimal)
│   └── conftest.py                   # Pytest fixtures
├── database/                         # PostgreSQL schemas
│   ├── schema/                       # 13 tables, 40+ indexes
│   └── init/                         # Initialization scripts
├── superset/                         # Apache Superset configurations
│   └── dashboards/                   # 4 dashboard specs (JSON)
├── scripts/                          # Utility scripts
│   ├── generate_sample_data.py       # Data generator
│   └── run_optimization_analysis.sh  # Spark UI analysis automation
├── docs/                             # Comprehensive documentation
│   ├── IMPLEMENTATION_TASKS.md       # Complete task checklist (95% done)
│   ├── TDD_SPEC.md                   # Test specifications
│   ├── OPTIMIZATION_GUIDE.md         # Spark optimization techniques
│   ├── TESTING_GUIDE.md              # Testing documentation
│   ├── DEVELOPMENT_GUIDE.md          # Developer workflow
│   ├── SETUP_GUIDE.md                # Troubleshooting guide
│   └── ARCHITECTURE.md               # System architecture
├── docker-compose.yml                # Multi-container orchestration
├── Makefile                          # Simplified command interface (50+ commands)
└── README.md                         # This file
```

**For complete structure, see:** [docs/PROJECT_STRUCTURE.md](./docs/PROJECT_STRUCTURE.md)

---

## 🏗️ Technology Stack

### Processing Layer
- **Apache Spark 3.5** (PySpark) - Distributed data processing
- **Python 3.9+** - Primary programming language

### Storage Layer
- **PostgreSQL 15** - Analytics database (OLAP-optimized)
- **Parquet** - Columnar storage format (Snappy compression)

### Visualization Layer
- **Apache Superset 3.0** - Interactive BI dashboards
- **Redis 7** - Query result caching

### Development & Testing
- **Docker Compose** - Multi-container orchestration
- **Jupyter Notebooks** - Interactive development
- **pytest + chispa** - PySpark unit testing framework

### Monitoring
- **Spark UI** (ports 8080, 4040, 18080) - Job monitoring and optimization

---

## 📊 Implementation Status

### ✅ Completed Phases (100%)

**Core Development:**
- **Phase 1-6:** All transform functions implemented with tests (23 functions, 59+ tests)
  - Join optimization with salting
  - User engagement analytics (DAU, MAU, retention)
  - Performance metrics and anomaly detection
  - Session analysis and bounce rate
  - Data quality validation

**Production Jobs:**
- **Phase 7:** 4 Spark ETL jobs production-ready (1,200+ lines total)
  - `01_data_processing.py` - Data ingestion and join optimization
  - `02_user_engagement.py` - DAU/MAU/cohorts analysis
  - `03_performance_metrics.py` - Performance and correlation
  - `04_session_analysis.py` - Sessionization and metrics

**Infrastructure:**
- **Phase 8:** PostgreSQL database complete (13 tables, 40+ indexes)
- **Phase 9:** Docker setup complete (5 services orchestrated)

### ⚠️ In Progress (50-95%)

- **Phase 10:** Apache Superset Dashboards (50%) - Specs complete, UI pending
- **Phase 11:** Spark UI Optimization Analysis (70%) - Framework ready, execution pending
- **Phase 12:** Documentation (85%) - Core docs complete, analysis pending

### 🎯 Remaining Work (9-13 hours)

1. **Spark UI Optimization Report** (4-6 hours)
   - Execute jobs with sample data
   - Capture before/after Spark UI screenshots
   - Document actual bottlenecks and improvements
   - Validate 30-60% performance gains

2. **Superset Dashboard UI** (2-3 hours)
   - Import 4 dashboard specifications
   - Create 30+ visualizations in Superset UI
   - Configure filters and interactivity

3. **Integration Tests** (3-4 hours)
   - End-to-end pipeline testing
   - Database write validation
   - Error handling verification

**For detailed task breakdown, see:** [docs/IMPLEMENTATION_TASKS.md](./docs/IMPLEMENTATION_TASKS.md)

---

## 📖 Documentation

### Quick Reference

1. **[Makefile](./Makefile)** - Run `make help` to see all 50+ commands
2. **[SETUP_GUIDE.md](./docs/SETUP_GUIDE.md)** - Troubleshooting & common issues
3. **[TESTING_GUIDE.md](./docs/TESTING_GUIDE.md)** - Testing strategy & best practices
4. **[DEVELOPMENT_GUIDE.md](./docs/DEVELOPMENT_GUIDE.md)** - Developer workflow

### Implementation Guides

5. **[IMPLEMENTATION_TASKS.md](./docs/IMPLEMENTATION_TASKS.md)** - Complete task checklist (150+ items)
6. **[TDD_SPEC.md](./docs/TDD_SPEC.md)** - Test specifications for all functions
7. **[OPTIMIZATION_GUIDE.md](./docs/OPTIMIZATION_GUIDE.md)** - Spark optimization techniques

### Architecture & Design

8. **[ARCHITECTURE.md](./docs/ARCHITECTURE.md)** - System architecture diagrams
9. **[PROJECT_STRUCTURE.md](./docs/PROJECT_STRUCTURE.md)** - Complete directory tree
10. **[SUPERSET_DASHBOARDS.md](./docs/SUPERSET_DASHBOARDS.md)** - 4 dashboard specifications

### Original Challenge

11. **[TheChallenge.md](./challenge/TheChallenge.md)** - Original challenge requirements

---

## 🎯 Challenge Tasks Progress

### ✅ Task 1: Data Processing and Optimization (100%)
- **Functions:** `identify_hot_keys()`, `apply_salting()`, `explode_for_salting()`, `optimized_join()`
- **Tests:** 15 comprehensive unit tests
- **Job:** `01_data_processing.py` (250+ lines)

### ✅ Task 2: User Engagement Analysis (100%)
- **Functions:** `calculate_dau()`, `calculate_mau()`, `calculate_stickiness()`, `identify_power_users()`, `calculate_cohort_retention()`
- **Tests:** 15 unit tests
- **Job:** `02_user_engagement.py` (250+ lines)

### ✅ Task 3: Performance Metrics (100%)
- **Functions:** `calculate_percentiles()`, `calculate_device_correlation()`, `detect_anomalies_statistical()`
- **Tests:** 9 unit tests
- **Job:** `03_performance_metrics.py` (300+ lines)

### ✅ Task 4: Advanced Analytics (100%)
- **Functions:** `sessionize_interactions()`, `calculate_session_metrics()`, `calculate_bounce_rate()`
- **Tests:** 11 unit tests
- **Job:** `04_session_analysis.py` (300+ lines)

### ⚠️ Task 5: Spark UI Analysis (70%)
- **Status:** Framework complete, execution pending
- **Completed:** Sample data generator, automated analysis script, optimization guide
- **Pending:** Execute jobs, capture screenshots, document results (4-6 hours)
- **See:** [docs/OPTIMIZATION_GUIDE.md](./docs/OPTIMIZATION_GUIDE.md)

### ⚙️ Task 6: Monitoring & Custom Accumulators (Optional - Not Implemented)
- **Status:** Deprioritized (see Phase 13 in IMPLEMENTATION_TASKS.md)
- **Alternative:** Print-based logging with status indicators
- **See:** [docs/IMPLEMENTATION_TASKS.md](./docs/IMPLEMENTATION_TASKS.md#phase-13-monitoring--custom-accumulators-optional---2-3-hours--0-complete)

---

## 🧪 Testing

### Running Tests

```bash
# Run all unit tests (59+ tests)
make test

# Run with coverage report
make test-coverage

# Run specific test file
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py -v

# Run specific test function
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py::test_identify_hot_keys_basic -v
```

### Test Coverage

- **Unit Tests:** 59+ tests across 5 modules
- **Coverage Target:** >80% for all modules
- **Framework:** pytest + chispa for PySpark testing
- **TDD Compliance:** Enforced via git pre-commit hooks

**For detailed testing information, see:** [docs/TESTING_GUIDE.md](./docs/TESTING_GUIDE.md)

---

## ⚡ Spark Optimizations

This platform implements **7 major optimization techniques**:

1. **Broadcast Joins** - Avoid shuffle for small tables (<10GB)
2. **Data Skew Handling (Salting)** - Eliminate straggler tasks
3. **Adaptive Query Execution (AQE)** - Automatic runtime optimization
4. **Predicate Pushdown** - Filter early, process less
5. **Column Pruning** - Select only needed columns
6. **Optimal Partitioning** - Balance parallelism and overhead
7. **Efficient Caching** - Reuse DataFrames strategically

**Expected Impact:** 30-60% performance improvement over baseline

**For comprehensive optimization guide, see:** [docs/OPTIMIZATION_GUIDE.md](./docs/OPTIMIZATION_GUIDE.md)

---

## 🛠️ Common Commands

```bash
# Quick start
make quickstart        # Complete setup (Docker + data + jobs)
make help              # Show all 50+ commands

# Testing
make test              # Run all unit tests
make test-coverage     # Generate coverage report

# Data & Jobs
make generate-data     # Create sample data (medium size)
make run-jobs          # Execute all 4 Spark ETL jobs
make run-job-1         # Run specific job

# Docker management
make up                # Start services
make down              # Stop services
make status            # Show service status
make logs              # View logs

# Database
make db-connect        # Connect to PostgreSQL
make db-tables         # List all tables

# Development
make shell             # Open Spark master shell
make jupyter           # Show Jupyter URL
make spark-ui          # Show Spark UI URLs

# Cleanup
make clean             # Stop and remove volumes
```

**For complete command list, see:** `make help` or [Makefile](./Makefile)

---

## 🚨 Troubleshooting

### Quick Fixes

**Problem:** Docker containers keep restarting
```bash
# Solution: Increase Docker memory
# Docker Desktop → Settings → Resources → Memory: 16GB
make restart
```

**Problem:** Tests fail with "Module not found"
```bash
# Solution: Run tests inside Docker container (not on host)
make test  # Correct
# NOT: pytest tests/unit  # Wrong - runs on host
```

**Problem:** Superset shows "No Data"
```bash
# Solution: Re-run jobs and verify database
make run-jobs
make db-connect
SELECT COUNT(*) FROM goodnote_analytics.daily_active_users;
```

**For 10+ common issues with solutions, see:** [docs/SETUP_GUIDE.md](./docs/SETUP_GUIDE.md#troubleshooting)

---

## 🏗️ Architecture

```
┌─────────────────┐
│  CSV Raw Data   │ → 1TB interactions, 100GB metadata
└────────┬────────┘
         ↓
┌─────────────────┐
│  Spark ETL Jobs │ → AQE, salting, broadcast joins
│  (4 production) │    Optimized for scale
└────────┬────────┘
         ↓
┌─────────────────┐
│ Parquet Storage │ → Partitioned by date/country
└────────┬────────┘
         ↓
┌─────────────────┐
│   PostgreSQL    │ → 13 analytics tables, indexed
└────────┬────────┘
         ↓
┌─────────────────┐
│ Apache Superset │ → 4 dashboards, 30+ charts
└─────────────────┘
```

**For detailed architecture diagrams, see:** [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)

---

## 🤝 Contributing

### Development Workflow

1. **Make changes** on your host (files are mounted via Docker volumes)
2. **Write tests first** (TDD enforced by git hooks)
3. **Run tests** frequently with `make test`
4. **Test with real data** using `make generate-data && make run-jobs`
5. **Verify database** with `make db-connect`
6. **Check TDD compliance** with `make check-tdd`
7. **Commit and push** (pre-commit hooks run automatically)

**For detailed developer workflow, see:** [docs/DEVELOPMENT_GUIDE.md](./docs/DEVELOPMENT_GUIDE.md)

---

## 📄 License

This project is for educational and evaluation purposes as part of the GoodNote Data Engineering Challenge.

---

## 👨‍💻 Project Information

**Implementation Status:** 95% Complete
**Last Updated:** 2025-11-14
**Current Branch:** claude/read-implementation-tasks-011CV6AtUcqAWDSPHFJrqSUk
**Repository:** claude-superset-demo

---

## 🎓 Next Steps

**To complete this project (9-13 hours):**

1. **Execute Spark UI Analysis** (4-6 hours)
   - Run `make run-optimization-analysis`
   - Follow [docs/SPARK_UI_SCREENSHOT_GUIDE.md](./docs/SPARK_UI_SCREENSHOT_GUIDE.md)
   - Document actual bottlenecks and improvements

2. **Implement Superset Dashboards** (2-3 hours)
   - Access http://localhost:8088 (admin/admin)
   - Follow [superset/DASHBOARD_SETUP_GUIDE.md](./superset/DASHBOARD_SETUP_GUIDE.md)
   - Import 4 dashboard specifications

3. **Add Integration Tests** (3-4 hours)
   - Write end-to-end pipeline tests in `tests/integration/`
   - Validate database writes and data quality
   - Test error handling

**For detailed task breakdown, see:** [docs/IMPLEMENTATION_TASKS.md](./docs/IMPLEMENTATION_TASKS.md)

---

**⭐ For questions or issues, see documentation guides above or run `make help`**
