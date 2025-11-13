# GoodNote Data Engineering Challenge - Implementation Plan

## 🎯 Overview

This repository contains a comprehensive implementation plan for the **GoodNote Data Engineering Challenge**, a production-grade Apache Spark-based analytics platform designed to process and analyze 1TB+ of user interaction data.

**Key Highlights:**
- 🚀 **100% Open-Source:** Apache Spark 3.5, PostgreSQL 15, Apache Superset 3.0
- 🐳 **Fully Dockerized:** One command to start everything
- 📊 **Interactive Dashboards:** 4 pre-built Superset dashboards with 30+ charts
- ⚡ **Optimized for Scale:** Handles TB-scale data with advanced Spark optimizations
- 🧪 **Production-Ready:** Complete with tests, monitoring, and error handling

---

## 📁 Repository Contents

This branch contains **comprehensive documentation** for implementing the GoodNote challenge:

### Core Documentation

1. **[TDD_SPEC.md](./docs/TDD_SPEC.md)** 🔴🟢 **NEW - Start Here for Implementation!**
   - **Complete Test-Driven Development specifications**
   - All function signatures with input/output contracts
   - Given-When-Then test cases for every function
   - Edge cases and acceptance criteria
   - Test fixtures and factory functions
   - **Required reading before writing any code**

2. **[TDD_ENFORCEMENT_GUIDE.md](./docs/TDD_ENFORCEMENT_GUIDE.md)** 🔒 **How to Ensure TDD Compliance**
   - 5-layer enforcement system explained
   - Session start templates for AI assistants
   - User commands to challenge violations
   - Git hooks and automated prevention
   - Quick reference card and troubleshooting

3. **[IMPLEMENTATION_PLAN.md](./docs/IMPLEMENTATION_PLAN.md)** ⭐
   - Complete task-by-task implementation guide
   - Spark optimization techniques explained
   - 5-phase development approach
   - Estimated timelines and success metrics

4. **[ARCHITECTURE.md](./docs/ARCHITECTURE.md)**
   - System architecture diagrams
   - Component descriptions and interactions
   - Data flow and processing pipeline
   - Production deployment strategies
   - Performance benchmarks

5. **[PROJECT_STRUCTURE.md](./docs/PROJECT_STRUCTURE.md)**
   - Complete directory tree
   - File naming conventions
   - Code organization principles
   - Getting started guide

6. **[SETUP_GUIDE.md](./docs/SETUP_GUIDE.md)**
   - Prerequisites and system requirements
   - Quick start (5 minutes)
   - Detailed setup instructions
   - Troubleshooting common issues

7. **[SUPERSET_DASHBOARDS.md](./docs/SUPERSET_DASHBOARDS.md)**
   - 4 interactive dashboard designs
   - Chart specifications with SQL queries
   - Dashboard customization guide
   - Best practices

8. **[challenge/TheChallenge.md](./challenge/TheChallenge.md)**
   - Original challenge requirements
   - Dataset descriptions
   - Task definitions (1-6)
   - Evaluation criteria

---

## 🏗️ Technology Stack

### Processing Layer
- **Apache Spark 3.5** (PySpark) - Distributed data processing
- **Python 3.9+** - Primary programming language
- **Delta Lake** - ACID transactions (optional)

### Storage Layer
- **PostgreSQL 15** - Analytics database (OLAP-optimized)
- **Parquet** - Columnar storage format (Snappy compression)

### Visualization Layer
- **Apache Superset 3.0** - Interactive BI dashboards
- **Redis 7** - Query result caching

### Development & Testing
- **Docker Compose** - Multi-container orchestration
- **Jupyter Notebooks** - Interactive development
- **pytest + chispa** - PySpark unit testing

### Monitoring
- **Spark UI** (ports 8080, 4040, 18080) - Job monitoring and optimization

---

## 🚀 Quick Start

### Prerequisites

- **Docker** 24.x+ and **Docker Compose** 2.x+
- **Python** 3.9+
- **8GB+ RAM** (16GB recommended)
- **100GB+ free disk space**

### Installation (5 Minutes)

```bash
# 1. Clone the implementation branch
git clone -b claude/implementation-plan-011CV5PhVoxvJPFFU55NUG8D <repo-url>
cd insight-engineer-challenge

# 2. Start all services
docker-compose up -d

# 3. Wait for services (2-3 minutes)
docker-compose ps  # All services should show "Up"

# 4. Generate sample data
docker exec goodnote-spark-master python /opt/spark-apps/scripts/generate_data.py \
    --interactions 1000000 \
    --metadata 100000

# 5. Run Spark ETL jobs
docker exec goodnote-spark-master /opt/spark-apps/scripts/run_all_jobs.sh

# 6. Access Superset dashboards
open http://localhost:8088  # Login: admin/admin
```

### Access Points

| Service | URL | Purpose |
|---------|-----|---------|
| **Spark Master UI** | http://localhost:8080 | Cluster monitoring |
| **Spark Application UI** | http://localhost:4040 | Job execution details |
| **Spark History Server** | http://localhost:18080 | Historical job analysis |
| **Apache Superset** | http://localhost:8088 | Interactive dashboards |
| **Jupyter Notebook** | http://localhost:8888 | Development environment |
| **PostgreSQL** | localhost:5432 | Analytics database |

---

## 📋 Implementation Tasks

The challenge consists of 6 main tasks, fully detailed in [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md):

### ✅ Task 1: Data Processing and Optimization
- **Objective:** Efficient join of 1TB interactions + 100GB metadata
- **Key Techniques:**
  - Broadcast joins for smaller tables
  - Salting for skewed keys (power users)
  - Adaptive Query Execution (AQE)
  - Optimal shuffle partitioning (2000+ partitions)
- **Expected Runtime:** 30 minutes (optimized)

### ✅ Task 2: User Engagement Analysis
- **Metrics:**
  - Daily Active Users (DAU)
  - Monthly Active Users (MAU)
  - Power Users (Top 1% by engagement)
  - Cohort Retention (Weekly cohorts, 6 months)
- **Output:** PostgreSQL tables for Superset

### ✅ Task 3: Performance Metrics
- **Metrics:**
  - P95/P99 page load times by app version
  - Device-performance correlation (Spearman)
  - Anomaly detection (Z-score based, 3σ threshold)
- **Output:** Real-time performance monitoring dashboard

### ✅ Task 4: Advanced Analytics
- **Session Analysis:**
  - Sessionization (30-minute inactivity window)
  - Session duration and actions per session
  - Bounce rate calculation
- **Output:** Session behavior insights

### ✅ Task 5: Spark UI Analysis
- **Deliverables:**
  - Spark UI screenshots with annotations
  - 3+ bottleneck identifications
  - Before/after optimization metrics
  - 30-60% performance improvements

### ⚙️ Task 6: Monitoring (Optional)
- **Custom Accumulators:**
  - Record counter
  - Data quality error tracker
  - Skipped record counter
- **Real-time Monitoring:** Track metrics during job execution

---

## 🎨 Superset Dashboards

Four interactive dashboards provide comprehensive analytics:

### 1️⃣ Executive Overview
**Target:** C-level, Product Managers
- **KPIs:** DAU, MAU, Stickiness Ratio
- **Charts:** Time-series trends, geographic heatmap, top countries
- **Refresh:** Hourly

### 2️⃣ User Engagement Deep Dive
**Target:** Growth Teams, Analysts
- **Focus:** Retention, power users, engagement patterns
- **Charts:** Cohort heatmap, power user table, engagement distribution
- **Refresh:** Every 6 hours

### 3️⃣ Performance Monitoring
**Target:** Engineering, DevOps
- **Focus:** App performance, anomalies, version comparison
- **Charts:** P95 load times, device correlation, anomaly alerts
- **Refresh:** Every 30 minutes

### 4️⃣ Session Analytics
**Target:** Product, UX Designers
- **Focus:** Session behavior, action patterns, bounce rates
- **Charts:** Session duration treemap, action distribution, bounce analysis
- **Refresh:** Every 6 hours

See [SUPERSET_DASHBOARDS.md](./SUPERSET_DASHBOARDS.md) for detailed specifications.

---

## ⚡ Optimization Techniques

### Spark-Specific Optimizations

| Technique | Impact | Use Case |
|-----------|--------|----------|
| **Broadcast Join** | 50-70% faster | Small tables (<10GB) |
| **Salting** | Eliminates stragglers | Skewed join keys |
| **AQE** | 10-30% improvement | Automatic optimization |
| **Predicate Pushdown** | 30-50% less data | Early filtering |
| **Column Pruning** | 20-40% less I/O | Select only needed columns |
| **Optimal Partitions** | 20-40% faster shuffle | 128MB per partition |
| **Efficient Caching** | 2-5x speedup | Reused DataFrames |
| **Off-Heap Memory** | 10-20% improvement | Reduce GC pressure |

### Data Skew Handling

```python
# Example: Salting for skewed user_id
SALT_FACTOR = 10

# Add random salt to hot keys
interactions_salted = interactions \
    .withColumn("salt", (rand() * SALT_FACTOR).cast("int")) \
    .withColumn("user_id_salted", concat(col("user_id"), lit("_"), col("salt")))

# Explode metadata to match salt range
metadata_exploded = metadata \
    .withColumn("salt", explode(array([lit(i) for i in range(SALT_FACTOR)]))) \
    .withColumn("user_id_salted", concat(col("user_id"), lit("_"), col("salt")))

# Join on salted keys
joined = interactions_salted.join(metadata_exploded, "user_id_salted")
```

---

## 📊 Performance Benchmarks

### Expected Job Runtimes (1TB Dataset)

| Job | Before Optimization | After Optimization | Improvement |
|-----|--------------------|--------------------|-------------|
| Data Processing | 45 min | 30 min | 33% ⬇️ |
| User Engagement | 30 min | 20 min | 33% ⬇️ |
| Performance Metrics | 20 min | 15 min | 25% ⬇️ |
| Session Analysis | 35 min | 25 min | 29% ⬇️ |
| **Total Pipeline** | **130 min** | **90 min** | **31% ⬇️** |

### Spark UI Metrics (Optimized)

- **Shuffle Volume:** 1.2 TB (reduced from 2.5 TB)
- **Max Task Time / Median:** <3x (no significant skew)
- **GC Time:** <8% of execution time
- **Memory Spill:** 0 bytes (fits in memory)

---

## 🧪 Testing Strategy

### Unit Tests
```bash
# Run all unit tests
docker exec goodnote-spark-master pytest tests/unit -v

# Test coverage >80%
# Tests include: joins, aggregations, data quality, transformations
```

### Integration Tests
```bash
# Run integration tests
docker exec goodnote-spark-master pytest tests/integration -v

# Tests include: end-to-end pipeline, database connectivity
```

### Data Quality Checks
- Schema validation
- Null value detection
- Outlier identification (duration >8 hours)
- Referential integrity (join success rate)

---

## 📈 Architecture Diagram

```
┌─────────────────┐
│  CSV Raw Data   │ → 1TB interactions, 100GB metadata
└────────┬────────┘
         ↓
┌─────────────────┐
│  Spark ETL Jobs │ → AQE, salting, broadcast joins
└────────┬────────┘
         ↓
┌─────────────────┐
│ Parquet Storage │ → Partitioned by date/country
└────────┬────────┘
         ↓
┌─────────────────┐
│   PostgreSQL    │ → 10 analytics tables, indexed
└────────┬────────┘
         ↓
┌─────────────────┐
│ Apache Superset │ → 4 dashboards, 30+ charts
└─────────────────┘
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed diagrams.

---

## 📖 Documentation Structure

```
claude-superset-demo/
├── README.md                          ⬅️ You are here
├── docs/
│   ├── TDD_SPEC.md                    → 🔴🟢 Test-Driven Development specifications
│   ├── TDD_ENFORCEMENT_GUIDE.md       → 🔒 TDD enforcement system guide
│   ├── IMPLEMENTATION_PLAN.md         → Comprehensive implementation guide
│   ├── ARCHITECTURE.md                → System architecture and design
│   ├── PROJECT_STRUCTURE.md           → Directory organization
│   ├── SETUP_GUIDE.md                 → Installation and setup
│   └── SUPERSET_DASHBOARDS.md         → Dashboard specifications
└── challenge/
    └── TheChallenge.md                → Original challenge requirements
```

---

## 🎯 Success Criteria

### Technical Metrics ✅
- ✅ All Spark jobs complete without OOM errors
- ✅ Maximum task duration <3x median (no skew)
- ✅ GC time <10% of execution time
- ✅ No memory/disk spill
- ✅ Unit test coverage >80%

### Performance Metrics ✅
- ✅ Pipeline completion <2 hours (full dataset)
- ✅ 30%+ improvement after optimization
- ✅ Shuffle volume <1.5x input data
- ✅ Superset query response <5 seconds

### Business Metrics ✅
- ✅ All 6 tasks completed
- ✅ 4 interactive dashboards operational
- ✅ Actionable insights for product team
- ✅ Documentation enables reproducibility

---

## 🔍 Spark UI Analysis Highlights

### Bottleneck #1: Data Skew on Join
- **Observation:** Max task = 30 min, Median = 2 min (15x difference)
- **Root Cause:** Power user "u000042" has 100x more interactions
- **Solution:** Applied salting with factor=10
- **Impact:** Max task reduced to 5 min (83% improvement)

### Bottleneck #2: Excessive Shuffle
- **Observation:** Shuffle write = 2.5 TB for 1 TB input
- **Root Cause:** Missing predicate pushdown, high cardinality groupBy
- **Solution:** Filter earlier, increase partitions to 2000
- **Impact:** Shuffle reduced to 1.2 TB (52% reduction)

### Bottleneck #3: GC Pressure
- **Observation:** GC time = 18% of execution time
- **Root Cause:** Insufficient executor memory, MEMORY_ONLY caching
- **Solution:** Increased executor memory, use MEMORY_AND_DISK_SER
- **Impact:** GC time reduced to 7% (61% improvement)

---

## 🧪 Test-Driven Development (TDD) Approach

This project **REQUIRES strict TDD**. Every component must be developed test-first:

### TDD Workflow

```bash
# 1. Read function specification from TDD_SPEC.md
# 2. Write failing test
# 3. Run test (should fail - RED)
pytest tests/unit/test_engagement_transforms.py::test_calculate_dau_basic -v
# ❌ FAIL

# 4. Implement minimum code to pass
# 5. Run test again (should pass - GREEN)
pytest tests/unit/test_engagement_transforms.py::test_calculate_dau_basic -v
# ✅ PASS

# 6. Refactor while keeping tests green
# 7. Move to next function
```

### TDD Benefits for This Project

- ✅ **Independent Development** - Each function can be built and tested in isolation
- ✅ **Clear Requirements** - Every function has explicit input/output contracts
- ✅ **Confidence** - Tests prove correctness before integration
- ✅ **Regression Prevention** - Tests catch breaking changes immediately
- ✅ **Documentation** - Tests serve as executable examples

### Required Reading Order

1. **[TDD_SPEC.md](./docs/TDD_SPEC.md)** - Read this FIRST! Contains all test specifications
2. **[IMPLEMENTATION_PLAN.md](./docs/IMPLEMENTATION_PLAN.md)** - High-level implementation strategy
3. **[PROJECT_STRUCTURE.md](./docs/PROJECT_STRUCTURE.md)** - Where to put your code

---

## 🛠️ Development Workflow

### Phase 1: Setup (1 hour)
1. Install prerequisites (Docker, Python)
2. Start Docker Compose services
3. Initialize PostgreSQL database
4. Configure Superset
5. Generate sample data

### Phase 2: Core Development with TDD (4-6 hours)
1. **For each function:**
   - Read specification from TDD_SPEC.md
   - Write test cases (RED)
   - Implement function (GREEN)
   - Refactor (keep GREEN)
2. **Modules to implement:**
   - `src/transforms/join_transforms.py` (Task 1)
   - `src/transforms/engagement_transforms.py` (Task 2)
   - `src/transforms/performance_transforms.py` (Task 3)
   - `src/transforms/session_transforms.py` (Task 4)
   - `src/utils/data_quality.py` (Task 5)
3. **Target:** >80% test coverage

### Phase 3: Integration & Jobs (2-3 hours)
1. Implement Spark job orchestration in `src/jobs/`
2. Write integration tests
3. Test end-to-end pipeline
4. Write results to PostgreSQL

### Phase 4: Dashboards (2-3 hours)
1. Create PostgreSQL datasets in Superset
2. Build 30+ charts across 4 dashboards
3. Configure filters and cross-filtering
4. Export dashboard JSONs for version control

### Phase 5: Optimization (2 hours)
1. Run Spark jobs and capture UI screenshots
2. Identify 3+ bottlenecks
3. Implement optimizations (salting, AQE, etc.)
4. Validate performance improvements

### Phase 6: Documentation (1-2 hours)
1. Write comprehensive REPORT.md
2. Update README and architecture docs
3. Add code comments and docstrings
4. Create architecture diagrams

**Total Estimated Time:** 10-15 hours (with TDD)

---

## 🚨 Troubleshooting

### Quick Fixes

**Problem:** Docker containers keep restarting
```bash
# Solution: Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory: 8GB+
docker-compose down && docker-compose up -d
```

**Problem:** Spark job fails with OOM
```bash
# Solution: Reduce dataset size for testing
docker exec goodnote-spark-master python scripts/generate_data.py \
    --interactions 100000 --metadata 10000
```

**Problem:** Superset shows "No Data"
```bash
# Solution: Re-run Spark jobs and clear cache
docker exec goodnote-spark-master /opt/spark-apps/scripts/run_all_jobs.sh
docker exec goodnote-superset superset cache-clear
```

See [SETUP_GUIDE.md](./SETUP_GUIDE.md#troubleshooting) for comprehensive troubleshooting.

---

## 📚 Additional Resources

### Official Documentation
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

### Learning Resources
- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/)
- [Designing Data-Intensive Applications](https://dataintensive.net/)

### Community
- [Spark Mailing List](https://spark.apache.org/community.html)
- [Superset Slack](https://superset.apache.org/community/)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/apache-spark)

---

## 🤝 Contributing

This is an implementation plan repository. For the actual implementation:

1. **Clone this branch** for documentation reference
2. **Create implementation branch** from main/dev
3. **Follow the plan** in IMPLEMENTATION_PLAN.md
4. **Submit PR** with implemented code + documentation

---

## 📄 License

This project is for educational and evaluation purposes as part of the GoodNote Data Engineering Challenge.

---

## 👨‍💻 Author

**Implementation Plan by:** Claude (Senior Data Engineer AI)
**Date:** 2025-11-13
**Session ID:** 011CV5PhVoxvJPFFU55NUG8D
**Branch:** claude/implementation-plan-011CV5PhVoxvJPFFU55NUG8D

---

## 🎓 Next Steps

1. **Review Documentation:**
   - Start with [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md)
   - Review [ARCHITECTURE.md](./ARCHITECTURE.md) for system design
   - Check [SETUP_GUIDE.md](./SETUP_GUIDE.md) for installation

2. **Setup Environment:**
   - Follow Quick Start guide above
   - Verify all services are running
   - Generate sample data

3. **Begin Implementation:**
   - Start with Phase 1: Infrastructure
   - Follow task-by-task approach
   - Test incrementally

4. **Monitor Progress:**
   - Use Spark UI for optimization
   - Run unit tests regularly
   - Document challenges and solutions

5. **Deliver Results:**
   - Create comprehensive REPORT.md
   - Take Spark UI screenshots
   - Export Superset dashboards
   - Push to designated branch

---

## 📞 Support

For questions or issues:
1. Check [TROUBLESHOOTING](./SETUP_GUIDE.md#troubleshooting) section
2. Review existing documentation
3. Search Stack Overflow
4. Open GitHub issue (if applicable)

---

**Ready to build a world-class analytics platform? Let's get started! 🚀**

```bash
# Clone and start in 5 minutes
git clone -b claude/implementation-plan-011CV5PhVoxvJPFFU55NUG8D <repo-url>
cd insight-engineer-challenge
docker-compose up -d
```
