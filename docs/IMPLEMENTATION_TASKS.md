# GoodNote Analytics Platform - Implementation Task List

## Overview

This document provides a **complete task checklist** for implementing the GoodNote Analytics Platform using strict Test-Driven Development (TDD).

**Total Functions to Implement:** 25 functions across 5 tasks
**Estimated Time:** 10-15 hours
**Test Coverage Target:** >80%

---

## Phase 0: Setup (1 hour)

### Infrastructure Setup
- [ ] Install Docker Desktop and verify running
- [ ] Install Python 3.9+ with pip
- [ ] Install pytest, pyspark, chispa libraries
- [ ] Run `./setup-tdd.sh` to install git hooks
- [ ] Verify TDD enforcement with `./check-tdd.sh`
- [ ] Create project directory structure
  - [ ] `src/config/`
  - [ ] `src/transforms/`
  - [ ] `src/utils/`
  - [ ] `src/jobs/`
  - [ ] `src/schemas/`
  - [ ] `tests/unit/`
  - [ ] `tests/integration/`
  - [ ] `tests/fixtures/`

### Test Fixtures Setup
- [ ] Create `tests/fixtures/basic_interactions.csv`
- [ ] Create `tests/fixtures/skewed_interactions.csv`
- [ ] Create `tests/fixtures/edge_cases.csv`
- [ ] Create `tests/fixtures/session_test.csv`
- [ ] Implement fixture factory functions in `tests/conftest.py`
  - [ ] `create_interactions_df()`
  - [ ] `create_metadata_df()`
  - [ ] `create_skewed_interactions()`

---

## Phase 1: Data Schemas (0.5 hours)

### Module: `src/schemas/`

#### Task 1.1: Interactions Schema
**File:** `src/schemas/interactions_schema.py`
- [ ] Write tests for schema validation
- [ ] Define `INTERACTIONS_SCHEMA` (StructType)
- [ ] Define `VALID_ACTION_TYPES` constant
- [ ] Define `MAX_DURATION_MS` constant
- [ ] Define `MIN_DURATION_MS` constant
- [ ] Tests pass ✅

#### Task 1.2: Metadata Schema
**File:** `src/schemas/metadata_schema.py`
- [ ] Write tests for schema validation
- [ ] Define `METADATA_SCHEMA` (StructType)
- [ ] Define `VALID_COUNTRIES` list
- [ ] Define `VALID_DEVICE_TYPES` list
- [ ] Define `VALID_SUBSCRIPTION_TYPES` list
- [ ] Tests pass ✅

---

## Phase 2: Data Quality Utilities (2 hours)

### Module: `src/utils/data_quality.py`
**Test File:** `tests/unit/test_data_quality.py`

#### Task 2.1: Schema Validation
**Function:** `validate_schema(df, expected_schema, strict=True)`
- [ ] Write test: `test_validate_schema_valid`
- [ ] Write test: `test_validate_schema_missing_column`
- [ ] Write test: `test_validate_schema_wrong_type`
- [ ] Implement `validate_schema()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 2.2: NULL Detection
**Function:** `detect_nulls(df, non_nullable_columns)`
- [ ] Write test: `test_detect_nulls_none`
- [ ] Write test: `test_detect_nulls_present`
- [ ] Implement `detect_nulls()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 2.3: Outlier Detection
**Function:** `detect_outliers(df, column, method, ...)`
- [ ] Write test: `test_detect_outliers_iqr`
- [ ] Write test: `test_detect_outliers_threshold`
- [ ] Implement `detect_outliers()` function
- [ ] All tests pass (RED → GREEN) ✅

---

## Phase 3: Data Processing & Join Optimization (3 hours)

### Module: `src/transforms/join_transforms.py`
**Test File:** `tests/unit/test_join_transforms.py`

#### Task 3.1: Hot Key Identification
**Function:** `identify_hot_keys(df, key_column, threshold_percentile=0.99)`
- [ ] Write test: `test_identify_hot_keys_basic`
- [ ] Write test: `test_identify_hot_keys_uniform_distribution`
- [ ] Write test: `test_identify_hot_keys_multiple`
- [ ] Write test: `test_identify_hot_keys_invalid_column`
- [ ] Implement `identify_hot_keys()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 3.2: Salting Application
**Function:** `apply_salting(df, hot_keys_df, key_column, salt_factor=10)`
- [ ] Write test: `test_apply_salting_basic`
- [ ] Write test: `test_apply_salting_distribution`
- [ ] Write test: `test_apply_salting_no_hot_keys`
- [ ] Write test: `test_apply_salting_invalid_salt_factor`
- [ ] Implement `apply_salting()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 3.3: Explode for Salting
**Function:** `explode_for_salting(df, hot_keys_df, key_column, salt_factor=10)`
- [ ] Write test: `test_explode_for_salting_basic`
- [ ] Write test: `test_explode_for_salting_mixed`
- [ ] Write test: `test_explode_for_salting_preserves_data`
- [ ] Implement `explode_for_salting()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 3.4: Optimized Join
**Function:** `optimized_join(large_df, small_df, join_key, ...)`
- [ ] Write test: `test_optimized_join_broadcast`
- [ ] Write test: `test_optimized_join_with_salting`
- [ ] Write test: `test_optimized_join_no_skew`
- [ ] Write test: `test_optimized_join_left_join`
- [ ] Implement `optimized_join()` function
- [ ] All tests pass (RED → GREEN) ✅

---

## Phase 4: User Engagement Analytics (3 hours)

### Module: `src/transforms/engagement_transforms.py`
**Test File:** `tests/unit/test_engagement_transforms.py`

#### Task 4.1: Daily Active Users
**Function:** `calculate_dau(interactions_df)`
- [ ] Write test: `test_calculate_dau_basic`
- [ ] Write test: `test_calculate_dau_single_user`
- [ ] Write test: `test_calculate_dau_user_multiple_interactions`
- [ ] Write test: `test_calculate_dau_empty`
- [ ] Write test: `test_calculate_dau_missing_columns`
- [ ] Implement `calculate_dau()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 4.2: Monthly Active Users
**Function:** `calculate_mau(interactions_df)`
- [ ] Write test: `test_calculate_mau_basic`
- [ ] Write test: `test_calculate_mau_daily_active_user`
- [ ] Implement `calculate_mau()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 4.3: Stickiness Ratio
**Function:** `calculate_stickiness(dau_df, mau_df)`
- [ ] Write test: `test_calculate_stickiness_perfect`
- [ ] Write test: `test_calculate_stickiness_low`
- [ ] Write test: `test_calculate_stickiness_single_day`
- [ ] Implement `calculate_stickiness()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 4.4: Power Users
**Function:** `identify_power_users(interactions_df, metadata_df, percentile=0.99)`
- [ ] Write test: `test_identify_power_users_top_1_percent`
- [ ] Write test: `test_identify_power_users_filters_outliers`
- [ ] Write test: `test_identify_power_users_joins_metadata`
- [ ] Write test: `test_identify_power_users_calculates_all_metrics`
- [ ] Implement `identify_power_users()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 4.5: Cohort Retention
**Function:** `calculate_cohort_retention(interactions_df, metadata_df, ...)`
- [ ] Write test: `test_calculate_cohort_retention_perfect`
- [ ] Write test: `test_calculate_cohort_retention_declining`
- [ ] Write test: `test_calculate_cohort_retention_multiple_cohorts`
- [ ] Write test: `test_calculate_cohort_retention_inactive_week`
- [ ] Implement `calculate_cohort_retention()` function
- [ ] All tests pass (RED → GREEN) ✅

---

## Phase 5: Performance Metrics (2 hours)

### Module: `src/transforms/performance_transforms.py`
**Test File:** `tests/unit/test_performance_transforms.py`

#### Task 5.1: Percentile Calculations
**Function:** `calculate_percentiles(df, value_column, group_by_columns, percentiles)`
- [ ] Write test: `test_calculate_percentiles_basic`
- [ ] Write test: `test_calculate_percentiles_multiple_groups`
- [ ] Write test: `test_calculate_percentiles_accuracy`
- [ ] Implement `calculate_percentiles()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 5.2: Device Correlation
**Function:** `calculate_device_correlation(interactions_df, metadata_df)`
- [ ] Write test: `test_calculate_device_correlation_basic`
- [ ] Write test: `test_calculate_device_correlation_multiple_users`
- [ ] Implement `calculate_device_correlation()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 5.3: Statistical Anomaly Detection
**Function:** `detect_anomalies_statistical(df, value_column, z_threshold=3.0, ...)`
- [ ] Write test: `test_detect_anomalies_statistical_basic`
- [ ] Write test: `test_detect_anomalies_statistical_grouped`
- [ ] Write test: `test_detect_anomalies_statistical_no_anomalies`
- [ ] Implement `detect_anomalies_statistical()` function
- [ ] All tests pass (RED → GREEN) ✅

---

## Phase 6: Session Analysis (2 hours)

### Module: `src/transforms/session_transforms.py`
**Test File:** `tests/unit/test_session_transforms.py`

#### Task 6.1: Sessionization
**Function:** `sessionize_interactions(interactions_df, session_timeout_seconds=1800)`
- [ ] Write test: `test_sessionize_interactions_single_session`
- [ ] Write test: `test_sessionize_interactions_multiple_sessions`
- [ ] Write test: `test_sessionize_interactions_multiple_users`
- [ ] Write test: `test_sessionize_interactions_exact_timeout`
- [ ] Implement `sessionize_interactions()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 6.2: Session Metrics
**Function:** `calculate_session_metrics(sessionized_df)`
- [ ] Write test: `test_calculate_session_metrics_bounce`
- [ ] Write test: `test_calculate_session_metrics_multi_action`
- [ ] Write test: `test_calculate_session_metrics_multiple_sessions`
- [ ] Implement `calculate_session_metrics()` function
- [ ] All tests pass (RED → GREEN) ✅

#### Task 6.3: Bounce Rate
**Function:** `calculate_bounce_rate(session_metrics_df, group_by_columns=None)`
- [ ] Write test: `test_calculate_bounce_rate_all_bounces`
- [ ] Write test: `test_calculate_bounce_rate_no_bounces`
- [ ] Write test: `test_calculate_bounce_rate_mixed`
- [ ] Write test: `test_calculate_bounce_rate_grouped`
- [ ] Implement `calculate_bounce_rate()` function
- [ ] All tests pass (RED → GREEN) ✅

---

## Phase 7: Spark Jobs & Integration (2-3 hours)

### Job 1: Data Processing
**File:** `src/jobs/01_data_processing.py`
- [ ] Write integration test
- [ ] Implement job to read raw data
- [ ] Apply optimized join with skew handling
- [ ] Write enriched data to Parquet
- [ ] Integration test passes ✅

### Job 2: User Engagement
**File:** `src/jobs/02_user_engagement.py`
- [ ] Write integration test
- [ ] Calculate DAU, MAU, stickiness
- [ ] Identify power users
- [ ] Calculate cohort retention
- [ ] Write results to PostgreSQL
- [ ] Integration test passes ✅

### Job 3: Performance Metrics
**File:** `src/jobs/03_performance_metrics.py`
- [ ] Write integration test
- [ ] Calculate P95/P99 by app version
- [ ] Calculate device correlation
- [ ] Detect anomalies
- [ ] Write results to PostgreSQL
- [ ] Integration test passes ✅

### Job 4: Session Analysis
**File:** `src/jobs/04_session_analysis.py`
- [ ] Write integration test
- [ ] Sessionize interactions
- [ ] Calculate session metrics
- [ ] Calculate bounce rates
- [ ] Write results to PostgreSQL
- [ ] Integration test passes ✅

### Job 5: Monitoring (Optional)
**File:** `src/jobs/05_monitoring.py`
- [ ] Implement custom accumulators
- [ ] Track record counts, errors, skipped records
- [ ] Log accumulator values
- [ ] Integration test passes ✅

---

## Phase 8: Database & Configuration (1 hour)

### PostgreSQL Setup
**File:** `database/schema.sql`
- [ ] Define table: `daily_active_users`
- [ ] Define table: `monthly_active_users`
- [ ] Define table: `user_stickiness`
- [ ] Define table: `power_users`
- [ ] Define table: `cohort_retention`
- [ ] Define table: `performance_by_version`
- [ ] Define table: `device_performance`
- [ ] Define table: `anomalies`
- [ ] Define table: `session_metrics`
- [ ] Define table: `bounce_rates`

**File:** `database/indexes.sql`
- [ ] Create indexes on date columns
- [ ] Create indexes on user_id columns
- [ ] Create indexes for common queries

### Configuration Files
**File:** `src/config/spark_config.py`
- [ ] Implement `create_spark_session()` with optimizations
- [ ] Configure AQE (Adaptive Query Execution)
- [ ] Set memory fractions
- [ ] Set shuffle partitions

**File:** `src/config/database_config.py`
- [ ] Implement `get_postgres_connection_props()`
- [ ] Implement `write_to_postgres()` helper

---

## Phase 9: Docker & Environment (1 hour)

### Docker Compose
**File:** `docker/docker-compose.yml`
- [ ] Define Spark master service
- [ ] Define Spark worker services (2x)
- [ ] Define PostgreSQL service
- [ ] Define Apache Superset service
- [ ] Define Redis service (for Superset)
- [ ] Configure volumes and networks

### Environment Setup
**File:** `.env.example`
- [ ] Define database credentials
- [ ] Define Spark configurations
- [ ] Define Superset admin credentials

---

## Phase 10: Apache Superset Dashboards (2-3 hours)

### Dashboard 1: Executive Overview
**File:** `superset/dashboards/01_executive_overview.json`
- [ ] Create DAU/MAU time series chart
- [ ] Create stickiness ratio gauge
- [ ] Create geographic heatmap
- [ ] Create top countries bar chart
- [ ] Add date range filter
- [ ] Configure auto-refresh (hourly)

### Dashboard 2: User Engagement Deep Dive
**File:** `superset/dashboards/02_user_engagement.json`
- [ ] Create cohort retention heatmap
- [ ] Create power users table
- [ ] Create engagement distribution histogram
- [ ] Add country/device filters
- [ ] Configure auto-refresh (6 hours)

### Dashboard 3: Performance Monitoring
**File:** `superset/dashboards/03_performance_monitoring.json`
- [ ] Create P95 load time line chart
- [ ] Create device performance comparison
- [ ] Create anomaly alerts table
- [ ] Add app version filter
- [ ] Configure auto-refresh (30 minutes)

### Dashboard 4: Session Analytics
**File:** `superset/dashboards/04_session_analytics.json`
- [ ] Create session duration treemap
- [ ] Create action distribution pie chart
- [ ] Create bounce rate by device
- [ ] Add date range filter
- [ ] Configure auto-refresh (6 hours)

---

## Phase 11: Optimization & Analysis (2 hours)

### Spark UI Analysis
- [ ] Run Job 1 on sample data
- [ ] Capture Spark UI screenshots (Stages, Tasks, Executors)
- [ ] Identify Bottleneck #1 (e.g., data skew)
- [ ] Implement optimization (e.g., salting)
- [ ] Re-run and verify improvement
- [ ] Identify Bottleneck #2 (e.g., excessive shuffle)
- [ ] Implement optimization (e.g., predicate pushdown)
- [ ] Re-run and verify improvement
- [ ] Identify Bottleneck #3 (e.g., GC pressure)
- [ ] Implement optimization (e.g., memory tuning)
- [ ] Re-run and verify improvement
- [ ] Document before/after metrics

---

## Phase 12: Documentation & Reporting (1-2 hours)

### Technical Report
**File:** `docs/REPORT.md`
- [ ] Executive summary
- [ ] Architecture overview
- [ ] Implementation approach
- [ ] TDD methodology and results
- [ ] Optimization techniques and impact
- [ ] Spark UI analysis with screenshots
- [ ] Performance benchmarks
- [ ] Challenges and solutions
- [ ] Future improvements

### Code Documentation
- [ ] Add docstrings to all functions
- [ ] Add inline comments for complex logic
- [ ] Update README.md with final instructions
- [ ] Create architecture diagrams (optional)

---

## Final Verification Checklist

### Tests & Coverage
- [ ] Run all unit tests: `pytest tests/unit -v`
- [ ] Verify >80% coverage: `pytest --cov=src --cov-report=html`
- [ ] Run integration tests: `pytest tests/integration -v`
- [ ] All tests pass ✅

### TDD Compliance
- [ ] Run compliance check: `./check-tdd.sh`
- [ ] All source files have corresponding tests ✅
- [ ] No orphaned test files ✅
- [ ] Git hooks installed and working ✅

### Functionality
- [ ] All Spark jobs complete successfully
- [ ] Data written to PostgreSQL correctly
- [ ] All Superset dashboards display data
- [ ] Filters and cross-filtering work
- [ ] No OOM errors or failures

### Performance
- [ ] Pipeline completes in <2 hours (full dataset)
- [ ] 30%+ improvement after optimization
- [ ] Max task time <3x median (no skew)
- [ ] GC time <10% of execution time
- [ ] Superset queries respond in <5 seconds

### Documentation
- [ ] All documentation files complete
- [ ] README.md has clear instructions
- [ ] REPORT.md comprehensive
- [ ] Code well-commented
- [ ] Spark UI screenshots included

---

## Summary Statistics

**Total Tasks:** 150+ checkboxes
**Functions to Implement:** 25 functions
**Test Cases to Write:** 71 test cases
**Spark Jobs:** 5 jobs
**Database Tables:** 10 tables
**Superset Dashboards:** 4 dashboards (30+ charts)
**Documentation Files:** 8+ files

**Estimated Timeline:**
- Setup & Schemas: 1.5 hours
- Core Functions (TDD): 10 hours
- Integration & Jobs: 3 hours
- Dashboards: 3 hours
- Optimization: 2 hours
- Documentation: 2 hours
- **Total: 21.5 hours**

**Success Criteria:**
- ✅ All tests pass with >80% coverage
- ✅ TDD compliance verified
- ✅ All jobs run without errors
- ✅ Dashboards operational
- ✅ Performance targets met
- ✅ Documentation complete

---

## Quick Start

```bash
# 1. Setup environment
./setup-tdd.sh

# 2. Start with Phase 0 (Setup)
mkdir -p src/{config,transforms,utils,jobs,schemas}
mkdir -p tests/{unit,integration,fixtures}

# 3. Pick a function from TDD_SPEC.md
# Start with something simple like validate_schema()

# 4. Follow TDD cycle for each function:
#    - Write test (RED)
#    - Implement function (GREEN)
#    - Refactor (keep GREEN)
#    - Commit

# 5. Verify compliance regularly
./check-tdd.sh

# 6. Run full test suite
pytest tests/ -v --cov=src
```

---

**Last Updated:** 2025-11-13
**Version:** 1.0
**Reference:** See `docs/TDD_SPEC.md` for detailed function specifications
