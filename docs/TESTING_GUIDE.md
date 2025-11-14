# Testing Guide - GoodNote Analytics Platform

Comprehensive testing documentation for the GoodNote Analytics Platform, following Test-Driven Development (TDD) principles.

---

## Overview

This project follows **strict TDD methodology** with automated git hook enforcement:

- **59+ unit tests** across 5 modules with >80% coverage target
- **Pytest + Chispa** framework for PySpark testing
- **Pre-commit hooks** enforce test-before-code policy
- **CI/CD ready** with automated test execution
- **Data quality validation** built into all ETL jobs

---

## Quick Start

```bash
# Run all unit tests
make test

# Run with coverage report
make test-coverage

# Run specific test file
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py -v

# Run specific test function
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py::test_identify_hot_keys_basic -v
```

---

## Test Organization

### Unit Tests (59+ Tests - Complete)

All tests located in `tests/unit/` with TDD-compliant implementation.

| Module | Test File | Tests | Coverage | Status |
|--------|-----------|-------|----------|--------|
| **Data Quality** | `test_data_quality.py` | 9 | Schema, nulls, outliers | ✅ Complete |
| **Join Transforms** | `test_join_transforms.py` | 15 | Hot keys, salting, joins | ✅ Complete |
| **Engagement** | `test_engagement_transforms.py` | 15 | DAU, MAU, retention | ✅ Complete |
| **Performance** | `test_performance_transforms.py` | 9 | Percentiles, correlation | ✅ Complete |
| **Sessions** | `test_session_transforms.py` | 11 | Sessionization, bounce | ✅ Complete |

### Integration Tests (Pending)

```bash
# Location: tests/integration/ (minimal coverage)
make test-integration

# Status: End-to-end pipeline testing pending (3-4 hours estimated)
```

**Planned Integration Tests:**
- Full ETL pipeline execution
- Database write/read validation
- Multi-job dependency testing
- Data quality across job boundaries

---

## Running Tests

### Using Makefile (Recommended)

```bash
# All unit tests (59+ tests)
make test

# Unit tests only
make test-unit

# Integration tests
make test-integration

# Coverage report (HTML + terminal)
make test-coverage

# Check TDD compliance
make check-tdd
```

### Manual Commands

```bash
# Basic test execution
docker exec goodnote-spark-master pytest tests/unit -v

# With short traceback
docker exec goodnote-spark-master pytest tests/unit -v --tb=short

# Specific module
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py -v

# Specific test class
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py::TestIdentifyHotKeys -v

# Specific test function
docker exec goodnote-spark-master pytest tests/unit/test_join_transforms.py::test_identify_hot_keys_basic -v

# With coverage
docker exec goodnote-spark-master pytest tests/unit \
    --cov=src \
    --cov-report=html \
    --cov-report=term \
    -v
```

### CI/CD Testing

```bash
# Full CI test suite (used in automation)
make ci-test

# What it does:
# 1. Start Docker services
# 2. Wait 30 seconds for readiness
# 3. Run all unit tests
# 4. Generate small sample data
# 5. Execute all 4 Spark jobs
```

---

## Test Fixtures

### Session-Scoped Spark Fixture

Defined in `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.

    Configuration:
    - Local mode with 2 cores
    - Reduced shuffle partitions (2) for speed
    - 2GB driver memory
    - ERROR log level to reduce noise
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("goodnote-tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
```

### Sample Data Fixtures

```python
@pytest.fixture
def sample_interactions(spark):
    """Basic interactions test data (3 rows)."""
    data = [
        ("u001", "2023-01-01 10:00:00", "page_view", "p001", 5000, "1.0.0"),
        ("u001", "2023-01-01 10:05:00", "edit", "p001", 120000, "1.0.0"),
        ("u002", "2023-01-01 10:10:00", "page_view", "p002", 3000, "1.0.0"),
    ]
    return spark.createDataFrame(data, ["user_id", "timestamp", ...])

@pytest.fixture
def sample_metadata(spark):
    """Basic metadata test data (2 rows)."""
    data = [
        ("u001", "2023-01-01", "US", "iPad", "premium"),
        ("u002", "2023-01-01", "UK", "iPhone", "free"),
    ]
    return spark.createDataFrame(data, ["user_id", "join_date", ...])
```

**Note:** Most tests generate data inline for flexibility. No factory functions implemented (see `docs/IMPLEMENTATION_TASKS.md - Phase 0`).

---

## Test Data Generation Strategies

### Strategy 1: Inline Data Creation (Preferred)

Used in 90% of tests for maximum flexibility:

```python
def test_identify_hot_keys_basic(spark):
    """Test hot key identification."""
    # Arrange - Create test data inline
    data = [
        ("u001", 10000),  # Hot key (99th percentile)
        ("u002", 100),    # Normal key
        ("u003", 150),    # Normal key
    ]
    df = spark.createDataFrame(data, ["user_id", "count"])

    # Act
    hot_keys = identify_hot_keys(df, "user_id", threshold_percentile=0.99)

    # Assert
    assert hot_keys.count() == 1
    assert hot_keys.first()["user_id"] == "u001"
```

### Strategy 2: Parameterized Tests

For testing multiple scenarios:

```python
@pytest.mark.parametrize("percentile,expected_count", [
    (0.50, 5),   # Median - 5 users above
    (0.90, 1),   # 90th percentile - 1 user
    (0.99, 0),   # 99th percentile - none (all below)
])
def test_identify_hot_keys_percentiles(spark, percentile, expected_count):
    """Test different percentile thresholds."""
    data = [(f"u{i:03d}", 100 + i * 10) for i in range(10)]
    df = spark.createDataFrame(data, ["user_id", "count"])

    hot_keys = identify_hot_keys(df, "user_id", threshold_percentile=percentile)
    assert hot_keys.count() == expected_count
```

### Strategy 3: Large Dataset Simulation

For performance and skew testing:

```python
def test_apply_salting_large_dataset(spark):
    """Test salting with realistic data volume."""
    # 10,000 normal users + 5 hot keys
    normal_data = [(f"u{i:05d}", i) for i in range(10000)]
    hot_data = [("hot_u001", 100000), ("hot_u002", 95000)]

    all_data = normal_data + hot_data
    df = spark.createDataFrame(all_data, ["user_id", "interaction_id"])

    # Test salting logic...
```

---

## Data Quality Validation

### Built-In Quality Checks

All ETL jobs include these validations:

#### 1. Schema Validation
```python
from src.utils.data_quality import validate_schema

is_valid, errors = validate_schema(df, expected_schema, strict=True)
if not is_valid:
    raise ValueError(f"Schema validation failed: {errors}")
```

**Tests:**
- Exact column match
- Data type verification
- Nullability enforcement
- Extra/missing column detection

#### 2. Null Detection
```python
from src.utils.data_quality import detect_nulls

null_report = detect_nulls(df, non_nullable_columns=["user_id", "timestamp"])
if null_report:
    print(f"⚠️  Warning: Null values found: {null_report}")
```

**Tests:**
- Null counts per column
- Non-nullable column enforcement
- Threshold-based alerting

#### 3. Outlier Detection
```python
from src.utils.data_quality import detect_outliers

outliers_df = detect_outliers(
    df,
    column="duration_ms",
    method="iqr",  # or "threshold"
    factor=3.0
)
```

**Tests:**
- IQR method (1.5x and 3.0x factors)
- Threshold method (min/max bounds)
- Edge cases (empty data, all outliers, no outliers)

---

## Coverage Requirements

### Target Coverage: >80%

```bash
# Generate coverage report
make test-coverage

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Current Coverage Status

**Core Modules:**
- ✅ `src/transforms/join_transforms.py` - 95%+ (15 tests)
- ✅ `src/transforms/engagement_transforms.py` - 90%+ (15 tests)
- ✅ `src/transforms/performance_transforms.py` - 85%+ (9 tests)
- ✅ `src/transforms/session_transforms.py` - 90%+ (11 tests)
- ✅ `src/utils/data_quality.py` - 95%+ (9 tests)

**Untested/Low Coverage:**
- ⚠️ `src/jobs/*.py` - Integration tests pending
- ⚠️ `src/config/*.py` - Configuration modules (low priority)
- ⚠️ `scripts/*.py` - Utility scripts (low priority)

---

## TDD Compliance

### Pre-Commit Hook Enforcement

**Hook Location:** `.git/hooks/pre-commit`

**What it checks:**
```bash
# Run check-tdd.sh before every commit
./check-tdd.sh

# Blocks commits if:
# 1. New Python files added without corresponding tests
# 2. Test coverage drops below threshold
# 3. Any tests are failing
```

### Manual TDD Verification

```bash
# Check TDD compliance
make check-tdd

# Or directly:
bash ./check-tdd.sh
```

### Bypassing Hooks (Use Sparingly!)

```bash
# For documentation-only commits
git commit --no-verify -m "Update documentation"

# WARNING: Only use for:
# - Documentation updates (.md files)
# - Configuration changes
# - Non-code files
```

---

## Test Examples

### Example 1: Basic Function Test

```python
def test_calculate_dau_basic(spark):
    """
    GIVEN: Interactions from 3 unique users on 2023-01-01
    WHEN: calculate_dau() is called
    THEN: Returns 3 DAU for that date
    """
    # Arrange
    data = [
        ("u001", "2023-01-01 10:00:00", "page_view"),
        ("u001", "2023-01-01 11:00:00", "edit"),
        ("u002", "2023-01-01 12:00:00", "page_view"),
        ("u003", "2023-01-01 13:00:00", "click"),
    ]
    df = spark.createDataFrame(data, ["user_id", "timestamp", "action"])

    # Act
    dau_df = calculate_dau(df)

    # Assert
    assert dau_df.count() == 1
    row = dau_df.first()
    assert row["date"] == "2023-01-01"
    assert row["dau"] == 3
```

### Example 2: Chispa Assertion

```python
from chispa.dataframe_comparer import assert_df_equality

def test_apply_salting_output_schema(spark):
    """Test that salting preserves original columns."""
    # Arrange
    interactions = spark.createDataFrame([...])
    hot_keys = spark.createDataFrame([...])

    # Act
    salted_df = apply_salting(interactions, hot_keys, "user_id", salt_factor=10)

    # Assert - check exact DataFrame equality
    expected_df = spark.createDataFrame([...])
    assert_df_equality(salted_df, expected_df, ignore_row_order=True)
```

### Example 3: Exception Testing

```python
def test_validate_schema_raises_on_none(spark):
    """Test that None DataFrame raises ValueError."""
    expected_schema = StructType([...])

    with pytest.raises(ValueError, match="DataFrame cannot be None"):
        validate_schema(None, expected_schema)
```

---

## Troubleshooting

### Issue: Tests Fail with "Module Not Found"

```bash
ModuleNotFoundError: No module named 'pyspark'
ModuleNotFoundError: No module named 'src'
```

**Solution:**
```bash
# Run tests INSIDE Docker container (not on host)
make test  # Correct

# NOT this:
pytest tests/unit  # Wrong - runs on host machine
```

### Issue: Tests Pass Locally But Fail in CI

**Common Causes:**
1. **Missing Docker services:** Ensure `make up` runs first
2. **Timing issues:** Add `sleep 30` after service start
3. **Environment differences:** Check PYTHONPATH in container

**Solution:**
```bash
# Use CI test command
make ci-test

# Manually replicate CI environment
make clean
make up
sleep 30
make test
```

### Issue: Coverage Report Not Generated

```bash
# If htmlcov/ directory missing
docker exec goodnote-spark-master pytest tests/unit \
    --cov=src \
    --cov-report=html \
    --cov-report=term \
    -v

# Then copy from container
docker cp goodnote-spark-master:/opt/spark-apps/htmlcov ./htmlcov
```

### Issue: Slow Test Execution

**Optimization tips:**
```python
# 1. Reduce shuffle partitions in conftest.py
.config("spark.sql.shuffle.partitions", "2")  # Default: 200

# 2. Use local[2] mode (not local[*])
.master("local[2]")

# 3. Set log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# 4. Run specific test files instead of all tests
pytest tests/unit/test_data_quality.py -v  # Faster
```

---

## Best Practices

### 1. Follow Arrange-Act-Assert Pattern

```python
def test_function_name(spark):
    """Clear docstring explaining test."""
    # Arrange - Set up test data
    df = spark.createDataFrame([...])

    # Act - Execute function
    result = function_under_test(df)

    # Assert - Verify results
    assert result.count() == expected_count
```

### 2. Use Descriptive Test Names

```python
# ✅ Good
def test_identify_hot_keys_returns_users_above_99th_percentile():
    ...

# ❌ Bad
def test_hot_keys():
    ...
```

### 3. Write Tests BEFORE Implementation (TDD)

```python
# Step 1: Write failing test
def test_new_feature(spark):
    result = new_feature(df)  # Function doesn't exist yet
    assert result.count() == 5

# Step 2: Run test - should fail
# Step 3: Implement minimum code to pass
# Step 4: Refactor
```

### 4. Test Edge Cases

```python
# Normal case
def test_function_normal_data():
    ...

# Edge cases
def test_function_empty_dataframe():
    ...

def test_function_single_row():
    ...

def test_function_all_nulls():
    ...

def test_function_duplicate_keys():
    ...
```

### 5. Use Fixtures for Common Data

```python
# Define once in conftest.py
@pytest.fixture
def sample_interactions(spark):
    return spark.createDataFrame([...])

# Reuse in multiple tests
def test_function_a(sample_interactions):
    result = function_a(sample_interactions)
    ...

def test_function_b(sample_interactions):
    result = function_b(sample_interactions)
    ...
```

---

## References

- **TDD Specifications:** [docs/TDD_SPEC.md](./TDD_SPEC.md)
- **Implementation Tasks:** [docs/IMPLEMENTATION_TASKS.md](./IMPLEMENTATION_TASKS.md)
- **Makefile Commands:** Run `make help` or see [Makefile](../Makefile)
- **Chispa Documentation:** https://github.com/MrPowers/chispa
- **Pytest Documentation:** https://docs.pytest.org/

---

**Document Version:** 1.0
**Last Updated:** 2025-11-14
**Status:** Complete for unit tests, integration tests pending
