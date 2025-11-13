# TDD Specification - GoodNote Analytics Platform

## Overview

This document provides comprehensive Test-Driven Development (TDD) specifications for implementing the GoodNote Data Engineering Challenge. Every component is designed to be developed following strict TDD principles: **write tests first, then implement**.

**TDD Workflow:**
1. Read function specification from this document
2. Write failing test cases
3. Implement minimum code to pass tests
4. Refactor while keeping tests green
5. Move to next function

**Key Principles:**
- Every function has explicit input/output contracts
- All edge cases are documented before coding
- Test fixtures are specified in detail
- Acceptance criteria define "done"

---

## Table of Contents

1. [Data Schemas](#data-schemas)
2. [Test Fixtures Specification](#test-fixtures-specification)
3. [Task 1: Data Processing Specifications](#task-1-data-processing-specifications)
4. [Task 2: User Engagement Specifications](#task-2-user-engagement-specifications)
5. [Task 3: Performance Metrics Specifications](#task-3-performance-metrics-specifications)
6. [Task 4: Session Analysis Specifications](#task-4-session-analysis-specifications)
7. [Task 5: Data Quality Specifications](#task-5-data-quality-specifications)
8. [Testing Strategy](#testing-strategy)

---

## Data Schemas

### Interactions Schema

**Location:** `src/schemas/interactions_schema.py`

```python
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, IntegerType, LongType
)

INTERACTIONS_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("action_type", StringType(), nullable=False),
    StructField("page_id", StringType(), nullable=False),
    StructField("duration_ms", LongType(), nullable=False),
    StructField("app_version", StringType(), nullable=False)
])

# Valid action types
VALID_ACTION_TYPES = ["page_view", "edit", "create", "delete", "share"]

# Constraints
MAX_DURATION_MS = 8 * 60 * 60 * 1000  # 8 hours
MIN_DURATION_MS = 0
```

**Test Cases:**
- ✓ Schema validation accepts valid data
- ✓ Schema validation rejects NULL user_id
- ✓ Schema validation rejects NULL timestamp
- ✓ Schema validation rejects invalid action_type

### Metadata Schema

**Location:** `src/schemas/metadata_schema.py`

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)

METADATA_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("join_date", DateType(), nullable=False),
    StructField("country", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("subscription_type", StringType(), nullable=False)
])

# Valid values
VALID_COUNTRIES = ["US", "UK", "DE", "FR", "JP", "AU", "CA", "IN", "BR", "MX"]
VALID_DEVICE_TYPES = ["iPad", "iPhone", "Mac", "Android_Phone", "Android_Tablet"]
VALID_SUBSCRIPTION_TYPES = ["free", "basic", "premium", "enterprise"]
```

**Test Cases:**
- ✓ Schema validation accepts valid metadata
- ✓ Schema validation rejects invalid country codes
- ✓ Schema validation rejects invalid device types
- ✓ Schema validation rejects invalid subscription types

---

## Test Fixtures Specification

### Fixture 1: Basic Interactions Dataset

**Location:** `tests/fixtures/basic_interactions.csv`

**Purpose:** Simple dataset for basic functionality testing

**Specification:**
```
user_id,timestamp,action_type,page_id,duration_ms,app_version
u001,2023-01-01 10:00:00,page_view,p001,5000,1.0.0
u001,2023-01-01 10:05:00,edit,p001,120000,1.0.0
u002,2023-01-01 10:10:00,page_view,p002,3000,1.0.0
u002,2023-01-01 10:15:00,create,p003,60000,1.0.0
u003,2023-01-02 09:00:00,page_view,p001,4000,1.0.1
```

**Properties:**
- Total users: 3
- Total interactions: 5
- Date range: 2023-01-01 to 2023-01-02
- DAU on 2023-01-01: 2
- DAU on 2023-01-02: 1

### Fixture 2: Skewed Dataset

**Location:** `tests/fixtures/skewed_interactions.csv`

**Purpose:** Test data skew handling

**Specification:**
```python
# Generated programmatically
# Power user: u001 with 10,000 interactions
# Normal users: u002-u100 with 100 interactions each
# Date: 2023-01-01
```

**Properties:**
- Total users: 100
- Power user (u001): 10,000 interactions (90.9% of data)
- Normal users: 100 interactions each
- Skew factor: 100:1

**Test Cases:**
- ✓ Join completes without OOM
- ✓ Max task time < 3x median task time after salting

### Fixture 3: Edge Cases Dataset

**Location:** `tests/fixtures/edge_cases.csv`

**Purpose:** Test edge cases and data quality

**Specification:**
```
user_id,timestamp,action_type,page_id,duration_ms,app_version
u001,2023-01-01 10:00:00,page_view,p001,-100,1.0.0        # Negative duration
u002,2023-01-01 10:00:00,page_view,p001,50000000,1.0.0    # > 8 hours
u003,2023-01-01 10:00:00,invalid_action,p001,5000,1.0.0   # Invalid action
u004,2023-01-01 10:00:00,page_view,p001,5000,              # Missing app_version
```

**Expected Behavior:**
- Negative duration: Filter out or set to 0
- Duration > 8 hours: Flag as outlier, exclude from percentile calculations
- Invalid action: Filter out
- Missing app_version: Handle as NULL or "unknown"

### Fixture 4: Session Analysis Dataset

**Location:** `tests/fixtures/session_test.csv`

**Purpose:** Test sessionization logic

**Specification:**
```
user_id,timestamp,action_type,page_id,duration_ms,app_version
u001,2023-01-01 10:00:00,page_view,p001,5000,1.0.0    # Session 1 start
u001,2023-01-01 10:05:00,edit,p001,10000,1.0.0        # Session 1
u001,2023-01-01 10:40:00,page_view,p002,5000,1.0.0    # Session 2 (>30 min gap)
u001,2023-01-01 10:42:00,edit,p002,8000,1.0.0         # Session 2
```

**Expected Output:**
- User u001 should have 2 sessions
- Session 1: 2 actions, duration: 5 minutes
- Session 2: 2 actions, duration: 2 minutes

### Fixture Factory Functions

**Location:** `tests/conftest.py`

```python
def create_interactions_df(spark, data=None, num_users=10, num_interactions=100):
    """
    Create test interactions DataFrame.

    Args:
        spark: SparkSession
        data: Optional list of tuples (user_id, timestamp, action_type, page_id, duration_ms, app_version)
        num_users: Number of users to generate (if data=None)
        num_interactions: Total interactions to generate (if data=None)

    Returns:
        DataFrame with INTERACTIONS_SCHEMA
    """
    pass

def create_metadata_df(spark, data=None, num_users=10):
    """
    Create test metadata DataFrame.

    Args:
        spark: SparkSession
        data: Optional list of tuples (user_id, join_date, country, device_type, subscription_type)
        num_users: Number of users to generate (if data=None)

    Returns:
        DataFrame with METADATA_SCHEMA
    """
    pass

def create_skewed_interactions(spark, power_user_id="u001", power_user_interactions=10000, normal_users=99, normal_interactions=100):
    """
    Create skewed dataset for testing skew handling.

    Args:
        spark: SparkSession
        power_user_id: ID of power user
        power_user_interactions: Number of interactions for power user
        normal_users: Number of normal users
        normal_interactions: Interactions per normal user

    Returns:
        DataFrame with skewed data
    """
    pass
```

---

## Task 1: Data Processing Specifications

### Module: `src/transforms/join_transforms.py`

### Function 1.1: `identify_hot_keys`

**Purpose:** Identify skewed keys that require special handling

**Signature:**
```python
def identify_hot_keys(
    df: DataFrame,
    key_column: str,
    threshold_percentile: float = 0.99
) -> DataFrame:
    """
    Identify hot keys (skewed values) in a DataFrame.

    Args:
        df: Input DataFrame
        key_column: Column name to analyze for skew
        threshold_percentile: Percentile threshold (default 0.99 = top 1%)

    Returns:
        DataFrame with columns: [key_column, count]
        Only includes keys above the threshold

    Raises:
        ValueError: If key_column doesn't exist in df
    """
```

**Test Cases:**

#### Test 1.1.1: Basic Hot Key Identification
```python
def test_identify_hot_keys_basic():
    """
    GIVEN: DataFrame with 100 users, u001 has 10,000 interactions, others have 100
    WHEN: identify_hot_keys() is called with threshold_percentile=0.99
    THEN:
        - Returns DataFrame with 1 row (u001)
        - u001 has count=10,000
    """
```

#### Test 1.1.2: No Hot Keys
```python
def test_identify_hot_keys_uniform_distribution():
    """
    GIVEN: DataFrame with uniform distribution (all users have 100 interactions)
    WHEN: identify_hot_keys() is called
    THEN: Returns empty DataFrame
    """
```

#### Test 1.1.3: Multiple Hot Keys
```python
def test_identify_hot_keys_multiple():
    """
    GIVEN: DataFrame with 100 users, top 5 have 5,000 interactions each
    WHEN: identify_hot_keys() is called with threshold_percentile=0.95
    THEN: Returns DataFrame with 5 rows (top 5 users)
    """
```

#### Test 1.1.4: Invalid Column
```python
def test_identify_hot_keys_invalid_column():
    """
    GIVEN: DataFrame without 'nonexistent_column'
    WHEN: identify_hot_keys() is called with key_column='nonexistent_column'
    THEN: Raises ValueError with message "Column 'nonexistent_column' not found"
    """
```

**Acceptance Criteria:**
- ✓ Correctly identifies top 1% of keys by frequency
- ✓ Returns empty DataFrame when no skew exists
- ✓ Handles edge case of single key
- ✓ Validates input column exists
- ✓ Performance: < 30 seconds for 1B rows

---

### Function 1.2: `apply_salting`

**Purpose:** Apply salting to hot keys for skew mitigation

**Signature:**
```python
def apply_salting(
    df: DataFrame,
    hot_keys_df: DataFrame,
    key_column: str,
    salt_factor: int = 10
) -> DataFrame:
    """
    Apply salting to hot keys by adding random salt suffix.

    Args:
        df: Input DataFrame
        hot_keys_df: DataFrame with hot keys (output from identify_hot_keys)
        key_column: Column to salt
        salt_factor: Number of salt buckets (default 10)

    Returns:
        DataFrame with additional columns:
            - salt (IntegerType): Random salt value 0 to salt_factor-1
            - {key_column}_salted (StringType): Concatenated key with salt

    Raises:
        ValueError: If salt_factor < 2
        ValueError: If key_column not in df or hot_keys_df
    """
```

**Test Cases:**

#### Test 1.2.1: Basic Salting
```python
def test_apply_salting_basic():
    """
    GIVEN:
        - df with u001 (hot key), u002, u003 (normal keys)
        - hot_keys_df contains only u001
        - salt_factor = 10
    WHEN: apply_salting() is called
    THEN:
        - u001 rows have salt values 0-9 (uniformly distributed)
        - u001 rows have user_id_salted = "u001_0", "u001_1", ..., "u001_9"
        - u002, u003 rows have salt = 0
        - u002, u003 rows have user_id_salted = "u002_0", "u003_0"
    """
```

#### Test 1.2.2: Salt Distribution
```python
def test_apply_salting_distribution():
    """
    GIVEN: 10,000 rows with user_id = u001 (hot key)
    WHEN: apply_salting() is called with salt_factor=10
    THEN:
        - Each salt value (0-9) appears ~1,000 times
        - Distribution variance < 10% (fairly uniform)
    """
```

#### Test 1.2.3: No Hot Keys
```python
def test_apply_salting_no_hot_keys():
    """
    GIVEN: hot_keys_df is empty
    WHEN: apply_salting() is called
    THEN:
        - All rows have salt = 0
        - All rows have {key_column}_salted = "{key_column}_0"
    """
```

#### Test 1.2.4: Invalid Salt Factor
```python
def test_apply_salting_invalid_salt_factor():
    """
    GIVEN: salt_factor = 1
    WHEN: apply_salting() is called
    THEN: Raises ValueError with message "salt_factor must be >= 2"
    """
```

**Acceptance Criteria:**
- ✓ Salts only hot keys, leaves normal keys unchanged
- ✓ Salt distribution is approximately uniform
- ✓ Original key_column is preserved
- ✓ Validates salt_factor >= 2
- ✓ Performance: < 5% overhead compared to unsalted join

---

### Function 1.3: `explode_for_salting`

**Purpose:** Explode small table to match salt factor for join

**Signature:**
```python
def explode_for_salting(
    df: DataFrame,
    hot_keys_df: DataFrame,
    key_column: str,
    salt_factor: int = 10
) -> DataFrame:
    """
    Explode rows for hot keys to match salt factor.

    Args:
        df: Small table to explode (e.g., metadata)
        hot_keys_df: DataFrame with hot keys
        key_column: Column to match for explosion
        salt_factor: Number of salt buckets

    Returns:
        DataFrame with:
            - Original rows for non-hot keys (salt=0)
            - Exploded rows for hot keys (salt=0 to salt_factor-1)
            - {key_column}_salted column

    Example:
        Input: user_id=u001 (hot key), user_id=u002 (normal)
        Output:
            - u001 repeated 10 times with salt 0-9
            - u002 appears once with salt 0
    """
```

**Test Cases:**

#### Test 1.3.1: Basic Explosion
```python
def test_explode_for_salting_basic():
    """
    GIVEN:
        - df with 1 row: user_id=u001, country=US
        - hot_keys_df contains u001
        - salt_factor = 10
    WHEN: explode_for_salting() is called
    THEN:
        - Returns 10 rows
        - All rows have user_id=u001, country=US
        - Salt values range from 0 to 9
        - user_id_salted = "u001_0", "u001_1", ..., "u001_9"
    """
```

#### Test 1.3.2: Mixed Hot and Normal Keys
```python
def test_explode_for_salting_mixed():
    """
    GIVEN:
        - df with 2 rows: u001 (hot), u002 (normal)
        - hot_keys_df contains only u001
        - salt_factor = 10
    WHEN: explode_for_salting() is called
    THEN:
        - Returns 11 rows total
        - 10 rows for u001 (salt 0-9)
        - 1 row for u002 (salt 0)
    """
```

#### Test 1.3.3: Data Integrity
```python
def test_explode_for_salting_preserves_data():
    """
    GIVEN: df with user_id=u001, country=US, device_type=iPad, subscription_type=premium
    WHEN: explode_for_salting() is called
    THEN: All metadata fields (country, device_type, subscription_type) are preserved across all exploded rows
    """
```

**Acceptance Criteria:**
- ✓ Explodes only hot keys
- ✓ Preserves all original columns
- ✓ Creates correct number of rows (original + hot_keys * (salt_factor - 1))
- ✓ Performance: Minimal memory overhead (lazy evaluation)

---

### Function 1.4: `optimized_join`

**Purpose:** High-level function to perform optimized join with skew handling

**Signature:**
```python
def optimized_join(
    large_df: DataFrame,
    small_df: DataFrame,
    join_key: str,
    join_type: str = "inner",
    enable_broadcast: bool = True,
    enable_salting: bool = True,
    skew_threshold: float = 0.99,
    salt_factor: int = 10
) -> DataFrame:
    """
    Perform optimized join with automatic skew detection and mitigation.

    Strategy:
        1. If small_df fits broadcast threshold -> broadcast join
        2. Else, detect hot keys in large_df
        3. If hot keys found -> apply salting
        4. Perform join on salted keys
        5. Clean up salt columns

    Args:
        large_df: Large DataFrame (e.g., interactions)
        small_df: Small DataFrame (e.g., metadata)
        join_key: Column to join on
        join_type: "inner", "left", "right", "outer"
        enable_broadcast: Try broadcast join if possible
        enable_salting: Apply salting if skew detected
        skew_threshold: Percentile threshold for hot key detection
        salt_factor: Number of salt buckets

    Returns:
        Joined DataFrame with salt columns removed

    Raises:
        ValueError: If join_key not in both DataFrames
    """
```

**Test Cases:**

#### Test 1.4.1: Small Table - Broadcast Join
```python
def test_optimized_join_broadcast():
    """
    GIVEN:
        - large_df: 1M interactions
        - small_df: 1K metadata (< broadcast threshold)
    WHEN: optimized_join() is called with enable_broadcast=True
    THEN:
        - Uses broadcast join (verify in query plan)
        - No salting applied
        - All rows joined correctly
    """
```

#### Test 1.4.2: Skewed Data - Salted Join
```python
def test_optimized_join_with_salting():
    """
    GIVEN:
        - large_df: 100K interactions with u001 having 50K (50%)
        - small_df: 100 metadata (too large to broadcast)
        - enable_salting=True
    WHEN: optimized_join() is called
    THEN:
        - Detects u001 as hot key
        - Applies salting
        - Join completes successfully
        - No salt columns in output
    """
```

#### Test 1.4.3: Uniform Data - Standard Join
```python
def test_optimized_join_no_skew():
    """
    GIVEN:
        - large_df: 100K interactions, uniformly distributed
        - small_df: 100 metadata
    WHEN: optimized_join() is called
    THEN:
        - No hot keys detected
        - Standard sort-merge join used
        - All rows joined correctly
    """
```

#### Test 1.4.4: Left Join Semantics
```python
def test_optimized_join_left_join():
    """
    GIVEN:
        - large_df: interactions with users u001, u002, u003
        - small_df: metadata with only u001, u002 (u003 missing)
        - join_type="left"
    WHEN: optimized_join() is called
    THEN:
        - All 3 users present in result
        - u003 has NULL values for metadata columns
    """
```

**Acceptance Criteria:**
- ✓ Automatically chooses optimal join strategy
- ✓ Handles skewed data without OOM
- ✓ Preserves join semantics (inner, left, right, outer)
- ✓ Cleans up intermediate columns
- ✓ Performance: 30%+ faster than naive join for skewed data

---

## Task 2: User Engagement Specifications

### Module: `src/transforms/engagement_transforms.py`

### Function 2.1: `calculate_dau`

**Purpose:** Calculate Daily Active Users

**Signature:**
```python
def calculate_dau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Daily Active Users (DAU).

    Definition: A user is "active" if they have at least 1 interaction on a given date.

    Args:
        interactions_df: DataFrame with schema [user_id, timestamp, ...]

    Returns:
        DataFrame with schema:
            - date (DateType): Calendar date
            - dau (LongType): Count of distinct active users
            - total_interactions (LongType): Total interactions for the day
            - total_duration_ms (LongType): Sum of all durations
            - avg_duration_per_user (DoubleType): total_duration_ms / dau

    Raises:
        ValueError: If required columns (user_id, timestamp) are missing
    """
```

**Test Cases:**

#### Test 2.1.1: Basic DAU Calculation
```python
def test_calculate_dau_basic():
    """
    GIVEN:
        - 2023-01-01: u001 (3 interactions), u002 (2 interactions)
        - 2023-01-02: u001 (1 interaction), u003 (1 interaction)
    WHEN: calculate_dau() is called
    THEN:
        - 2023-01-01: dau=2, total_interactions=5
        - 2023-01-02: dau=2, total_interactions=2
    """
```

#### Test 2.1.2: Single User, Multiple Days
```python
def test_calculate_dau_single_user():
    """
    GIVEN: u001 has interactions on 7 consecutive days
    WHEN: calculate_dau() is called
    THEN:
        - 7 rows returned
        - Each date has dau=1
    """
```

#### Test 2.1.3: User Active Multiple Times Same Day
```python
def test_calculate_dau_user_multiple_interactions():
    """
    GIVEN: u001 has 10 interactions on 2023-01-01
    WHEN: calculate_dau() is called
    THEN:
        - 2023-01-01: dau=1 (not 10 - distinct users)
        - total_interactions=10
    """
```

#### Test 2.1.4: Empty Input
```python
def test_calculate_dau_empty():
    """
    GIVEN: Empty DataFrame with correct schema
    WHEN: calculate_dau() is called
    THEN: Returns empty DataFrame with output schema
    """
```

#### Test 2.1.5: Missing Columns
```python
def test_calculate_dau_missing_columns():
    """
    GIVEN: DataFrame without 'timestamp' column
    WHEN: calculate_dau() is called
    THEN: Raises ValueError with message "Required column 'timestamp' not found"
    """
```

**Acceptance Criteria:**
- ✓ Correctly counts distinct users per day
- ✓ Aggregates interaction counts and durations
- ✓ Handles users with multiple interactions per day
- ✓ Returns empty result for empty input
- ✓ Validates required columns exist
- ✓ Performance: < 2 minutes for 1B interactions

---

### Function 2.2: `calculate_mau`

**Purpose:** Calculate Monthly Active Users

**Signature:**
```python
def calculate_mau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Monthly Active Users (MAU).

    Definition: A user is "active" if they have at least 1 interaction in a given month.

    Args:
        interactions_df: DataFrame with schema [user_id, timestamp, ...]

    Returns:
        DataFrame with schema:
            - month (DateType): First day of month (e.g., 2023-01-01)
            - mau (LongType): Count of distinct active users
            - total_interactions (LongType): Total interactions for the month

    Raises:
        ValueError: If required columns are missing
    """
```

**Test Cases:**

#### Test 2.2.1: Basic MAU Calculation
```python
def test_calculate_mau_basic():
    """
    GIVEN:
        - Jan 2023: u001 (active on Jan 1, 5, 20), u002 (active on Jan 15)
        - Feb 2023: u001 (active on Feb 3), u003 (active on Feb 10)
    WHEN: calculate_mau() is called
    THEN:
        - Jan 2023: mau=2
        - Feb 2023: mau=2
    """
```

#### Test 2.2.2: User Active Full Month
```python
def test_calculate_mau_daily_active_user():
    """
    GIVEN: u001 has interactions on all 31 days of January 2023
    WHEN: calculate_mau() is called
    THEN:
        - Jan 2023: mau=1 (counted once, not 31 times)
    """
```

**Acceptance Criteria:**
- ✓ Correctly counts distinct users per month
- ✓ User counted once per month regardless of activity frequency
- ✓ Month truncation correct (first day of month)
- ✓ Performance: < 2 minutes for 1B interactions

---

### Function 2.3: `calculate_stickiness`

**Purpose:** Calculate DAU/MAU stickiness ratio

**Signature:**
```python
def calculate_stickiness(dau_df: DataFrame, mau_df: DataFrame) -> DataFrame:
    """
    Calculate stickiness ratio (average DAU / MAU) per month.

    Stickiness indicates how frequently monthly users engage daily.
    Higher stickiness = better engagement.

    Args:
        dau_df: Output from calculate_dau() [date, dau, ...]
        mau_df: Output from calculate_mau() [month, mau, ...]

    Returns:
        DataFrame with schema:
            - month (DateType): First day of month
            - avg_dau (DoubleType): Average DAU for the month
            - mau (LongType): MAU for the month
            - stickiness_ratio (DoubleType): avg_dau / mau (0.0 to 1.0)

    Raises:
        ValueError: If schemas don't match expected format
    """
```

**Test Cases:**

#### Test 2.3.1: Perfect Stickiness
```python
def test_calculate_stickiness_perfect():
    """
    GIVEN:
        - January has 31 days
        - All 31 days have DAU=10
        - MAU for January = 10
    WHEN: calculate_stickiness() is called
    THEN:
        - avg_dau = 10
        - mau = 10
        - stickiness_ratio = 1.0 (perfect stickiness)
    """
```

#### Test 2.3.2: Low Stickiness
```python
def test_calculate_stickiness_low():
    """
    GIVEN:
        - MAU for January = 100
        - Average DAU for January = 10
    WHEN: calculate_stickiness() is called
    THEN:
        - stickiness_ratio = 0.1 (10%)
        - Indicates users active 3 days/month on average
    """
```

#### Test 2.3.3: Single Day Data
```python
def test_calculate_stickiness_single_day():
    """
    GIVEN:
        - Only Jan 1, 2023 has data
        - DAU = 5, MAU = 5
    WHEN: calculate_stickiness() is called
    THEN:
        - stickiness_ratio = 1.0
    """
```

**Acceptance Criteria:**
- ✓ Stickiness ratio between 0.0 and 1.0
- ✓ Correctly calculates average DAU per month
- ✓ Handles partial months correctly
- ✓ Performance: < 10 seconds

---

### Function 2.4: `identify_power_users`

**Purpose:** Identify top 1% users by engagement

**Signature:**
```python
def identify_power_users(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    percentile: float = 0.99,
    max_duration_ms: int = 28800000  # 8 hours
) -> DataFrame:
    """
    Identify power users (top percentile by total engagement).

    Engagement Metric: total_duration_ms (after outlier filtering)

    Args:
        interactions_df: User interactions
        metadata_df: User metadata
        percentile: Threshold percentile (0.99 = top 1%)
        max_duration_ms: Filter out durations above this (outliers)

    Returns:
        DataFrame with schema:
            - user_id (StringType)
            - total_duration_ms (LongType)
            - total_interactions (LongType)
            - unique_pages (LongType)
            - days_active (LongType)
            - hours_spent (DoubleType): total_duration_ms / 3600000
            - avg_duration_per_interaction (DoubleType)
            - percentile_rank (DoubleType): 99.0
            - country, device_type, subscription_type (from metadata)

    Raises:
        ValueError: If percentile not in (0, 1)
    """
```

**Test Cases:**

#### Test 2.4.1: Top 1% Identification
```python
def test_identify_power_users_top_1_percent():
    """
    GIVEN:
        - 100 users with total_duration uniformly distributed from 1000ms to 100,000ms
        - percentile=0.99
    WHEN: identify_power_users() is called
    THEN:
        - Returns 1 user (top 1%)
        - User has highest total_duration_ms
    """
```

#### Test 2.4.2: Outlier Filtering
```python
def test_identify_power_users_filters_outliers():
    """
    GIVEN:
        - u001: 3 interactions: [5000ms, 120000ms, 50000000ms (13.8 hours)]
        - max_duration_ms = 28800000 (8 hours)
    WHEN: identify_power_users() is called
    THEN:
        - Outlier (50000000ms) is excluded
        - u001 total_duration_ms = 5000 + 120000 = 125000ms
    """
```

#### Test 2.4.3: Enrichment with Metadata
```python
def test_identify_power_users_joins_metadata():
    """
    GIVEN:
        - u001 is power user
        - metadata: u001 -> country=US, device_type=iPad, subscription_type=premium
    WHEN: identify_power_users() is called
    THEN:
        - Result includes country=US, device_type=iPad, subscription_type=premium
    """
```

#### Test 2.4.4: Multiple Metrics
```python
def test_identify_power_users_calculates_all_metrics():
    """
    GIVEN: u001 with known interaction pattern
    WHEN: identify_power_users() is called
    THEN:
        - total_interactions is correct
        - unique_pages is correct (distinct page_ids)
        - days_active is correct (distinct dates)
        - hours_spent = total_duration_ms / 3600000
        - avg_duration_per_interaction = total_duration / total_interactions
    """
```

**Acceptance Criteria:**
- ✓ Returns exactly top N% users
- ✓ Filters outliers before percentile calculation
- ✓ Joins with metadata successfully
- ✓ Calculates all metrics correctly
- ✓ Performance: < 3 minutes for 1B interactions

---

### Function 2.5: `calculate_cohort_retention`

**Purpose:** Calculate weekly cohort retention rates

**Signature:**
```python
def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",
    analysis_weeks: int = 26
) -> DataFrame:
    """
    Calculate cohort retention analysis.

    Cohort Definition: Users grouped by join week
    Retention: % of cohort active in week N after joining

    Args:
        interactions_df: User interactions
        metadata_df: User metadata with join_date
        cohort_period: "week" or "month" (default: "week")
        analysis_weeks: Number of weeks to analyze (default: 26 = 6 months)

    Returns:
        DataFrame with schema:
            - cohort_week (DateType): First day of cohort week
            - weeks_since_join (IntegerType): 0, 1, 2, ..., analysis_weeks
            - cohort_size (LongType): Total users in cohort
            - active_users (LongType): Users active in this week
            - retention_rate (DoubleType): (active_users / cohort_size) * 100

    Example:
        cohort_week=2023-01-01, weeks_since_join=0, retention_rate=100.0 (Week 0)
        cohort_week=2023-01-01, weeks_since_join=1, retention_rate=80.0  (Week 1)
        cohort_week=2023-01-01, weeks_since_join=2, retention_rate=65.0  (Week 2)
    """
```

**Test Cases:**

#### Test 2.5.1: Perfect Retention
```python
def test_calculate_cohort_retention_perfect():
    """
    GIVEN:
        - Cohort of 10 users joined on 2023-01-01
        - All 10 users active every week for 4 weeks
    WHEN: calculate_cohort_retention() is called
    THEN:
        - Week 0: retention_rate = 100.0
        - Week 1: retention_rate = 100.0
        - Week 2: retention_rate = 100.0
        - Week 3: retention_rate = 100.0
    """
```

#### Test 2.5.2: Declining Retention
```python
def test_calculate_cohort_retention_declining():
    """
    GIVEN:
        - Cohort of 100 users joined on 2023-01-01
        - Week 0: 100 active
        - Week 1: 80 active
        - Week 2: 60 active
        - Week 3: 50 active
    WHEN: calculate_cohort_retention() is called
    THEN:
        - Week 0: retention_rate = 100.0
        - Week 1: retention_rate = 80.0
        - Week 2: retention_rate = 60.0
        - Week 3: retention_rate = 50.0
    """
```

#### Test 2.5.3: Multiple Cohorts
```python
def test_calculate_cohort_retention_multiple_cohorts():
    """
    GIVEN:
        - Cohort A: 50 users joined 2023-01-01
        - Cohort B: 30 users joined 2023-01-08
    WHEN: calculate_cohort_retention() is called
    THEN:
        - Returns separate retention curves for each cohort
        - Cohort sizes are correct (50 and 30)
    """
```

#### Test 2.5.4: User Not Active in Week N
```python
def test_calculate_cohort_retention_inactive_week():
    """
    GIVEN:
        - User u001 joined 2023-01-01
        - Active in week 0, 1, 3 (but NOT week 2)
    WHEN: calculate_cohort_retention() is called
    THEN:
        - Week 0: u001 counted as active
        - Week 1: u001 counted as active
        - Week 2: u001 NOT counted as active
        - Week 3: u001 counted as active
    """
```

**Acceptance Criteria:**
- ✓ Retention rates between 0.0 and 100.0
- ✓ Week 0 always has 100% retention (by definition)
- ✓ Handles multiple cohorts correctly
- ✓ Users can "return" after inactive periods
- ✓ Performance: < 5 minutes for 1B interactions

---

## Task 3: Performance Metrics Specifications

### Module: `src/transforms/performance_transforms.py`

### Function 3.1: `calculate_percentiles`

**Purpose:** Calculate percentile metrics for performance analysis

**Signature:**
```python
def calculate_percentiles(
    df: DataFrame,
    value_column: str,
    group_by_columns: List[str],
    percentiles: List[float] = [0.50, 0.95, 0.99]
) -> DataFrame:
    """
    Calculate percentile metrics grouped by dimensions.

    Args:
        df: Input DataFrame
        value_column: Column to calculate percentiles on (e.g., duration_ms)
        group_by_columns: Columns to group by (e.g., ["app_version", "date"])
        percentiles: List of percentiles (default: [0.50, 0.95, 0.99])

    Returns:
        DataFrame with schema:
            - {group_by_columns} (original types)
            - p50_{value_column} (DoubleType): 50th percentile
            - p95_{value_column} (DoubleType): 95th percentile
            - p99_{value_column} (DoubleType): 99th percentile
            - count (LongType): Number of records
            - avg_{value_column} (DoubleType): Average
            - stddev_{value_column} (DoubleType): Standard deviation

    Raises:
        ValueError: If value_column or group_by_columns not in df
        ValueError: If percentiles not in (0, 1)
    """
```

**Test Cases:**

#### Test 3.1.1: Basic Percentiles
```python
def test_calculate_percentiles_basic():
    """
    GIVEN:
        - app_version=1.0.0 with durations: [1000, 2000, 3000, 4000, 5000] ms
    WHEN: calculate_percentiles() with percentiles=[0.50, 0.95]
    THEN:
        - p50_duration_ms ≈ 3000 (median)
        - p95_duration_ms ≈ 4900 (95th percentile)
        - count = 5
    """
```

#### Test 3.1.2: Multiple Groups
```python
def test_calculate_percentiles_multiple_groups():
    """
    GIVEN:
        - app_version=1.0.0, date=2023-01-01: durations [1000, 2000, 3000]
        - app_version=1.0.0, date=2023-01-02: durations [4000, 5000, 6000]
        - app_version=2.0.0, date=2023-01-01: durations [500, 1000, 1500]
    WHEN: calculate_percentiles() grouped by ["app_version", "date"]
    THEN:
        - Returns 3 rows (one per group)
        - Each row has correct percentiles for its group
    """
```

#### Test 3.1.3: Approximate Percentile Accuracy
```python
def test_calculate_percentiles_accuracy():
    """
    GIVEN: 10,000 values uniformly distributed from 1 to 10,000
    WHEN: calculate_percentiles() with p50, p95, p99
    THEN:
        - p50 ≈ 5000 (±1% error acceptable)
        - p95 ≈ 9500 (±1% error acceptable)
        - p99 ≈ 9900 (±1% error acceptable)
    """
```

**Acceptance Criteria:**
- ✓ Returns correct percentile values (±1% for approximate)
- ✓ Handles multiple grouping dimensions
- ✓ Includes count, avg, stddev
- ✓ Performance: < 2 minutes for 1B rows with 100 groups

---

### Function 3.2: `calculate_device_correlation`

**Purpose:** Calculate correlation between device type and performance

**Signature:**
```python
def calculate_device_correlation(
    interactions_df: DataFrame,
    metadata_df: DataFrame
) -> DataFrame:
    """
    Calculate device-performance correlation metrics.

    Returns aggregated metrics by device type for comparison.

    Args:
        interactions_df: User interactions with duration_ms
        metadata_df: User metadata with device_type

    Returns:
        DataFrame with schema:
            - device_type (StringType)
            - avg_duration_ms (DoubleType)
            - p95_duration_ms (DoubleType)
            - total_interactions (LongType)
            - unique_users (LongType)
            - interactions_per_user (DoubleType)

        Sorted by avg_duration_ms descending (slowest first)
    """
```

**Test Cases:**

#### Test 3.2.1: Basic Device Comparison
```python
def test_calculate_device_correlation_basic():
    """
    GIVEN:
        - iPad users: avg_duration = 2000ms, 1000 interactions
        - iPhone users: avg_duration = 3000ms, 500 interactions
        - Mac users: avg_duration = 1500ms, 200 interactions
    WHEN: calculate_device_correlation() is called
    THEN:
        - Returns 3 rows (one per device type)
        - Sorted by avg_duration_ms DESC: iPhone (3000), iPad (2000), Mac (1500)
        - All metrics calculated correctly
    """
```

#### Test 3.2.2: Multiple Users Per Device
```python
def test_calculate_device_correlation_multiple_users():
    """
    GIVEN:
        - iPad: u001 (100 interactions), u002 (50 interactions), u003 (50 interactions)
    WHEN: calculate_device_correlation() is called
    THEN:
        - device_type=iPad: unique_users=3, total_interactions=200
        - interactions_per_user = 200 / 3 ≈ 66.67
    """
```

**Acceptance Criteria:**
- ✓ Aggregates by device type correctly
- ✓ Joins interactions with metadata
- ✓ Sorts by avg_duration_ms descending
- ✓ Calculates interactions_per_user correctly
- ✓ Performance: < 3 minutes for 1B interactions

---

### Function 3.3: `detect_anomalies_statistical`

**Purpose:** Detect anomalies using Z-score method

**Signature:**
```python
def detect_anomalies_statistical(
    df: DataFrame,
    value_column: str,
    z_threshold: float = 3.0,
    group_by_columns: List[str] = None
) -> DataFrame:
    """
    Detect statistical anomalies using Z-score method.

    Anomaly: |value - μ| > z_threshold * σ

    Args:
        df: Input DataFrame
        value_column: Column to analyze for anomalies
        z_threshold: Z-score threshold (default: 3.0 = 3 sigma)
        group_by_columns: Optional grouping (e.g., ["user_id"] for per-user baseline)

    Returns:
        DataFrame with only anomalous records, including:
            - Original columns
            - z_score (DoubleType): Calculated Z-score
            - baseline_mean (DoubleType): Mean used for comparison
            - baseline_stddev (DoubleType): Stddev used for comparison
            - anomaly_type (StringType): "high" or "low"
    """
```

**Test Cases:**

#### Test 3.3.1: Simple Anomaly Detection
```python
def test_detect_anomalies_statistical_basic():
    """
    GIVEN:
        - 100 values: 95 values around mean=1000 (stddev=100)
        - 5 outliers: [2000, 2500, 3000, -500, -1000]
        - z_threshold=3.0 (3 sigma = 300)
    WHEN: detect_anomalies_statistical() is called
    THEN:
        - Returns 5 rows (the outliers)
        - z_scores > 3.0 or < -3.0
        - anomaly_type correctly labeled ("high" or "low")
    """
```

#### Test 3.3.2: Grouped Anomaly Detection
```python
def test_detect_anomalies_statistical_grouped():
    """
    GIVEN:
        - User u001: avg=1000ms, stddev=100ms, one value=2000ms (10 sigma!)
        - User u002: avg=5000ms, stddev=500ms, one value=7000ms (4 sigma)
        - group_by_columns=["user_id"]
        - z_threshold=3.0
    WHEN: detect_anomalies_statistical() is called
    THEN:
        - u001 anomaly detected (10 > 3)
        - u002 anomaly detected (4 > 3)
        - Each user's baseline (mean, stddev) is per-user
    """
```

#### Test 3.3.3: No Anomalies
```python
def test_detect_anomalies_statistical_no_anomalies():
    """
    GIVEN: All values within 2 sigma of mean
    WHEN: detect_anomalies_statistical() is called with z_threshold=3.0
    THEN: Returns empty DataFrame with correct schema
    """
```

**Acceptance Criteria:**
- ✓ Correctly identifies anomalies using Z-score
- ✓ Supports global and grouped baselines
- ✓ Labels anomaly type (high/low)
- ✓ Returns empty result if no anomalies
- ✓ Performance: < 3 minutes for 1B rows

---

## Task 4: Session Analysis Specifications

### Module: `src/transforms/session_transforms.py`

### Function 4.1: `sessionize_interactions`

**Purpose:** Sessionize user interactions based on inactivity timeout

**Signature:**
```python
def sessionize_interactions(
    interactions_df: DataFrame,
    session_timeout_seconds: int = 1800  # 30 minutes
) -> DataFrame:
    """
    Create sessions from user interactions based on inactivity timeout.

    Session Boundary: Gap > session_timeout_seconds between consecutive interactions

    Args:
        interactions_df: DataFrame with [user_id, timestamp, ...]
        session_timeout_seconds: Inactivity timeout in seconds (default: 1800 = 30 min)

    Returns:
        DataFrame with original columns plus:
            - session_id (StringType): "{user_id}_{session_number}"
            - is_new_session (IntegerType): 1 if starts new session, else 0
            - time_since_prev_seconds (LongType): Seconds since previous interaction (NULL for first)

    Example:
        u001, 10:00:00 -> session_id="u001_0", is_new_session=1
        u001, 10:05:00 -> session_id="u001_0", is_new_session=0
        u001, 10:40:00 -> session_id="u001_1", is_new_session=1 (>30 min gap)
    """
```

**Test Cases:**

#### Test 4.1.1: Single Session
```python
def test_sessionize_interactions_single_session():
    """
    GIVEN:
        - User u001 with 3 interactions:
          - 10:00:00
          - 10:10:00 (10 min later)
          - 10:25:00 (15 min later)
        - session_timeout_seconds = 1800 (30 min)
    WHEN: sessionize_interactions() is called
    THEN:
        - All 3 interactions have session_id="u001_0"
        - is_new_session: [1, 0, 0]
        - time_since_prev_seconds: [NULL, 600, 900]
    """
```

#### Test 4.1.2: Multiple Sessions
```python
def test_sessionize_interactions_multiple_sessions():
    """
    GIVEN:
        - User u001 with 4 interactions:
          - 10:00:00 (Session 1 start)
          - 10:10:00 (Session 1)
          - 10:45:00 (Session 2 start - 35 min gap)
          - 11:00:00 (Session 2 - 15 min gap)
    WHEN: sessionize_interactions() is called
    THEN:
        - Interactions 1-2: session_id="u001_0"
        - Interactions 3-4: session_id="u001_1"
        - is_new_session: [1, 0, 1, 0]
    """
```

#### Test 4.1.3: Multiple Users
```python
def test_sessionize_interactions_multiple_users():
    """
    GIVEN:
        - User u001: 2 interactions (1 session)
        - User u002: 2 interactions (1 session)
    WHEN: sessionize_interactions() is called
    THEN:
        - u001 sessions independent of u002
        - session_id: ["u001_0", "u001_0", "u002_0", "u002_0"]
    """
```

#### Test 4.1.4: Boundary Condition - Exactly Timeout
```python
def test_sessionize_interactions_exact_timeout():
    """
    GIVEN:
        - Interactions at 10:00:00 and 10:30:00 (exactly 1800 seconds)
        - session_timeout_seconds = 1800
    WHEN: sessionize_interactions() is called
    THEN:
        - Gap = 1800 seconds (exactly at boundary)
        - Should NOT create new session (use > not >=)
        - Both interactions in session_id="u001_0"
    """
```

**Acceptance Criteria:**
- ✓ Creates sessions based on inactivity gaps
- ✓ Sessions are per-user (independent)
- ✓ Handles boundary conditions correctly (> not >=)
- ✓ Preserves chronological order within user
- ✓ Performance: < 5 minutes for 1B interactions

---

### Function 4.2: `calculate_session_metrics`

**Purpose:** Calculate aggregate metrics per session

**Signature:**
```python
def calculate_session_metrics(sessionized_df: DataFrame) -> DataFrame:
    """
    Calculate session-level metrics.

    Args:
        sessionized_df: Output from sessionize_interactions() with session_id

    Returns:
        DataFrame with schema:
            - user_id (StringType)
            - session_id (StringType)
            - session_start (TimestampType): First interaction timestamp
            - session_end (TimestampType): Last interaction timestamp
            - session_duration_seconds (LongType): session_end - session_start
            - actions_per_session (LongType): Count of interactions
            - total_duration_ms (LongType): Sum of duration_ms
            - unique_pages (LongType): Distinct page_ids viewed
            - unique_action_types (LongType): Distinct action types
            - is_bounce (BooleanType): True if actions_per_session == 1
    """
```

**Test Cases:**

#### Test 4.2.1: Single Action Session (Bounce)
```python
def test_calculate_session_metrics_bounce():
    """
    GIVEN: Session with 1 interaction at 10:00:00
    WHEN: calculate_session_metrics() is called
    THEN:
        - session_duration_seconds = 0 (start == end)
        - actions_per_session = 1
        - is_bounce = True
    """
```

#### Test 4.2.2: Multi-Action Session
```python
def test_calculate_session_metrics_multi_action():
    """
    GIVEN:
        - Session u001_0 with 3 interactions:
          - 10:00:00, page_view, p001, 5000ms
          - 10:05:00, edit, p001, 120000ms
          - 10:10:00, page_view, p002, 3000ms
    WHEN: calculate_session_metrics() is called
    THEN:
        - session_start = 10:00:00
        - session_end = 10:10:00
        - session_duration_seconds = 600 (10 minutes)
        - actions_per_session = 3
        - total_duration_ms = 128000
        - unique_pages = 2 (p001, p002)
        - unique_action_types = 2 (page_view, edit)
        - is_bounce = False
    """
```

#### Test 4.2.3: Aggregation Across Multiple Sessions
```python
def test_calculate_session_metrics_multiple_sessions():
    """
    GIVEN:
        - User u001 has 2 sessions
        - Session u001_0: 3 actions
        - Session u001_1: 2 actions
    WHEN: calculate_session_metrics() is called
    THEN:
        - Returns 2 rows (one per session)
        - Each row has correct metrics for that session
    """
```

**Acceptance Criteria:**
- ✓ Correctly calculates session duration
- ✓ Aggregates action counts and unique values
- ✓ Identifies bounce sessions
- ✓ One row per session
- ✓ Performance: < 3 minutes for 100M sessions

---

### Function 4.3: `calculate_bounce_rate`

**Purpose:** Calculate bounce rate by dimensions

**Signature:**
```python
def calculate_bounce_rate(
    session_metrics_df: DataFrame,
    group_by_columns: List[str] = None
) -> DataFrame:
    """
    Calculate bounce rate (% of single-action sessions).

    Bounce Rate = (sessions with 1 action) / (total sessions) * 100

    Args:
        session_metrics_df: Output from calculate_session_metrics()
        group_by_columns: Optional grouping (e.g., ["device_type", "country"])

    Returns:
        DataFrame with schema:
            - {group_by_columns} (if provided)
            - total_sessions (LongType)
            - bounce_sessions (LongType)
            - bounce_rate (DoubleType): Percentage (0.0 to 100.0)
            - avg_actions_per_session (DoubleType)
    """
```

**Test Cases:**

#### Test 4.3.1: 100% Bounce Rate
```python
def test_calculate_bounce_rate_all_bounces():
    """
    GIVEN: 10 sessions, all with actions_per_session=1
    WHEN: calculate_bounce_rate() is called
    THEN:
        - total_sessions = 10
        - bounce_sessions = 10
        - bounce_rate = 100.0
    """
```

#### Test 4.3.2: 0% Bounce Rate
```python
def test_calculate_bounce_rate_no_bounces():
    """
    GIVEN: 10 sessions, all with actions_per_session>1
    WHEN: calculate_bounce_rate() is called
    THEN:
        - total_sessions = 10
        - bounce_sessions = 0
        - bounce_rate = 0.0
    """
```

#### Test 4.3.3: Mixed Sessions
```python
def test_calculate_bounce_rate_mixed():
    """
    GIVEN: 100 sessions, 30 with actions_per_session=1, 70 with actions_per_session>1
    WHEN: calculate_bounce_rate() is called
    THEN:
        - total_sessions = 100
        - bounce_sessions = 30
        - bounce_rate = 30.0
    """
```

#### Test 4.3.4: Grouped Bounce Rate
```python
def test_calculate_bounce_rate_grouped():
    """
    GIVEN:
        - device_type=iPad: 50 sessions, 10 bounces
        - device_type=iPhone: 30 sessions, 15 bounces
    WHEN: calculate_bounce_rate(group_by_columns=["device_type"])
    THEN:
        - iPad: bounce_rate = 20.0
        - iPhone: bounce_rate = 50.0
    """
```

**Acceptance Criteria:**
- ✓ Bounce rate between 0.0 and 100.0
- ✓ Correctly identifies single-action sessions
- ✓ Supports grouping by dimensions
- ✓ Performance: < 1 minute for 100M sessions

---

## Task 5: Data Quality Specifications

### Module: `src/utils/data_quality.py`

### Function 5.1: `validate_schema`

**Purpose:** Validate DataFrame schema matches expected schema

**Signature:**
```python
def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    strict: bool = True
) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema.

    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        strict: If True, column order must match; if False, only names and types

    Returns:
        Tuple of (is_valid: bool, errors: List[str])
        - is_valid: True if schema matches
        - errors: List of error messages (empty if valid)

    Example errors:
        - "Missing column: 'user_id'"
        - "Column 'duration_ms' has type LongType but expected IntegerType"
        - "Extra column: 'unknown_column'"
    """
```

**Test Cases:**

#### Test 5.1.1: Valid Schema
```python
def test_validate_schema_valid():
    """
    GIVEN: DataFrame with exact schema match
    WHEN: validate_schema() is called
    THEN:
        - Returns (True, [])
    """
```

#### Test 5.1.2: Missing Column
```python
def test_validate_schema_missing_column():
    """
    GIVEN: DataFrame missing 'app_version' column
    WHEN: validate_schema() is called
    THEN:
        - Returns (False, ["Missing column: 'app_version'"])
    """
```

#### Test 5.1.3: Wrong Type
```python
def test_validate_schema_wrong_type():
    """
    GIVEN: DataFrame with 'duration_ms' as IntegerType instead of LongType
    WHEN: validate_schema() is called
    THEN:
        - Returns (False, ["Column 'duration_ms' has type IntegerType but expected LongType"])
    """
```

**Acceptance Criteria:**
- ✓ Detects missing columns
- ✓ Detects type mismatches
- ✓ Detects extra columns (if strict=True)
- ✓ Returns clear error messages
- ✓ Performance: < 1 second

---

### Function 5.2: `detect_nulls`

**Purpose:** Detect NULL values in non-nullable columns

**Signature:**
```python
def detect_nulls(
    df: DataFrame,
    non_nullable_columns: List[str]
) -> DataFrame:
    """
    Detect NULL values in specified columns.

    Args:
        df: DataFrame to check
        non_nullable_columns: List of column names that should not have NULLs

    Returns:
        DataFrame with only rows containing NULLs in specified columns
        Adds column: null_columns (ArrayType<StringType>) listing which columns are NULL

    If no NULLs found, returns empty DataFrame with same schema
    """
```

**Test Cases:**

#### Test 5.2.1: No NULLs
```python
def test_detect_nulls_none():
    """
    GIVEN: DataFrame with no NULL values
    WHEN: detect_nulls() is called
    THEN: Returns empty DataFrame
    """
```

#### Test 5.2.2: NULLs Present
```python
def test_detect_nulls_present():
    """
    GIVEN:
        - Row 1: user_id=NULL, timestamp=2023-01-01 (NULL user_id)
        - Row 2: user_id=u001, timestamp=NULL (NULL timestamp)
        - Row 3: user_id=u002, timestamp=2023-01-02 (no NULLs)
    WHEN: detect_nulls(non_nullable_columns=["user_id", "timestamp"])
    THEN:
        - Returns 2 rows (rows 1 and 2)
        - Row 1: null_columns = ["user_id"]
        - Row 2: null_columns = ["timestamp"]
    """
```

**Acceptance Criteria:**
- ✓ Identifies all rows with NULLs
- ✓ Lists which columns are NULL
- ✓ Returns empty result if no NULLs
- ✓ Performance: < 2 minutes for 1B rows

---

### Function 5.3: `detect_outliers`

**Purpose:** Detect outliers using IQR or threshold method

**Signature:**
```python
def detect_outliers(
    df: DataFrame,
    column: str,
    method: str = "iqr",
    iqr_multiplier: float = 1.5,
    threshold_min: float = None,
    threshold_max: float = None
) -> DataFrame:
    """
    Detect outliers in a numeric column.

    Methods:
        - "iqr": Interquartile Range (Q1 - 1.5*IQR, Q3 + 1.5*IQR)
        - "threshold": Fixed min/max thresholds

    Args:
        df: DataFrame to check
        column: Numeric column to analyze
        method: "iqr" or "threshold"
        iqr_multiplier: Multiplier for IQR method (default: 1.5)
        threshold_min: Minimum threshold (for threshold method)
        threshold_max: Maximum threshold (for threshold method)

    Returns:
        DataFrame with only outlier rows, plus columns:
            - outlier_reason (StringType): "below_min", "above_max", or "iqr_outlier"
            - outlier_value (DoubleType): The outlier value
    """
```

**Test Cases:**

#### Test 5.3.1: IQR Method
```python
def test_detect_outliers_iqr():
    """
    GIVEN:
        - 100 values: 1-100
        - Q1=25, Q3=75, IQR=50
        - Lower bound: 25 - 1.5*50 = -50
        - Upper bound: 75 + 1.5*50 = 150
        - Plus outliers: [-100, 200]
    WHEN: detect_outliers(method="iqr") is called
    THEN:
        - Returns 2 rows (outliers: -100, 200)
        - outlier_reason: ["below_min", "above_max"]
    """
```

#### Test 5.3.2: Threshold Method
```python
def test_detect_outliers_threshold():
    """
    GIVEN:
        - Values: [100, 200, ..., 29000000 (8 hours+)]
        - threshold_max = 28800000 (8 hours)
    WHEN: detect_outliers(method="threshold", threshold_max=28800000) is called
    THEN:
        - Returns rows with duration_ms > 28800000
        - outlier_reason = "above_max"
    """
```

**Acceptance Criteria:**
- ✓ IQR method correctly calculates bounds
- ✓ Threshold method uses fixed values
- ✓ Returns outlier reason
- ✓ Performance: < 3 minutes for 1B rows

---

## Testing Strategy

### Test Organization

```
tests/
├── unit/                    # Fast, isolated tests
│   ├── test_join_transforms.py
│   ├── test_engagement_transforms.py
│   ├── test_performance_transforms.py
│   ├── test_session_transforms.py
│   └── test_data_quality.py
├── integration/             # End-to-end tests
│   ├── test_full_pipeline.py
│   └── test_postgres_write.py
└── conftest.py              # Shared fixtures
```

### Unit Test Guidelines

**Characteristics:**
- Fast: < 5 seconds per test
- Isolated: No external dependencies
- Use in-memory Spark DataFrames
- Focus on business logic

**Structure:**
```python
def test_function_name_scenario():
    """
    GIVEN: Test setup and preconditions
    WHEN: Function is called with specific inputs
    THEN: Expected outputs and assertions
    """
    # Arrange
    input_df = create_test_df(...)

    # Act
    result_df = function_under_test(input_df)

    # Assert
    expected_df = create_expected_df(...)
    assertDataFrameEqual(result_df, expected_df)
```

### Integration Test Guidelines

**Characteristics:**
- Slower: < 5 minutes per test
- Tests complete workflows
- May use Docker services (PostgreSQL, etc.)
- Focus on data flow and integration

**Example:**
```python
def test_full_engagement_pipeline():
    """Test complete user engagement pipeline end-to-end."""
    # 1. Load test data
    interactions_df = load_test_interactions()
    metadata_df = load_test_metadata()

    # 2. Run pipeline
    dau_df = calculate_dau(interactions_df)
    mau_df = calculate_mau(interactions_df)
    power_users_df = identify_power_users(interactions_df, metadata_df)

    # 3. Validate results
    assert dau_df.count() > 0
    assert mau_df.count() > 0
    assert power_users_df.count() == expected_power_user_count

    # 4. Test database write
    write_to_postgres(dau_df, "daily_active_users")
    verify_postgres_data("daily_active_users")
```

### Test Fixtures (conftest.py)

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("goodnote-tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

@pytest.fixture
def sample_interactions(spark):
    """Create sample interactions DataFrame."""
    return create_interactions_df(
        spark,
        num_users=100,
        num_interactions=1000
    )

@pytest.fixture
def sample_metadata(spark):
    """Create sample metadata DataFrame."""
    return create_metadata_df(
        spark,
        num_users=100
    )

@pytest.fixture
def skewed_interactions(spark):
    """Create skewed interactions for testing salting."""
    return create_skewed_interactions(
        spark,
        power_user_id="u001",
        power_user_interactions=10000,
        normal_users=99,
        normal_interactions=100
    )
```

### Coverage Requirements

- **Unit Tests:** >80% code coverage
- **Integration Tests:** All major workflows
- **Edge Cases:** All documented edge cases tested

### Test Execution

```bash
# Run all unit tests
pytest tests/unit -v

# Run specific test file
pytest tests/unit/test_engagement_transforms.py -v

# Run with coverage
pytest tests/unit --cov=src --cov-report=html

# Run integration tests (requires Docker)
docker-compose up -d
pytest tests/integration -v
```

---

## TDD Development Workflow

### Step-by-Step Process

1. **Read Specification** - Review function spec from this document
2. **Write Test Cases** - Implement all test cases for the function
3. **Run Tests** - Verify all tests fail (RED)
4. **Implement Function** - Write minimum code to pass tests
5. **Run Tests** - Verify all tests pass (GREEN)
6. **Refactor** - Improve code while keeping tests green
7. **Integration** - Test with other components
8. **Commit** - Commit tests and implementation together

### Example TDD Session

```bash
# 1. Create test file
touch tests/unit/test_engagement_transforms.py

# 2. Write test for calculate_dau (should fail)
# ... implement test_calculate_dau_basic ...

# 3. Run test (should see failure)
pytest tests/unit/test_engagement_transforms.py::test_calculate_dau_basic -v
# ❌ FAIL: Function not implemented

# 4. Implement calculate_dau
# ... implement function ...

# 5. Run test again
pytest tests/unit/test_engagement_transforms.py::test_calculate_dau_basic -v
# ✅ PASS

# 6. Write next test
# ... implement test_calculate_dau_empty ...

# 7. Run tests
pytest tests/unit/test_engagement_transforms.py -v
# ❌ FAIL: Empty input not handled

# 8. Fix implementation
# ... add empty input handling ...

# 9. Run all tests
pytest tests/unit/test_engagement_transforms.py -v
# ✅ ALL PASS

# 10. Commit
git add tests/unit/test_engagement_transforms.py src/transforms/engagement_transforms.py
git commit -m "Add calculate_dau with TDD"
```

---

## Success Criteria

### Per Function
- ✓ All specified test cases implemented
- ✓ All tests pass
- ✓ Edge cases handled
- ✓ Error conditions raise appropriate exceptions
- ✓ Performance meets specified targets

### Per Module
- ✓ >80% code coverage
- ✓ All functions have docstrings
- ✓ Integration tests pass
- ✓ No pylint warnings

### Overall Project
- ✓ All 6 tasks implemented with TDD
- ✓ Full pipeline runs end-to-end
- ✓ Data written to PostgreSQL
- ✓ Superset dashboards display correct data
- ✓ Documentation complete

---

## Appendix: Quick Reference

### Common Test Patterns

```python
# Pattern 1: DataFrame Equality
from chispa import assert_df_equality
assert_df_equality(result_df, expected_df, ignore_row_order=True)

# Pattern 2: Column Value Assertions
assert result_df.filter(col("dau") < 0).count() == 0

# Pattern 3: Exception Testing
with pytest.raises(ValueError, match="Column .* not found"):
    function_with_error(df)

# Pattern 4: Approximate Equality
assert abs(result - expected) < 0.01  # Within 1%

# Pattern 5: Schema Validation
assert set(result_df.columns) == {"date", "dau", "mau"}
```

### Performance Testing

```python
import time

def test_calculate_dau_performance():
    """Test that calculate_dau completes within 2 minutes for 1B rows."""
    large_df = create_interactions_df(spark, num_interactions=1_000_000_000)

    start_time = time.time()
    result_df = calculate_dau(large_df)
    result_df.count()  # Trigger computation
    elapsed_time = time.time() - start_time

    assert elapsed_time < 120  # 2 minutes
```

---

**Document Version:** 1.0
**Last Updated:** 2025-11-13
**Status:** Ready for Implementation
