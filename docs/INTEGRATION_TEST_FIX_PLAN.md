# Integration Test Fix Implementation Plan

## Current Status
- **Tests Passing:** 26/42 (61.9%)
- **Tests Failing:** 16/42 (38.1%)
- **Goal:** Fix all 16 remaining failures

---

## Root Cause Analysis

### Category 1: Column Name Mismatches (7 tests)
**Issue:** Implementation uses different column names than tests expect

#### 1.1 Session Metrics Column Names
**Files:** `src/transforms/session_transforms.py`
**Affected Tests:**
- `test_session_pipeline_complete`
- `test_session_metrics_accuracy`
- `test_bounce_rate_calculation`
- `test_grouped_bounce_rate`
- `test_session_performance`

**Problem:**
- Implementation returns: `actions_count`, `session_duration_ms`
- Tests expect: `action_count`, `session_duration_seconds`

**Fix:**
```python
# In calculate_session_metrics():
# Line ~148: Change "actions_count" → "action_count"
# Line ~157-165: Add conversion from milliseconds to seconds:
.withColumn("session_duration_seconds", F.col("session_duration_ms") / 1000)
```

#### 1.2 Performance Metrics Column Names
**Files:** `src/transforms/performance_transforms.py`
**Affected Tests:**
- `test_performance_pipeline_complete`
- `test_percentile_calculation_accuracy`

**Problem:**
- Implementation returns: `p50_duration_ms`, `p95_duration_ms`, `p99_duration_ms`
- Tests expect: `p50`, `p95`, `p99`

**Fix:**
```python
# In calculate_percentiles():
# Line ~78: Change percentile naming
# From: percentile_name = f"p{int(p * 100)}_{value_column}"
# To:   percentile_name = f"p{int(p * 100)}"
```

---

### Category 2: Function Parameter Mismatches (2 tests)

#### 2.1 Cohort Retention Parameter Name
**Files:** `src/transforms/engagement_transforms.py`
**Affected Tests:**
- `test_cohort_retention_weekly`
- `test_empty_cohort_handling`

**Problem:**
- Function signature: `analysis_weeks`
- Test calls with: `retention_weeks`

**Fix:**
```python
# In calculate_cohort_retention():
# Line ~174: Add parameter alias or rename
def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",
    retention_weeks: int = 26,  # Changed from analysis_weeks
    analysis_weeks: int = None   # Keep for backwards compat
) -> DataFrame:
    # Use retention_weeks if provided, else analysis_weeks
    weeks = retention_weeks if analysis_weeks is None else analysis_weeks
```

---

### Category 3: Missing/Wrong Columns in Data (2 tests)

#### 3.1 Power User - Missing page_id Column
**Files:** `src/transforms/engagement_transforms.py`
**Affected Tests:**
- `test_power_user_identification`

**Problem:**
- Function tries to count distinct `page_id` column
- Test data doesn't have `page_id` column

**Fix:**
```python
# In identify_power_users():
# Line ~141: Make page_id optional
# Check if page_id exists before counting
if "page_id" in filtered_df.columns:
    unique_pages_expr = F.countDistinct("page_id").alias("unique_pages")
else:
    unique_pages_expr = F.lit(None).cast("long").alias("unique_pages")

user_metrics = filtered_df.groupBy("user_id").agg(
    F.sum("duration_ms").alias("total_duration_ms"),
    F.count("*").alias("total_interactions"),
    unique_pages_expr,
    F.countDistinct("date").alias("days_active")
)
```

---

### Category 4: Stickiness Calculation Error (1 test)

#### 4.1 Unresolved Column 'date' in Stickiness
**Files:** `src/transforms/engagement_transforms.py`
**Affected Tests:**
- `test_stickiness_ratio_calculation`

**Problem:**
- Test tries to order by 'date' column which doesn't exist in stickiness output
- Stickiness function returns columns with 'month' not 'date'

**Fix:**
```python
# This is a TEST issue, not implementation issue
# In test file: tests/integration/test_user_engagement_job.py
# Change: stickiness_results = stickiness_df.orderBy("date").collect()
# To:     stickiness_results = stickiness_df.orderBy("month").collect()
```

---

### Category 5: End-to-End Pipeline Issues (2 tests)

#### 5.1 Complete Pipeline Execution
**Affected Tests:**
- `test_complete_pipeline_execution`
- `test_pipeline_error_handling`

**Problem:** Likely cascading failures from column name issues above

**Fix:** Should be resolved after fixing Categories 1-4

---

### Category 6: Engagement Performance Test (1 test)

#### 6.1 Engagement Performance Timeout
**Affected Tests:**
- `test_engagement_performance`

**Problem:** Unknown - need to run test to see specific error

**Investigation Needed:** Run isolated test to identify issue

---

### Category 7: Skewed Data Test (1 test)

#### 7.1 Skewed Data Join Test
**Affected Tests:**
- `test_data_processing_with_skewed_data`

**Problem:** Unknown - output was truncated

**Investigation Needed:** Run isolated test to see full error

---

## Implementation Priority Order

### Phase 1: Quick Wins - Column Renaming (Expected: +7 tests)
1. Fix session metrics column names (5 tests)
2. Fix performance metrics column names (2 tests)

### Phase 2: Parameter Fixes (Expected: +3 tests)
3. Fix cohort retention parameter (2 tests)
4. Fix stickiness test ordering (1 test)

### Phase 3: Data Column Fixes (Expected: +1 test)
5. Fix power user page_id issue (1 test)

### Phase 4: Investigate Remaining (Expected: +5 tests)
6. Debug and fix end-to-end pipeline tests (2 tests)
7. Debug engagement performance test (1 test)
8. Debug skewed data test (1 test)

---

## Detailed Implementation Steps

### Step 1: Session Transforms Column Fix
**File:** `src/transforms/session_transforms.py`
```python
# Around line 147-165
session_metrics = sessionized_df.groupBy("user_id", "session_id").agg(
    F.count("*").alias("action_count"),  # Changed from actions_count
    # ... rest of aggregations
)

# After calculating session_duration_ms, add:
session_metrics = session_metrics.withColumn(
    "session_duration_seconds",
    (F.col("session_duration_ms") / 1000.0).cast("double")
)
```

### Step 2: Performance Transforms Column Fix
**File:** `src/transforms/performance_transforms.py`
```python
# Around line 78
for i, p in enumerate(percentiles):
    percentile_name = f"p{int(p * 100)}"  # Removed _{value_column}
    row_dict[percentile_name] = float(percentile_values[i])
```

### Step 3: Cohort Retention Parameter Fix
**File:** `src/transforms/engagement_transforms.py`
```python
# Line 170-174
def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",
    retention_weeks: int = 26  # Changed from analysis_weeks
) -> DataFrame:
```
Update all references to `analysis_weeks` → `retention_weeks` in function body

### Step 4: Power User page_id Fix
**File:** `src/transforms/engagement_transforms.py`
```python
# Line 137-143
# Add conditional page_id handling
if "page_id" in filtered_df.columns:
    unique_pages_expr = F.countDistinct("page_id").alias("unique_pages")
else:
    unique_pages_expr = F.lit(None).cast("long").alias("unique_pages")

user_metrics = filtered_df.groupBy("user_id").agg(
    F.sum("duration_ms").alias("total_duration_ms"),
    F.count("*").alias("total_interactions"),
    unique_pages_expr,
    F.countDistinct("date").alias("days_active")
)
```

### Step 5: Stickiness Test Fix
**File:** `tests/integration/test_user_engagement_job.py`
```python
# Find line with: stickiness_df.orderBy("date")
# Change to: stickiness_df.orderBy("month")
```

---

## Testing Strategy

### After Each Phase:
```bash
make test-integration
```

### Verify Specific Category:
```bash
# Session tests
docker exec goodnote-spark-dev pytest tests/integration/test_session_analysis_job.py -v

# Performance tests
docker exec goodnote-spark-dev pytest tests/integration/test_performance_metrics_job.py -v

# Engagement tests
docker exec goodnote-spark-dev pytest tests/integration/test_user_engagement_job.py -v
```

---

## Expected Outcomes

| Phase | Tests Fixed | Cumulative Pass | Pass Rate |
|-------|-------------|-----------------|-----------|
| Start | 0 | 26/42 | 61.9% |
| Phase 1 | +7 | 33/42 | 78.6% |
| Phase 2 | +3 | 36/42 | 85.7% |
| Phase 3 | +1 | 37/42 | 88.1% |
| Phase 4 | +5 | 42/42 | 100% |

---

## Risk Assessment

### Low Risk (Phases 1-3)
- Simple column renames
- Parameter name changes
- Well-understood fixes

### Medium Risk (Phase 4)
- Requires investigation
- May uncover additional issues
- Could need deeper changes

---

## Rollback Plan

Each fix is isolated and can be reverted independently:
```bash
git diff src/transforms/session_transforms.py
git checkout src/transforms/session_transforms.py  # If needed
```

---

## Verification Checklist

- [ ] Phase 1: All session/performance column name fixes applied
- [ ] Phase 2: All parameter name fixes applied
- [ ] Phase 3: page_id conditional logic added
- [ ] Phase 4: All remaining tests investigated
- [ ] Final: All 42 tests passing
- [ ] Code review completed
- [ ] Changes committed with proper message
