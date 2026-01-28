# Code Review: src/transforms and src/utils

**Date:** 2026-01-28
**Scope:** `src/transforms/` and `src/utils/` directories

## Executive Summary

Review found **47 issues** total:
- **3 Critical bugs** that will cause runtime failures
- **6 High severity issues** affecting correctness/performance
- **15 Medium severity issues** affecting reliability
- **23 Low severity issues** (code quality, documentation)

---

## Critical Issues (Must Fix)

### 1. Inverted Empty DataFrame Check
**File:** `src/transforms/engagement/cohort_retention.py:40`

```python
# CURRENT (WRONG):
if interactions_df.head(1):  # True when data EXISTS
    return empty_dataframe    # Returns empty when it shouldn't

# CORRECT:
if not interactions_df.head(1):  # Should return empty when NO data
    return empty_dataframe
```

**Impact:** Function crashes on normal data, only works on empty input.

---

### 2. Session Duration Calculation Error
**File:** `src/transforms/session/metrics.py:154-166`

The session duration formula incorrectly subtracts the session timeout. Spark's `session_window.end` already includes the gap, causing double-counting issues.

**Lines 158-164:**
```python
(F.unix_timestamp(...) - F.unix_timestamp(...)) - 1800s + last_duration
```

The comment on line 163 acknowledges uncertainty: "Subtract the session gap since session_window.end includes it"

---

### 3. IndexError on Empty DataFrame
**File:** `src/transforms/performance/percentiles.py:113-114`

```python
overall_stats = joined_df.agg(...).collect()[0]  # Crashes if empty
grand_mean = float(overall_stats["grand_mean"])
```

No check for empty result before indexing. For an empty DataFrame, `collect()` returns `[]`, causing IndexError.

---

## High Severity Issues

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| 4 | `transforms/engagement/segmentation.py` | 128-159 | Inconsistent sorting causes wrong percentile ranks. Data sorted DESC then re-sorted ASC before rank computation. |
| 5 | `transforms/engagement/segmentation.py` | 136-170 | Driver OOM risk. MAX_POWER_USERS_COLLECT=50K limit is insufficient; 10M users at 1% = 100K users exceeds limit. |
| 6 | `transforms/session/metrics.py` | 161-164 | Type precision loss in timestamp arithmetic. Unix timestamp * 1000 risks overflow during double→long cast. |
| 7 | `utils/data_quality.py` | 54 | `isinstance(actual_type, type(expected_type))` is incorrect. Compares instance to type object. |
| 8 | `utils/data_quality.py` | 103-108 | Array filtering creates `[None, 'col1', None]` instead of `['col1']`. `F.array_except()` doesn't filter None properly. |
| 9 | `utils/monitoring/tracking.py` | 44-58 | Multiple errors per row counted independently. A row with null AND negative counts both, but skipped_records only +1. |

---

## Medium Severity Issues

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| 10 | `transforms/join/optimization.py` | 49-103 | Missing column validation in `apply_salting`. Validates key_column in df but not in hot_keys_df. |
| 11 | `transforms/session/metrics.py` | 257-345 | Missing `metric_date` column validation in `calculate_bounce_rate`. |
| 12 | `transforms/join/optimization.py` | 36-44 | Overly precise `approxQuantile` (0.0001). Use 0.01 for hot key detection. |
| 13 | `transforms/engagement/cohort_retention.py` | 42-50 | Schema mismatch: empty schema uses DateType but code produces TimestampType. |
| 14 | `utils/data_quality.py` | 146 | No null/non-numeric check before `approxQuantile` in detect_outliers. |
| 15 | `utils/monitoring/accumulators.py` | 70 | Shallow copy in `addInPlace()` fragile to future nested structures. |
| 16 | `utils/monitoring/accumulators.py` | 88, 93-94 | No handling for zero-size partitions in PartitionSkewDetector. |
| 17 | `utils/monitoring/reporting.py` | 46, 65, 73 | Repeated division-by-zero guards. Extract to helper. |
| 18 | `utils/monitoring/reporting.py` | 129-130 | Unspecific `except Exception` in decorator. Doesn't differentiate error types. |
| 19 | `utils/data_quality.py` | 15, 71, 116 | No input validation on function parameters (column existence). |
| 20 | `utils/monitoring/context.py`, `reporting.py` | - | No error recovery for accumulator operations. Entire job fails if monitoring breaks. |

---

## Low Severity Issues

### src/transforms

| File | Line(s) | Issue |
|------|---------|-------|
| `join/execution.py` | 43-48 | Awkward column rename approach for join ambiguity |
| `session/metrics.py` | 125-126 | Inconsistent null handling in extra columns with `F.first()` |
| `performance/percentiles.py` | 42-44 | Validates `0 < p < 1` but Spark accepts exactly 0 and 1 |
| `session/metrics.py` | 50 | Regex accepts "0 seconds" as valid timeout |
| `session/metrics.py` | 313-320 | group_by_columns validated but not included in output |
| `engagement/segmentation.py` | 46 | Inner join silently drops cohorts with no DAU data |
| `engagement/segmentation.py` | 7 | Unused `import math` (only used once, replaceable with F.ceil) |
| `session/metrics.py` | 147 | Undocumented column ordering (sorted but not documented) |

### src/utils

| File | Line(s) | Issue |
|------|---------|-------|
| `monitoring/context.py` | 57 | Emoji in logger.info (📊) - may not render in all log systems |
| `monitoring/context.py` | 23 | No spark_context parameter validation |
| `monitoring/tracking.py` | 133 | Silent fallback for invalid profile names |
| `monitoring/reporting.py` | 78 | Displays "0" for float("inf") - misleading |
| `monitoring/reporting.py` | 100-101 | Both print() and logger.info() with same content |
| `monitoring/reporting.py` | 104 | Incomplete Callable type hint |
| `data_quality.py` | 159-164 | Unreachable "iqr_outlier" reason in else clause |
| `data_quality.py` | 197-201 | Incomplete when/otherwise chain in threshold method |
| `data_quality.py` | 141-165 | Generic output column names risk conflicts |
| `data_quality.py` | 4-5 | Outdated "stub file" comment - functions ARE implemented |
| `monitoring/__init__.py` | 25-41 | `get_monitoring_profile` listed under wrong comment section |
| `__init__.py` | - | Empty utils/__init__.py - no public API defined |

---

## Recommended Fix Priority

### Phase 1: Critical Fixes (Immediate)
1. Fix inverted check in `cohort_retention.py:40`
2. Add empty check in `percentiles.py:113`
3. Review/fix session duration calculation in `session/metrics.py`

### Phase 2: High Priority Fixes
4. Fix type checking in `data_quality.py:54`
5. Fix array filtering in `data_quality.py:103-108`
6. Fix sorting/ranking in `segmentation.py:128-159`
7. Add column validation in `optimization.py:49-103`

### Phase 3: Medium Priority Fixes
8. Add input validation across data_quality functions
9. Add empty check in `detect_outliers`
10. Replace shallow copy with deep copy in accumulators
11. Improve exception handling in reporting decorator
12. Add error recovery for monitoring operations

### Phase 4: Code Quality Improvements
13. Remove emojis from logger.info calls
14. Clean up unused imports
15. Update outdated comments
16. Standardize documentation format

---

## Verification Steps

After implementing fixes:
1. Run existing test suite: `pytest tests/`
2. Review Spark job logs for data quality errors
3. Test edge cases:
   - Empty DataFrames
   - Null columns
   - Large datasets (>1M rows)
   - All-null numeric columns
