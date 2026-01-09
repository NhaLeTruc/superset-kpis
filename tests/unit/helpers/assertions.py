"""
Custom Test Assertions

Provides reusable assertion helpers for common test patterns.
"""
from typing import List, Optional
from pyspark.sql import DataFrame


def assert_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> None:
    """
    Assert DataFrame has expected columns.

    Args:
        df: DataFrame to check
        expected_columns: List of expected column names

    Raises:
        AssertionError: If columns don't match
    """
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)

    missing = expected_columns_set - actual_columns
    extra = actual_columns - expected_columns_set

    if missing or extra:
        error_msg = []
        if missing:
            error_msg.append(f"Missing columns: {sorted(missing)}")
        if extra:
            error_msg.append(f"Extra columns: {sorted(extra)}")
        raise AssertionError("; ".join(error_msg))


def assert_column_exists(df: DataFrame, column_name: str) -> None:
    """
    Assert a specific column exists in DataFrame.

    Args:
        df: DataFrame to check
        column_name: Expected column name

    Raises:
        AssertionError: If column doesn't exist
    """
    if column_name not in df.columns:
        raise AssertionError(
            f"Column '{column_name}' not found. Available: {df.columns}"
        )


def assert_no_nulls(df: DataFrame, columns: List[str]) -> None:
    """
    Assert specified columns have no NULL values.

    Args:
        df: DataFrame to check
        columns: List of column names to check

    Raises:
        AssertionError: If any column has NULLs
    """
    from pyspark.sql import functions as F

    for col in columns:
        assert_column_exists(df, col)

        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise AssertionError(
                f"Column '{col}' has {null_count} NULL values"
            )


def assert_percentile_accuracy(
    actual: float,
    expected: float,
    tolerance_pct: float = 5.0
) -> None:
    """
    Assert percentile calculation is within tolerance.

    Percentiles use approximation, so we allow some variance.

    Args:
        actual: Actual percentile value
        expected: Expected percentile value
        tolerance_pct: Allowed percentage difference (default: 5%)

    Raises:
        AssertionError: If difference exceeds tolerance
    """
    if expected == 0:
        if actual != 0:
            raise AssertionError(
                f"Expected 0 but got {actual}"
            )
        return

    diff_pct = abs((actual - expected) / expected) * 100

    if diff_pct > tolerance_pct:
        raise AssertionError(
            f"Percentile {actual} differs from expected {expected} "
            f"by {diff_pct:.2f}% (tolerance: {tolerance_pct}%)"
        )


def assert_retention_curve(
    results: DataFrame,
    week_number: int,
    expected_rate_min: float,
    expected_rate_max: float,
    cohort_week: Optional[str] = None
) -> None:
    """
    Assert retention rate is within expected range for a given week.

    Args:
        results: DataFrame with cohort retention results
        week_number: Week to check
        expected_rate_min: Minimum expected retention rate (0-100)
        expected_rate_max: Maximum expected retention rate (0-100)
        cohort_week: Optional specific cohort to check

    Raises:
        AssertionError: If retention rate is out of range
    """
    from pyspark.sql import functions as F

    # Filter to specific week
    filtered = results.filter(F.col("week_number") == week_number)

    # Further filter by cohort if specified
    if cohort_week:
        from datetime import datetime
        cohort_date = datetime.strptime(cohort_week, "%Y-%m-%d").date()
        filtered = filtered.filter(F.col("cohort_week") == cohort_date)

    if filtered.count() == 0:
        raise AssertionError(
            f"No retention data found for week {week_number}"
        )

    # Get retention rate
    retention_row = filtered.select("retention_rate").first()
    if not retention_row:
        raise AssertionError(
            f"No retention rate found for week {week_number}"
        )

    actual_rate = retention_row["retention_rate"]

    if not (expected_rate_min <= actual_rate <= expected_rate_max):
        raise AssertionError(
            f"Retention rate {actual_rate:.2f}% for week {week_number} "
            f"is outside expected range [{expected_rate_min}, {expected_rate_max}]"
        )


def assert_value_in_range(
    actual: float,
    min_val: float,
    max_val: float,
    label: str = "Value"
) -> None:
    """
    Assert value is within expected range.

    Args:
        actual: Actual value
        min_val: Minimum expected value
        max_val: Maximum expected value
        label: Label for error message

    Raises:
        AssertionError: If value is out of range
    """
    if not (min_val <= actual <= max_val):
        raise AssertionError(
            f"{label} {actual} is outside expected range [{min_val}, {max_val}]"
        )


def assert_approximately_equal(
    actual: float,
    expected: float,
    tolerance: float = 0.01,
    label: str = "Value"
) -> None:
    """
    Assert two float values are approximately equal.

    Args:
        actual: Actual value
        expected: Expected value
        tolerance: Absolute tolerance
        label: Label for error message

    Raises:
        AssertionError: If difference exceeds tolerance
    """
    diff = abs(actual - expected)
    if diff > tolerance:
        raise AssertionError(
            f"{label}: expected {expected}, got {actual} (diff: {diff}, tolerance: {tolerance})"
        )


def assert_row_count(df: DataFrame, expected: int) -> None:
    """
    Assert DataFrame has expected number of rows.

    Args:
        df: DataFrame to check
        expected: Expected row count

    Raises:
        AssertionError: If row count doesn't match
    """
    actual = df.count()
    if actual != expected:
        raise AssertionError(
            f"Expected {expected} rows, got {actual}"
        )


def assert_distinct_count(
    df: DataFrame,
    column: str,
    expected: int
) -> None:
    """
    Assert distinct count for a column matches expected.

    Args:
        df: DataFrame to check
        column: Column name
        expected: Expected distinct count

    Raises:
        AssertionError: If distinct count doesn't match
    """
    assert_column_exists(df, column)

    actual = df.select(column).distinct().count()
    if actual != expected:
        raise AssertionError(
            f"Expected {expected} distinct values in '{column}', got {actual}"
        )
