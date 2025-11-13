"""
Join optimization transforms for handling data skew and improving join performance.

Following TDD: Tests written first, implementation comes after RED state.
Reference: docs/TDD_SPEC.md - Task 1 (Join Optimization Specifications)
"""
from typing import Tuple, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    # Validate that key_column exists
    if key_column not in df.columns:
        raise ValueError(f"Column '{key_column}' not found in DataFrame")

    # Group by key_column and count occurrences
    key_counts = df.groupBy(key_column).count()

    # Calculate the threshold value at the given percentile
    # Using approxQuantile with relative error 0.01
    threshold_value = key_counts.approxQuantile("count", [threshold_percentile], 0.01)[0]

    # Filter to only keys above the threshold
    hot_keys = key_counts.filter(F.col("count") > threshold_value)

    return hot_keys
