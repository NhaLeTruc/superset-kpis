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
    # Validate salt_factor
    if salt_factor < 2:
        raise ValueError("salt_factor must be >= 2")

    # Validate key_column exists in df
    if key_column not in df.columns:
        raise ValueError(f"Column '{key_column}' not found in DataFrame")

    # Left join with hot_keys to identify which keys are hot
    # Add a marker column to identify hot keys
    hot_keys_marked = hot_keys_df.select(key_column).withColumn("is_hot_key", F.lit(True))

    # Join with original dataframe
    df_with_marker = df.join(hot_keys_marked, on=key_column, how="left")

    # For hot keys: random salt 0 to (salt_factor-1)
    # For non-hot keys: salt = 0
    df_with_salt = df_with_marker.withColumn(
        "salt",
        F.when(
            F.col("is_hot_key").isNotNull(),
            (F.rand() * salt_factor).cast("int")
        ).otherwise(F.lit(0))
    )

    # Create {key_column}_salted column
    salted_column_name = f"{key_column}_salted"
    df_with_salted = df_with_salt.withColumn(
        salted_column_name,
        F.concat(F.col(key_column), F.lit("_"), F.col("salt").cast("string"))
    )

    # Remove the marker column
    result_df = df_with_salted.drop("is_hot_key")

    return result_df
