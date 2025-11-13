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

    # Filter to only keys at or above the threshold (>= to include threshold value)
    hot_keys = key_counts.filter(F.col("count") >= threshold_value)

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
    # Mark hot keys by left joining
    hot_keys_marked = hot_keys_df.select(key_column).withColumn("is_hot_key", F.lit(True))
    df_with_marker = df.join(hot_keys_marked, on=key_column, how="left")

    # Create salt values array for explosion
    # For hot keys: array(0, 1, 2, ..., salt_factor-1)
    # For non-hot keys: array(0)
    df_with_salt_array = df_with_marker.withColumn(
        "salt_array",
        F.when(
            F.col("is_hot_key").isNotNull(),
            F.array(*[F.lit(i) for i in range(salt_factor)])
        ).otherwise(F.array(F.lit(0)))
    )

    # Explode the salt array to create multiple rows
    df_exploded = df_with_salt_array.withColumn("salt", F.explode(F.col("salt_array")))

    # Create {key_column}_salted column
    salted_column_name = f"{key_column}_salted"
    df_with_salted = df_exploded.withColumn(
        salted_column_name,
        F.concat(F.col(key_column), F.lit("_"), F.col("salt").cast("string"))
    )

    # Remove temporary columns
    result_df = df_with_salted.drop("is_hot_key", "salt_array")

    return result_df


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
    # Validate join_key exists in both DataFrames
    if join_key not in large_df.columns:
        raise ValueError(f"Column '{join_key}' not found in large DataFrame")
    if join_key not in small_df.columns:
        raise ValueError(f"Column '{join_key}' not found in small DataFrame")

    # Strategy 1: Try broadcast join if enabled
    if enable_broadcast:
        # Simply use broadcast hint - Spark will use it if small_df is small enough
        result_df = large_df.join(F.broadcast(small_df), on=join_key, how=join_type)
        return result_df

    # Strategy 2: Check for skew and apply salting if needed
    if enable_salting:
        # Detect hot keys in large_df
        hot_keys_df = identify_hot_keys(large_df, key_column=join_key, threshold_percentile=skew_threshold)

        # If hot keys found, apply salting
        if hot_keys_df.count() > 0:
            # Apply salting to large_df
            large_salted = apply_salting(large_df, hot_keys_df, key_column=join_key, salt_factor=salt_factor)

            # Explode small_df to match salted keys
            small_exploded = explode_for_salting(small_df, hot_keys_df, key_column=join_key, salt_factor=salt_factor)

            # Join on salted keys
            salted_key = f"{join_key}_salted"
            result_df = large_salted.join(small_exploded, on=salted_key, how=join_type)

            # Clean up salt columns
            result_df = result_df.drop("salt", salted_key)

            return result_df

    # Strategy 3: Standard join (no broadcast, no skew detected)
    result_df = large_df.join(small_df, on=join_key, how=join_type)

    return result_df
