"""
Join Optimization - Hot Key Detection and Salting

Functions for identifying data skew and applying salting techniques
to mitigate hot key problems in distributed joins.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import HOT_KEY_THRESHOLD_PERCENTILE


def identify_hot_keys(
    df: DataFrame, key_column: str, threshold_percentile: float = HOT_KEY_THRESHOLD_PERCENTILE
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
    # Using approxQuantile with 0.001 relative error for accuracy in edge cases
    threshold_value = key_counts.approxQuantile("count", [threshold_percentile], 0.001)[0]

    # Filter to only keys strictly above the threshold (> to exclude uniform distributions)
    hot_keys = key_counts.filter(F.col("count") > threshold_value)

    return hot_keys


def apply_salting(
    df: DataFrame, hot_keys_df: DataFrame, key_column: str, salt_factor: int = 10
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

    # Validate key_column exists in hot_keys_df
    if key_column not in hot_keys_df.columns:
        raise ValueError(f"Column '{key_column}' not found in hot_keys DataFrame")

    # Left join with hot_keys to identify which keys are hot
    # Add a marker column to identify hot keys
    hot_keys_marked = hot_keys_df.select(key_column).withColumn("is_hot_key", F.lit(True))

    # Join with original dataframe
    df_with_marker = df.join(hot_keys_marked, on=key_column, how="left")

    # For hot keys: random salt 0 to (salt_factor-1)
    # For non-hot keys: salt = 0
    df_with_salt = df_with_marker.withColumn(
        "salt",
        F.when(F.col("is_hot_key").isNotNull(), (F.rand() * salt_factor).cast("int")).otherwise(
            F.lit(0)
        ),
    )

    # Create {key_column}_salted column
    salted_column_name = f"{key_column}_salted"
    df_with_salted = df_with_salt.withColumn(
        salted_column_name, F.concat(F.col(key_column), F.lit("_"), F.col("salt").cast("string"))
    )

    # Remove the marker column
    result_df = df_with_salted.drop("is_hot_key")

    return result_df


def explode_for_salting(
    df: DataFrame, hot_keys_df: DataFrame, key_column: str, salt_factor: int = 10
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

    Raises:
        ValueError: If key_column not in df or hot_keys_df
    """
    # Validate key_column exists in both DataFrames
    if key_column not in df.columns:
        raise ValueError(f"Column '{key_column}' not found in DataFrame")
    if key_column not in hot_keys_df.columns:
        raise ValueError(f"Column '{key_column}' not found in hot_keys DataFrame")

    # Mark hot keys by left joining
    hot_keys_marked = hot_keys_df.select(key_column).withColumn("is_hot_key", F.lit(True))
    df_with_marker = df.join(hot_keys_marked, on=key_column, how="left")

    # Create salt values array for explosion
    # For hot keys: array(0, 1, 2, ..., salt_factor-1)
    # For non-hot keys: array(0)
    df_with_salt_array = df_with_marker.withColumn(
        "salt_array",
        F.when(
            F.col("is_hot_key").isNotNull(), F.array(*[F.lit(i) for i in range(salt_factor)])
        ).otherwise(F.array(F.lit(0))),
    )

    # Explode the salt array to create multiple rows
    df_exploded = df_with_salt_array.withColumn("salt", F.explode(F.col("salt_array")))

    # Create {key_column}_salted column
    salted_column_name = f"{key_column}_salted"
    df_with_salted = df_exploded.withColumn(
        salted_column_name, F.concat(F.col(key_column), F.lit("_"), F.col("salt").cast("string"))
    )

    # Remove temporary columns
    result_df = df_with_salted.drop("is_hot_key", "salt_array")

    return result_df
