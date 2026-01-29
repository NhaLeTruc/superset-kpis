"""
Join Execution - Optimized Join Implementation

Implements automatic join optimization with broadcast hints,
skew detection, and salting strategies.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import HOT_KEY_THRESHOLD_PERCENTILE

from .optimization import apply_salting, explode_for_salting, identify_hot_keys


def _perform_salted_join(
    large_df: DataFrame,
    small_df: DataFrame,
    hot_keys_df: DataFrame,
    join_key: str,
    join_type: str,
    salt_factor: int,
) -> DataFrame:
    """
    Perform a salted join to handle skewed keys.

    Args:
        large_df: Large DataFrame to salt
        small_df: Small DataFrame to explode
        hot_keys_df: DataFrame containing hot keys
        join_key: Column to join on
        join_type: Type of join ("inner", "left", etc.)
        salt_factor: Number of salt buckets

    Returns:
        Joined DataFrame with salt columns removed
    """
    # Apply salting to large_df
    large_salted = apply_salting(
        large_df, hot_keys_df, key_column=join_key, salt_factor=salt_factor
    )

    # Explode small_df to match salted keys - rename join_key to avoid ambiguity
    small_exploded = explode_for_salting(
        small_df, hot_keys_df, key_column=join_key, salt_factor=salt_factor
    )
    # Rename the join key in small table to avoid duplicate column after join
    small_exploded = small_exploded.withColumnRenamed(join_key, f"{join_key}_small")

    # Join on salted keys
    salted_key = f"{join_key}_salted"
    result_df = large_salted.join(small_exploded, on=salted_key, how=join_type)

    # Clean up salt columns and renamed join key
    result_df = result_df.drop("salt", salted_key, f"{join_key}_small")

    return result_df


def optimized_join(
    large_df: DataFrame,
    small_df: DataFrame,
    join_key: str,
    join_type: str = "inner",
    hot_keys_df: DataFrame = None,
    enable_broadcast: bool = True,
    enable_salting: bool = True,
    skew_threshold: float = HOT_KEY_THRESHOLD_PERCENTILE,
    salt_factor: int = 10,
) -> DataFrame:
    """
    Perform optimized join with automatic skew detection and mitigation.

    Strategy:
        1. If small_df fits broadcast threshold -> broadcast join
        2. Else, detect hot keys in large_df (or use provided hot_keys_df)
        3. If hot keys found -> apply salting
        4. Perform join on salted keys
        5. Clean up salt columns

    Args:
        large_df: Large DataFrame (e.g., interactions)
        small_df: Small DataFrame (e.g., metadata)
        join_key: Column to join on
        join_type: "inner", "left", "right", "outer"
        hot_keys_df: Optional pre-computed hot keys DataFrame (output from identify_hot_keys)
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

    # If hot_keys_df is explicitly provided, always use salting
    if hot_keys_df is not None and hot_keys_df.head(1) and enable_salting:
        return _perform_salted_join(
            large_df, small_df, hot_keys_df, join_key, join_type, salt_factor
        )

    # Strategy 1: Try broadcast join if enabled and small_df is within size limit
    if enable_broadcast:
        # Get Spark's autoBroadcastJoinThreshold (default 10MB)
        spark = small_df.sparkSession
        broadcast_threshold = int(
            spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "10485760")
        )

        # Estimate small_df size using Spark's size estimator
        # Cache temporarily to get accurate size estimate
        small_df_cached = small_df.cache()
        small_df_count = small_df_cached.count()

        # Rough estimate: assume ~100 bytes per row (adjust based on schema)
        # For more accuracy, could use small_df_cached.rdd.map(lambda r: len(str(r))).sum()
        estimated_size = small_df_count * 100  # Conservative estimate

        if estimated_size <= broadcast_threshold:
            # Safe to broadcast
            result_df = large_df.join(F.broadcast(small_df_cached), on=join_key, how=join_type)
            small_df_cached.unpersist()
            return result_df
        else:
            # Too large for broadcast - let Spark decide join strategy
            small_df_cached.unpersist()

    # Strategy 2: Check for skew and apply salting if needed
    if enable_salting:
        # Detect hot keys in large_df
        detected_hot_keys = identify_hot_keys(
            large_df, key_column=join_key, threshold_percentile=skew_threshold
        )

        # If hot keys found, apply salting
        if detected_hot_keys.head(1):
            return _perform_salted_join(
                large_df, small_df, detected_hot_keys, join_key, join_type, salt_factor
            )

    # Strategy 3: Standard join (no broadcast, no skew detected)
    result_df = large_df.join(small_df, on=join_key, how=join_type)

    return result_df
