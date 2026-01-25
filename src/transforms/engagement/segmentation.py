"""
User Segmentation

Calculates stickiness ratio and identifies power users.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.schemas.columns import COL_USER_ID, COL_TIMESTAMP, COL_DURATION_MS, COL_PAGE_ID
from src.config.constants import HOT_KEY_THRESHOLD_PERCENTILE


def calculate_stickiness(dau_df: DataFrame, mau_df: DataFrame) -> DataFrame:
    """
    Calculate stickiness ratio (average DAU / MAU) per month.

    Args:
        dau_df: Output from calculate_dau() [date, daily_active_users, ...]
        mau_df: Output from calculate_mau() [month, monthly_active_users, ...]

    Returns:
        DataFrame with [month, avg_dau, monthly_active_users, stickiness_ratio]
    """
    # Extract month from date in DAU dataframe
    dau_with_month = dau_df.withColumn("month", F.trunc(F.col("date"), "month"))

    # Calculate average DAU per month
    avg_dau_per_month = dau_with_month.groupBy("month").agg(
        F.avg("daily_active_users").alias("avg_dau")
    )

    # Join with MAU
    stickiness_df = avg_dau_per_month.join(mau_df, on="month", how="inner")

    # Calculate stickiness ratio as decimal (0.0 to 1.0)
    stickiness_df = stickiness_df.withColumn(
        "stickiness_ratio",
        (F.col("avg_dau") / F.col("monthly_active_users")).cast("double")
    )

    return stickiness_df.select("month", "avg_dau", "monthly_active_users", "stickiness_ratio")


def identify_power_users(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    percentile: float = HOT_KEY_THRESHOLD_PERCENTILE,
    max_duration_ms: int = 28800000
) -> DataFrame:
    """
    Identify power users (top percentile by total engagement).

    Args:
        interactions_df: User interactions [user_id, timestamp, duration_ms, page_id, ...]
        metadata_df: User metadata [user_id, country, device_type, subscription_type, ...]
        percentile: Threshold percentile (0.99 = top 1%)
        max_duration_ms: Filter out durations above this (outliers)

    Returns:
        DataFrame with power user metrics and metadata
    """
    # Validate percentile (0.0 = all users, 1.0 = only top user)
    if percentile < 0 or percentile > 1:
        raise ValueError("percentile must be between 0 and 1 (inclusive)")

    # Filter outliers
    filtered_df = interactions_df.filter(F.col(COL_DURATION_MS) <= max_duration_ms)

    # Add date column for days_active calculation
    filtered_df = filtered_df.withColumn("date", F.to_date(F.col(COL_TIMESTAMP)))

    # Build aggregation expressions
    agg_exprs = [
        F.sum(COL_DURATION_MS).alias("total_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct("date").alias("days_active")
    ]

    # Add unique_pages only if page_id column exists
    if COL_PAGE_ID in filtered_df.columns:
        agg_exprs.append(F.countDistinct(COL_PAGE_ID).alias("unique_pages"))

    # Calculate user-level metrics
    user_metrics = filtered_df.groupBy(COL_USER_ID).agg(*agg_exprs)

    # Add unique_pages as null if it wasn't calculated
    if "unique_pages" not in user_metrics.columns:
        user_metrics = user_metrics.withColumn("unique_pages", F.lit(None).cast("long"))

    # Calculate derived metrics
    user_metrics = user_metrics.withColumn(
        "hours_spent",
        (F.col("total_duration_ms") / 3600000.0).cast("double")
    )
    user_metrics = user_metrics.withColumn(
        "avg_duration_per_interaction",
        (F.col("total_duration_ms") / F.col("total_interactions")).cast("double")
    )
    # Add alias for test compatibility
    user_metrics = user_metrics.withColumn(
        "avg_duration_ms",
        F.col("avg_duration_per_interaction")
    )

    # Calculate how many users to include based on percentile threshold
    # Original semantics: include users where percent_rank >= percentile
    # percent_rank = (rank - 1) / (n - 1), so we derive count from that formula
    import math
    total_users = user_metrics.count()

    if total_users == 0:
        power_users = user_metrics
    elif total_users == 1:
        # Single user is always a power user
        power_users = user_metrics
    else:
        # Calculate exact count to include (matches percent_rank >= percentile semantics)
        users_to_include = total_users - math.ceil(percentile * (total_users - 1))
        users_to_include = max(1, users_to_include)  # Always include at least 1

        # Sort by total_duration_ms descending and take top N
        # This avoids a global window function which would move all data to one partition
        power_users = user_metrics.orderBy(F.col("total_duration_ms").desc()).limit(users_to_include)

    # Calculate percentile rank without window function (avoids single-partition warning)
    # Power users is a small dataset (top 1%), so collect is safe
    power_users_count = power_users.count()

    if power_users_count <= 1:
        # Single user or empty: assign max percentile
        power_users = power_users.withColumn("percentile_rank", F.lit(100.0))
    else:
        # Collect, sort, compute rank mathematically, then recreate DataFrame
        spark = power_users.sparkSession
        schema = power_users.schema

        # Collect and sort by total_duration_ms ascending
        rows = power_users.collect()
        sorted_rows = sorted(rows, key=lambda r: r["total_duration_ms"])

        # Compute percentile_rank for each row
        # Row 0 (lowest) -> percentile * 100, Row N-1 (highest) -> 100
        ranked_data = []
        for i, row in enumerate(sorted_rows):
            pct_rank = percentile * 100 + (i / (power_users_count - 1)) * (1 - percentile) * 100
            ranked_data.append((*row, float(pct_rank)))

        # Recreate DataFrame with percentile_rank column
        from pyspark.sql.types import DoubleType, StructField
        new_schema = schema.add(StructField("percentile_rank", DoubleType(), True))
        power_users = spark.createDataFrame(ranked_data, schema=new_schema)

    # Join with metadata
    result_df = power_users.join(metadata_df, on=COL_USER_ID, how="left")

    return result_df
