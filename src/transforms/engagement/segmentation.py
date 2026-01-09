"""
User Segmentation

Calculates stickiness ratio and identifies power users.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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

    # Calculate stickiness ratio as percentage
    stickiness_df = stickiness_df.withColumn(
        "stickiness_ratio",
        ((F.col("avg_dau") / F.col("monthly_active_users")) * 100.0).cast("double")
    )

    return stickiness_df.select("month", "avg_dau", "monthly_active_users", "stickiness_ratio")


def identify_power_users(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    percentile: float = 0.99,
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
    filtered_df = interactions_df.filter(F.col("duration_ms") <= max_duration_ms)

    # Add date column for days_active calculation
    filtered_df = filtered_df.withColumn("date", F.to_date(F.col("timestamp")))

    # Build aggregation expressions
    agg_exprs = [
        F.sum("duration_ms").alias("total_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct("date").alias("days_active")
    ]

    # Add unique_pages only if page_id column exists
    if "page_id" in filtered_df.columns:
        agg_exprs.append(F.countDistinct("page_id").alias("unique_pages"))

    # Calculate user-level metrics
    user_metrics = filtered_df.groupBy("user_id").agg(*agg_exprs)

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

    # Calculate threshold for top percentile
    threshold_value = user_metrics.approxQuantile("total_duration_ms", [percentile], 0.01)[0]

    # Filter to power users
    power_users = user_metrics.filter(F.col("total_duration_ms") >= threshold_value)

    # Add percentile rank
    power_users = power_users.withColumn("percentile_rank", F.lit(percentile * 100))

    # Join with metadata
    result_df = power_users.join(metadata_df, on="user_id", how="left")

    return result_df
