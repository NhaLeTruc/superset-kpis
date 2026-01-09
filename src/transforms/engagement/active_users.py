"""
Active User Metrics

Calculates Daily Active Users (DAU) and Monthly Active Users (MAU).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_dau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Daily Active Users (DAU).

    Args:
        interactions_df: DataFrame with [user_id, timestamp, duration_ms, ...]

    Returns:
        DataFrame with [date, daily_active_users, total_interactions, total_duration_ms, avg_duration_per_user]
    """
    # Validate required columns
    if "user_id" not in interactions_df.columns:
        raise ValueError("Required column 'user_id' not found in DataFrame")
    if "timestamp" not in interactions_df.columns:
        raise ValueError("Required column 'timestamp' not found in DataFrame")

    # Extract date from timestamp
    df_with_date = interactions_df.withColumn("date", F.to_date(F.col("timestamp")))

    # Group by date and calculate metrics
    dau_df = df_with_date.groupBy("date").agg(
        F.countDistinct("user_id").alias("daily_active_users"),
        F.count("*").alias("total_interactions"),
        F.sum("duration_ms").alias("total_duration_ms")
    )

    # Calculate avg_duration_per_user
    dau_df = dau_df.withColumn(
        "avg_duration_per_user",
        (F.col("total_duration_ms") / F.col("daily_active_users")).cast("double")
    )

    return dau_df


def calculate_mau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Monthly Active Users (MAU).

    Args:
        interactions_df: DataFrame with [user_id, timestamp, ...]

    Returns:
        DataFrame with [month, monthly_active_users, total_interactions]
    """
    # Validate required columns
    if "user_id" not in interactions_df.columns:
        raise ValueError("Required column 'user_id' not found in DataFrame")
    if "timestamp" not in interactions_df.columns:
        raise ValueError("Required column 'timestamp' not found in DataFrame")

    # Extract month (first day of month)
    df_with_month = interactions_df.withColumn(
        "month",
        F.trunc(F.col("timestamp"), "month")
    )

    # Group by month and calculate MAU
    mau_df = df_with_month.groupBy("month").agg(
        F.countDistinct("user_id").alias("monthly_active_users"),
        F.count("*").alias("total_interactions")
    )

    return mau_df
