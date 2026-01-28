"""
Active User Metrics

Calculates Daily Active Users (DAU) and Monthly Active Users (MAU).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.schemas.columns import COL_USER_ID, COL_TIMESTAMP, COL_DURATION_MS


def calculate_dau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Daily Active Users (DAU).

    Definition: An "active user" is any user with at least one interaction
    (any action_type) recorded on a given day. No minimum duration or
    interaction count is required.

    Args:
        interactions_df: DataFrame with [user_id, timestamp, duration_ms, ...]

    Returns:
        DataFrame with [date, daily_active_users, total_interactions, total_duration_ms, avg_duration_per_user]
    """
    # Validate required columns
    if COL_USER_ID not in interactions_df.columns:
        raise ValueError(f"Required column '{COL_USER_ID}' not found in DataFrame")
    if COL_TIMESTAMP not in interactions_df.columns:
        raise ValueError(f"Required column '{COL_TIMESTAMP}' not found in DataFrame")

    # Extract date from timestamp
    df_with_date = interactions_df.withColumn("date", F.to_date(F.col(COL_TIMESTAMP)))

    # Group by date and calculate metrics
    dau_df = df_with_date.groupBy("date").agg(
        F.countDistinct(COL_USER_ID).alias("daily_active_users"),
        F.count("*").alias("total_interactions"),
        F.sum(COL_DURATION_MS).alias("total_duration_ms")
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

    Definition: An "active user" is any user with at least one interaction
    (any action_type) recorded during the calendar month. No minimum duration
    or interaction count is required.

    Args:
        interactions_df: DataFrame with [user_id, timestamp, ...]

    Returns:
        DataFrame with [month, monthly_active_users, total_interactions]
    """
    # Validate required columns
    if COL_USER_ID not in interactions_df.columns:
        raise ValueError(f"Required column '{COL_USER_ID}' not found in DataFrame")
    if COL_TIMESTAMP not in interactions_df.columns:
        raise ValueError(f"Required column '{COL_TIMESTAMP}' not found in DataFrame")

    # Extract month (first day of month)
    df_with_month = interactions_df.withColumn(
        "month",
        F.trunc(F.col(COL_TIMESTAMP), "month")
    )

    # Group by month and calculate MAU
    mau_df = df_with_month.groupBy("month").agg(
        F.countDistinct(COL_USER_ID).alias("monthly_active_users"),
        F.count("*").alias("total_interactions")
    )

    return mau_df
