"""
Engagement analytics transforms for user behavior analysis.

Following TDD: Tests written first, implementation comes after RED state.
Reference: docs/TDD_SPEC.md - Task 2 (Engagement Analytics Specifications)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def calculate_dau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Daily Active Users (DAU).

    Args:
        interactions_df: DataFrame with [user_id, timestamp, duration_ms, ...]

    Returns:
        DataFrame with [date, dau, total_interactions, total_duration_ms, avg_duration_per_user]
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
        F.countDistinct("user_id").alias("dau"),
        F.count("*").alias("total_interactions"),
        F.sum("duration_ms").alias("total_duration_ms")
    )

    # Calculate avg_duration_per_user
    dau_df = dau_df.withColumn(
        "avg_duration_per_user",
        (F.col("total_duration_ms") / F.col("dau")).cast("double")
    )

    return dau_df


def calculate_mau(interactions_df: DataFrame) -> DataFrame:
    """
    Calculate Monthly Active Users (MAU).

    Args:
        interactions_df: DataFrame with [user_id, timestamp, ...]

    Returns:
        DataFrame with [month, mau, total_interactions]
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
        F.countDistinct("user_id").alias("mau"),
        F.count("*").alias("total_interactions")
    )

    return mau_df


def calculate_stickiness(dau_df: DataFrame, mau_df: DataFrame) -> DataFrame:
    """
    Calculate stickiness ratio (average DAU / MAU) per month.

    Args:
        dau_df: Output from calculate_dau() [date, dau, ...]
        mau_df: Output from calculate_mau() [month, mau, ...]

    Returns:
        DataFrame with [month, avg_dau, mau, stickiness_ratio]
    """
    # Extract month from date in DAU dataframe
    dau_with_month = dau_df.withColumn("month", F.trunc(F.col("date"), "month"))

    # Calculate average DAU per month
    avg_dau_per_month = dau_with_month.groupBy("month").agg(
        F.avg("dau").alias("avg_dau")
    )

    # Join with MAU
    stickiness_df = avg_dau_per_month.join(mau_df, on="month", how="inner")

    # Calculate stickiness ratio
    stickiness_df = stickiness_df.withColumn(
        "stickiness_ratio",
        (F.col("avg_dau") / F.col("mau")).cast("double")
    )

    return stickiness_df.select("month", "avg_dau", "mau", "stickiness_ratio")


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

    # Calculate user-level metrics
    user_metrics = filtered_df.groupBy("user_id").agg(
        F.sum("duration_ms").alias("total_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct("page_id").alias("unique_pages"),
        F.countDistinct("date").alias("days_active")
    )

    # Calculate derived metrics
    user_metrics = user_metrics.withColumn(
        "hours_spent",
        (F.col("total_duration_ms") / 3600000.0).cast("double")
    )
    user_metrics = user_metrics.withColumn(
        "avg_duration_per_interaction",
        (F.col("total_duration_ms") / F.col("total_interactions")).cast("double")
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


def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",
    analysis_weeks: int = 26
) -> DataFrame:
    """
    Calculate cohort retention analysis.

    Args:
        interactions_df: User interactions [user_id, timestamp, ...]
        metadata_df: User metadata with join_date [user_id, join_date, ...]
        cohort_period: "week" or "month" (default: "week")
        analysis_weeks: Number of weeks to analyze

    Returns:
        DataFrame with [cohort_week, weeks_since_join, cohort_size, active_users, retention_rate]
    """
    # Calculate cohort week (first day of week user joined)
    metadata_with_cohort = metadata_df.withColumn(
        "cohort_week",
        F.date_trunc("week", F.col("join_date"))
    )

    # Add week from interactions
    interactions_with_week = interactions_df.withColumn(
        "interaction_week",
        F.date_trunc("week", F.col("timestamp"))
    )

    # Join interactions with cohort data
    joined_df = interactions_with_week.join(
        metadata_with_cohort.select("user_id", "cohort_week"),
        on="user_id",
        how="inner"
    )

    # Calculate weeks since join
    joined_df = joined_df.withColumn(
        "weeks_since_join",
        ((F.unix_timestamp("interaction_week") - F.unix_timestamp("cohort_week")) / (7 * 24 * 3600)).cast("int")
    )

    # Filter to analysis period
    joined_df = joined_df.filter(
        (F.col("weeks_since_join") >= 0) &
        (F.col("weeks_since_join") < analysis_weeks)
    )

    # Get cohort sizes
    cohort_sizes = metadata_with_cohort.groupBy("cohort_week").agg(
        F.count("user_id").alias("cohort_size")
    )

    # Count active users per cohort per week
    active_users_per_week = joined_df.groupBy("cohort_week", "weeks_since_join").agg(
        F.countDistinct("user_id").alias("active_users")
    )

    # Join with cohort sizes
    retention_df = active_users_per_week.join(cohort_sizes, on="cohort_week", how="left")

    # Calculate retention rate
    retention_df = retention_df.withColumn(
        "retention_rate",
        ((F.col("active_users") / F.col("cohort_size")) * 100.0).cast("double")
    )

    # Create complete weeks range for each cohort (including weeks with 0 active users)
    # Get all cohorts
    cohorts = cohort_sizes.select("cohort_week", "cohort_size").collect()

    # Generate all week combinations
    from itertools import product
    all_combinations = []
    for cohort_row in cohorts:
        for week in range(analysis_weeks):
            all_combinations.append((
                cohort_row["cohort_week"],
                week,
                cohort_row["cohort_size"]
            ))

    # Create complete grid DataFrame
    if all_combinations:
        from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType
        grid_schema = StructType([
            StructField("cohort_week", DateType(), nullable=False),
            StructField("weeks_since_join", IntegerType(), nullable=False),
            StructField("cohort_size", LongType(), nullable=False)
        ])
        complete_grid = interactions_df.sql_ctx.createDataFrame(all_combinations, schema=grid_schema)

        # Left join retention data with complete grid
        result_df = complete_grid.join(
            retention_df.select("cohort_week", "weeks_since_join", "active_users", "retention_rate"),
            on=["cohort_week", "weeks_since_join"],
            how="left"
        )

        # Fill nulls with 0
        result_df = result_df.fillna({"active_users": 0, "retention_rate": 0.0})
    else:
        result_df = retention_df

    return result_df.select(
        "cohort_week",
        "weeks_since_join",
        "cohort_size",
        "active_users",
        "retention_rate"
    )
