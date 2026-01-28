"""
Cohort Retention Analysis

Calculates user retention rates by cohort over time.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

from src.schemas.columns import COL_REGISTRATION_DATE, COL_TIMESTAMP, COL_USER_ID


def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",  # noqa: ARG001 - Reserved for future use (month support)
    retention_weeks: int = 26,
) -> DataFrame:
    """
    Calculate cohort retention analysis.

    Args:
        interactions_df: User interactions [user_id, timestamp, ...]
        metadata_df: User metadata with registration_date [user_id, registration_date, ...]
        cohort_period: "week" or "month" (default: "week")
        retention_weeks: Number of weeks to analyze (renamed from analysis_weeks)

    Returns:
        DataFrame with [cohort_week, week_number, cohort_size, active_users, retention_rate]
    """
    # Handle empty interactions dataframe (use head(1) to avoid full scan)
    if interactions_df.head(1):
        # Return empty dataframe with correct schema
        empty_schema = StructType(
            [
                StructField("cohort_week", DateType(), nullable=False),
                StructField("week_number", IntegerType(), nullable=False),
                StructField("cohort_size", LongType(), nullable=False),
                StructField("active_users", LongType(), nullable=False),
                StructField("retention_rate", DoubleType(), nullable=False),
            ]
        )
        return interactions_df.sql_ctx.createDataFrame([], schema=empty_schema)

    # Calculate cohort week (first day of week user registered)
    metadata_with_cohort = metadata_df.withColumn(
        "cohort_week", F.date_trunc("week", F.col(COL_REGISTRATION_DATE))
    )

    # Add week from interactions
    interactions_with_week = interactions_df.withColumn(
        "interaction_week", F.date_trunc("week", F.col(COL_TIMESTAMP))
    )

    # Join interactions with cohort data
    joined_df = interactions_with_week.join(
        metadata_with_cohort.select(COL_USER_ID, "cohort_week"), on=COL_USER_ID, how="inner"
    )

    # Calculate weeks since join
    joined_df = joined_df.withColumn(
        "weeks_since_join",
        (
            (F.unix_timestamp("interaction_week") - F.unix_timestamp("cohort_week"))
            / (7 * 24 * 3600)
        ).cast("int"),
    )

    # Filter to retention period
    joined_df = joined_df.filter(
        (F.col("weeks_since_join") >= 0) & (F.col("weeks_since_join") < retention_weeks)
    )

    # Get cohort sizes (use countDistinct to handle potential duplicates in metadata)
    cohort_sizes = metadata_with_cohort.groupBy("cohort_week").agg(
        F.countDistinct(COL_USER_ID).alias("cohort_size")
    )

    # Count active users per cohort per week
    active_users_per_week = joined_df.groupBy("cohort_week", "weeks_since_join").agg(
        F.countDistinct(COL_USER_ID).alias("active_users")
    )

    # Join with cohort sizes
    retention_df = active_users_per_week.join(cohort_sizes, on="cohort_week", how="left")

    # Calculate retention rate as decimal (0.0 to 1.0)
    retention_df = retention_df.withColumn(
        "retention_rate", (F.col("active_users") / F.col("cohort_size")).cast("double")
    )

    # Create complete weeks range for each cohort (including weeks with 0 active users)
    # Use pure Spark approach with crossJoin for scalability (avoids driver-side collect)
    spark = interactions_df.sparkSession
    weeks_df = spark.range(retention_weeks).withColumnRenamed("id", "weeks_since_join")

    # Cross join cohorts with weeks to create complete grid
    complete_grid = cohort_sizes.crossJoin(weeks_df)

    # Left join retention data with complete grid
    result_df = complete_grid.join(
        retention_df.select("cohort_week", "weeks_since_join", "active_users", "retention_rate"),
        on=["cohort_week", "weeks_since_join"],
        how="left",
    )

    # Fill nulls with 0
    result_df = result_df.fillna({"active_users": 0, "retention_rate": 0.0})

    # Rename weeks_since_join to week_number for test compatibility
    result_df = result_df.withColumnRenamed("weeks_since_join", "week_number")

    return result_df.select(
        "cohort_week", "week_number", "cohort_size", "active_users", "retention_rate"
    )
