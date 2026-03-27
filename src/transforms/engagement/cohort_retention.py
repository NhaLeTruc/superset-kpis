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
    StringType,
    StructField,
    StructType,
)

from src.schemas.columns import (
    COL_COUNTRY,
    COL_DEVICE_TYPE,
    COL_REGISTRATION_DATE,
    COL_SUBSCRIPTION_TYPE,
    COL_TIMESTAMP,
    COL_USER_ID,
)


def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",  # noqa: ARG001 - Reserved for future use (month support)
    retention_weeks: int = 26,
) -> DataFrame:
    """
    Calculate overall cohort retention analysis.

    Args:
        interactions_df: User interactions [user_id, timestamp, ...]
        metadata_df: User metadata with registration_date [user_id, registration_date, ...]
        cohort_period: "week" or "month" (default: "week")
        retention_weeks: Number of weeks to analyze

    Returns:
        DataFrame with [cohort_week, week_number, cohort_size, retained_users, retention_rate]
        cohort_week is a DATE (Monday of the registration week).
        retention_rate is in the range 0.0–1.0.
    """
    if interactions_df.limit(1).count() == 0:
        empty_schema = StructType(
            [
                StructField("cohort_week", DateType(), nullable=False),
                StructField("week_number", IntegerType(), nullable=False),
                StructField("cohort_size", LongType(), nullable=False),
                StructField("retained_users", LongType(), nullable=False),
                StructField("retention_rate", DoubleType(), nullable=False),
            ]
        )
        spark = interactions_df.sparkSession
        return spark.createDataFrame([], schema=empty_schema)

    # Cohort week = Monday of the registration week, stored as DATE
    metadata_with_cohort = metadata_df.withColumn(
        "cohort_week",
        F.date_trunc("week", F.col(COL_REGISTRATION_DATE)).cast("date"),
    )

    # Interaction week = Monday of the interaction week
    interactions_with_week = interactions_df.withColumn(
        "interaction_week",
        F.date_trunc("week", F.col(COL_TIMESTAMP)).cast("date"),
    )

    # Join interactions with cohort data
    joined_df = interactions_with_week.join(
        metadata_with_cohort.select(COL_USER_ID, "cohort_week"), on=COL_USER_ID, how="inner"
    )

    # Weeks since registration (datediff gives days; divide by 7)
    joined_df = joined_df.withColumn(
        "weeks_since_join",
        (F.datediff(F.col("interaction_week"), F.col("cohort_week")) / 7).cast("int"),
    ).filter(
        (F.col("weeks_since_join") >= 0) & (F.col("weeks_since_join") < retention_weeks)
    )

    # Cohort sizes
    cohort_sizes = metadata_with_cohort.groupBy("cohort_week").agg(
        F.countDistinct(COL_USER_ID).alias("cohort_size")
    )

    # Retained users per cohort per week
    retained_per_week = joined_df.groupBy("cohort_week", "weeks_since_join").agg(
        F.countDistinct(COL_USER_ID).alias("retained_users")
    )

    # Join retained counts with cohort sizes
    retention_df = retained_per_week.join(cohort_sizes, on="cohort_week", how="left")
    retention_df = retention_df.withColumn(
        "retention_rate", (F.col("retained_users") / F.col("cohort_size")).cast("double")
    )

    # Build complete grid (every cohort × every week, including zero-activity weeks)
    spark = interactions_df.sparkSession
    weeks_df = spark.range(retention_weeks).withColumnRenamed("id", "weeks_since_join")
    complete_grid = cohort_sizes.crossJoin(F.broadcast(weeks_df))

    result_df = complete_grid.join(
        retention_df.select(
            "cohort_week", "weeks_since_join", "retained_users", "retention_rate"
        ),
        on=["cohort_week", "weeks_since_join"],
        how="left",
    )

    result_df = result_df.fillna({"retained_users": 0, "retention_rate": 0.0})
    result_df = result_df.withColumnRenamed("weeks_since_join", "week_number")

    return result_df.select(
        "cohort_week", "week_number", "cohort_size", "retained_users", "retention_rate"
    )


def _compute_segment_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    segment_name: str,
    segment_col: str,
    retention_weeks: int,
) -> DataFrame:
    """Compute cohort retention for a single segment dimension."""
    # Attach cohort_week and segment_value to each user
    metadata_with_cohort = (
        metadata_df.withColumn(
            "cohort_week",
            F.date_trunc("week", F.col(COL_REGISTRATION_DATE)).cast("date"),
        )
        .withColumn("segment_value", F.col(segment_col).cast("string"))
        .select(COL_USER_ID, "cohort_week", "segment_value")
    )

    # Cohort sizes per (cohort_week, segment_value)
    cohort_sizes = metadata_with_cohort.groupBy("cohort_week", "segment_value").agg(
        F.countDistinct(COL_USER_ID).alias("cohort_size")
    )

    # Add interaction week
    interactions_with_week = interactions_df.withColumn(
        "interaction_week",
        F.date_trunc("week", F.col(COL_TIMESTAMP)).cast("date"),
    )

    # Join interactions with user cohort + segment
    joined_df = interactions_with_week.join(
        metadata_with_cohort, on=COL_USER_ID, how="inner"
    ).withColumn(
        "weeks_since_join",
        (F.datediff(F.col("interaction_week"), F.col("cohort_week")) / 7).cast("int"),
    ).filter(
        (F.col("weeks_since_join") >= 0) & (F.col("weeks_since_join") < retention_weeks)
    )

    # Retained users per (cohort_week, segment_value, week)
    retained_per_week = joined_df.groupBy(
        "cohort_week", "segment_value", "weeks_since_join"
    ).agg(F.countDistinct(COL_USER_ID).alias("retained_users"))

    retention_df = retained_per_week.join(
        cohort_sizes, on=["cohort_week", "segment_value"], how="left"
    ).withColumn(
        "retention_rate", (F.col("retained_users") / F.col("cohort_size")).cast("double")
    )

    # Complete grid: every (cohort, segment_value, week) combination
    spark = interactions_df.sparkSession
    weeks_df = spark.range(retention_weeks).withColumnRenamed("id", "weeks_since_join")
    complete_grid = cohort_sizes.crossJoin(F.broadcast(weeks_df))

    result_df = complete_grid.join(
        retention_df.select(
            "cohort_week", "segment_value", "weeks_since_join", "retained_users", "retention_rate"
        ),
        on=["cohort_week", "segment_value", "weeks_since_join"],
        how="left",
    )

    result_df = result_df.fillna({"retained_users": 0, "retention_rate": 0.0})

    return result_df.select(
        F.col("cohort_week"),
        F.col("weeks_since_join").alias("week_number"),
        F.lit(segment_name).alias("segment_type"),
        F.col("segment_value"),
        F.col("cohort_size"),
        F.col("retained_users"),
        F.col("retention_rate"),
    )


def calculate_cohort_retention_by_segment(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    retention_weeks: int = 26,
) -> DataFrame:
    """
    Calculate cohort retention broken down by user segment.

    Computes retention for three segment dimensions (subscription_type, device_type,
    country) and unions the results into a single table suitable for Superset filtering.

    Args:
        interactions_df: User interactions [user_id, timestamp, ...]
        metadata_df: User metadata including subscription_type, device_type, country
        retention_weeks: Number of weeks to analyze

    Returns:
        DataFrame with [cohort_week, week_number, segment_type, segment_value,
                        cohort_size, retained_users, retention_rate]
        Filter on segment_type to get a specific breakdown in Superset.
    """
    if interactions_df.limit(1).count() == 0:
        empty_schema = StructType(
            [
                StructField("cohort_week", DateType(), nullable=False),
                StructField("week_number", IntegerType(), nullable=False),
                StructField("segment_type", StringType(), nullable=False),
                StructField("segment_value", StringType(), nullable=False),
                StructField("cohort_size", LongType(), nullable=False),
                StructField("retained_users", LongType(), nullable=False),
                StructField("retention_rate", DoubleType(), nullable=False),
            ]
        )
        spark = interactions_df.sparkSession
        return spark.createDataFrame([], schema=empty_schema)

    segments = [
        ("subscription_type", COL_SUBSCRIPTION_TYPE),
        ("device_type", COL_DEVICE_TYPE),
        ("country", COL_COUNTRY),
    ]

    result = _compute_segment_retention(
        interactions_df, metadata_df, segments[0][0], segments[0][1], retention_weeks
    )
    for seg_name, seg_col in segments[1:]:
        result = result.union(
            _compute_segment_retention(
                interactions_df, metadata_df, seg_name, seg_col, retention_weeks
            )
        )

    return result
