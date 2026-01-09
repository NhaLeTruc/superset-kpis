"""
Cohort Retention Analysis

Calculates user retention rates by cohort over time.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_cohort_retention(
    interactions_df: DataFrame,
    metadata_df: DataFrame,
    cohort_period: str = "week",
    retention_weeks: int = 26
) -> DataFrame:
    """
    Calculate cohort retention analysis.

    Args:
        interactions_df: User interactions [user_id, timestamp, ...]
        metadata_df: User metadata with join_date [user_id, join_date, ...]
        cohort_period: "week" or "month" (default: "week")
        retention_weeks: Number of weeks to analyze (renamed from analysis_weeks)

    Returns:
        DataFrame with [cohort_week, week_number, cohort_size, active_users, retention_rate]
    """
    # Handle empty interactions dataframe
    if interactions_df.count() == 0:
        # Return empty dataframe with correct schema
        from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType, DoubleType
        empty_schema = StructType([
            StructField("cohort_week", DateType(), nullable=False),
            StructField("week_number", IntegerType(), nullable=False),
            StructField("cohort_size", LongType(), nullable=False),
            StructField("active_users", LongType(), nullable=False),
            StructField("retention_rate", DoubleType(), nullable=False)
        ])
        return interactions_df.sql_ctx.createDataFrame([], schema=empty_schema)

    # Calculate cohort week (first day of week user joined)
    # Try join_date first, fall back to registration_date
    join_date_col = "join_date" if "join_date" in metadata_df.columns else "registration_date"
    metadata_with_cohort = metadata_df.withColumn(
        "cohort_week",
        F.date_trunc("week", F.col(join_date_col))
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

    # Filter to retention period
    joined_df = joined_df.filter(
        (F.col("weeks_since_join") >= 0) &
        (F.col("weeks_since_join") < retention_weeks)
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
    cohorts = cohort_sizes.select("cohort_week", "cohort_size").collect()

    # Generate all week combinations
    from itertools import product
    all_combinations = []
    for cohort_row in cohorts:
        for week in range(retention_weeks):
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

    # Rename weeks_since_join to week_number for test compatibility
    result_df = result_df.withColumnRenamed("weeks_since_join", "week_number")

    return result_df.select(
        "cohort_week",
        "week_number",
        "cohort_size",
        "active_users",
        "retention_rate"
    )
