"""
Performance metrics transforms for analyzing application performance.

Following TDD: Tests written first, implementation comes after RED state.
Reference: docs/TDD_SPEC.md - Task 3 (Performance Metrics Specifications)
"""
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def calculate_percentiles(
    df: DataFrame,
    value_column: str,
    group_by_columns: List[str],
    percentiles: List[float] = [0.50, 0.95, 0.99]
) -> DataFrame:
    """
    Calculate percentile metrics grouped by dimensions.

    Args:
        df: Input DataFrame
        value_column: Column to calculate percentiles on
        group_by_columns: Columns to group by
        percentiles: List of percentiles (default: [0.50, 0.95, 0.99])

    Returns:
        DataFrame with percentile metrics and statistics
    """
    # Validate columns exist
    if value_column not in df.columns:
        raise ValueError(f"Column '{value_column}' not found in DataFrame")
    for col in group_by_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    # Validate percentiles
    for p in percentiles:
        if p <= 0 or p >= 1:
            raise ValueError(f"Percentile {p} must be between 0 and 1")

    # Group by specified columns and calculate statistics
    agg_exprs = [
        F.count("*").alias("count"),
        F.avg(value_column).alias(f"avg_{value_column}"),
        F.stddev(value_column).alias(f"stddev_{value_column}")
    ]

    # Calculate percentiles using approxQuantile
    # We need to do this per group, so we'll use a different approach
    result_df = df.groupBy(*group_by_columns).agg(*agg_exprs)

    # For each group, calculate percentiles
    # We'll collect group keys and calculate percentiles for each group
    groups = result_df.select(*group_by_columns).collect()

    # Create percentile columns
    percentile_results = []
    for group_row in groups:
        # Build filter condition for this group
        filter_condition = None
        for col in group_by_columns:
            col_condition = F.col(col) == group_row[col]
            if filter_condition is None:
                filter_condition = col_condition
            else:
                filter_condition = filter_condition & col_condition

        # Filter to this group
        group_df = df.filter(filter_condition)

        # Calculate percentiles for this group
        percentile_values = group_df.approxQuantile(value_column, percentiles, 0.01)

        # Create a row with group keys + percentile values
        row_dict = {col: group_row[col] for col in group_by_columns}
        for i, p in enumerate(percentiles):
            percentile_name = f"p{int(p * 100)}_{value_column}"
            row_dict[percentile_name] = float(percentile_values[i])

        percentile_results.append(row_dict)

    # Create DataFrame from percentile results
    if percentile_results:
        from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType, DateType

        # Build schema dynamically
        schema_fields = []
        for col in group_by_columns:
            # Get original column type from df
            orig_field = [f for f in df.schema.fields if f.name == col][0]
            schema_fields.append(StructField(col, orig_field.dataType, nullable=False))

        for p in percentiles:
            percentile_name = f"p{int(p * 100)}_{value_column}"
            schema_fields.append(StructField(percentile_name, DoubleType(), nullable=True))

        percentile_schema = StructType(schema_fields)
        percentile_df = df.sql_ctx.createDataFrame(percentile_results, schema=percentile_schema)

        # Join with aggregated statistics
        result_df = result_df.join(percentile_df, on=group_by_columns, how="inner")

    return result_df


def calculate_device_correlation(
    interactions_df: DataFrame,
    metadata_df: DataFrame
) -> DataFrame:
    """
    Calculate device-performance correlation metrics.

    Args:
        interactions_df: User interactions with duration_ms
        metadata_df: User metadata with device_type

    Returns:
        DataFrame with device performance metrics, sorted by avg_duration_ms DESC
    """
    # Join interactions with metadata
    joined_df = interactions_df.join(metadata_df, on="user_id", how="inner")

    # Aggregate by device_type
    device_metrics = joined_df.groupBy("device_type").agg(
        F.avg("duration_ms").alias("avg_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct("user_id").alias("unique_users")
    )

    # Calculate interactions_per_user
    device_metrics = device_metrics.withColumn(
        "interactions_per_user",
        (F.col("total_interactions") / F.col("unique_users")).cast("double")
    )

    # Calculate p95 duration per device
    # Group by device and calculate p95
    device_groups = joined_df.select("device_type").distinct().collect()

    p95_results = []
    for device_row in device_groups:
        device = device_row["device_type"]
        device_df = joined_df.filter(F.col("device_type") == device)
        p95_value = device_df.approxQuantile("duration_ms", [0.95], 0.01)[0]
        p95_results.append((device, float(p95_value)))

    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    p95_schema = StructType([
        StructField("device_type", StringType(), nullable=False),
        StructField("p95_duration_ms", DoubleType(), nullable=True)
    ])
    p95_df = interactions_df.sql_ctx.createDataFrame(p95_results, schema=p95_schema)

    # Join p95 with device_metrics
    result_df = device_metrics.join(p95_df, on="device_type", how="left")

    # Sort by avg_duration_ms descending
    result_df = result_df.orderBy(F.col("avg_duration_ms").desc())

    return result_df.select(
        "device_type",
        "avg_duration_ms",
        "p95_duration_ms",
        "total_interactions",
        "unique_users",
        "interactions_per_user"
    )


def detect_anomalies_statistical(
    df: DataFrame,
    value_column: str,
    z_threshold: float = 3.0,
    group_by_columns: List[str] = None
) -> DataFrame:
    """
    Detect statistical anomalies using Z-score method.

    Anomaly: |value - μ| > z_threshold * σ

    Args:
        df: Input DataFrame
        value_column: Column to analyze for anomalies
        z_threshold: Z-score threshold (default: 3.0 = 3 sigma)
        group_by_columns: Optional grouping for per-group baseline

    Returns:
        DataFrame with only anomalous records including z_score, baseline stats, anomaly_type
    """
    if group_by_columns is None:
        group_by_columns = []

    # Calculate baseline statistics (mean and stddev)
    if group_by_columns:
        # Calculate per-group statistics
        window_spec = Window.partitionBy(*group_by_columns)

        df_with_stats = df.withColumn(
            "baseline_mean",
            F.avg(value_column).over(window_spec)
        ).withColumn(
            "baseline_stddev",
            F.stddev(value_column).over(window_spec)
        )
    else:
        # Calculate global statistics
        stats = df.agg(
            F.avg(value_column).alias("mean"),
            F.stddev(value_column).alias("stddev")
        ).collect()[0]

        baseline_mean = stats["mean"]
        baseline_stddev = stats["stddev"]

        df_with_stats = df.withColumn("baseline_mean", F.lit(baseline_mean))
        df_with_stats = df_with_stats.withColumn("baseline_stddev", F.lit(baseline_stddev))

    # Calculate Z-score
    df_with_zscore = df_with_stats.withColumn(
        "z_score",
        ((F.col(value_column) - F.col("baseline_mean")) / F.col("baseline_stddev")).cast("double")
    )

    # Filter to anomalies (|z_score| > threshold)
    anomalies = df_with_zscore.filter(F.abs(F.col("z_score")) > z_threshold)

    # Add anomaly_type (high or low)
    anomalies = anomalies.withColumn(
        "anomaly_type",
        F.when(F.col(value_column) > F.col("baseline_mean"), F.lit("high"))
        .otherwise(F.lit("low"))
    )

    return anomalies
