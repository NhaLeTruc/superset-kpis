"""
Statistical Anomaly Detection

Implements Z-score based anomaly detection with iterative baseline refinement.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config.constants import Z_SCORE_ANOMALY_THRESHOLD


def detect_anomalies_statistical(
    df: DataFrame,
    value_column: str,
    z_threshold: float = Z_SCORE_ANOMALY_THRESHOLD,
    group_by_columns: list[str] | None = None,
) -> DataFrame:
    """
    Detect statistical anomalies using Z-score method.

    Anomaly: |value - μ| > z_threshold * σ

    The baseline (mean and stddev) is calculated iteratively:
    1. Calculate initial baseline with all data
    2. Identify preliminary anomalies
    3. Recalculate baseline excluding preliminary anomalies
    4. Calculate final z-scores and return anomalies

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

    # Step 1: Calculate initial baseline statistics with all data
    if group_by_columns:
        # Calculate per-group statistics
        window_spec = Window.partitionBy(*group_by_columns)

        df_with_initial_stats = df.withColumn(
            "initial_mean", F.avg(value_column).over(window_spec)
        ).withColumn("initial_stddev", F.stddev(value_column).over(window_spec))
    else:
        # Calculate global statistics
        stats = df.agg(
            F.avg(value_column).alias("mean"), F.stddev(value_column).alias("stddev")
        ).collect()[0]

        initial_mean = stats["mean"]
        initial_stddev = stats["stddev"]

        df_with_initial_stats = df.withColumn("initial_mean", F.lit(initial_mean))
        df_with_initial_stats = df_with_initial_stats.withColumn(
            "initial_stddev", F.lit(initial_stddev)
        )

    # Step 2: Calculate initial z-scores and identify non-anomalous data
    # When stddev is 0 or null (all values identical), z-score is 0 (no anomalies possible)
    df_with_initial_zscore = df_with_initial_stats.withColumn(
        "initial_z_score",
        F.when(
            (F.col("initial_stddev").isNull()) | (F.col("initial_stddev") == 0),
            F.lit(0.0),
        )
        .otherwise((F.col(value_column) - F.col("initial_mean")) / F.col("initial_stddev"))
        .cast("double"),
    )

    # Filter to non-anomalous data (|z| <= threshold)
    non_anomalous_df = df_with_initial_zscore.filter(F.abs(F.col("initial_z_score")) <= z_threshold)

    # Step 3: Recalculate baseline excluding preliminary anomalies
    if group_by_columns:
        # Calculate refined per-group statistics on non-anomalous data
        refined_stats = non_anomalous_df.groupBy(*group_by_columns).agg(
            F.avg(value_column).alias("baseline_mean"),
            F.stddev(value_column).alias("baseline_stddev"),
        )

        # Join refined baseline back to original data
        df_with_refined_stats = df.join(refined_stats, on=group_by_columns, how="left")
    else:
        # Calculate refined global statistics on non-anomalous data
        refined_stats = non_anomalous_df.agg(
            F.avg(value_column).alias("mean"), F.stddev(value_column).alias("stddev")
        ).collect()[0]

        baseline_mean = refined_stats["mean"]
        baseline_stddev = refined_stats["stddev"]

        df_with_refined_stats = df.withColumn("baseline_mean", F.lit(baseline_mean))
        df_with_refined_stats = df_with_refined_stats.withColumn(
            "baseline_stddev", F.lit(baseline_stddev)
        )

    # Step 4: Calculate final z-scores using refined baseline
    # When stddev is 0 or null (all values identical), z-score is 0 (no anomalies possible)
    df_with_zscore = df_with_refined_stats.withColumn(
        "z_score",
        F.when(
            (F.col("baseline_stddev").isNull()) | (F.col("baseline_stddev") == 0),
            F.lit(0.0),
        )
        .otherwise((F.col(value_column) - F.col("baseline_mean")) / F.col("baseline_stddev"))
        .cast("double"),
    )

    # Filter to anomalies (|z_score| > threshold)
    anomalies = df_with_zscore.filter(F.abs(F.col("z_score")) > z_threshold)

    # Add anomaly_type (high or low)
    anomalies = anomalies.withColumn(
        "anomaly_type",
        F.when(F.col(value_column) > F.col("baseline_mean"), F.lit("high")).otherwise(F.lit("low")),
    )

    return anomalies
