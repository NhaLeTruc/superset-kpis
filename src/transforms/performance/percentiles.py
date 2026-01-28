"""
Percentile and Correlation Analysis

Functions for calculating percentile metrics and device-performance correlations.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import DEFAULT_PERCENTILES
from src.schemas.columns import COL_DEVICE_TYPE, COL_DURATION_MS, COL_USER_ID


def calculate_percentiles(
    df: DataFrame,
    value_column: str,
    group_by_columns: list[str],
    percentiles: list[float] = DEFAULT_PERCENTILES,
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

    # Validate percentiles (Spark accepts 0.0 to 1.0 inclusive)
    for p in percentiles:
        if p < 0 or p > 1:
            raise ValueError(f"Percentile {p} must be between 0 and 1 (inclusive)")

    # Build aggregation expressions: stats + percentiles in a single pass
    agg_exprs = [
        F.count("*").alias("count"),
        F.avg(value_column).alias(f"avg_{value_column}"),
        F.stddev(value_column).alias(f"stddev_{value_column}"),
    ]

    for p in percentiles:
        percentile_name = f"p{int(p * 100)}"
        agg_exprs.append(F.percentile_approx(value_column, p).alias(percentile_name))

    result_df = df.groupBy(*group_by_columns).agg(*agg_exprs)

    return result_df


def calculate_device_correlation(
    interactions_df: DataFrame, metadata_df: DataFrame | None = None
) -> DataFrame:
    """
    Identify correlation between device type and app performance using one-way ANOVA.

    Since device_type is categorical and duration_ms is continuous, Pearson correlation
    is not appropriate. One-way ANOVA tests whether there is a statistically significant
    difference in performance across device types. Eta-squared (eta^2) quantifies the
    effect size as the proportion of variance in duration explained by device type.

    Args:
        interactions_df: User interactions with duration_ms (and optionally device_type)
        metadata_df: User metadata with device_type. Optional if interactions_df
            already contains device_type column (e.g., for enriched data).

    Returns:
        DataFrame with per-device performance metrics and ANOVA correlation stats,
        sorted by avg_duration_ms DESC.

        Columns:
            device_type, avg_duration_ms, p95_duration_ms, total_interactions,
            unique_users, interactions_per_user, f_statistic, eta_squared
    """
    # Skip join if interactions_df already has device_type (e.g., enriched data)
    if COL_DEVICE_TYPE in interactions_df.columns:
        joined_df = interactions_df
    elif metadata_df is not None:
        joined_df = interactions_df.join(metadata_df, on=COL_USER_ID, how="left")
    else:
        raise ValueError(
            f"Column '{COL_DEVICE_TYPE}' not found in interactions_df and metadata_df is None"
        )

    # Per-device descriptive statistics in a single aggregation pass
    device_metrics = joined_df.groupBy(COL_DEVICE_TYPE).agg(
        F.avg(COL_DURATION_MS).alias("avg_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct(COL_USER_ID).alias("unique_users"),
        F.percentile_approx(COL_DURATION_MS, 0.95).alias("p95_duration_ms"),
        F.variance(COL_DURATION_MS).alias("_group_variance"),
    )

    device_metrics = device_metrics.withColumn(
        "interactions_per_user",
        (F.col("total_interactions") / F.col("unique_users")).cast("double"),
    )

    # --- One-way ANOVA: device_type (categorical) vs duration_ms (continuous) ---
    overall_stats_rows = joined_df.agg(
        F.avg(COL_DURATION_MS).alias("grand_mean"), F.count("*").alias("N")
    ).collect()

    # Handle empty DataFrame case
    if not overall_stats_rows or overall_stats_rows[0]["N"] == 0:
        # Return empty result with expected schema
        return (
            device_metrics.withColumn("f_statistic", F.lit(None).cast("double"))
            .withColumn("eta_squared", F.lit(None).cast("double"))
            .drop("_group_variance")
            .select(
                COL_DEVICE_TYPE,
                "avg_duration_ms",
                "p95_duration_ms",
                "total_interactions",
                "unique_users",
                "interactions_per_user",
                "f_statistic",
                "eta_squared",
            )
        )

    overall_stats = overall_stats_rows[0]
    grand_mean = float(overall_stats["grand_mean"])
    N = int(overall_stats["N"])

    k = int(device_metrics.count())

    # Compute SSB and SSW from per-group stats already calculated
    anova_components = device_metrics.agg(
        F.sum(
            F.col("total_interactions") * F.pow(F.col("avg_duration_ms") - F.lit(grand_mean), 2)
        ).alias("SSB"),
        F.sum(
            F.when(
                F.col("_group_variance").isNotNull(),
                (F.col("total_interactions") - 1) * F.col("_group_variance"),
            ).otherwise(0.0)
        ).alias("SSW"),
    ).collect()[0]

    SSB = float(anova_components["SSB"])
    SSW = float(anova_components["SSW"])
    SST = SSB + SSW

    # F-statistic requires k > 1 and SSW > 0 (non-zero within-group variance)
    f_statistic = SSB / (k - 1) / (SSW / (N - k)) if k > 1 and k < N and SSW > 0 else None

    eta_squared = SSB / SST if SST > 0 else None

    # Attach ANOVA results as columns for downstream consumption
    result_df = (
        device_metrics.withColumn("f_statistic", F.lit(f_statistic).cast("double"))
        .withColumn("eta_squared", F.lit(eta_squared).cast("double"))
        .drop("_group_variance")
        .orderBy(F.col("avg_duration_ms").desc())
    )

    return result_df.select(
        COL_DEVICE_TYPE,
        "avg_duration_ms",
        "p95_duration_ms",
        "total_interactions",
        "unique_users",
        "interactions_per_user",
        "f_statistic",
        "eta_squared",
    )
