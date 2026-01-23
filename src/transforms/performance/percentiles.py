"""
Percentile and Correlation Analysis

Functions for calculating percentile metrics and device-performance correlations.
"""
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.schemas.columns import COL_USER_ID, COL_DURATION_MS, COL_DEVICE_TYPE
from src.config.constants import DEFAULT_PERCENTILES


def calculate_percentiles(
    df: DataFrame,
    value_column: str,
    group_by_columns: List[str],
    percentiles: List[float] = DEFAULT_PERCENTILES
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

        # Calculate percentiles for this group with higher accuracy
        percentile_values = group_df.approxQuantile(value_column, percentiles, 0.0001)

        # Create a row with group keys + percentile values
        row_dict = {col: group_row[col] for col in group_by_columns}
        for i, p in enumerate(percentiles):
            percentile_name = f"p{int(p * 100)}"
            row_dict[percentile_name] = float(percentile_values[i])

        percentile_results.append(row_dict)

    # Create DataFrame from percentile results
    if percentile_results:
        from pyspark.sql.types import StructType, StructField, DoubleType

        # Build schema dynamically
        schema_fields = []
        for col in group_by_columns:
            # Get original column type from df
            orig_field = [f for f in df.schema.fields if f.name == col][0]
            schema_fields.append(StructField(col, orig_field.dataType, nullable=False))

        for p in percentiles:
            percentile_name = f"p{int(p * 100)}"
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
    # TODO: Ensure metadata_df has 'user_id' and 'device_type' columns
    # TODO: Ensure interactions_df has 'user_id' and 'duration_ms' columns
    # TODO: Handle missing device_type and multiple device types per user if necessary
    joined_df = interactions_df.join(metadata_df, on=COL_USER_ID, how="left")

    # Aggregate by device_type
    device_metrics = joined_df.groupBy(COL_DEVICE_TYPE).agg(
        F.avg(COL_DURATION_MS).alias("avg_duration_ms"),
        F.count("*").alias("total_interactions"),
        F.countDistinct(COL_USER_ID).alias("unique_users")
    )

    # Calculate interactions_per_user
    device_metrics = device_metrics.withColumn(
        "interactions_per_user",
        (F.col("total_interactions") / F.col("unique_users")).cast("double")
    )

    # Calculate p95 duration per device
    # Group by device and calculate p95
    device_groups = metadata_df.select(COL_DEVICE_TYPE).distinct()

    p95_results = []
    for device_row in device_groups.collect():
        device = device_row[COL_DEVICE_TYPE]
        device_df = joined_df.filter(F.col(COL_DEVICE_TYPE) == device)
        p95_value = device_df.approxQuantile(COL_DURATION_MS, [0.95], 0.0001)[0]
        p95_results.append((device, float(p95_value)))


    p95_schema = StructType([
        StructField(COL_DEVICE_TYPE, StringType(), nullable=False),
        StructField("p95_duration_ms", DoubleType(), nullable=True)
    ])
    p95_df = interactions_df.sql_ctx.createDataFrame(p95_results, schema=p95_schema)

    # Join p95 with device_metrics
    result_df = device_metrics.join(p95_df, on=COL_DEVICE_TYPE, how="left")

    # Sort by avg_duration_ms descending
    result_df = result_df.orderBy(F.col("avg_duration_ms").desc())

    return result_df.select(
        COL_DEVICE_TYPE,
        "avg_duration_ms",
        "p95_duration_ms",
        "total_interactions",
        "unique_users",
        "interactions_per_user"
    )
