"""
Data quality validation functions.

Provides schema validation, null detection, and outlier detection for Spark DataFrames.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


if TYPE_CHECKING:
    from pyspark.sql.types import StructType


def validate_schema(
    df: DataFrame | None, expected_schema: StructType | None, strict: bool = True
) -> tuple[bool, list[str]]:
    """
    Validate DataFrame schema.

    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        strict: If True, column order must match; if False, only names and types

    Returns:
        Tuple of (is_valid: bool, errors: List[str])
        - is_valid: True if schema matches
        - errors: List of error messages (empty if valid)

    Example errors:
        - "Missing column: 'user_id'"
        - "Column 'duration_ms' has type LongType but expected IntegerType"
        - "Extra column: 'unknown_column'"

    Raises:
        ValueError: If df or expected_schema is None
    """
    if df is None:
        raise ValueError("DataFrame cannot be None")
    if expected_schema is None:
        raise ValueError("expected_schema cannot be None")

    errors = []
    actual_schema = df.schema

    # Create dictionaries for easier lookup
    expected_fields = {field.name: field for field in expected_schema.fields}
    actual_fields = {field.name: field for field in actual_schema.fields}

    # Check for missing columns
    for col_name in expected_fields:
        if col_name not in actual_fields:
            errors.append(f"Missing column: '{col_name}'")

    # Check for type mismatches
    for col_name, expected_field in expected_fields.items():
        if col_name in actual_fields:
            expected_type = expected_field.dataType
            actual_type = actual_fields[col_name].dataType

            # Compare types directly - Spark types implement __eq__ for proper comparison
            # This handles complex types like ArrayType(StringType) vs ArrayType(IntegerType)
            if actual_type != expected_type:
                errors.append(
                    f"Column '{col_name}' has type {actual_type.simpleString()} but expected {expected_type.simpleString()}"
                )

    # Check for extra columns (only in strict mode)
    if strict:
        for col_name in actual_fields:
            if col_name not in expected_fields:
                errors.append(f"Extra column: '{col_name}'")

    is_valid = len(errors) == 0
    return is_valid, errors


def detect_nulls(df: DataFrame, non_nullable_columns: list[str]) -> DataFrame:
    """
    Detect NULL values in specified columns.

    Args:
        df: DataFrame to check
        non_nullable_columns: List of column names that should not have NULLs

    Returns:
        DataFrame with only rows containing NULLs in specified columns
        Adds column: null_columns (ArrayType<StringType>) listing which columns are NULL

    If no NULLs found, returns empty DataFrame with same schema
    """
    # Validate columns exist
    missing_cols = [col for col in non_nullable_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    # Build filter condition: any of the specified columns is NULL
    if not non_nullable_columns:
        # No columns to check - return empty DataFrame
        return df.limit(0).withColumn("null_columns", F.array())

    # Create condition: col1.isNull() OR col2.isNull() OR ...
    null_condition = None
    for col_name in non_nullable_columns:
        if null_condition is None:
            null_condition = F.col(col_name).isNull()
        else:
            null_condition = null_condition | F.col(col_name).isNull()

    # Filter rows that have at least one NULL
    rows_with_nulls = df.filter(null_condition)

    # Build null_columns array: list which columns are NULL for each row
    # Using array() with when() to conditionally include column names
    null_columns_expr = F.array(
        *[F.when(F.col(col_name).isNull(), F.lit(col_name)) for col_name in non_nullable_columns]
    )

    # Filter out None values from the array using native Spark SQL expression
    # array_compact removes null values (available in Spark 3.4+), fallback to filter with SQL expr
    try:
        # Try array_compact first (Spark 3.4+)
        null_columns_filtered = F.array_compact(null_columns_expr)
    except AttributeError:
        # Fallback: use expr-based filter for older Spark versions
        null_columns_filtered = F.expr(f"filter({null_columns_expr}, x -> x is not null)")

    # Add the null_columns column
    result_df = rows_with_nulls.withColumn("null_columns", null_columns_filtered)

    return result_df


def detect_outliers(  # noqa: PLR0912
    df: DataFrame,
    column: str,
    method: str = "iqr",
    iqr_multiplier: float = 1.5,
    threshold_min: float | None = None,
    threshold_max: float | None = None,
) -> DataFrame:
    """
    Detect outliers in a numeric column.

    Methods:
        - "iqr": Interquartile Range (Q1 - 1.5*IQR, Q3 + 1.5*IQR)
        - "threshold": Fixed min/max thresholds

    Args:
        df: DataFrame to check
        column: Numeric column to analyze (must be numeric type)
        method: "iqr" or "threshold"
        iqr_multiplier: Multiplier for IQR method (default: 1.5, must be positive)
        threshold_min: Minimum threshold (for threshold method)
        threshold_max: Maximum threshold (for threshold method)

    Returns:
        DataFrame with only outlier rows, plus columns:
            - outlier_reason (StringType): "below_min", "above_max", or "iqr_outlier"
            - outlier_value (DoubleType): The outlier value

    Raises:
        ValueError: If column doesn't exist, is not numeric, or iqr_multiplier <= 0
    """
    # Validate column exists
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    # Validate column is numeric
    from pyspark.sql.types import NumericType

    column_type = df.schema[column].dataType
    if not isinstance(column_type, NumericType):
        raise ValueError(f"Column '{column}' must be numeric, got {column_type.simpleString()}")

    # Validate iqr_multiplier is positive
    if iqr_multiplier <= 0:
        raise ValueError(f"iqr_multiplier must be positive, got {iqr_multiplier}")

    if method == "iqr":
        # Filter out nulls before calculating quartiles
        df_filtered = df.filter(F.col(column).isNotNull())

        # Handle empty DataFrame or all-null column
        if df_filtered.limit(1).count() == 0:
            return (
                df.limit(0)
                .withColumn("outlier_reason", F.lit(""))
                .withColumn("outlier_value", F.lit(0.0))
            )

        # Calculate quartiles using approxQuantile
        quantiles = df_filtered.approxQuantile(column, [0.25, 0.75], 0.01)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1

        # Calculate bounds
        lower_bound = q1 - iqr_multiplier * iqr
        upper_bound = q3 + iqr_multiplier * iqr

        # Filter outliers
        outliers = df.filter((F.col(column) < lower_bound) | (F.col(column) > upper_bound))

        # Add outlier_reason and outlier_value
        result_df = outliers.withColumn("outlier_value", F.col(column).cast("double"))
        result_df = result_df.withColumn(
            "outlier_reason",
            F.when(F.col(column) < lower_bound, F.lit("iqr_outlier_below"))
            .when(F.col(column) > upper_bound, F.lit("iqr_outlier_above"))
            .otherwise(F.lit("iqr_outlier")),
        )

        return result_df

    elif method == "threshold":
        # Build filter condition based on thresholds
        filter_condition = None

        if threshold_min is not None:
            filter_condition = F.col(column) < threshold_min

        if threshold_max is not None:
            max_condition = F.col(column) > threshold_max
            if filter_condition is not None:
                filter_condition = filter_condition | max_condition
            else:
                filter_condition = max_condition

        # If no thresholds provided, return empty DataFrame
        if filter_condition is None:
            return (
                df.limit(0)
                .withColumn("outlier_reason", F.lit(""))
                .withColumn("outlier_value", F.lit(0.0))
            )

        # Filter outliers
        outliers = df.filter(filter_condition)

        # Add outlier_value and outlier_reason
        result_df = outliers.withColumn("outlier_value", F.col(column).cast("double"))

        # Determine reason based on which threshold was violated
        if threshold_min is not None and threshold_max is not None:
            reason_expr = (
                F.when(F.col(column) < threshold_min, F.lit("below_min"))
                .when(F.col(column) > threshold_max, F.lit("above_max"))
                .otherwise(F.lit(None))
            )
        elif threshold_min is not None:
            reason_expr = F.lit("below_min")
        else:  # threshold_max is not None
            reason_expr = F.lit("above_max")

        result_df = result_df.withColumn("outlier_reason", reason_expr)

        return result_df

    else:
        raise ValueError(f"Unknown method: {method}. Use 'iqr' or 'threshold'")
