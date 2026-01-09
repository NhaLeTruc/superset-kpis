"""
Session Metrics Calculation

Functions for calculating session-level metrics including action counts,
session duration, bounce detection, and bounce rate analysis.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, BooleanType
from typing import Optional, List


def calculate_session_metrics(sessionized_df: DataFrame) -> DataFrame:
    """
    Calculate metrics for each session.

    Computes:
    - action_count: Number of interactions in the session
    - session_duration_ms: Time from first to last interaction + last action duration
    - session_duration_seconds: Session duration in seconds (converted from ms)
    - is_bounce: Boolean indicating single-action sessions
    - avg_action_duration_ms: Average duration of actions in session

    Args:
        sessionized_df: DataFrame with columns [user_id, session_id, timestamp, duration_ms]

    Returns:
        DataFrame with session-level metrics

    Schema:
        - user_id: String
        - session_id: String
        - action_count: Integer (number of interactions)
        - session_duration_ms: Long (total session time in milliseconds)
        - session_duration_seconds: Double (total session time in seconds)
        - is_bounce: Boolean (True if only 1 action)
        - avg_action_duration_ms: Double (average action duration)
        - session_start_time: Timestamp (first interaction)
        - session_end_time: Timestamp (last interaction)

    Example:
        >>> metrics_df = calculate_session_metrics(sessionized_df)
        >>> metrics_df.filter("is_bounce = true").count()
        1542  # Number of bounce sessions
    """
    # Validate required columns
    required_cols = ["user_id", "session_id", "timestamp", "duration_ms"]
    missing_cols = [col for col in required_cols if col not in sessionized_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Identify extra columns to preserve (columns that aren't part of the required set)
    all_cols = set(sessionized_df.columns)
    required_set = set(required_cols)
    extra_cols = all_cols - required_set - {"session_id"}  # session_id is in groupBy

    # Build aggregation expressions
    agg_exprs = [
        F.count("*").alias("action_count"),
        F.min("timestamp").alias("session_start_time"),
        F.max("timestamp").alias("session_end_time"),
        F.sum("duration_ms").alias("total_action_duration_ms"),
        F.avg("duration_ms").alias("avg_action_duration_ms"),
        # Get the duration of the last action
        F.max(F.struct("timestamp", "duration_ms")).getField("duration_ms").alias("last_action_duration_ms")
    ]

    # Add first() for each extra column to preserve them
    for col_name in sorted(extra_cols):  # Sort for deterministic order
        agg_exprs.append(F.first(col_name).alias(col_name))

    # Group by session and calculate metrics
    session_metrics = sessionized_df.groupBy("user_id", "session_id").agg(*agg_exprs)

    # Calculate session duration: time span + last action duration
    # For single action: session_duration = action duration
    # For multiple actions: session_duration = (last_timestamp - first_timestamp) + last_action_duration
    session_metrics_with_duration = session_metrics.withColumn(
        "session_duration_ms",
        F.when(
            F.col("action_count") == 1,
            F.col("last_action_duration_ms")
        ).otherwise(
            ((F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time")) * 1000).cast(LongType()) +
            F.col("last_action_duration_ms")
        )
    )

    # Mark bounce sessions (single action)
    session_metrics_with_bounce = session_metrics_with_duration.withColumn(
        "is_bounce",
        (F.col("action_count") == 1).cast(BooleanType())
    )

    # Add session_duration_seconds column (convert from milliseconds)
    result_df = session_metrics_with_bounce.withColumn(
        "session_duration_seconds",
        (F.col("session_duration_ms") / 1000.0).cast("double")
    )

    # Return all columns (preserves extra columns from input DataFrame)
    return result_df


def calculate_bounce_rate(
    session_metrics_df: DataFrame,
    group_by_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Calculate bounce rate from session metrics.

    Bounce rate = (Number of single-action sessions) / (Total sessions)

    Can be calculated overall or grouped by dimensions (e.g., device_type, country).

    Args:
        session_metrics_df: DataFrame with column [is_bounce]
        group_by_columns: Optional list of columns to group by (default: None for overall)

    Returns:
        DataFrame with bounce rate metrics

    Schema (ungrouped):
        - total_sessions: Long
        - bounced_sessions: Long
        - bounce_rate: Double (percentage: 0.0 to 100.0)

    Schema (grouped):
        - <group_by_columns>: Original types
        - total_sessions: Long
        - bounced_sessions: Long
        - bounce_rate: Double (percentage: 0.0 to 100.0)

    Example:
        >>> # Overall bounce rate
        >>> bounce_df = calculate_bounce_rate(session_metrics_df)
        >>> bounce_df.show()
        +--------------+----------------+-----------+
        |total_sessions|bounced_sessions|bounce_rate|
        +--------------+----------------+-----------+
        |          1000|             320|       0.32|
        +--------------+----------------+-----------+

        >>> # Bounce rate by device
        >>> bounce_by_device = calculate_bounce_rate(
        ...     session_metrics_df,
        ...     group_by_columns=["device_type"]
        ... )
        >>> bounce_by_device.show()
        +-----------+--------------+----------------+-----------+
        |device_type|total_sessions|bounced_sessions|bounce_rate|
        +-----------+--------------+----------------+-----------+
        |iPad       |           500|             120|       0.24|
        |iPhone     |           500|             200|       0.40|
        +-----------+--------------+----------------+-----------+
    """
    # Validate required columns
    if "is_bounce" not in session_metrics_df.columns:
        raise ValueError("Missing required column: is_bounce")

    # Determine grouping columns
    if group_by_columns is None:
        group_cols = []
    else:
        group_cols = group_by_columns
        # Validate group columns exist
        missing_cols = [col for col in group_cols if col not in session_metrics_df.columns]
        if missing_cols:
            raise ValueError(f"Missing group_by columns: {missing_cols}")

    # Calculate metrics
    if group_cols:
        # Grouped bounce rate
        bounce_stats = session_metrics_df.groupBy(*group_cols).agg(
            F.count("*").alias("total_sessions"),
            F.sum(F.col("is_bounce").cast("int")).alias("bounced_sessions")
        )
    else:
        # Overall bounce rate
        bounce_stats = session_metrics_df.agg(
            F.count("*").alias("total_sessions"),
            F.sum(F.col("is_bounce").cast("int")).alias("bounced_sessions")
        )

    # Calculate bounce rate as percentage (handle division by zero)
    result_df = bounce_stats.withColumn(
        "bounce_rate",
        F.when(
            F.col("total_sessions") > 0,
            (F.col("bounced_sessions").cast(DoubleType()) / F.col("total_sessions")) * 100.0
        ).otherwise(0.0)
    )

    return result_df
