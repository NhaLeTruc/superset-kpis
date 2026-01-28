"""
Session Metrics Calculation

Functions for calculating session-level metrics including action counts,
session duration, bounce detection, and bounce rate analysis.

Uses Spark's native session_window() for efficient single-pass sessionization
and aggregation.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import session_window
from pyspark.sql.types import BooleanType, DoubleType, LongType

from src.schemas.columns import (
    COL_ACTION_COUNT,
    COL_BOUNCE_RATE,
    COL_DURATION_MS,
    COL_IS_BOUNCE,
    COL_METRIC_DATE,
    COL_SESSION_DURATION_MS,
    COL_SESSION_ID,
    COL_TIMESTAMP,
    COL_USER_ID,
)


def _parse_session_timeout(session_timeout: str) -> int:
    """
    Parse session timeout string and return value in seconds.

    Supported formats:
        - "<number> seconds" (e.g., "1800 seconds")
        - "<number> second" (e.g., "1 second")

    Args:
        session_timeout: Timeout string in supported format

    Returns:
        Timeout value in seconds

    Raises:
        ValueError: If format is not supported
    """
    pattern = r"^(\d+)\s+seconds?$"
    match = re.match(pattern, session_timeout.strip())
    if not match:
        raise ValueError(
            f"Invalid session_timeout format: '{session_timeout}'. "
            f"Expected format: '<number> seconds' (e.g., '1800 seconds')"
        )
    timeout_value = int(match.group(1))
    if timeout_value <= 0:
        raise ValueError(f"Session timeout must be positive, got {timeout_value} seconds")
    return timeout_value


def calculate_session_metrics(
    interactions_df: DataFrame, session_timeout: str = "1800 seconds"
) -> DataFrame:
    """
    Calculate session metrics using Spark's native session_window().

    Sessions are created when the time gap between consecutive interactions
    for a user exceeds the session timeout (default: 30 minutes).

    Computes:
    - action_count: Number of interactions in the session
    - session_duration_ms: Time from first to last interaction + last action duration
    - session_duration_seconds: Session duration in seconds (converted from ms)
    - is_bounce: Boolean indicating single-action sessions
    - avg_action_duration_ms: Average duration of actions in session

    Args:
        interactions_df: DataFrame with columns [user_id, timestamp, duration_ms]
        session_timeout: Gap duration that triggers new session (default: "1800 seconds")

    Returns:
        DataFrame with session-level metrics

    Schema:
        - user_id: String
        - session_id: String (format: user_id_YYYYMMDDHHmmss)
        - action_count: Integer (number of interactions)
        - session_duration_ms: Long (total session time in milliseconds)
        - session_duration_seconds: Double (total session time in seconds)
        - is_bounce: Boolean (True if only 1 action)
        - avg_action_duration_ms: Double (average action duration)
        - session_start_time: Timestamp (first interaction)
        - session_end_time: Timestamp (last interaction)

    Example:
        >>> metrics_df = calculate_session_metrics(interactions_df, session_timeout="1800 seconds")
        >>> metrics_df.filter("is_bounce = true").count()
        1542  # Number of bounce sessions
    """
    # Validate session_timeout format early (fail-fast)
    timeout_seconds = _parse_session_timeout(session_timeout)

    # Validate required columns
    required_cols = [COL_USER_ID, COL_TIMESTAMP, COL_DURATION_MS]
    missing_cols = [col for col in required_cols if col not in interactions_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Identify extra columns to preserve (columns that aren't part of the required set)
    all_cols = set(interactions_df.columns)
    required_set = set(required_cols)
    extra_cols = all_cols - required_set

    # Build aggregation expressions
    agg_exprs = [
        F.count("*").alias(COL_ACTION_COUNT),
        F.sum(COL_DURATION_MS).alias("total_action_duration_ms"),
        F.avg(COL_DURATION_MS).alias("avg_action_duration_ms"),
        # Get the duration of the last action
        F.max(F.struct(COL_TIMESTAMP, COL_DURATION_MS))
        .getField(COL_DURATION_MS)
        .alias("last_action_duration_ms"),
    ]

    # Add first() for each extra column to preserve them
    for col_name in sorted(extra_cols):  # Sort for deterministic order
        agg_exprs.append(F.first(col_name).alias(col_name))

    # Use session_window for native sessionization + aggregation
    session_metrics = interactions_df.groupBy(
        F.col(COL_USER_ID), session_window(COL_TIMESTAMP, session_timeout)
    ).agg(*agg_exprs)

    # Extract session boundaries and create session_id
    session_metrics_with_id = session_metrics.select(
        F.col(COL_USER_ID),
        F.col("session_window.start").alias("session_start_time"),
        F.col("session_window.end").alias("session_end_time"),
        # Generate session_id from user_id + start timestamp
        F.concat(
            F.col(COL_USER_ID), F.lit("_"), F.date_format("session_window.start", "yyyyMMddHHmmss")
        ).alias(COL_SESSION_ID),
        F.col(COL_ACTION_COUNT),
        F.col("total_action_duration_ms"),
        F.col("avg_action_duration_ms"),
        F.col("last_action_duration_ms"),
        # Include extra columns
        *[F.col(col_name) for col_name in sorted(extra_cols)],
    )

    # Calculate session duration: time span + last action duration
    # For single action: session_duration = action duration
    # For multiple actions: session_duration = (last_timestamp - first_timestamp) + last_action_duration
    # Note: session_window.end is the timestamp of last event + gap, so we use start times
    session_metrics_with_duration = session_metrics_with_id.withColumn(
        COL_SESSION_DURATION_MS,
        F.when(F.col(COL_ACTION_COUNT) == 1, F.col("last_action_duration_ms")).otherwise(
            # session_window end already accounts for the gap, so compute from actual timestamps
            (
                (F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time"))
                * 1000
            ).cast(LongType())
            -
            # Subtract the session gap since session_window.end includes it
            (F.lit(timeout_seconds) * 1000).cast(LongType())
            + F.col("last_action_duration_ms")
        ),
    )

    # Mark bounce sessions (single action)
    session_metrics_with_bounce = session_metrics_with_duration.withColumn(
        COL_IS_BOUNCE, (F.col(COL_ACTION_COUNT) == 1).cast(BooleanType())
    )

    # Add session_duration_seconds column (convert from milliseconds)
    result_df = session_metrics_with_bounce.withColumn(
        "session_duration_seconds", (F.col(COL_SESSION_DURATION_MS) / 1000.0).cast("double")
    )

    # Return all columns (preserves extra columns from input DataFrame)
    return result_df


def calculate_session_frequency(
    session_metrics_df: DataFrame,
    group_by_columns: list[str] | None = None,
) -> DataFrame:
    """
    Calculate user-level session frequency metrics.

    Measures how often users create sessions, which is a key engagement indicator.

    Args:
        session_metrics_df: DataFrame with session metrics (must include user_id, metric_date)
        group_by_columns: Optional list of columns to preserve (e.g., device_type, country)

    Returns:
        DataFrame with session frequency metrics per user

    Schema:
        - user_id: String
        - total_sessions: Long (total number of sessions for user)
        - active_days: Long (number of unique days with sessions)
        - avg_sessions_per_day: Double (total_sessions / active_days)
        - first_session_date: Date (date of first session)
        - last_session_date: Date (date of most recent session)
        - <group_by_columns>: Original types (if specified)

    Example:
        >>> freq_df = calculate_session_frequency(session_metrics_df)
        >>> freq_df.show()
        +--------+--------------+-----------+--------------------+
        | user_id|total_sessions|active_days|avg_sessions_per_day|
        +--------+--------------+-----------+--------------------+
        |user_001|            15|          7|                2.14|
        |user_002|             3|          3|                1.00|
        +--------+--------------+-----------+--------------------+
    """
    # Validate required columns
    required_cols = [COL_USER_ID, COL_METRIC_DATE]
    missing_cols = [col for col in required_cols if col not in session_metrics_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Build aggregation expressions
    agg_exprs = [
        F.count("*").alias("total_sessions"),
        F.countDistinct(COL_METRIC_DATE).alias("active_days"),
        F.min(COL_METRIC_DATE).alias("first_session_date"),
        F.max(COL_METRIC_DATE).alias("last_session_date"),
    ]

    # Determine group columns
    group_cols = [COL_USER_ID]
    if group_by_columns:
        # Validate group columns exist
        missing_group = [col for col in group_by_columns if col not in session_metrics_df.columns]
        if missing_group:
            raise ValueError(f"Missing group_by columns: {missing_group}")
        # Add first() for preserved columns
        for col_name in group_by_columns:
            agg_exprs.append(F.first(col_name).alias(col_name))

    # Calculate frequency metrics per user
    freq_df = session_metrics_df.groupBy(*group_cols).agg(*agg_exprs)

    # Calculate avg_sessions_per_day
    freq_df = freq_df.withColumn(
        "avg_sessions_per_day",
        F.when(F.col("active_days") > 0, F.col("total_sessions") / F.col("active_days")).otherwise(
            0.0
        ),
    )

    return freq_df


def calculate_bounce_rate(
    session_metrics_df: DataFrame, group_by_columns: list[str] | None = None
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
    if COL_IS_BOUNCE not in session_metrics_df.columns:
        raise ValueError(f"Missing required column: {COL_IS_BOUNCE}")

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
            F.sum(F.col(COL_IS_BOUNCE).cast("int")).alias("bounced_sessions"),
        )
    else:
        # Overall bounce rate
        bounce_stats = session_metrics_df.agg(
            F.count("*").alias("total_sessions"),
            F.sum(F.col(COL_IS_BOUNCE).cast("int")).alias("bounced_sessions"),
        )

    # Calculate bounce rate as percentage (handle division by zero)
    result_df = bounce_stats.withColumn(
        COL_BOUNCE_RATE,
        F.when(
            F.col("total_sessions") > 0,
            (F.col("bounced_sessions").cast(DoubleType()) / F.col("total_sessions")) * 100.0,
        ).otherwise(0.0),
    )

    return result_df
