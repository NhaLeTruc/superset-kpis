"""
Session Analysis Transforms

Functions for sessionizing user interactions and calculating session-based metrics.
Implements session timeout logic, bounce rate calculation, and session quality metrics.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType
from typing import Optional, List


def sessionize_interactions(
    interactions_df: DataFrame,
    session_timeout_seconds: int = 1800
) -> DataFrame:
    """
    Assign session IDs to user interactions based on time gaps.

    A new session starts when the time gap between consecutive interactions
    for a user exceeds the session timeout (default: 30 minutes).

    Args:
        interactions_df: DataFrame with columns [user_id, timestamp, ...]
        session_timeout_seconds: Maximum time gap (seconds) within a session (default: 1800)

    Returns:
        DataFrame with added 'session_id' column

    Algorithm:
        1. Sort interactions by user_id and timestamp
        2. Calculate time gap from previous interaction (per user)
        3. Mark new session when gap >= timeout
        4. Assign incremental session numbers per user
        5. Create unique session_id: user_id_session_number

    Example:
        >>> df = sessionize_interactions(interactions_df, session_timeout_seconds=1800)
        >>> df.select("user_id", "timestamp", "session_id").show()
        +-------+-------------------+-------------+
        |user_id|timestamp          |session_id   |
        +-------+-------------------+-------------+
        |u001   |2023-01-01 10:00:00|u001_0       |
        |u001   |2023-01-01 10:10:00|u001_0       |
        |u001   |2023-01-01 11:00:00|u001_1       |
        +-------+-------------------+-------------+
    """
    # Validate required columns
    required_cols = ["user_id", "timestamp"]
    missing_cols = [col for col in required_cols if col not in interactions_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Define window: partition by user, order by timestamp
    user_window = Window.partitionBy("user_id").orderBy("timestamp")

    # Calculate time difference from previous interaction (in seconds)
    df_with_lag = interactions_df.withColumn(
        "prev_timestamp",
        F.lag("timestamp", 1).over(user_window)
    )

    df_with_gap = df_with_lag.withColumn(
        "time_gap_seconds",
        F.when(
            F.col("prev_timestamp").isNull(),
            0
        ).otherwise(
            (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp"))
        )
    )

    # Mark new session when gap >= timeout
    df_with_session_flag = df_with_gap.withColumn(
        "is_new_session",
        F.when(
            (F.col("time_gap_seconds") >= session_timeout_seconds) | (F.col("prev_timestamp").isNull()),
            1
        ).otherwise(0)
    )

    # Cumulative sum of session flags gives session number per user
    df_with_session_num = df_with_session_flag.withColumn(
        "session_number",
        F.sum("is_new_session").over(user_window)
    )

    # Create unique session_id: user_id + "_" + session_number
    df_with_session_id = df_with_session_num.withColumn(
        "session_id",
        F.concat(
            F.col("user_id"),
            F.lit("_"),
            (F.col("session_number") - 1).cast(StringType())
        )
    )

    # Drop temporary columns
    result_df = df_with_session_id.drop(
        "prev_timestamp",
        "time_gap_seconds",
        "is_new_session",
        "session_number"
    )

    return result_df


def calculate_session_metrics(sessionized_df: DataFrame) -> DataFrame:
    """
    Calculate metrics for each session.

    Computes:
    - actions_count: Number of interactions in the session
    - session_duration_ms: Time from first to last interaction + last action duration
    - is_bounce: Boolean indicating single-action sessions
    - avg_action_duration_ms: Average duration of actions in session

    Args:
        sessionized_df: DataFrame with columns [user_id, session_id, timestamp, duration_ms]

    Returns:
        DataFrame with session-level metrics

    Schema:
        - user_id: String
        - session_id: String
        - actions_count: Integer (number of interactions)
        - session_duration_ms: Long (total session time in milliseconds)
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

    # Group by session and calculate metrics
    session_metrics = sessionized_df.groupBy("user_id", "session_id").agg(
        F.count("*").alias("actions_count"),
        F.min("timestamp").alias("session_start_time"),
        F.max("timestamp").alias("session_end_time"),
        F.sum("duration_ms").alias("total_action_duration_ms"),
        F.avg("duration_ms").alias("avg_action_duration_ms"),
        # Get the duration of the last action
        F.max(F.struct("timestamp", "duration_ms")).getField("duration_ms").alias("last_action_duration_ms")
    )

    # Calculate session duration: time span + last action duration
    # For single action: session_duration = action duration
    # For multiple actions: session_duration = (last_timestamp - first_timestamp) + last_action_duration
    session_metrics_with_duration = session_metrics.withColumn(
        "session_duration_ms",
        F.when(
            F.col("actions_count") == 1,
            F.col("last_action_duration_ms")
        ).otherwise(
            ((F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time")) * 1000).cast(LongType()) +
            F.col("last_action_duration_ms")
        )
    )

    # Mark bounce sessions (single action)
    session_metrics_with_bounce = session_metrics_with_duration.withColumn(
        "is_bounce",
        (F.col("actions_count") == 1).cast(IntegerType())
    )

    # Select final columns
    result_df = session_metrics_with_bounce.select(
        "user_id",
        "session_id",
        "actions_count",
        "session_duration_ms",
        "is_bounce",
        "avg_action_duration_ms",
        "session_start_time",
        "session_end_time"
    )

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
        - bounce_rate: Double (0.0 to 1.0)

    Schema (grouped):
        - <group_by_columns>: Original types
        - total_sessions: Long
        - bounced_sessions: Long
        - bounce_rate: Double

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
            F.sum("is_bounce").alias("bounced_sessions")
        )
    else:
        # Overall bounce rate
        bounce_stats = session_metrics_df.agg(
            F.count("*").alias("total_sessions"),
            F.sum("is_bounce").alias("bounced_sessions")
        )

    # Calculate bounce rate (handle division by zero)
    result_df = bounce_stats.withColumn(
        "bounce_rate",
        F.when(
            F.col("total_sessions") > 0,
            F.col("bounced_sessions").cast(DoubleType()) / F.col("total_sessions")
        ).otherwise(0.0)
    )

    return result_df
