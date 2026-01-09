"""
Session Sessionization

Implements session assignment logic based on time gaps between user interactions.
A new session starts when the time gap exceeds the configured timeout threshold.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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
