"""
Unit tests for session metrics transforms.

Tests calculate_session_metrics() and calculate_bounce_rate() functions from session transforms.
"""

from datetime import datetime

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transforms.session import calculate_bounce_rate, calculate_session_metrics


class TestCalculateSessionMetrics:
    """Tests for calculate_session_metrics() function."""

    def test_calculate_session_metrics_bounce(self, spark):
        """
        GIVEN: A session with single action
        WHEN: calculate_session_metrics() is called
        THEN:
            - is_bounce=True
            - action_count=1
            - session_duration_ms=duration of single action
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", "s001", datetime(2023, 1, 1, 10, 0, 0), 5000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_session_metrics(df)

        # Assert
        results = result_df.collect()
        assert len(results) == 1
        assert results[0]["is_bounce"] is True
        assert results[0]["action_count"] == 1
        assert results[0]["session_duration_ms"] == 5000

    def test_calculate_session_metrics_multi_action(self, spark):
        """
        GIVEN: A session with multiple actions
        WHEN: calculate_session_metrics() is called
        THEN:
            - is_bounce=False
            - action_count=3
            - session_duration_ms=time from first to last + last duration
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", "s001", datetime(2023, 1, 1, 10, 0, 0), 5000),
            ("u001", "s001", datetime(2023, 1, 1, 10, 10, 0), 120000),
            ("u001", "s001", datetime(2023, 1, 1, 10, 15, 0), 2000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_session_metrics(df)

        # Assert
        results = result_df.collect()
        assert len(results) == 1
        assert results[0]["is_bounce"] is False
        assert results[0]["action_count"] == 3
        # Duration: 15 min (900000 ms) + last action (2000 ms) = 902000 ms
        assert results[0]["session_duration_ms"] == 902000

    def test_calculate_session_metrics_multiple_sessions(self, spark):
        """
        GIVEN: Multiple sessions from different users
        WHEN: calculate_session_metrics() is called
        THEN: Returns separate metrics for each session
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            # u001, s001 - bounce
            ("u001", "s001", datetime(2023, 1, 1, 10, 0, 0), 5000),
            # u001, s002 - multi-action
            ("u001", "s002", datetime(2023, 1, 1, 11, 0, 0), 3000),
            ("u001", "s002", datetime(2023, 1, 1, 11, 5, 0), 4000),
            # u002, s003 - multi-action
            ("u002", "s003", datetime(2023, 1, 1, 10, 0, 0), 2000),
            ("u002", "s003", datetime(2023, 1, 1, 10, 3, 0), 3000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_session_metrics(df)

        # Assert
        results = result_df.orderBy("user_id", "session_id").collect()
        assert len(results) == 3

        # s001 - bounce
        assert results[0]["is_bounce"] is True
        assert results[0]["action_count"] == 1

        # s002 - multi-action
        assert results[1]["is_bounce"] is False
        assert results[1]["action_count"] == 2

        # s003 - multi-action
        assert results[2]["is_bounce"] is False
        assert results[2]["action_count"] == 2

    def test_calculate_session_metrics_empty_dataframe(self, spark):
        """
        GIVEN: Empty DataFrame with correct schema
        WHEN: calculate_session_metrics() is called
        THEN: Returns empty DataFrame with output schema
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        empty_df = spark.createDataFrame([], schema=schema)

        # Act
        result_df = calculate_session_metrics(empty_df)

        # Assert
        assert result_df.count() == 0
        assert "session_id" in result_df.columns
        assert "session_duration_ms" in result_df.columns

    def test_calculate_session_metrics_zero_duration(self, spark):
        """
        GIVEN: Session with zero duration action
        WHEN: calculate_session_metrics() is called
        THEN: Handles zero duration correctly
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [("u001", "s001", datetime(2023, 1, 1, 10, 0, 0), 0)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_session_metrics(df)

        # Assert
        result = result_df.collect()[0]
        assert result["session_duration_ms"] == 0
        assert result["is_bounce"] is True


class TestCalculateBounceRate:
    """Tests for calculate_bounce_rate() function."""

    def test_calculate_bounce_rate_all_bounces(self, spark):
        """
        GIVEN: All sessions are bounces
        WHEN: calculate_bounce_rate() is called
        THEN: bounce_rate=1.0 (100%)
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("is_bounce", IntegerType(), nullable=False),
                StructField("action_count", IntegerType(), nullable=False),
                StructField("session_duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", "s001", 1, 1, 5000),
            ("u002", "s002", 1, 1, 3000),
            ("u003", "s003", 1, 1, 4000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_bounce_rate(df)

        # Assert
        results = result_df.collect()
        assert len(results) == 1
        assert results[0]["bounce_rate"] == 100.0  # Now returns percentage
        assert results[0]["total_sessions"] == 3
        assert results[0]["bounced_sessions"] == 3

    def test_calculate_bounce_rate_no_bounces(self, spark):
        """
        GIVEN: No sessions are bounces
        WHEN: calculate_bounce_rate() is called
        THEN: bounce_rate=0.0 (0%)
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("is_bounce", IntegerType(), nullable=False),
                StructField("action_count", IntegerType(), nullable=False),
                StructField("session_duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", "s001", 0, 2, 125000),
            ("u002", "s002", 0, 3, 300000),
            ("u003", "s003", 0, 2, 180000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_bounce_rate(df)

        # Assert
        results = result_df.collect()
        assert len(results) == 1
        assert results[0]["bounce_rate"] == 0.0
        assert results[0]["total_sessions"] == 3
        assert results[0]["bounced_sessions"] == 0

    def test_calculate_bounce_rate_mixed(self, spark):
        """
        GIVEN: 2 bounces out of 5 sessions
        WHEN: calculate_bounce_rate() is called
        THEN: bounce_rate=0.4 (40%)
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("is_bounce", IntegerType(), nullable=False),
                StructField("action_count", IntegerType(), nullable=False),
                StructField("session_duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", "s001", 1, 1, 5000),
            ("u001", "s002", 0, 2, 125000),
            ("u002", "s003", 1, 1, 3000),
            ("u002", "s004", 0, 3, 300000),
            ("u003", "s005", 0, 2, 180000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_bounce_rate(df)

        # Assert
        results = result_df.collect()
        assert len(results) == 1
        assert abs(results[0]["bounce_rate"] - 40.0) < 0.1  # Now returns percentage (40%)
        assert results[0]["total_sessions"] == 5
        assert results[0]["bounced_sessions"] == 2

    def test_calculate_bounce_rate_grouped(self, spark):
        """
        GIVEN: Sessions with device_type grouping
        WHEN: calculate_bounce_rate() is called with group_by_columns
        THEN: Separate bounce rates per device_type
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("session_id", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
                StructField("is_bounce", IntegerType(), nullable=False),
                StructField("action_count", IntegerType(), nullable=False),
                StructField("session_duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            # iPad: 1 bounce out of 2
            ("u001", "s001", "iPad", 1, 1, 5000),
            ("u001", "s002", "iPad", 0, 2, 125000),
            # iPhone: 0 bounces out of 2
            ("u002", "s003", "iPhone", 0, 3, 300000),
            ("u002", "s004", "iPhone", 0, 2, 180000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_bounce_rate(df, group_by_columns=["device_type"])

        # Assert
        results = result_df.orderBy("device_type").collect()
        assert len(results) == 2

        # iPad
        assert results[0]["device_type"] == "iPad"
        assert abs(results[0]["bounce_rate"] - 50.0) < 0.1  # Now returns percentage (50%)
        assert results[0]["total_sessions"] == 2
        assert results[0]["bounced_sessions"] == 1

        # iPhone
        assert results[1]["device_type"] == "iPhone"
        assert results[1]["bounce_rate"] == 0.0  # 0% bounce rate
        assert results[1]["total_sessions"] == 2
        assert results[1]["bounced_sessions"] == 0
