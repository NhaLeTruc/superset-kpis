"""
Unit tests for session analysis transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 3 (Session Analysis Specifications)
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DoubleType
)
from datetime import datetime, timedelta
from src.transforms.session_transforms import (
    sessionize_interactions,
    calculate_session_metrics,
    calculate_bounce_rate
)


class TestSessionizeInteractions:
    """Tests for sessionize_interactions() function."""

    def test_sessionize_interactions_single_session(self, spark):
        """
        GIVEN: u001 has 3 interactions within 30 minutes
        WHEN: sessionize_interactions() is called with 1800s timeout
        THEN: All 3 interactions assigned to same session_id
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),
            StructField("page_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), "page_view", "p001", 5000),
            ("u001", datetime(2023, 1, 1, 10, 10, 0), "edit", "p001", 120000),
            ("u001", datetime(2023, 1, 1, 10, 25, 0), "save", "p001", 2000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = sessionize_interactions(df, session_timeout_seconds=1800)

        # Assert
        results = result_df.collect()
        assert len(results) == 3

        # All should have same session_id
        session_ids = [row["session_id"] for row in results]
        assert len(set(session_ids)) == 1
        assert session_ids[0] is not None

    def test_sessionize_interactions_multiple_sessions(self, spark):
        """
        GIVEN: u001 has interactions with >30 min gaps
        WHEN: sessionize_interactions() is called
        THEN: Interactions split into separate sessions
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),
            StructField("page_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            # Session 1
            ("u001", datetime(2023, 1, 1, 10, 0, 0), "page_view", "p001", 5000),
            ("u001", datetime(2023, 1, 1, 10, 10, 0), "edit", "p001", 120000),
            # Session 2 (40 min later)
            ("u001", datetime(2023, 1, 1, 10, 50, 0), "page_view", "p002", 3000),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), "save", "p002", 2000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = sessionize_interactions(df, session_timeout_seconds=1800)

        # Assert
        results = result_df.orderBy("timestamp").collect()
        assert len(results) == 4

        # First 2 should have same session_id
        assert results[0]["session_id"] == results[1]["session_id"]
        # Last 2 should have same session_id but different from first 2
        assert results[2]["session_id"] == results[3]["session_id"]
        assert results[0]["session_id"] != results[2]["session_id"]

    def test_sessionize_interactions_multiple_users(self, spark):
        """
        GIVEN: Multiple users with concurrent sessions
        WHEN: sessionize_interactions() is called
        THEN: Sessions are separate per user
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),
            StructField("page_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), "page_view", "p001", 5000),
            ("u002", datetime(2023, 1, 1, 10, 0, 0), "page_view", "p002", 3000),
            ("u001", datetime(2023, 1, 1, 10, 10, 0), "edit", "p001", 120000),
            ("u002", datetime(2023, 1, 1, 10, 10, 0), "save", "p002", 2000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = sessionize_interactions(df, session_timeout_seconds=1800)

        # Assert
        u001_sessions = result_df.filter(F.col("user_id") == "u001").select("session_id").distinct().collect()
        u002_sessions = result_df.filter(F.col("user_id") == "u002").select("session_id").distinct().collect()

        assert len(u001_sessions) == 1
        assert len(u002_sessions) == 1
        assert u001_sessions[0]["session_id"] != u002_sessions[0]["session_id"]

    def test_sessionize_interactions_exact_timeout(self, spark):
        """
        GIVEN: Interactions exactly 30 minutes apart
        WHEN: sessionize_interactions() is called
        THEN: Creates new session (>=timeout triggers new session)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),
            StructField("page_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), "page_view", "p001", 5000),
            ("u001", datetime(2023, 1, 1, 10, 30, 0), "page_view", "p002", 3000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = sessionize_interactions(df, session_timeout_seconds=1800)

        # Assert
        results = result_df.orderBy("timestamp").collect()
        # Should be separate sessions (>=timeout)
        assert results[0]["session_id"] != results[1]["session_id"]


class TestCalculateSessionMetrics:
    """Tests for calculate_session_metrics() function."""

    def test_calculate_session_metrics_bounce(self, spark):
        """
        GIVEN: A session with single action
        WHEN: calculate_session_metrics() is called
        THEN:
            - is_bounce=True
            - actions_count=1
            - session_duration_ms=duration of single action
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

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
        assert results[0]["actions_count"] == 1
        assert results[0]["session_duration_ms"] == 5000

    def test_calculate_session_metrics_multi_action(self, spark):
        """
        GIVEN: A session with multiple actions
        WHEN: calculate_session_metrics() is called
        THEN:
            - is_bounce=False
            - actions_count=3
            - session_duration_ms=time from first to last + last duration
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

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
        assert results[0]["actions_count"] == 3
        # Duration: 15 min (900000 ms) + last action (2000 ms) = 902000 ms
        assert results[0]["session_duration_ms"] == 902000

    def test_calculate_session_metrics_multiple_sessions(self, spark):
        """
        GIVEN: Multiple sessions from different users
        WHEN: calculate_session_metrics() is called
        THEN: Returns separate metrics for each session
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

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
        assert results[0]["actions_count"] == 1

        # s002 - multi-action
        assert results[1]["is_bounce"] is False
        assert results[1]["actions_count"] == 2

        # s003 - multi-action
        assert results[2]["is_bounce"] is False
        assert results[2]["actions_count"] == 2


class TestCalculateBounceRate:
    """Tests for calculate_bounce_rate() function."""

    def test_calculate_bounce_rate_all_bounces(self, spark):
        """
        GIVEN: All sessions are bounces
        WHEN: calculate_bounce_rate() is called
        THEN: bounce_rate=1.0 (100%)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("is_bounce", IntegerType(), nullable=False),
            StructField("actions_count", IntegerType(), nullable=False),
            StructField("session_duration_ms", LongType(), nullable=False)
        ])

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
        assert results[0]["bounce_rate"] == 1.0
        assert results[0]["total_sessions"] == 3
        assert results[0]["bounced_sessions"] == 3

    def test_calculate_bounce_rate_no_bounces(self, spark):
        """
        GIVEN: No sessions are bounces
        WHEN: calculate_bounce_rate() is called
        THEN: bounce_rate=0.0 (0%)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("is_bounce", IntegerType(), nullable=False),
            StructField("actions_count", IntegerType(), nullable=False),
            StructField("session_duration_ms", LongType(), nullable=False)
        ])

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
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("is_bounce", IntegerType(), nullable=False),
            StructField("actions_count", IntegerType(), nullable=False),
            StructField("session_duration_ms", LongType(), nullable=False)
        ])

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
        assert abs(results[0]["bounce_rate"] - 0.4) < 0.001
        assert results[0]["total_sessions"] == 5
        assert results[0]["bounced_sessions"] == 2

    def test_calculate_bounce_rate_grouped(self, spark):
        """
        GIVEN: Sessions with device_type grouping
        WHEN: calculate_bounce_rate() is called with group_by_columns
        THEN: Separate bounce rates per device_type
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("is_bounce", IntegerType(), nullable=False),
            StructField("actions_count", IntegerType(), nullable=False),
            StructField("session_duration_ms", LongType(), nullable=False)
        ])

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
        assert abs(results[0]["bounce_rate"] - 0.5) < 0.001
        assert results[0]["total_sessions"] == 2
        assert results[0]["bounced_sessions"] == 1

        # iPhone
        assert results[1]["device_type"] == "iPhone"
        assert results[1]["bounce_rate"] == 0.0
        assert results[1]["total_sessions"] == 2
        assert results[1]["bounced_sessions"] == 0
