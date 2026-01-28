"""
Unit tests for session metrics calculation functions.

Tests calculate_session_metrics, calculate_session_frequency,
and calculate_bounce_rate functions.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StructField,
    StructType,
)


class TestParseSessionTimeout:
    """Test suite for _parse_session_timeout helper function."""

    def test_parses_seconds_format(self):
        """Test parsing of valid seconds format."""
        from src.transforms.session.metrics import _parse_session_timeout

        assert _parse_session_timeout("1800 seconds") == 1800
        assert _parse_session_timeout("60 seconds") == 60
        assert _parse_session_timeout("1 second") == 1

    def test_handles_whitespace(self):
        """Test that leading/trailing whitespace is handled."""
        from src.transforms.session.metrics import _parse_session_timeout

        assert _parse_session_timeout("  1800 seconds  ") == 1800

    def test_raises_error_for_invalid_format(self):
        """Test ValueError for invalid format."""
        from src.transforms.session.metrics import _parse_session_timeout

        with pytest.raises(ValueError, match="Invalid session_timeout format"):
            _parse_session_timeout("30 minutes")

        with pytest.raises(ValueError, match="Invalid session_timeout format"):
            _parse_session_timeout("invalid")

    def test_raises_error_for_zero_or_negative(self):
        """Test ValueError for non-positive values."""
        from src.transforms.session.metrics import _parse_session_timeout

        with pytest.raises(ValueError, match="must be positive"):
            _parse_session_timeout("0 seconds")


class TestCalculateSessionMetrics:
    """Test suite for calculate_session_metrics function."""

    def test_basic_session_creation(self, spark):
        """Test that sessions are created from interactions."""
        from src.transforms.session.metrics import calculate_session_metrics

        # Two interactions close together = one session
        data = [
            ("u001", "2023-01-01 10:00:00", 5000),
            ("u001", "2023-01-01 10:05:00", 3000),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should have 1 session
        assert result.count() == 1

        row = result.collect()[0]
        assert row["user_id"] == "u001"
        assert row["action_count"] == 2

    def test_session_split_on_timeout(self, spark):
        """Test that sessions are split when timeout is exceeded."""
        from src.transforms.session.metrics import calculate_session_metrics

        # Two interactions 2 hours apart = two sessions (with 30 min timeout)
        data = [
            ("u001", "2023-01-01 10:00:00", 5000),
            ("u001", "2023-01-01 12:00:00", 3000),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should have 2 sessions
        assert result.count() == 2

    def test_bounce_detection_single_action(self, spark):
        """Test that single-action sessions are marked as bounces."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2023-01-01 10:00:00", 5000),  # Single action = bounce
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df)

        row = result.collect()[0]
        assert row["is_bounce"] is True
        assert row["action_count"] == 1

    def test_non_bounce_multiple_actions(self, spark):
        """Test that multi-action sessions are not bounces."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2023-01-01 10:00:00", 5000),
            ("u001", "2023-01-01 10:01:00", 3000),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df)

        row = result.collect()[0]
        assert row["is_bounce"] is False
        assert row["action_count"] == 2

    def test_session_id_format(self, spark):
        """Test that session_id has correct format."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2023-01-15 14:30:45", 5000),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df)

        row = result.collect()[0]
        # Format: user_id_YYYYMMDDHHmmss
        assert row["session_id"].startswith("u001_")
        assert "20230115" in row["session_id"]

    def test_session_duration_calculation(self, spark):
        """Test that session duration is calculated correctly."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2023-01-01 10:00:00", 5000),
            ("u001", "2023-01-01 10:01:00", 3000),  # 60 seconds later
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df)

        row = result.collect()[0]
        # Duration should include time span + last action duration
        assert row["session_duration_ms"] > 0
        assert "session_duration_seconds" in result.columns

    def test_raises_error_for_missing_columns(self, spark):
        """Test ValueError for missing required columns."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [("u001", "2023-01-01 10:00:00")]
        df = spark.createDataFrame(data, ["user_id", "timestamp"])

        with pytest.raises(ValueError, match="Missing required columns"):
            calculate_session_metrics(df)

    def test_multiple_users(self, spark):
        """Test session metrics for multiple users."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2023-01-01 10:00:00", 5000),
            ("u001", "2023-01-01 10:05:00", 3000),
            ("u002", "2023-01-01 11:00:00", 4000),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df)

        # Should have 2 sessions (1 for each user)
        assert result.count() == 2

        u001_session = result.filter("user_id = 'u001'").collect()[0]
        u002_session = result.filter("user_id = 'u002'").collect()[0]

        assert u001_session["action_count"] == 2
        assert u002_session["action_count"] == 1


class TestCalculateSessionFrequency:
    """Test suite for calculate_session_frequency function."""

    def test_basic_frequency_calculation(self, spark):
        """Test basic session frequency calculation."""
        from src.transforms.session.metrics import calculate_session_frequency

        # User with 3 sessions across 2 days
        data = [
            ("u001", "2023-01-01"),
            ("u001", "2023-01-01"),
            ("u001", "2023-01-02"),
        ]
        df = spark.createDataFrame(data, ["user_id", "metric_date"]).withColumn(
            "metric_date", F.to_date("metric_date")
        )

        result = calculate_session_frequency(df)

        row = result.collect()[0]
        assert row["total_sessions"] == 3
        assert row["active_days"] == 2
        assert row["avg_sessions_per_day"] == 1.5

    def test_first_and_last_session_dates(self, spark):
        """Test that first and last session dates are captured."""
        from src.transforms.session.metrics import calculate_session_frequency

        data = [
            ("u001", "2023-01-05"),
            ("u001", "2023-01-10"),
            ("u001", "2023-01-15"),
        ]
        df = spark.createDataFrame(data, ["user_id", "metric_date"]).withColumn(
            "metric_date", F.to_date("metric_date")
        )

        result = calculate_session_frequency(df)

        row = result.collect()[0]
        assert str(row["first_session_date"]) == "2023-01-05"
        assert str(row["last_session_date"]) == "2023-01-15"

    def test_raises_error_for_missing_columns(self, spark):
        """Test ValueError for missing required columns."""
        from src.transforms.session.metrics import calculate_session_frequency

        data = [("u001",)]
        df = spark.createDataFrame(data, ["user_id"])

        with pytest.raises(ValueError, match="Missing required columns"):
            calculate_session_frequency(df)


class TestCalculateBounceRate:
    """Test suite for calculate_bounce_rate function."""

    def test_basic_bounce_rate(self, spark):
        """Test basic bounce rate calculation."""
        from src.transforms.session.metrics import calculate_bounce_rate

        # 2 bounces out of 4 sessions = 50%
        data = [
            (True,),
            (True,),
            (False,),
            (False,),
        ]
        df = spark.createDataFrame(data, ["is_bounce"])

        result = calculate_bounce_rate(df)

        row = result.collect()[0]
        assert row["total_sessions"] == 4
        assert row["bounced_sessions"] == 2
        assert row["bounce_rate"] == 50.0

    def test_grouped_bounce_rate(self, spark):
        """Test bounce rate grouped by dimension."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [
            ("iPad", True),
            ("iPad", False),
            ("iPhone", True),
            ("iPhone", True),
        ]
        df = spark.createDataFrame(data, ["device_type", "is_bounce"])

        result = calculate_bounce_rate(df, group_by_columns=["device_type"])

        # Should have 2 rows (one per device)
        assert result.count() == 2

        ipad = result.filter("device_type = 'iPad'").collect()[0]
        iphone = result.filter("device_type = 'iPhone'").collect()[0]

        assert ipad["bounce_rate"] == 50.0  # 1/2
        assert iphone["bounce_rate"] == 100.0  # 2/2

    def test_zero_sessions_returns_zero_bounce_rate(self, spark):
        """Test that zero sessions returns 0.0 bounce rate."""
        from src.transforms.session.metrics import calculate_bounce_rate

        schema = StructType(
            [
                StructField("is_bounce", BooleanType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)

        result = calculate_bounce_rate(df)

        row = result.collect()[0]
        assert row["total_sessions"] == 0
        assert row["bounce_rate"] == 0.0

    def test_raises_error_for_missing_is_bounce(self, spark):
        """Test ValueError when is_bounce column is missing."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [(1,)]
        df = spark.createDataFrame(data, ["other_column"])

        with pytest.raises(ValueError, match="Missing required column"):
            calculate_bounce_rate(df)

    def test_raises_error_for_missing_group_column(self, spark):
        """Test ValueError for missing group_by column."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [(True,)]
        df = spark.createDataFrame(data, ["is_bounce"])

        with pytest.raises(ValueError, match="Missing group_by columns"):
            calculate_bounce_rate(df, group_by_columns=["nonexistent"])

    def test_multiple_group_columns(self, spark):
        """Test bounce rate with multiple group columns."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [
            ("iPad", "US", True),
            ("iPad", "US", False),
            ("iPad", "UK", True),
            ("iPhone", "US", True),
        ]
        df = spark.createDataFrame(data, ["device_type", "country", "is_bounce"])

        result = calculate_bounce_rate(df, group_by_columns=["device_type", "country"])

        # Should have 3 groups: (iPad, US), (iPad, UK), (iPhone, US)
        assert result.count() == 3
