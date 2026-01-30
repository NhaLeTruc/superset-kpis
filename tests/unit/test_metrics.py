"""
Unit tests for session metrics calculation functions.

Tests _parse_session_timeout and calculate_session_frequency functions.
"""

import pytest
from pyspark.sql import functions as F


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
