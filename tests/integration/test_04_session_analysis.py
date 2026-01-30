"""
Integration tests for the Session Analysis job (Job 04).

Tests the complete session analysis pipeline including:
- Session metrics calculation using session_window()
- Session frequency analysis
- Bounce rate calculations
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class TestSessionMetricsPipeline:
    """Integration tests for session metrics calculation."""

    def test_session_creation_from_interactions(self, spark):
        """Test that sessions are correctly created from interactions."""
        from src.transforms.session.metrics import calculate_session_metrics

        # Create interactions that form clear sessions
        data = [
            # Session 1: 3 interactions within 5 minutes
            ("u001", "2024-01-01 10:00:00", 5000),
            ("u001", "2024-01-01 10:02:00", 3000),
            ("u001", "2024-01-01 10:04:00", 4000),
            # Session 2: 2 hours later (new session with 30-min timeout)
            ("u001", "2024-01-01 12:00:00", 6000),
            ("u001", "2024-01-01 12:05:00", 2000),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout=1800)  # 30 minutes

        # Should have 2 sessions
        assert result.count() == 2

        sessions = result.orderBy("session_start_time").collect()

        # Session 1: 3 actions
        assert sessions[0]["action_count"] == 3
        assert sessions[0]["is_bounce"] is False

        # Session 2: 2 actions
        assert sessions[1]["action_count"] == 2
        assert sessions[1]["is_bounce"] is False

    def test_bounce_session_detection(self, spark):
        """Test that single-action sessions are marked as bounces."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            # Bounce session (single action)
            ("u001", "2024-01-01 10:00:00", 5000),
            # Non-bounce session (2 actions)
            ("u002", "2024-01-01 10:00:00", 3000),
            ("u002", "2024-01-01 10:05:00", 4000),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout=1800)

        # Verify bounce detection
        u001_session = result.filter(F.col("user_id") == "u001").collect()[0]
        u002_session = result.filter(F.col("user_id") == "u002").collect()[0]

        assert u001_session["is_bounce"] is True
        assert u002_session["is_bounce"] is False

    def test_session_duration_calculation(self, spark):
        """Test that session duration is calculated correctly."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2024-01-01 10:00:00", 5000),  # 5 seconds
            ("u001", "2024-01-01 10:01:00", 3000),  # 3 seconds
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout=1800)

        session = result.collect()[0]

        # Duration should be positive and include action durations
        assert session["session_duration_ms"] > 0
        assert "session_duration_seconds" in result.columns

    def test_multiple_users_sessions(self, spark):
        """Test session calculation for multiple users."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            # User 1: 2 sessions
            ("u001", "2024-01-01 10:00:00", 5000),
            ("u001", "2024-01-01 10:05:00", 3000),
            ("u001", "2024-01-01 12:00:00", 4000),  # New session
            # User 2: 1 session
            ("u002", "2024-01-01 11:00:00", 2000),
            ("u002", "2024-01-01 11:10:00", 3000),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout=1800)

        # Total 3 sessions
        assert result.count() == 3

        # User 1 has 2 sessions
        u001_sessions = result.filter(F.col("user_id") == "u001").count()
        assert u001_sessions == 2

        # User 2 has 1 session
        u002_sessions = result.filter(F.col("user_id") == "u002").count()
        assert u002_sessions == 1


class TestSessionFrequencyPipeline:
    """Integration tests for session frequency analysis."""

    def test_session_frequency_calculation(self, spark):
        """Test session frequency metrics calculation."""
        from src.transforms.session.metrics import calculate_session_frequency

        # Create session-level data
        data = [
            # User 1: 3 sessions across 2 days
            ("u001", "2024-01-01"),
            ("u001", "2024-01-01"),
            ("u001", "2024-01-02"),
            # User 2: 1 session
            ("u002", "2024-01-01"),
        ]

        df = spark.createDataFrame(data, ["user_id", "metric_date"]).withColumn(
            "metric_date", F.to_date("metric_date")
        )

        result = calculate_session_frequency(df)

        # Verify user 1 metrics
        u001 = result.filter(F.col("user_id") == "u001").collect()[0]
        assert u001["total_sessions"] == 3
        assert u001["active_days"] == 2
        assert u001["avg_sessions_per_day"] == 1.5

        # Verify user 2 metrics
        u002 = result.filter(F.col("user_id") == "u002").collect()[0]
        assert u002["total_sessions"] == 1
        assert u002["active_days"] == 1


class TestBounceRatePipeline:
    """Integration tests for bounce rate calculations."""

    def test_overall_bounce_rate(self, spark):
        """Test overall bounce rate calculation."""
        from src.transforms.session.metrics import calculate_bounce_rate

        # 4 sessions: 2 bounces
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
        """Test bounce rate grouped by dimensions."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [
            ("iPad", True),
            ("iPad", False),
            ("iPhone", True),
            ("iPhone", True),
            ("iPhone", False),
        ]

        df = spark.createDataFrame(data, ["device_type", "is_bounce"])

        result = calculate_bounce_rate(df, group_by_columns=["device_type"])

        # iPad: 1/2 = 50%
        ipad = result.filter(F.col("device_type") == "iPad").collect()[0]
        assert ipad["bounce_rate"] == 50.0

        # iPhone: 2/3 = 66.67%
        iphone = result.filter(F.col("device_type") == "iPhone").collect()[0]
        assert abs(iphone["bounce_rate"] - 66.67) < 0.1

    def test_zero_bounce_rate(self, spark):
        """Test bounce rate when no bounces."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [
            (False,),
            (False,),
            (False,),
        ]

        df = spark.createDataFrame(data, ["is_bounce"])

        result = calculate_bounce_rate(df)

        row = result.collect()[0]
        assert row["bounce_rate"] == 0.0

    def test_hundred_percent_bounce_rate(self, spark):
        """Test bounce rate when all sessions are bounces."""
        from src.transforms.session.metrics import calculate_bounce_rate

        data = [
            (True,),
            (True,),
            (True,),
        ]

        df = spark.createDataFrame(data, ["is_bounce"])

        result = calculate_bounce_rate(df)

        row = result.collect()[0]
        assert row["bounce_rate"] == 100.0


class TestSessionAnalysisDataQuality:
    """Integration tests for data quality in session analysis."""

    def test_handles_empty_interactions(self, spark):
        """Test that pipeline handles empty interactions gracefully."""
        from src.transforms.session.metrics import calculate_session_metrics

        schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        empty_df = spark.createDataFrame([], schema)
        result = calculate_session_metrics(empty_df, session_timeout=1800)

        assert result.count() == 0

    def test_preserves_extra_columns(self, spark):
        """Test that extra columns are preserved through session calculation."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2024-01-01 10:00:00", 5000, "iPad", "US"),
            ("u001", "2024-01-01 10:05:00", 3000, "iPad", "US"),
        ]

        df = spark.createDataFrame(
            data, ["user_id", "timestamp", "duration_ms", "device_type", "country"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        result = calculate_session_metrics(df, session_timeout=1800)

        # Extra columns should be preserved
        assert "device_type" in result.columns
        assert "country" in result.columns

    def test_session_id_uniqueness(self, spark):
        """Test that session IDs are unique."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2024-01-01 10:00:00", 5000),
            ("u001", "2024-01-01 12:00:00", 3000),  # New session
            ("u002", "2024-01-01 10:00:00", 4000),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_session_metrics(df, session_timeout=1800)

        session_ids = [r["session_id"] for r in result.collect()]
        assert len(session_ids) == len(set(session_ids))  # All unique

    def test_integer_timeout_parameter(self, spark):
        """Test that integer timeout parameter works."""
        from src.transforms.session.metrics import calculate_session_metrics

        data = [
            ("u001", "2024-01-01 10:00:00", 5000),
            ("u001", "2024-01-01 10:05:00", 3000),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        # Should work with integer
        result = calculate_session_metrics(df, session_timeout=1800)
        assert result.count() == 1

        # Should also work with string
        result2 = calculate_session_metrics(df, session_timeout="1800 seconds")
        assert result2.count() == 1
