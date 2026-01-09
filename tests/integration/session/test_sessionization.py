"""
Integration tests for sessionization and session metrics.

Tests sessionization logic, timeout handling, and session metric calculations.
"""
import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestSessionization:
    """Session creation and metrics integration tests."""

    def test_session_pipeline_complete(self, spark, sample_interactions_data):
        """Test complete session analysis pipeline."""
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics,
            calculate_bounce_rate
        )

        # Sessionize interactions (30-minute timeout)
        sessionized_df = sessionize_interactions(
            sample_interactions_data,
            session_timeout_seconds=1800
        )

        assert sessionized_df.count() > 0, "Should create sessions"

        # Verify session columns added
        assert "session_id" in sessionized_df.columns

        # Calculate session metrics
        session_metrics_df = calculate_session_metrics(sessionized_df)

        assert session_metrics_df.count() > 0, "Should calculate session metrics"

        # Verify metrics columns
        expected_cols = ["session_id", "user_id", "session_duration_ms",
                        "action_count", "is_bounce"]
        for col in expected_cols:
            assert col in session_metrics_df.columns, f"Missing column: {col}"

        # Calculate bounce rate
        bounce_rate_df = calculate_bounce_rate(session_metrics_df)

        assert bounce_rate_df.count() > 0, "Should calculate bounce rate"

        # Verify bounce rate is a percentage (0-100)
        bounce_values = bounce_rate_df.select("bounce_rate").collect()
        for row in bounce_values:
            rate = row["bounce_rate"]
            assert 0 <= rate <= 100, f"Bounce rate should be 0-100%, got {rate}"

    def test_sessionization_timeout_logic(self, spark):
        """Test sessionization correctly groups interactions by timeout."""
        from src.transforms.session import sessionize_interactions
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Create data with clear session boundaries
        base_time = datetime(2024, 1, 1, 10, 0, 0)

        data = [
            # Session 1: 3 interactions within 30 minutes
            ("int_1", "user_1", "open", base_time, 100),
            ("int_2", "user_1", "edit", base_time + timedelta(minutes=10), 200),
            ("int_3", "user_1", "save", base_time + timedelta(minutes=20), 150),

            # Session 2: 2 interactions after 35-minute gap
            ("int_4", "user_1", "open", base_time + timedelta(minutes=55), 100),
            ("int_5", "user_1", "close", base_time + timedelta(minutes=60), 50),

            # Different user - Session 3: 1 interaction (bounce)
            ("int_6", "user_2", "open", base_time, 100),
        ]

        df = spark.createDataFrame(data, schema)

        # Sessionize with 30-minute timeout
        sessionized = sessionize_interactions(df, session_timeout_seconds=1800)

        # Should create 3 distinct sessions
        session_count = sessionized.select("session_id").distinct().count()
        assert session_count == 3, f"Expected 3 sessions, got {session_count}"

        # User 1 should have 2 sessions
        user1_sessions = sessionized.filter(F.col("user_id") == "user_1") \
            .select("session_id").distinct().count()
        assert user1_sessions == 2, f"User 1 should have 2 sessions, got {user1_sessions}"

        # User 2 should have 1 session
        user2_sessions = sessionized.filter(F.col("user_id") == "user_2") \
            .select("session_id").distinct().count()
        assert user2_sessions == 1, f"User 2 should have 1 session, got {user2_sessions}"

    def test_session_metrics_accuracy(self, spark):
        """Test session metrics calculations are accurate."""
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics
        )
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # Create session with known metrics
        # Session duration: 20 minutes (from 10:00 to 10:20) + last action duration
        # Action count: 4
        # Not a bounce (more than 1 action)
        data = [
            ("int_1", "user_1", "open", base_time, 100),
            ("int_2", "user_1", "edit", base_time + timedelta(minutes=5), 200),
            ("int_3", "user_1", "save", base_time + timedelta(minutes=10), 150),
            ("int_4", "user_1", "close", base_time + timedelta(minutes=20), 50),
        ]

        df = spark.createDataFrame(data, schema)

        # Sessionize and calculate metrics
        sessionized = sessionize_interactions(df, session_timeout_seconds=1800)
        metrics = calculate_session_metrics(sessionized)

        # Should have 1 session
        assert metrics.count() == 1

        session = metrics.first()

        # Verify metrics
        assert session["action_count"] == 4, f"Expected 4 actions, got {session['action_count']}"
        assert session["is_bounce"] == False, "Should not be a bounce session"

        # Session duration should be 20 minutes (1200000ms) + last action (50ms) = 1200050ms
        assert session["session_duration_ms"] == 1200050

    def test_bounce_detection(self, spark):
        """Test bounce session detection."""
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics
        )
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        data = [
            # Bounce session (only 1 action)
            ("int_1", "user_1", "open", base_time, 100),

            # Non-bounce session (2 actions)
            ("int_2", "user_2", "open", base_time, 100),
            ("int_3", "user_2", "close", base_time + timedelta(minutes=5), 50),
        ]

        df = spark.createDataFrame(data, schema)

        # Calculate metrics
        sessionized = sessionize_interactions(df, session_timeout_seconds=1800)
        metrics = calculate_session_metrics(sessionized)

        # Should have 2 sessions
        assert metrics.count() == 2

        # Check bounce status
        results = {row["user_id"]: row for row in metrics.collect()}

        assert results["user_1"]["is_bounce"] == True, "User 1 session should be bounce"
        assert results["user_2"]["is_bounce"] == False, "User 2 session should not be bounce"

    def test_multiple_sessions_same_user(self, spark):
        """Test user with multiple sessions across different times."""
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics
        )
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # User has 3 sessions throughout the day
        data = [
            # Morning session (10:00-10:15)
            ("int_1", "user_1", "open", base_time, 100),
            ("int_2", "user_1", "edit", base_time + timedelta(minutes=10), 200),
            ("int_3", "user_1", "save", base_time + timedelta(minutes=15), 150),

            # Afternoon session (14:00-14:05) - after 4-hour gap
            ("int_4", "user_1", "open", base_time + timedelta(hours=4), 100),
            ("int_5", "user_1", "close", base_time + timedelta(hours=4, minutes=5), 50),

            # Evening session (20:00) - single action (bounce)
            ("int_6", "user_1", "open", base_time + timedelta(hours=10), 100),
        ]

        df = spark.createDataFrame(data, schema)

        # Sessionize
        sessionized = sessionize_interactions(df, session_timeout_seconds=1800)

        # Should create 3 sessions for same user
        sessions = sessionized.filter(F.col("user_id") == "user_1") \
            .select("session_id").distinct()

        session_count = sessions.count()
        assert session_count == 3, f"Expected 3 sessions for user_1, got {session_count}"

        # Calculate metrics
        metrics = calculate_session_metrics(sessionized)

        # Should have 3 session records
        user_metrics = metrics.filter(F.col("user_id") == "user_1")
        assert user_metrics.count() == 3

        # Verify bounce status
        bounces = user_metrics.filter(F.col("is_bounce") == True).count()
        assert bounces == 1, "Should have 1 bounce session"

        non_bounces = user_metrics.filter(F.col("is_bounce") == False).count()
        assert non_bounces == 2, "Should have 2 non-bounce sessions"

    def test_session_monitoring_integration(self, spark, sample_interactions_data):
        """Test session analysis with monitoring integration."""
        from src.transforms.session import sessionize_interactions, calculate_session_metrics
        from src.utils.monitoring import create_monitoring_context

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_session")

        # Track processing
        context["record_counter"].add(sample_interactions_data.count())

        # Sessionize
        sessionized = sessionize_interactions(sample_interactions_data)

        # Calculate metrics
        metrics = calculate_session_metrics(sessionized)

        assert metrics.count() > 0

        # Verify monitoring tracked records
        assert context["record_counter"].value == 1000

    def test_session_performance(self, spark, sample_interactions_data):
        """Test session analysis completes in reasonable time."""
        import time
        from src.transforms.session import (
            sessionize_interactions, calculate_session_metrics, calculate_bounce_rate
        )

        start_time = time.time()

        # Run complete session analysis
        sessionized = sessionize_interactions(sample_interactions_data)
        session_count = sessionized.count()

        metrics = calculate_session_metrics(sessionized)
        metrics_count = metrics.count()

        bounce = calculate_bounce_rate(metrics)
        bounce_count = bounce.count()

        elapsed_time = time.time() - start_time

        # Should complete in under 30 seconds
        assert elapsed_time < 30, \
            f"Session analysis took {elapsed_time:.2f}s, expected < 30s"
        assert session_count > 0 and metrics_count > 0 and bounce_count > 0
