"""
Integration tests for session metrics calculation.

Tests session_window() based sessionization logic, timeout handling, and session metric calculations.
"""

from datetime import datetime, timedelta

from pyspark.sql import functions as F


class TestSessionization:
    """Session creation and metrics integration tests."""

    def test_session_pipeline_complete(self, spark, sample_interactions_data):
        """Test complete session analysis pipeline."""
        from src.transforms.session import calculate_bounce_rate, calculate_session_metrics

        # Calculate session metrics directly from interactions
        session_metrics_df = calculate_session_metrics(
            sample_interactions_data, session_timeout="1800 seconds"
        )

        assert session_metrics_df.count() > 0, "Should calculate session metrics"

        # Verify metrics columns
        expected_cols = [
            "session_id",
            "user_id",
            "session_duration_ms",
            "action_count",
            "is_bounce",
        ]
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
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.session import calculate_session_metrics

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

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

        # Calculate session metrics with 30-minute timeout
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should create 3 distinct sessions
        session_count = metrics.count()
        assert session_count == 3, f"Expected 3 sessions, got {session_count}"

        # User 1 should have 2 sessions
        user1_sessions = metrics.filter(F.col("user_id") == "user_1").count()
        assert user1_sessions == 2, f"User 1 should have 2 sessions, got {user1_sessions}"

        # User 2 should have 1 session
        user2_sessions = metrics.filter(F.col("user_id") == "user_2").count()
        assert user2_sessions == 1, f"User 2 should have 1 session, got {user2_sessions}"

    def test_session_metrics_accuracy(self, spark):
        """Test session metrics calculations are accurate."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.session import calculate_session_metrics

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # Create session with known metrics
        # Action count: 4
        # Not a bounce (more than 1 action)
        data = [
            ("int_1", "user_1", "open", base_time, 100),
            ("int_2", "user_1", "edit", base_time + timedelta(minutes=5), 200),
            ("int_3", "user_1", "save", base_time + timedelta(minutes=10), 150),
            ("int_4", "user_1", "close", base_time + timedelta(minutes=20), 50),
        ]

        df = spark.createDataFrame(data, schema)

        # Calculate metrics
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should have 1 session
        assert metrics.count() == 1

        session = metrics.first()

        # Verify metrics
        assert session["action_count"] == 4, f"Expected 4 actions, got {session['action_count']}"
        assert not session["is_bounce"], "Should not be a bounce session"

        # Session duration should be positive
        assert session["session_duration_ms"] > 0, "Session duration should be positive"

    def test_bounce_detection(self, spark):
        """Test bounce session detection."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.session import calculate_session_metrics

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

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
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should have 2 sessions
        assert metrics.count() == 2

        # Check bounce status
        results = {row["user_id"]: row for row in metrics.collect()}

        assert results["user_1"]["is_bounce"], "User 1 session should be bounce"
        assert not results["user_2"]["is_bounce"], "User 2 session should not be bounce"

    def test_multiple_sessions_same_user(self, spark):
        """Test user with multiple sessions across different times."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.session import calculate_session_metrics

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

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

        # Calculate metrics
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")

        # Should create 3 sessions for same user
        user_metrics = metrics.filter(F.col("user_id") == "user_1")
        session_count = user_metrics.count()
        assert session_count == 3, f"Expected 3 sessions for user_1, got {session_count}"

        # Verify bounce status
        bounces = user_metrics.filter(F.col("is_bounce")).count()
        assert bounces == 1, "Should have 1 bounce session"

        non_bounces = user_metrics.filter(~F.col("is_bounce")).count()
        assert non_bounces == 2, "Should have 2 non-bounce sessions"

    def test_session_monitoring_integration(self, spark, sample_interactions_data):
        """Test session analysis with monitoring integration."""
        from src.transforms.session import calculate_session_metrics
        from src.utils.monitoring import create_monitoring_context

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_session")

        # Track processing
        context["record_counter"].add(sample_interactions_data.count())

        # Calculate metrics directly
        metrics = calculate_session_metrics(
            sample_interactions_data, session_timeout="1800 seconds"
        )

        assert metrics.count() > 0

        # Verify monitoring tracked records
        assert context["record_counter"].value == 1000

    def test_session_performance(self, spark, sample_interactions_data):
        """Test session analysis completes in reasonable time."""
        import time

        from src.transforms.session import calculate_bounce_rate, calculate_session_metrics

        start_time = time.time()

        # Run complete session analysis
        metrics = calculate_session_metrics(
            sample_interactions_data, session_timeout="1800 seconds"
        )
        metrics_count = metrics.count()

        bounce = calculate_bounce_rate(metrics)
        bounce_count = bounce.count()

        elapsed_time = time.time() - start_time

        # Should complete in under 30 seconds
        assert elapsed_time < 30, f"Session analysis took {elapsed_time:.2f}s, expected < 30s"
        assert metrics_count > 0 and bounce_count > 0

    def test_session_id_format(self, spark):
        """Test that session_id is generated with timestamp-based format."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.session import calculate_session_metrics

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        base_time = datetime(2024, 1, 15, 10, 30, 45)

        data = [
            ("int_1", "user_1", "open", base_time, 100),
            ("int_2", "user_1", "close", base_time + timedelta(minutes=5), 50),
        ]

        df = spark.createDataFrame(data, schema)

        # Calculate metrics
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")

        session = metrics.first()
        session_id = session["session_id"]

        # Session ID should be in format: user_id_YYYYMMDDHHmmss
        assert session_id.startswith("user_1_"), (
            f"Session ID should start with user_id_, got {session_id}"
        )
        assert "20240115" in session_id, f"Session ID should contain date, got {session_id}"
