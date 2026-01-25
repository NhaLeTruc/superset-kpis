"""
Integration tests for bounce rate calculations.

Tests bounce rate analysis and grouped bounce rate calculations.
"""
import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestBounceRates:
    """Bounce rate calculation integration tests."""

    def test_bounce_rate_calculation(self, spark):
        """Test bounce rate calculation."""
        from src.transforms.session import calculate_session_metrics, calculate_bounce_rate
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # Create 10 sessions: 3 bounces, 7 non-bounces = 30% bounce rate
        data = []

        # 3 bounce sessions
        for i in range(3):
            data.append((f"bounce_{i}", f"user_bounce_{i}", "open", base_time, 100))

        # 7 non-bounce sessions
        for i in range(7):
            data.append((f"non_bounce_{i}_1", f"user_active_{i}", "open", base_time, 100))
            data.append((f"non_bounce_{i}_2", f"user_active_{i}", "edit",
                        base_time + timedelta(minutes=5), 200))

        df = spark.createDataFrame(data, schema)

        # Calculate bounce rate
        metrics = calculate_session_metrics(df, session_timeout="1800 seconds")
        bounce_rate = calculate_bounce_rate(metrics)

        # Should have 1 overall bounce rate
        assert bounce_rate.count() == 1

        rate = bounce_rate.first()["bounce_rate"]

        # Should be 30% (3 bounces out of 10 sessions)
        assert rate == pytest.approx(30.0, abs=1.0)

    def test_grouped_bounce_rate(self, spark, sample_metadata_data):
        """Test bounce rate calculation grouped by dimensions."""
        from src.transforms.session import calculate_session_metrics, calculate_bounce_rate
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        interaction_schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        base_time = datetime(2024, 1, 1, 10, 0, 0)

        # Create sessions for different users with known bounce rates
        data = []

        # iOS users (low bounce rate: 1 bounce, 4 active)
        for i in range(5):
            user_id = f"user_{i}"
            if i == 0:  # First user bounces
                data.append((f"int_ios_{i}", user_id, "open", base_time, 100))
            else:  # Others have multiple actions
                data.append((f"int_ios_{i}_1", user_id, "open", base_time, 100))
                data.append((f"int_ios_{i}_2", user_id, "edit",
                            base_time + timedelta(minutes=5), 200))

        df = spark.createDataFrame(data, interaction_schema)

        # Add device type from metadata
        enriched_df = df.join(sample_metadata_data, "user_id", "left")

        # Calculate session metrics and bounce rate by device type
        metrics = calculate_session_metrics(enriched_df, session_timeout="1800 seconds")
        bounce_by_device = calculate_bounce_rate(metrics, group_by_columns=["device_type"])

        # Should have bounce rates for different device types
        assert bounce_by_device.count() > 0
