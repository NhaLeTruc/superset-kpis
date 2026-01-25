"""
Integration tests for DAU/MAU/stickiness calculations.

Tests daily active users, monthly active users, and stickiness ratio metrics.
"""
import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestDAUMAU:
    """DAU/MAU and stickiness integration tests."""

    def test_dau_calculation_accuracy(self, spark):
        """Test DAU calculation with known data."""
        from src.transforms.engagement import calculate_dau
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

        # Create specific test data
        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Day 1: 3 unique users
        # Day 2: 2 unique users (user_1 appears both days)
        data = [
            ("int_1", "user_1", "open", datetime(2024, 1, 1, 10, 0), 100),
            ("int_2", "user_2", "open", datetime(2024, 1, 1, 11, 0), 100),
            ("int_3", "user_3", "open", datetime(2024, 1, 1, 12, 0), 100),
            ("int_4", "user_1", "edit", datetime(2024, 1, 2, 10, 0), 100),  # Same user, next day
            ("int_5", "user_4", "open", datetime(2024, 1, 2, 11, 0), 100),
        ]

        df = spark.createDataFrame(data, schema)
        dau_df = calculate_dau(df)

        # Collect results
        dau_results = dau_df.orderBy("date").collect()

        # Verify counts
        assert len(dau_results) == 2, "Should have 2 days"
        assert dau_results[0]["daily_active_users"] == 3, "Day 1 should have 3 DAU"
        assert dau_results[1]["daily_active_users"] == 2, "Day 2 should have 2 DAU"

    def test_mau_calculation_accuracy(self, spark):
        """Test MAU calculation with known data."""
        from src.transforms.engagement import calculate_mau
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Create data spanning January and February
        data = []
        for i in range(5):
            # January interactions (5 unique users)
            data.append((f"jan_{i}", f"user_{i}", "open",
                        datetime(2024, 1, 15), 100))

        for i in range(3):
            # February interactions (3 unique users, 2 overlap with January)
            data.append((f"feb_{i}", f"user_{i}", "open",
                        datetime(2024, 2, 15), 100))

        df = spark.createDataFrame(data, schema)
        mau_df = calculate_mau(df)

        # Collect results
        mau_results = mau_df.orderBy("month").collect()

        # Verify counts
        assert len(mau_results) == 2, "Should have 2 months"
        assert mau_results[0]["monthly_active_users"] == 5, "January should have 5 MAU"
        assert mau_results[1]["monthly_active_users"] == 3, "February should have 3 MAU"

    def test_stickiness_ratio_calculation(self, spark):
        """Test stickiness ratio calculation."""
        from src.transforms.engagement import calculate_stickiness
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        # Create test DAU and MAU data
        dau_schema = StructType([
            StructField("date", StringType(), False),
            StructField("daily_active_users", IntegerType(), False),
        ])

        mau_schema = StructType([
            StructField("month", StringType(), False),
            StructField("monthly_active_users", IntegerType(), False),
        ])

        # January has MAU of 100, DAU ranges from 20-30
        dau_data = [
            ("2024-01-15", 20),
            ("2024-01-16", 25),
            ("2024-01-17", 30),
        ]

        mau_data = [
            ("2024-01", 100),
        ]

        dau_df = spark.createDataFrame(dau_data, dau_schema)
        mau_df = spark.createDataFrame(mau_data, mau_schema)

        stickiness_df = calculate_stickiness(dau_df, mau_df)

        # Verify calculations
        results = stickiness_df.orderBy("month").collect()
        assert len(results) == 1  # Only one month

        # Check stickiness ratio (avg DAU/MAU)
        # Avg DAU = (20 + 25 + 30) / 3 = 25
        # Stickiness = 25 / 100 = 0.25
        assert results[0]["stickiness_ratio"] == pytest.approx(0.25, rel=0.01)

    def test_engagement_metrics_complete(self, spark, sample_interactions_data, sample_metadata_data):
        """Test DAU/MAU/stickiness pipeline."""
        from src.transforms.engagement import (
            calculate_dau, calculate_mau, calculate_stickiness
        )

        # Calculate DAU
        dau_df = calculate_dau(sample_interactions_data)
        assert dau_df.count() > 0, "Should calculate DAU for at least one date"

        # Verify DAU structure
        assert "date" in dau_df.columns
        assert "daily_active_users" in dau_df.columns

        # Calculate MAU
        mau_df = calculate_mau(sample_interactions_data)
        assert mau_df.count() > 0, "Should calculate MAU for at least one month"

        # Verify MAU structure
        assert "month" in mau_df.columns
        assert "monthly_active_users" in mau_df.columns

        # Calculate stickiness
        stickiness_df = calculate_stickiness(dau_df, mau_df)
        assert stickiness_df.count() > 0, "Should calculate stickiness"

        # Verify stickiness is a decimal ratio (0.0-1.0)
        stickiness_values = stickiness_df.select("stickiness_ratio").collect()
        for row in stickiness_values:
            ratio = row["stickiness_ratio"]
            assert 0 <= ratio <= 1, f"Stickiness should be 0.0-1.0, got {ratio}"

    def test_engagement_metrics_with_monitoring(self, spark, sample_interactions_data,
                                               sample_metadata_data):
        """Test engagement calculations with monitoring integration."""
        from src.transforms.engagement import calculate_dau
        from src.utils.monitoring import create_monitoring_context

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_engagement")

        # Calculate DAU
        dau_df = calculate_dau(sample_interactions_data)

        # Track processing
        context["record_counter"].add(sample_interactions_data.count())

        # Verify monitoring
        assert context["record_counter"].value == 1000

    def test_engagement_performance(self, spark, sample_interactions_data, sample_metadata_data):
        """Test engagement calculations complete in reasonable time."""
        import time
        from src.transforms.engagement import (
            calculate_dau, calculate_mau, identify_power_users
        )

        start_time = time.time()

        # Run all engagement calculations
        dau_df = calculate_dau(sample_interactions_data)
        dau_count = dau_df.count()

        mau_df = calculate_mau(sample_interactions_data)
        mau_count = mau_df.count()

        power_users_df = identify_power_users(
            sample_interactions_data,
            sample_metadata_data,
            percentile=0.90
        )
        power_count = power_users_df.count()

        elapsed_time = time.time() - start_time

        # Should complete in under 30 seconds
        assert elapsed_time < 30, f"Engagement calculations took {elapsed_time:.2f}s, expected < 30s"
        assert dau_count > 0 and mau_count > 0 and power_count > 0
