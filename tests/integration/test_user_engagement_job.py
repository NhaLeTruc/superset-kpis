"""
Integration tests for the user engagement job (Job 02).

Tests the complete user engagement analytics pipeline including:
- DAU/MAU calculations
- Stickiness ratios
- Power user identification
- Cohort retention analysis
"""
import pytest
from pyspark.sql import functions as F
from datetime import datetime, timedelta


class TestUserEngagementJob:
    """Integration tests for user engagement job."""

    def test_engagement_pipeline_complete(self, spark, sample_interactions_data, sample_metadata_data):
        """Test complete user engagement pipeline."""
        from src.transforms.engagement_transforms import (
            calculate_dau, calculate_mau, calculate_stickiness,
            identify_power_users, calculate_cohort_retention
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

        # Verify stickiness is a percentage (0-100)
        stickiness_values = stickiness_df.select("stickiness_ratio").collect()
        for row in stickiness_values:
            ratio = row["stickiness_ratio"]
            assert 0 <= ratio <= 100, f"Stickiness should be 0-100%, got {ratio}"

        # Identify power users
        power_users_df = identify_power_users(
            sample_interactions_data,
            sample_metadata_data,
            percentile=0.90  # Top 10%
        )
        assert power_users_df.count() > 0, "Should identify some power users"

        # Verify power users have expected columns
        expected_cols = ["user_id", "total_interactions", "avg_duration_ms",
                        "country", "device_type", "subscription_type"]
        for col in expected_cols:
            assert col in power_users_df.columns, f"Missing column: {col}"

        # Calculate cohort retention
        cohort_retention_df = calculate_cohort_retention(
            sample_interactions_data,
            sample_metadata_data,
            cohort_period="week",
            retention_weeks=4
        )
        assert cohort_retention_df.count() > 0, "Should calculate cohort retention"

        # Verify retention structure
        assert "cohort_week" in cohort_retention_df.columns
        assert "week_number" in cohort_retention_df.columns
        assert "retention_rate" in cohort_retention_df.columns

    def test_dau_calculation_accuracy(self, spark):
        """Test DAU calculation with known data."""
        from src.transforms.engagement_transforms import calculate_dau
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
        from src.transforms.engagement_transforms import calculate_mau
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
        from src.transforms.engagement_transforms import calculate_stickiness
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
        results = stickiness_df.orderBy("date").collect()
        assert len(results) == 3

        # Check stickiness ratios (DAU/MAU * 100)
        assert results[0]["stickiness_ratio"] == pytest.approx(20.0, rel=0.01)
        assert results[1]["stickiness_ratio"] == pytest.approx(25.0, rel=0.01)
        assert results[2]["stickiness_ratio"] == pytest.approx(30.0, rel=0.01)

    def test_power_user_identification(self, spark, sample_metadata_data):
        """Test power user identification with varied activity levels."""
        from src.transforms.engagement_transforms import identify_power_users
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Create data with clear power users
        data = []
        interaction_id = 0

        # Power user (100 interactions)
        for i in range(100):
            data.append((f"int_{interaction_id}", "user_0", "edit",
                        datetime(2024, 1, 1) + timedelta(hours=i), 200))
            interaction_id += 1

        # Medium user (50 interactions)
        for i in range(50):
            data.append((f"int_{interaction_id}", "user_1", "edit",
                        datetime(2024, 1, 1) + timedelta(hours=i), 150))
            interaction_id += 1

        # Normal users (10 interactions each)
        for user_idx in range(2, 10):
            for i in range(10):
                data.append((f"int_{interaction_id}", f"user_{user_idx}", "open",
                            datetime(2024, 1, 1) + timedelta(hours=i), 100))
                interaction_id += 1

        df = spark.createDataFrame(data, schema)

        # Identify top 10% as power users
        power_users = identify_power_users(df, sample_metadata_data, percentile=0.90)

        # Should identify user_0 (100 interactions) as power user
        power_user_ids = [row["user_id"] for row in power_users.collect()]
        assert "user_0" in power_user_ids, "Top user should be identified as power user"

        # Verify metrics
        user_0_metrics = power_users.filter(F.col("user_id") == "user_0").first()
        assert user_0_metrics["total_interactions"] == 100

    def test_cohort_retention_weekly(self, spark, sample_metadata_data):
        """Test weekly cohort retention calculation."""
        from src.transforms.engagement_transforms import calculate_cohort_retention
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

        schema = StructType([
            StructField("interaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration_ms", IntegerType(), False),
        ])

        # Create cohort data
        # Week 0: 10 users join and are active
        # Week 1: 8 users return (80% retention)
        # Week 2: 6 users return (60% retention)

        base_date = datetime(2024, 1, 1)
        data = []
        interaction_id = 0

        # Week 0: All 10 users active
        for user_idx in range(10):
            data.append((f"int_{interaction_id}", f"user_{user_idx}", "open",
                        base_date, 100))
            interaction_id += 1

        # Week 1: 8 users return
        for user_idx in range(8):
            data.append((f"int_{interaction_id}", f"user_{user_idx}", "edit",
                        base_date + timedelta(days=7), 100))
            interaction_id += 1

        # Week 2: 6 users return
        for user_idx in range(6):
            data.append((f"int_{interaction_id}", f"user_{user_idx}", "save",
                        base_date + timedelta(days=14), 100))
            interaction_id += 1

        df = spark.createDataFrame(data, schema)

        # Calculate retention
        retention_df = calculate_cohort_retention(
            df, sample_metadata_data,
            cohort_period="week",
            retention_weeks=3
        )

        # Verify retention rates
        results = retention_df.orderBy("cohort_week", "week_number").collect()

        # Find week 0, 1, 2 retention rates
        week_0_retention = [r for r in results if r["week_number"] == 0]
        week_1_retention = [r for r in results if r["week_number"] == 1]
        week_2_retention = [r for r in results if r["week_number"] == 2]

        # Week 0 should be 100% (all users active)
        assert len(week_0_retention) > 0
        assert week_0_retention[0]["retention_rate"] == pytest.approx(100.0, rel=0.01)

        # Week 1 should be ~80% (8/10 users)
        if len(week_1_retention) > 0:
            assert week_1_retention[0]["retention_rate"] == pytest.approx(80.0, rel=0.01)

        # Week 2 should be ~60% (6/10 users)
        if len(week_2_retention) > 0:
            assert week_2_retention[0]["retention_rate"] == pytest.approx(60.0, rel=0.01)

    def test_engagement_metrics_with_monitoring(self, spark, sample_interactions_data,
                                               sample_metadata_data):
        """Test engagement calculations with monitoring integration."""
        from src.transforms.engagement_transforms import calculate_dau
        from src.utils.monitoring import create_monitoring_context

        # Create monitoring context
        context = create_monitoring_context(spark.sparkContext, "test_engagement")

        # Calculate DAU
        dau_df = calculate_dau(sample_interactions_data)

        # Track processing
        context["record_counter"].add(sample_interactions_data.count())

        # Verify monitoring
        assert context["record_counter"].value == 1000

    def test_empty_cohort_handling(self, spark, sample_metadata_data):
        """Test cohort retention handles empty cohorts gracefully."""
        from src.transforms.engagement_transforms import calculate_cohort_retention
        from src.schemas.interactions_schema import INTERACTIONS_SCHEMA

        # Empty DataFrame
        empty_df = spark.createDataFrame([], INTERACTIONS_SCHEMA)

        # Should not crash
        retention_df = calculate_cohort_retention(
            empty_df, sample_metadata_data,
            cohort_period="week",
            retention_weeks=4
        )

        # Should return empty result
        assert retention_df.count() == 0

    def test_engagement_performance(self, spark, sample_interactions_data, sample_metadata_data):
        """Test engagement calculations complete in reasonable time."""
        import time
        from src.transforms.engagement_transforms import (
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
