"""
Integration tests for power users and cohort retention.

Tests power user identification and cohort retention analysis.
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F


class TestCohortsAndPowerUsers:
    """Power user and cohort retention integration tests."""

    def test_power_user_identification(self, spark, sample_metadata_data):
        """Test power user identification with varied activity levels."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.engagement import identify_power_users

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        # Create data with clear power users
        data = []
        interaction_id = 0

        # Power user (100 interactions)
        for i in range(100):
            data.append(
                (
                    f"int_{interaction_id}",
                    "user_0",
                    "edit",
                    datetime(2024, 1, 1) + timedelta(hours=i),
                    200,
                )
            )
            interaction_id += 1

        # Medium user (50 interactions)
        for i in range(50):
            data.append(
                (
                    f"int_{interaction_id}",
                    "user_1",
                    "edit",
                    datetime(2024, 1, 1) + timedelta(hours=i),
                    150,
                )
            )
            interaction_id += 1

        # Normal users (10 interactions each)
        for user_idx in range(2, 10):
            for i in range(10):
                data.append(
                    (
                        f"int_{interaction_id}",
                        f"user_{user_idx}",
                        "open",
                        datetime(2024, 1, 1) + timedelta(hours=i),
                        100,
                    )
                )
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
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from src.transforms.engagement import calculate_cohort_retention

        schema = StructType(
            [
                StructField("interaction_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("action_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        # Create cohort data
        # Week 0: 10 users join and are active
        # Week 1: 8 users return (80% retention)
        # Week 2: 6 users return (60% retention)

        base_date = datetime(2024, 1, 1)
        data = []
        interaction_id = 0

        # Week 0: All 10 users active
        for user_idx in range(10):
            data.append((f"int_{interaction_id}", f"user_{user_idx}", "open", base_date, 100))
            interaction_id += 1

        # Week 1: 8 users return
        for user_idx in range(8):
            data.append(
                (
                    f"int_{interaction_id}",
                    f"user_{user_idx}",
                    "edit",
                    base_date + timedelta(days=7),
                    100,
                )
            )
            interaction_id += 1

        # Week 2: 6 users return
        for user_idx in range(6):
            data.append(
                (
                    f"int_{interaction_id}",
                    f"user_{user_idx}",
                    "save",
                    base_date + timedelta(days=14),
                    100,
                )
            )
            interaction_id += 1

        df = spark.createDataFrame(data, schema)

        # Create metadata with registration dates aligned to base_date
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("country", StringType(), False),
                StructField("device_type", StringType(), False),
                StructField("subscription_type", StringType(), False),
                StructField("registration_date", TimestampType(), False),
                StructField("app_version", StringType(), False),
            ]
        )

        metadata_data = []
        for user_idx in range(10):
            metadata_data.append(
                (
                    f"user_{user_idx}",
                    "US",
                    "iOS",
                    "free",
                    base_date,  # All users register on base_date to form a cohort
                    "1.0.0",
                )
            )

        metadata_df = spark.createDataFrame(metadata_data, metadata_schema)

        # Calculate retention
        retention_df = calculate_cohort_retention(
            df, metadata_df, cohort_period="week", retention_weeks=3
        )

        # Verify retention rates
        results = retention_df.orderBy("cohort_week", "week_number").collect()

        # Find week 0, 1, 2 retention rates
        week_0_retention = [r for r in results if r["week_number"] == 0]
        week_1_retention = [r for r in results if r["week_number"] == 1]
        week_2_retention = [r for r in results if r["week_number"] == 2]

        # Week 0 should be 1.0 (all users active)
        assert len(week_0_retention) > 0
        assert week_0_retention[0]["retention_rate"] == pytest.approx(1.0, rel=0.01)

        # Week 1 should be ~0.8 (8/10 users)
        if len(week_1_retention) > 0:
            assert week_1_retention[0]["retention_rate"] == pytest.approx(0.8, rel=0.01)

        # Week 2 should be ~0.6 (6/10 users)
        if len(week_2_retention) > 0:
            assert week_2_retention[0]["retention_rate"] == pytest.approx(0.6, rel=0.01)

    def test_engagement_pipeline_complete(
        self, spark, sample_interactions_data, sample_metadata_data
    ):
        """Test complete user engagement pipeline."""
        from src.transforms.engagement import (
            calculate_cohort_retention,
            calculate_dau,
            calculate_mau,
            calculate_stickiness,
            identify_power_users,
        )

        # Calculate DAU
        dau_df = calculate_dau(sample_interactions_data)
        assert dau_df.count() > 0, "Should calculate DAU for at least one date"

        # Calculate MAU
        mau_df = calculate_mau(sample_interactions_data)
        assert mau_df.count() > 0, "Should calculate MAU for at least one month"

        # Calculate stickiness
        stickiness_df = calculate_stickiness(dau_df, mau_df)
        assert stickiness_df.count() > 0, "Should calculate stickiness"

        # Identify power users
        power_users_df = identify_power_users(
            sample_interactions_data,
            sample_metadata_data,
            percentile=0.90,  # Top 10%
        )
        assert power_users_df.count() > 0, "Should identify some power users"

        # Verify power users have expected columns
        expected_cols = [
            "user_id",
            "total_interactions",
            "avg_duration_ms",
            "country",
            "device_type",
            "subscription_type",
        ]
        for col in expected_cols:
            assert col in power_users_df.columns, f"Missing column: {col}"

        # Calculate cohort retention
        cohort_retention_df = calculate_cohort_retention(
            sample_interactions_data, sample_metadata_data, cohort_period="week", retention_weeks=4
        )
        assert cohort_retention_df.count() > 0, "Should calculate cohort retention"

        # Verify retention structure
        assert "cohort_week" in cohort_retention_df.columns
        assert "week_number" in cohort_retention_df.columns
        assert "retention_rate" in cohort_retention_df.columns

    def test_empty_cohort_handling(self, spark, sample_metadata_data):
        """Test cohort retention handles empty cohorts gracefully."""
        from src.schemas.interactions_schema import INTERACTIONS_SCHEMA
        from src.transforms.engagement import calculate_cohort_retention

        # Empty DataFrame
        empty_df = spark.createDataFrame([], INTERACTIONS_SCHEMA)

        # Should not crash
        retention_df = calculate_cohort_retention(
            empty_df, sample_metadata_data, cohort_period="week", retention_weeks=4
        )

        # Should return empty result
        assert retention_df.count() == 0
