"""
Integration tests for the User Engagement Analytics job (Job 02).

Tests the complete user engagement pipeline including:
- Daily Active Users (DAU)
- Monthly Active Users (MAU)
- Stickiness Ratio (DAU/MAU)
- Power Users identification
- Cohort Retention analysis
"""

from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class TestUserEngagementPipeline:
    """Integration tests for user engagement analytics pipeline."""

    def test_dau_calculation(self, spark):
        """Test Daily Active Users calculation."""
        from src.transforms.engagement import calculate_dau

        # Create test data with known DAU pattern
        data = [
            ("u001", "2024-01-01 10:00:00", 1000),
            ("u001", "2024-01-01 11:00:00", 2000),  # Same user, same day
            ("u002", "2024-01-01 10:00:00", 1500),
            ("u003", "2024-01-01 10:00:00", 1000),
            ("u001", "2024-01-02 10:00:00", 3000),  # New day
            ("u002", "2024-01-02 10:00:00", 2500),
        ]

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_dau(df)

        # Day 1 should have 3 users, Day 2 should have 2 users
        day1 = result.filter(F.col("date") == "2024-01-01").collect()
        day2 = result.filter(F.col("date") == "2024-01-02").collect()

        assert len(day1) == 1
        assert day1[0]["daily_active_users"] == 3

        assert len(day2) == 1
        assert day2[0]["daily_active_users"] == 2

    def test_mau_calculation(self, spark):
        """Test Monthly Active Users calculation."""
        from src.transforms.engagement import calculate_mau

        # Create test data spanning multiple months
        data = []
        base_date = datetime(2024, 1, 15)

        # 10 users in January
        for i in range(10):
            data.append((f"u{i:03d}", base_date))

        # 5 users in February (3 returning, 2 new)
        feb_date = datetime(2024, 2, 15)
        for i in range(5):
            data.append((f"u{i:03d}", feb_date))

        df = spark.createDataFrame(data, ["user_id", "timestamp"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_mau(df)

        # January should have 10 MAU
        jan = result.filter(F.col("month") == "2024-01").collect()
        assert len(jan) == 1
        assert jan[0]["monthly_active_users"] == 10

        # February should have 5 MAU
        feb = result.filter(F.col("month") == "2024-02").collect()
        assert len(feb) == 1
        assert feb[0]["monthly_active_users"] == 5

    def test_stickiness_calculation(self, spark):
        """Test Stickiness Ratio (DAU/MAU) calculation."""
        from src.transforms.engagement import calculate_dau, calculate_mau, calculate_stickiness

        # Create data where we know the expected stickiness
        data = []
        base_date = datetime(2024, 1, 1)

        # 10 users active every day in January (high stickiness)
        for day in range(30):
            for user_idx in range(10):
                data.append((f"u{user_idx:03d}", base_date + timedelta(days=day), 1000))

        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        dau_df = calculate_dau(df)
        mau_df = calculate_mau(df)
        stickiness_df = calculate_stickiness(dau_df, mau_df)

        # With 10 DAU and 10 MAU, stickiness should be ~1.0 (100%)
        jan_stickiness = stickiness_df.filter(F.col("month") == "2024-01").collect()
        assert len(jan_stickiness) > 0

        # Average stickiness should be close to 1.0
        avg_stickiness = stickiness_df.agg(F.avg("stickiness_ratio")).collect()[0][0]
        assert avg_stickiness > 0.9  # Should be very high

    def test_power_users_identification(self, spark):
        """Test power users identification."""
        from src.transforms.engagement import identify_power_users

        # Create test data with clear power users
        data = []
        base_date = datetime(2024, 1, 1)

        # Power users (high activity) - 100 interactions each
        for i in range(5):
            for j in range(100):
                data.append(
                    (
                        f"power_{i}",
                        base_date + timedelta(hours=j),
                        5000,  # duration_ms
                    )
                )

        # Normal users (low activity) - 5 interactions each
        for i in range(95):
            for j in range(5):
                data.append(
                    (
                        f"normal_{i}",
                        base_date + timedelta(hours=j),
                        1000,
                    )
                )

        schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        interactions_df = spark.createDataFrame(data, schema)

        # Create minimal metadata
        users = [(f"power_{i}",) for i in range(5)] + [(f"normal_{i}",) for i in range(95)]
        metadata_df = spark.createDataFrame(users, ["user_id"])

        result = identify_power_users(interactions_df, metadata_df, percentile=0.95)

        # Power users should be identified
        power_users = result.filter(F.col("user_id").like("power_%")).count()
        assert power_users >= 3  # At least 3 of 5 power users should be identified

    def test_cohort_retention(self, spark):
        """Test cohort retention calculation."""
        from src.transforms.engagement import calculate_cohort_retention

        # Create data with known retention pattern
        data = []

        # Week 0 cohort: 100 users register
        cohort_date = datetime(2024, 1, 1)
        for i in range(100):
            # All 100 active in week 0
            data.append((f"u{i:03d}", cohort_date, cohort_date, 1000))

            # 50 return in week 1
            if i < 50:
                data.append((f"u{i:03d}", cohort_date, cohort_date + timedelta(weeks=1), 1000))

            # 25 return in week 2
            if i < 25:
                data.append((f"u{i:03d}", cohort_date, cohort_date + timedelta(weeks=2), 1000))

        interactions_df = spark.createDataFrame(
            [(d[0], d[2], d[3]) for d in data], ["user_id", "timestamp", "duration_ms"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        metadata_df = (
            spark.createDataFrame([(d[0], d[1]) for d in data], ["user_id", "registration_date"])
            .withColumn("registration_date", F.to_timestamp("registration_date"))
            .distinct()
        )

        result = calculate_cohort_retention(
            interactions_df, metadata_df, cohort_period="week", retention_weeks=4
        )

        # Verify retention rates exist
        assert result.count() > 0

        # Week 0 should have 100% retention (returned as decimal 1.0)
        week_0 = result.filter(F.col("week_number") == 0).collect()
        if week_0:
            assert week_0[0]["retention_rate"] == 1.0


class TestUserEngagementDataQuality:
    """Integration tests for data quality in user engagement pipeline."""

    def test_handles_empty_dataframe(self, spark):
        """Test that pipeline handles empty DataFrame gracefully."""
        from src.transforms.engagement import calculate_dau

        schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("duration_ms", IntegerType(), False),
            ]
        )

        empty_df = spark.createDataFrame([], schema)
        result = calculate_dau(empty_df)

        assert result.count() == 0

    def test_handles_single_user(self, spark):
        """Test calculation with single user."""
        from src.transforms.engagement import calculate_dau

        data = [("u001", "2024-01-01 10:00:00", 1000)]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_dau(df)

        assert result.count() == 1
        assert result.collect()[0]["daily_active_users"] == 1

    def test_handles_duplicate_interactions(self, spark):
        """Test that duplicate interactions don't inflate counts."""
        from src.transforms.engagement import calculate_dau

        # Same user, same timestamp multiple times
        data = [
            ("u001", "2024-01-01 10:00:00", 1000),
            ("u001", "2024-01-01 10:00:00", 2000),
            ("u001", "2024-01-01 10:00:00", 1500),
        ]
        df = spark.createDataFrame(data, ["user_id", "timestamp", "duration_ms"]).withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )

        result = calculate_dau(df)

        # Should still be 1 DAU
        assert result.collect()[0]["daily_active_users"] == 1
