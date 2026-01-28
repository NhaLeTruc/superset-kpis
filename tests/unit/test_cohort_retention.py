"""
Unit tests for cohort retention analysis.

Tests the calculate_cohort_retention function which calculates user retention
rates by cohort over time.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class TestCalculateCohortRetention:
    """Test suite for calculate_cohort_retention function."""

    def test_basic_cohort_retention(self, spark):
        """Test basic cohort retention calculation with simple data."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        # Create test interactions
        interactions_data = [
            # User 1: registered week 1, active week 0, 1, 2
            ("u001", "2023-01-02 10:00:00"),  # Week 0
            ("u001", "2023-01-09 10:00:00"),  # Week 1
            ("u001", "2023-01-16 10:00:00"),  # Week 2
            # User 2: registered week 1, active week 0 only
            ("u002", "2023-01-02 10:00:00"),  # Week 0
        ]
        interactions_df = spark.createDataFrame(
            interactions_data, ["user_id", "timestamp"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        # Create test metadata with registration dates
        metadata_data = [
            ("u001", "2023-01-02"),  # Week 1 cohort
            ("u002", "2023-01-02"),  # Week 1 cohort
        ]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        # Calculate retention
        result = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=4)

        # Verify schema
        assert "cohort_week" in result.columns
        assert "week_number" in result.columns
        assert "cohort_size" in result.columns
        assert "active_users" in result.columns
        assert "retention_rate" in result.columns

        # Verify results
        result_rows = result.orderBy("week_number").collect()
        assert len(result_rows) == 4  # 4 weeks of retention data

        # Week 0 should have 100% retention (both users active)
        week_0 = next(r for r in result_rows if r["week_number"] == 0)
        assert week_0["cohort_size"] == 2
        assert week_0["active_users"] == 2
        assert week_0["retention_rate"] == 1.0

        # Week 1 should have 50% retention (only u001)
        week_1 = next(r for r in result_rows if r["week_number"] == 1)
        assert week_1["active_users"] == 1
        assert week_1["retention_rate"] == 0.5

    def test_empty_interactions_returns_empty_dataframe(self, spark):
        """Test that empty interactions return empty DataFrame with correct schema."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        # Create empty interactions
        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )
        interactions_df = spark.createDataFrame([], schema)

        # Create metadata
        metadata_data = [("u001", "2023-01-01")]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        # Calculate retention
        result = calculate_cohort_retention(interactions_df, metadata_df)

        # Verify empty result with correct schema
        assert result.count() == 0
        assert "cohort_week" in result.columns
        assert "retention_rate" in result.columns

    def test_multiple_cohorts(self, spark):
        """Test retention with multiple cohort weeks."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        # Users from different cohort weeks
        interactions_data = [
            # Cohort 1 (Jan 2)
            ("u001", "2023-01-02 10:00:00"),
            ("u001", "2023-01-09 10:00:00"),
            # Cohort 2 (Jan 9)
            ("u002", "2023-01-09 10:00:00"),
            ("u002", "2023-01-16 10:00:00"),
        ]
        interactions_df = spark.createDataFrame(
            interactions_data, ["user_id", "timestamp"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        metadata_data = [
            ("u001", "2023-01-02"),  # Cohort week 1
            ("u002", "2023-01-09"),  # Cohort week 2
        ]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        # Calculate retention
        result = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=3)

        # Should have data for 2 cohorts x 3 weeks = 6 rows
        cohort_weeks = result.select("cohort_week").distinct().count()
        assert cohort_weeks == 2

    def test_retention_weeks_parameter(self, spark):
        """Test that retention_weeks parameter limits the analysis period."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        # Create test data
        interactions_data = [
            ("u001", "2023-01-02 10:00:00"),
        ]
        interactions_df = spark.createDataFrame(
            interactions_data, ["user_id", "timestamp"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        metadata_data = [("u001", "2023-01-02")]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        # Test with different retention_weeks values
        result_10 = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=10)
        result_5 = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=5)

        # Each should have rows for 0 to retention_weeks-1
        assert result_10.count() == 10
        assert result_5.count() == 5

    def test_retention_rate_is_decimal(self, spark):
        """Test that retention_rate is expressed as decimal (0.0 to 1.0)."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        interactions_data = [
            ("u001", "2023-01-02 10:00:00"),
            ("u002", "2023-01-02 10:00:00"),
            ("u003", "2023-01-02 10:00:00"),
            ("u004", "2023-01-02 10:00:00"),
        ]
        interactions_df = spark.createDataFrame(
            interactions_data, ["user_id", "timestamp"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        metadata_data = [
            ("u001", "2023-01-02"),
            ("u002", "2023-01-02"),
            ("u003", "2023-01-02"),
            ("u004", "2023-01-02"),
        ]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        result = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=2)

        # Week 0 retention should be 1.0 (100%)
        week_0 = result.filter("week_number = 0").collect()[0]
        assert week_0["retention_rate"] == 1.0
        assert 0.0 <= week_0["retention_rate"] <= 1.0

    def test_fills_missing_weeks_with_zero(self, spark):
        """Test that weeks with no activity are filled with zero retention."""
        from src.transforms.engagement.cohort_retention import calculate_cohort_retention

        # User only active in week 0 and week 3 (skip weeks 1, 2)
        interactions_data = [
            ("u001", "2023-01-02 10:00:00"),  # Week 0
            ("u001", "2023-01-23 10:00:00"),  # Week 3
        ]
        interactions_df = spark.createDataFrame(
            interactions_data, ["user_id", "timestamp"]
        ).withColumn("timestamp", F.to_timestamp("timestamp"))

        metadata_data = [("u001", "2023-01-02")]
        metadata_df = spark.createDataFrame(
            metadata_data, ["user_id", "registration_date"]
        ).withColumn("registration_date", F.to_timestamp("registration_date"))

        result = calculate_cohort_retention(interactions_df, metadata_df, retention_weeks=5)

        # Weeks 1 and 2 should have 0 active users
        week_1 = result.filter("week_number = 1").collect()[0]
        week_2 = result.filter("week_number = 2").collect()[0]

        assert week_1["active_users"] == 0
        assert week_1["retention_rate"] == 0.0
        assert week_2["active_users"] == 0
        assert week_2["retention_rate"] == 0.0
