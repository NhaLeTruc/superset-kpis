"""
Unit tests for cohort retention analysis.

Tests calculate_cohort_retention() function from engagement transforms.
"""

from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from src.transforms.engagement import calculate_cohort_retention


class TestCalculateCohortRetention:
    """Tests for calculate_cohort_retention() function."""

    def test_calculate_cohort_retention_perfect(self, spark):
        """
        GIVEN:
            - Cohort of 10 users joined on 2023-01-01
            - All 10 users active every week for 4 weeks
        WHEN: calculate_cohort_retention() is called
        THEN: All weeks have 100% retention
        """
        # Arrange - metadata (join dates)
        metadata_data = [(f"u{i:03d}", date(2023, 1, 1)) for i in range(1, 11)]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Interactions - all users active all 4 weeks
        interactions_data = []
        for user_num in range(1, 11):
            user_id = f"u{user_num:03d}"
            for week in range(4):
                interactions_data.append(
                    (user_id, datetime(2023, 1, 1, 10, 0, 0) + timedelta(weeks=week))
                )

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df, cohort_period="week", retention_weeks=4
        )

        # Assert
        results = result_df.orderBy("week_number").collect()
        for week in range(4):
            assert results[week]["retention_rate"] == 1.0

    def test_calculate_cohort_retention_declining(self, spark):
        """
        GIVEN:
            - Cohort of 100 users joined on 2023-01-01
            - Week 0: 100 active, Week 1: 80, Week 2: 60, Week 3: 50
        WHEN: calculate_cohort_retention() is called
        THEN: Retention rates are 100%, 80%, 60%, 50%
        """
        # Arrange - metadata
        metadata_data = [(f"u{i:03d}", date(2023, 1, 1)) for i in range(1, 101)]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Interactions
        interactions_data = []
        # Week 0: all 100 users
        for i in range(1, 101):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 1, 10, 0, 0)))
        # Week 1: 80 users (u001-u080)
        for i in range(1, 81):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 8, 10, 0, 0)))
        # Week 2: 60 users (u001-u060)
        for i in range(1, 61):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 15, 10, 0, 0)))
        # Week 3: 50 users (u001-u050)
        for i in range(1, 51):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 22, 10, 0, 0)))

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df, cohort_period="week", retention_weeks=4
        )

        # Assert
        results = result_df.orderBy("week_number").collect()
        assert results[0]["retention_rate"] == 1.0
        assert results[1]["retention_rate"] == 0.8
        assert results[2]["retention_rate"] == 0.6
        assert results[3]["retention_rate"] == 0.5

    def test_calculate_cohort_retention_multiple_cohorts(self, spark):
        """
        GIVEN:
            - Cohort A: 50 users joined 2023-01-01
            - Cohort B: 30 users joined 2023-01-08
        WHEN: calculate_cohort_retention() is called
        THEN:
            - Returns separate retention curves for each cohort
            - Cohort sizes are correct (50 and 30)
        """
        # Arrange - metadata
        metadata_data = []
        # Cohort A: u001-u050
        for i in range(1, 51):
            metadata_data.append((f"u{i:03d}", date(2023, 1, 1)))
        # Cohort B: u051-u080
        for i in range(51, 81):
            metadata_data.append((f"u{i:03d}", date(2023, 1, 8)))

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Interactions - all users active week 0
        interactions_data = []
        for i in range(1, 51):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 1, 10, 0, 0)))
        for i in range(51, 81):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 8, 10, 0, 0)))

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df, cohort_period="week", retention_weeks=2
        )

        # Assert
        # Note: date_trunc("week") returns Monday of the week
        # 2023-01-01 (Sunday) → 2022-12-26 (Monday)
        # 2023-01-08 (Sunday) → 2023-01-02 (Monday)
        cohort_a_results = result_df.filter(F.col("cohort_week") == date(2022, 12, 26)).collect()
        cohort_b_results = result_df.filter(F.col("cohort_week") == date(2023, 1, 2)).collect()

        assert len(cohort_a_results) > 0
        assert cohort_a_results[0]["cohort_size"] == 50

        assert len(cohort_b_results) > 0
        assert cohort_b_results[0]["cohort_size"] == 30

    def test_calculate_cohort_retention_inactive_week(self, spark):
        """
        GIVEN:
            - User u001 joined 2023-01-01
            - Active in week 0, 1, 3 (but NOT week 2)
        WHEN: calculate_cohort_retention() is called
        THEN:
            - u001 counted as active in weeks 0, 1, 3
            - u001 NOT counted in week 2
        """
        # Arrange
        metadata_data = [("u001", date(2023, 1, 1))]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0)),  # Week 0
            ("u001", datetime(2023, 1, 8, 10, 0, 0)),  # Week 1
            # NO interaction in week 2 (2023-01-15)
            ("u001", datetime(2023, 1, 22, 10, 0, 0)),  # Week 3
        ]
        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df, cohort_period="week", retention_weeks=4
        )

        # Assert
        results = result_df.orderBy("week_number").collect()
        assert results[0]["retained_users"] == 1  # Week 0
        assert results[1]["retained_users"] == 1  # Week 1
        assert results[2]["retained_users"] == 0  # Week 2 - NOT active
        assert results[3]["retained_users"] == 1  # Week 3 - back


class TestCalculateCohortRetentionBySegment:
    """Tests for calculate_cohort_retention_by_segment() function."""

    def test_returns_all_three_segment_types(self, spark):
        """
        GIVEN: Users with different subscription types, devices, and countries
        WHEN: calculate_cohort_retention_by_segment() is called
        THEN: Results contain rows for all three segment_type values
        """
        from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

        from src.transforms.engagement import calculate_cohort_retention_by_segment

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=True),
                StructField("device_type", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
            ]
        )
        from datetime import date, datetime

        metadata_data = [
            ("u001", date(2023, 1, 2), "free", "iPhone", "US"),
            ("u002", date(2023, 1, 2), "premium", "iPad", "UK"),
            ("u003", date(2023, 1, 2), "free", "Android Phone", "US"),
        ]
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        interactions_data = [
            ("u001", datetime(2023, 1, 2, 10, 0, 0)),
            ("u002", datetime(2023, 1, 2, 11, 0, 0)),
            ("u003", datetime(2023, 1, 9, 10, 0, 0)),
        ]
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        result_df = calculate_cohort_retention_by_segment(
            interactions_df, metadata_df, retention_weeks=2
        )

        segment_types = {row["segment_type"] for row in result_df.select("segment_type").collect()}
        assert "subscription_type" in segment_types
        assert "device_type" in segment_types
        assert "country" in segment_types

    def test_segment_retention_rates_are_correct(self, spark):
        """
        GIVEN:
            - 4 free users and 2 premium users, all in cohort week 2023-01-02
            - Week 0: all 6 active
            - Week 1: 2 free active, 1 premium active
        WHEN: calculate_cohort_retention_by_segment() is called
        THEN:
            - subscription_type=free  W0=1.0, W1=0.5
            - subscription_type=premium W0=1.0, W1=0.5
        """
        from datetime import date, datetime

        from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

        from src.transforms.engagement import calculate_cohort_retention_by_segment

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=True),
                StructField("device_type", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
            ]
        )
        metadata_data = [
            ("u001", date(2023, 1, 2), "free", "iPhone", "US"),
            ("u002", date(2023, 1, 2), "free", "iPhone", "US"),
            ("u003", date(2023, 1, 2), "free", "iPhone", "US"),
            ("u004", date(2023, 1, 2), "free", "iPhone", "US"),
            ("u005", date(2023, 1, 2), "premium", "iPad", "US"),
            ("u006", date(2023, 1, 2), "premium", "iPad", "US"),
        ]
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )
        # Week 0: all 6 active (2023-01-02)
        # Week 1: u001, u002 (free) + u005 (premium)
        interactions_data = [
            ("u001", datetime(2023, 1, 2, 10, 0, 0)),
            ("u002", datetime(2023, 1, 2, 10, 0, 0)),
            ("u003", datetime(2023, 1, 2, 10, 0, 0)),
            ("u004", datetime(2023, 1, 2, 10, 0, 0)),
            ("u005", datetime(2023, 1, 2, 10, 0, 0)),
            ("u006", datetime(2023, 1, 2, 10, 0, 0)),
            ("u001", datetime(2023, 1, 9, 10, 0, 0)),
            ("u002", datetime(2023, 1, 9, 10, 0, 0)),
            ("u005", datetime(2023, 1, 9, 10, 0, 0)),
        ]
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        result_df = calculate_cohort_retention_by_segment(
            interactions_df, metadata_df, retention_weeks=2
        )

        from pyspark.sql import functions as F

        free_rows = (
            result_df.filter(
                (F.col("segment_type") == "subscription_type") & (F.col("segment_value") == "free")
            )
            .orderBy("week_number")
            .collect()
        )
        premium_rows = (
            result_df.filter(
                (F.col("segment_type") == "subscription_type")
                & (F.col("segment_value") == "premium")
            )
            .orderBy("week_number")
            .collect()
        )

        assert free_rows[0]["retention_rate"] == pytest.approx(1.0)
        assert free_rows[1]["retention_rate"] == pytest.approx(0.5)
        assert premium_rows[0]["retention_rate"] == pytest.approx(1.0)
        assert premium_rows[1]["retention_rate"] == pytest.approx(0.5)

    def test_empty_interactions_returns_empty(self, spark):
        """
        GIVEN: Empty interactions DataFrame
        WHEN: calculate_cohort_retention_by_segment() is called
        THEN: Returns empty DataFrame with correct schema
        """
        from datetime import date

        from pyspark.sql.types import DateType, StringType, StructField, StructType

        from src.schemas.interactions_schema import INTERACTIONS_SCHEMA
        from src.transforms.engagement import calculate_cohort_retention_by_segment

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("registration_date", DateType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=True),
                StructField("device_type", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
            ]
        )
        metadata_df = spark.createDataFrame(
            [("u001", date(2023, 1, 1), "free", "iPhone", "US")],
            schema=metadata_schema,
        )
        empty_interactions = spark.createDataFrame([], INTERACTIONS_SCHEMA)

        result_df = calculate_cohort_retention_by_segment(
            empty_interactions, metadata_df, retention_weeks=4
        )

        assert result_df.count() == 0
        assert "segment_type" in result_df.columns
        assert "segment_value" in result_df.columns
        assert "retained_users" in result_df.columns
        assert "retention_rate" in result_df.columns
