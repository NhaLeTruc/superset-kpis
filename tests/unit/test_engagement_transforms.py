"""
Unit tests for engagement analytics transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 2 (Engagement Analytics Specifications)
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DateType, DoubleType
)
from datetime import datetime, timedelta, date
from src.transforms.engagement_transforms import (
    calculate_dau,
    calculate_mau,
    calculate_stickiness,
    identify_power_users,
    calculate_cohort_retention
)


class TestCalculateDAU:
    """Tests for calculate_dau() function."""

    def test_calculate_dau_basic(self, spark):
        """
        GIVEN:
            - 2023-01-01: u001 (3 interactions), u002 (2 interactions)
            - 2023-01-02: u001 (1 interaction), u003 (1 interaction)
        WHEN: calculate_dau() is called
        THEN:
            - 2023-01-01: dau=2, total_interactions=5
            - 2023-01-02: dau=2, total_interactions=2
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            # 2023-01-01: u001 (3 interactions), u002 (2 interactions)
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 5000),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), 3000),
            ("u001", datetime(2023, 1, 1, 12, 0, 0), 2000),
            ("u002", datetime(2023, 1, 1, 10, 30, 0), 4000),
            ("u002", datetime(2023, 1, 1, 14, 0, 0), 6000),
            # 2023-01-02: u001 (1 interaction), u003 (1 interaction)
            ("u001", datetime(2023, 1, 2, 9, 0, 0), 7000),
            ("u003", datetime(2023, 1, 2, 15, 0, 0), 3000),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        results = result_df.orderBy("date").collect()

        # 2023-01-01
        assert results[0]["date"] == date(2023, 1, 1)
        assert results[0]["dau"] == 2
        assert results[0]["total_interactions"] == 5
        assert results[0]["total_duration_ms"] == 20000

        # 2023-01-02
        assert results[1]["date"] == date(2023, 1, 2)
        assert results[1]["dau"] == 2
        assert results[1]["total_interactions"] == 2
        assert results[1]["total_duration_ms"] == 10000

    def test_calculate_dau_single_user(self, spark):
        """
        GIVEN: u001 has interactions on 7 consecutive days
        WHEN: calculate_dau() is called
        THEN:
            - 7 rows returned
            - Each date has dau=1
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0) + timedelta(days=i), 5000)
            for i in range(7)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        assert result_df.count() == 7
        results = result_df.collect()
        for row in results:
            assert row["dau"] == 1

    def test_calculate_dau_user_multiple_interactions(self, spark):
        """
        GIVEN: u001 has 10 interactions on 2023-01-01
        WHEN: calculate_dau() is called
        THEN:
            - 2023-01-01: dau=1 (not 10 - distinct users)
            - total_interactions=10
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, 1, 10, i, 0), 1000)
            for i in range(10)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        result = result_df.collect()[0]
        assert result["dau"] == 1
        assert result["total_interactions"] == 10

    def test_calculate_dau_empty(self, spark):
        """
        GIVEN: Empty DataFrame with correct schema
        WHEN: calculate_dau() is called
        THEN: Returns empty DataFrame with output schema
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        df = spark.createDataFrame([], schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        assert result_df.count() == 0
        assert "date" in result_df.columns
        assert "dau" in result_df.columns

    def test_calculate_dau_missing_columns(self, spark):
        """
        GIVEN: DataFrame without 'timestamp' column
        WHEN: calculate_dau() is called
        THEN: Raises ValueError
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        df = spark.createDataFrame([("u001", 1000)], schema=schema)

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            calculate_dau(df)
        assert "timestamp" in str(exc_info.value).lower()


class TestCalculateMAU:
    """Tests for calculate_mau() function."""

    def test_calculate_mau_basic(self, spark):
        """
        GIVEN:
            - Jan 2023: u001 (active on Jan 1, 5, 20), u002 (active on Jan 15)
            - Feb 2023: u001 (active on Feb 3), u003 (active on Feb 10)
        WHEN: calculate_mau() is called
        THEN:
            - Jan 2023: mau=2
            - Feb 2023: mau=2
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])

        data = [
            # January: u001, u002
            ("u001", datetime(2023, 1, 1, 10, 0, 0)),
            ("u001", datetime(2023, 1, 5, 10, 0, 0)),
            ("u001", datetime(2023, 1, 20, 10, 0, 0)),
            ("u002", datetime(2023, 1, 15, 10, 0, 0)),
            # February: u001, u003
            ("u001", datetime(2023, 2, 3, 10, 0, 0)),
            ("u003", datetime(2023, 2, 10, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_mau(df)

        # Assert
        results = result_df.orderBy("month").collect()

        # Jan 2023
        assert results[0]["month"] == date(2023, 1, 1)
        assert results[0]["mau"] == 2

        # Feb 2023
        assert results[1]["month"] == date(2023, 2, 1)
        assert results[1]["mau"] == 2

    def test_calculate_mau_daily_active_user(self, spark):
        """
        GIVEN: u001 has interactions on all 31 days of January 2023
        WHEN: calculate_mau() is called
        THEN:
            - Jan 2023: mau=1 (counted once, not 31 times)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])

        data = [
            ("u001", datetime(2023, 1, day, 10, 0, 0))
            for day in range(1, 32)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_mau(df)

        # Assert
        result = result_df.collect()[0]
        assert result["mau"] == 1


class TestCalculateStickiness:
    """Tests for calculate_stickiness() function."""

    def test_calculate_stickiness_perfect(self, spark):
        """
        GIVEN:
            - January has 31 days
            - All 31 days have DAU=10
            - MAU for January = 10
        WHEN: calculate_stickiness() is called
        THEN:
            - stickiness_ratio = 1.0 (perfect stickiness)
        """
        # Arrange - DAU DataFrame
        dau_data = [
            (date(2023, 1, day), 10, 100, 50000, 5000.0)
            for day in range(1, 32)
        ]
        dau_schema = StructType([
            StructField("date", DateType(), nullable=False),
            StructField("dau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False),
            StructField("total_duration_ms", LongType(), nullable=False),
            StructField("avg_duration_per_user", DoubleType(), nullable=False)
        ])
        dau_df = spark.createDataFrame(dau_data, schema=dau_schema)

        # Arrange - MAU DataFrame
        mau_data = [(date(2023, 1, 1), 10, 3100)]
        mau_schema = StructType([
            StructField("month", DateType(), nullable=False),
            StructField("mau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False)
        ])
        mau_df = spark.createDataFrame(mau_data, schema=mau_schema)

        # Act
        result_df = calculate_stickiness(dau_df, mau_df)

        # Assert
        result = result_df.collect()[0]
        assert result["month"] == date(2023, 1, 1)
        assert result["avg_dau"] == 10.0
        assert result["mau"] == 10
        assert result["stickiness_ratio"] == 1.0

    def test_calculate_stickiness_low(self, spark):
        """
        GIVEN:
            - MAU for January = 100
            - Average DAU for January = 10
        WHEN: calculate_stickiness() is called
        THEN:
            - stickiness_ratio = 0.1 (10%)
        """
        # Arrange - DAU (31 days, each with DAU=10)
        dau_data = [
            (date(2023, 1, day), 10, 100, 50000, 5000.0)
            for day in range(1, 32)
        ]
        dau_schema = StructType([
            StructField("date", DateType(), nullable=False),
            StructField("dau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False),
            StructField("total_duration_ms", LongType(), nullable=False),
            StructField("avg_duration_per_user", DoubleType(), nullable=False)
        ])
        dau_df = spark.createDataFrame(dau_data, schema=dau_schema)

        # MAU = 100
        mau_data = [(date(2023, 1, 1), 100, 3100)]
        mau_schema = StructType([
            StructField("month", DateType(), nullable=False),
            StructField("mau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False)
        ])
        mau_df = spark.createDataFrame(mau_data, schema=mau_schema)

        # Act
        result_df = calculate_stickiness(dau_df, mau_df)

        # Assert
        result = result_df.collect()[0]
        assert abs(result["stickiness_ratio"] - 0.1) < 0.01

    def test_calculate_stickiness_single_day(self, spark):
        """
        GIVEN:
            - Only Jan 1, 2023 has data
            - DAU = 5, MAU = 5
        WHEN: calculate_stickiness() is called
        THEN:
            - stickiness_ratio = 1.0
        """
        # Arrange
        dau_data = [(date(2023, 1, 1), 5, 50, 25000, 5000.0)]
        dau_schema = StructType([
            StructField("date", DateType(), nullable=False),
            StructField("dau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False),
            StructField("total_duration_ms", LongType(), nullable=False),
            StructField("avg_duration_per_user", DoubleType(), nullable=False)
        ])
        dau_df = spark.createDataFrame(dau_data, schema=dau_schema)

        mau_data = [(date(2023, 1, 1), 5, 50)]
        mau_schema = StructType([
            StructField("month", DateType(), nullable=False),
            StructField("mau", LongType(), nullable=False),
            StructField("total_interactions", LongType(), nullable=False)
        ])
        mau_df = spark.createDataFrame(mau_data, schema=mau_schema)

        # Act
        result_df = calculate_stickiness(dau_df, mau_df)

        # Assert
        result = result_df.collect()[0]
        assert result["stickiness_ratio"] == 1.0


class TestIdentifyPowerUsers:
    """Tests for identify_power_users() function."""

    def test_identify_power_users_top_1_percent(self, spark):
        """
        GIVEN:
            - 100 users with total_duration uniformly distributed
            - percentile=0.99
        WHEN: identify_power_users() is called
        THEN:
            - Returns 1 user (top 1%)
            - User has highest total_duration_ms
        """
        # Arrange - interactions
        interactions_data = []
        for user_num in range(1, 101):
            user_id = f"u{user_num:03d}"
            duration = user_num * 1000  # 1000, 2000, ..., 100000
            interactions_data.append((user_id, datetime(2023, 1, 1, 10, 0, 0), duration, "page1"))

        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False),
            StructField("page_id", StringType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = [(f"u{i:03d}", "US", "iPad", "premium") for i in range(1, 101)]
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("subscription_type", StringType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.99)

        # Assert
        assert result_df.count() == 1
        result = result_df.collect()[0]
        assert result["user_id"] == "u100"
        assert result["total_duration_ms"] == 100000

    def test_identify_power_users_filters_outliers(self, spark):
        """
        GIVEN:
            - u001: 3 interactions: [5000ms, 120000ms, 50000000ms (13.8 hours)]
            - max_duration_ms = 28800000 (8 hours)
        WHEN: identify_power_users() is called
        THEN:
            - Outlier (50000000ms) is excluded
            - u001 total_duration_ms = 5000 + 120000 = 125000ms
        """
        # Arrange
        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 5000, "page1"),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), 120000, "page2"),
            ("u001", datetime(2023, 1, 1, 12, 0, 0), 50000000, "page3"),  # Outlier
        ]
        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False),
            StructField("page_id", StringType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("subscription_type", StringType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(
            interactions_df, metadata_df,
            percentile=0.0,  # Include all users
            max_duration_ms=28800000
        )

        # Assert
        result = result_df.collect()[0]
        assert result["total_duration_ms"] == 125000

    def test_identify_power_users_joins_metadata(self, spark):
        """
        GIVEN:
            - u001 is power user
            - metadata: u001 -> country=US, device_type=iPad, subscription_type=premium
        WHEN: identify_power_users() is called
        THEN:
            - Result includes country=US, device_type=iPad, subscription_type=premium
        """
        # Arrange
        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 100000, "page1")
        ]
        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False),
            StructField("page_id", StringType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("subscription_type", StringType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.0)

        # Assert
        result = result_df.collect()[0]
        assert result["country"] == "US"
        assert result["device_type"] == "iPad"
        assert result["subscription_type"] == "premium"

    def test_identify_power_users_calculates_all_metrics(self, spark):
        """
        GIVEN: u001 with known interaction pattern
        WHEN: identify_power_users() is called
        THEN: All metrics calculated correctly
        """
        # Arrange
        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 10000, "page1"),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), 20000, "page2"),
            ("u001", datetime(2023, 1, 2, 10, 0, 0), 30000, "page1"),  # Different day
        ]
        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False),
            StructField("page_id", StringType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False),
            StructField("subscription_type", StringType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.0)

        # Assert
        result = result_df.collect()[0]
        assert result["total_interactions"] == 3
        assert result["unique_pages"] == 2  # page1, page2
        assert result["days_active"] == 2  # Jan 1, Jan 2
        assert result["total_duration_ms"] == 60000
        assert abs(result["hours_spent"] - 60000/3600000) < 0.01
        assert result["avg_duration_per_interaction"] == 20000.0


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
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("join_date", DateType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Interactions - all users active all 4 weeks
        interactions_data = []
        for user_num in range(1, 11):
            user_id = f"u{user_num:03d}"
            for week in range(4):
                interactions_data.append((
                    user_id,
                    datetime(2023, 1, 1, 10, 0, 0) + timedelta(weeks=week)
                ))

        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df,
            cohort_period="week", analysis_weeks=4
        )

        # Assert
        results = result_df.orderBy("weeks_since_join").collect()
        for week in range(4):
            assert results[week]["retention_rate"] == 100.0

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
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("join_date", DateType(), nullable=False)
        ])
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

        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df,
            cohort_period="week", analysis_weeks=4
        )

        # Assert
        results = result_df.orderBy("weeks_since_join").collect()
        assert results[0]["retention_rate"] == 100.0
        assert results[1]["retention_rate"] == 80.0
        assert results[2]["retention_rate"] == 60.0
        assert results[3]["retention_rate"] == 50.0

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

        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("join_date", DateType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Interactions - all users active week 0
        interactions_data = []
        for i in range(1, 51):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 1, 10, 0, 0)))
        for i in range(51, 81):
            interactions_data.append((f"u{i:03d}", datetime(2023, 1, 8, 10, 0, 0)))

        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df,
            cohort_period="week", analysis_weeks=2
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
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("join_date", DateType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0)),   # Week 0
            ("u001", datetime(2023, 1, 8, 10, 0, 0)),   # Week 1
            # NO interaction in week 2 (2023-01-15)
            ("u001", datetime(2023, 1, 22, 10, 0, 0)),  # Week 3
        ]
        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Act
        result_df = calculate_cohort_retention(
            interactions_df, metadata_df,
            cohort_period="week", analysis_weeks=4
        )

        # Assert
        results = result_df.orderBy("weeks_since_join").collect()
        assert results[0]["active_users"] == 1  # Week 0
        assert results[1]["active_users"] == 1  # Week 1
        assert results[2]["active_users"] == 0  # Week 2 - NOT active
        assert results[3]["active_users"] == 1  # Week 3 - back
