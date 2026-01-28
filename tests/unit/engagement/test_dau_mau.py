"""
Unit tests for DAU and MAU calculation.

Tests calculate_dau() and calculate_mau() functions from engagement transforms.
"""

from datetime import date, datetime, timedelta

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transforms.engagement import calculate_dau, calculate_mau


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
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

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

        # Assert using chispa
        expected_df = spark.createDataFrame(
            [
                (date(2023, 1, 1), 2, 5, 20000),
                (date(2023, 1, 2), 2, 2, 10000),
            ],
            ["date", "daily_active_users", "total_interactions", "total_duration_ms"],
        )

        assert_df_equality(
            result_df.select(
                "date", "daily_active_users", "total_interactions", "total_duration_ms"
            ),
            expected_df,
            ignore_row_order=True,
            ignore_nullable=True,
        )

    def test_calculate_dau_single_user(self, spark):
        """
        GIVEN: u001 has interactions on 7 consecutive days
        WHEN: calculate_dau() is called
        THEN:
            - 7 rows returned
            - Each date has dau=1
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0) + timedelta(days=i), 5000) for i in range(7)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        assert result_df.count() == 7
        results = result_df.collect()
        for row in results:
            assert row["daily_active_users"] == 1

    def test_calculate_dau_user_multiple_interactions(self, spark):
        """
        GIVEN: u001 has 10 interactions on 2023-01-01
        WHEN: calculate_dau() is called
        THEN:
            - 2023-01-01: dau=1 (not 10 - distinct users)
            - total_interactions=10
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [("u001", datetime(2023, 1, 1, 10, i, 0), 1000) for i in range(10)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        result = result_df.collect()[0]
        assert result["daily_active_users"] == 1
        assert result["total_interactions"] == 10

    def test_calculate_dau_empty(self, spark):
        """
        GIVEN: Empty DataFrame with correct schema
        WHEN: calculate_dau() is called
        THEN: Returns empty DataFrame with output schema
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        df = spark.createDataFrame([], schema=schema)

        # Act
        result_df = calculate_dau(df)

        # Assert
        assert result_df.count() == 0
        assert "date" in result_df.columns
        assert "daily_active_users" in result_df.columns

    def test_calculate_dau_missing_columns(self, spark):
        """
        GIVEN: DataFrame without 'timestamp' column
        WHEN: calculate_dau() is called
        THEN: Raises ValueError
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

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
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )

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

        # Assert using chispa
        expected_df = spark.createDataFrame(
            [
                (date(2023, 1, 1), 2),
                (date(2023, 2, 1), 2),
            ],
            ["month", "monthly_active_users"],
        )

        assert_df_equality(
            result_df.select("month", "monthly_active_users"),
            expected_df,
            ignore_row_order=True,
            ignore_nullable=True,
        )

    def test_calculate_mau_daily_active_user(self, spark):
        """
        GIVEN: u001 has interactions on all 31 days of January 2023
        WHEN: calculate_mau() is called
        THEN:
            - Jan 2023: mau=1 (counted once, not 31 times)
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
            ]
        )

        data = [("u001", datetime(2023, 1, day, 10, 0, 0)) for day in range(1, 32)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_mau(df)

        # Assert
        result = result_df.collect()[0]
        assert result["monthly_active_users"] == 1
