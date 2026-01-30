"""
Unit tests for user stickiness calculation.

Tests calculate_stickiness() function from engagement transforms.
"""

from datetime import date

from pyspark.sql.types import DateType, DoubleType, LongType, StructField, StructType

from src.transforms.engagement import calculate_stickiness


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
        dau_data = [(date(2023, 1, day), 10, 100, 50000, 5000.0) for day in range(1, 32)]
        dau_schema = StructType(
            [
                StructField("date", DateType(), nullable=False),
                StructField("daily_active_users", LongType(), nullable=False),
                StructField("total_interactions", LongType(), nullable=False),
                StructField("total_duration_ms", LongType(), nullable=False),
                StructField("avg_duration_per_user", DoubleType(), nullable=False),
            ]
        )
        dau_df = spark.createDataFrame(dau_data, schema=dau_schema)

        # Arrange - MAU DataFrame
        mau_data = [(date(2023, 1, 1), 10, 3100)]
        mau_schema = StructType(
            [
                StructField("month", DateType(), nullable=False),
                StructField("monthly_active_users", LongType(), nullable=False),
                StructField("total_interactions", LongType(), nullable=False),
            ]
        )
        mau_df = spark.createDataFrame(mau_data, schema=mau_schema)

        # Act
        result_df = calculate_stickiness(dau_df, mau_df)

        # Assert
        result = result_df.collect()[0]
        assert result["month"] == date(2023, 1, 1)
        assert result["avg_dau"] == 10.0
        assert result["monthly_active_users"] == 10
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
        dau_data = [(date(2023, 1, day), 10, 100, 50000, 5000.0) for day in range(1, 32)]
        dau_schema = StructType(
            [
                StructField("date", DateType(), nullable=False),
                StructField("daily_active_users", LongType(), nullable=False),
                StructField("total_interactions", LongType(), nullable=False),
                StructField("total_duration_ms", LongType(), nullable=False),
                StructField("avg_duration_per_user", DoubleType(), nullable=False),
            ]
        )
        dau_df = spark.createDataFrame(dau_data, schema=dau_schema)

        # MAU = 100
        mau_data = [(date(2023, 1, 1), 100, 3100)]
        mau_schema = StructType(
            [
                StructField("month", DateType(), nullable=False),
                StructField("monthly_active_users", LongType(), nullable=False),
                StructField("total_interactions", LongType(), nullable=False),
            ]
        )
        mau_df = spark.createDataFrame(mau_data, schema=mau_schema)

        # Act
        result_df = calculate_stickiness(dau_df, mau_df)

        # Assert
        result = result_df.collect()[0]
        assert abs(result["stickiness_ratio"] - 0.1) < 0.01
