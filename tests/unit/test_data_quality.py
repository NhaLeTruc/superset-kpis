"""
Unit tests for data quality validation functions.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 5 (Data Quality Specifications)
"""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.data_quality import detect_nulls, detect_outliers, validate_schema


class TestValidateSchema:
    """Tests for validate_schema() function."""

    def test_validate_schema_valid(self, spark):
        """
        GIVEN: DataFrame with exact schema match
        WHEN: validate_schema() is called
        THEN: Returns (True, [])
        """
        # Arrange
        expected_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("count", IntegerType(), nullable=False),
            ]
        )

        data = [("u001", 100), ("u002", 200)]
        df = spark.createDataFrame(data, schema=expected_schema)

        # Act
        is_valid, errors = validate_schema(df, expected_schema, strict=True)

        # Assert
        assert is_valid is True
        assert errors == []

    def test_validate_schema_missing_column(self, spark):
        """
        GIVEN: DataFrame missing 'app_version' column
        WHEN: validate_schema() is called
        THEN: Returns (False, ["Missing column: 'app_version'"])
        """
        # Arrange
        expected_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("count", IntegerType(), nullable=False),
                StructField("app_version", StringType(), nullable=False),
            ]
        )

        # DataFrame with only user_id and count (missing app_version)
        actual_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("count", IntegerType(), nullable=False),
            ]
        )

        data = [("u001", 100), ("u002", 200)]
        df = spark.createDataFrame(data, schema=actual_schema)

        # Act
        is_valid, errors = validate_schema(df, expected_schema, strict=True)

        # Assert
        assert is_valid is False
        assert len(errors) == 1
        assert "Missing column: 'app_version'" in errors[0]

    def test_validate_schema_wrong_type(self, spark):
        """
        GIVEN: DataFrame with 'duration_ms' as IntegerType instead of LongType
        WHEN: validate_schema() is called
        THEN: Returns (False, ["Column 'duration_ms' has type IntegerType but expected LongType"])
        """
        # Arrange
        expected_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        # DataFrame with duration_ms as IntegerType (wrong!)
        actual_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("duration_ms", IntegerType(), nullable=False),  # Wrong type
            ]
        )

        data = [("u001", 5000), ("u002", 3000)]
        df = spark.createDataFrame(data, schema=actual_schema)

        # Act
        is_valid, errors = validate_schema(df, expected_schema, strict=True)

        # Assert
        assert is_valid is False
        assert len(errors) == 1
        assert "duration_ms" in errors[0]
        assert "int" in errors[0].lower()
        assert "bigint" in errors[0].lower() or "long" in errors[0].lower()


class TestDetectNulls:
    """Tests for detect_nulls() function."""

    def test_detect_nulls_none(self, spark):
        """
        GIVEN: DataFrame with no NULL values
        WHEN: detect_nulls() is called
        THEN: Returns empty DataFrame
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=True),
                StructField("timestamp", TimestampType(), nullable=True),
                StructField("count", IntegerType(), nullable=True),
            ]
        )

        data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 100),
            ("u002", datetime(2023, 1, 2, 11, 0, 0), 200),
            ("u003", datetime(2023, 1, 3, 12, 0, 0), 300),
        ]
        df = spark.createDataFrame(data, schema=schema)

        non_nullable_columns = ["user_id", "timestamp"]

        # Act
        result_df = detect_nulls(df, non_nullable_columns)

        # Assert
        assert result_df.count() == 0, "Expected empty DataFrame when no NULLs present"

    def test_detect_nulls_present(self, spark):
        """
        GIVEN:
            - Row 1: user_id=NULL, timestamp=2023-01-01 (NULL user_id)
            - Row 2: user_id=u001, timestamp=NULL (NULL timestamp)
            - Row 3: user_id=u002, timestamp=2023-01-02 (no NULLs)
        WHEN: detect_nulls(non_nullable_columns=["user_id", "timestamp"])
        THEN:
            - Returns 2 rows (rows 1 and 2)
            - Row 1: null_columns = ["user_id"]
            - Row 2: null_columns = ["timestamp"]
        """
        # Arrange
        schema = StructType(
            [
                StructField("user_id", StringType(), nullable=True),
                StructField("timestamp", TimestampType(), nullable=True),
                StructField("count", IntegerType(), nullable=True),
            ]
        )

        data = [
            (None, datetime(2023, 1, 1, 10, 0, 0), 100),  # NULL user_id
            ("u001", None, 200),  # NULL timestamp
            ("u002", datetime(2023, 1, 2, 11, 0, 0), 300),  # No NULLs
        ]
        df = spark.createDataFrame(data, schema=schema)

        non_nullable_columns = ["user_id", "timestamp"]

        # Act
        result_df = detect_nulls(df, non_nullable_columns)

        # Assert
        assert result_df.count() == 2, "Expected 2 rows with NULLs"

        # Verify schema includes null_columns
        assert "null_columns" in result_df.columns, "Expected 'null_columns' column in result"

        # Use filter-based assertions (order-independent)
        user_null_rows = result_df.filter(F.col("user_id").isNull()).collect()
        assert len(user_null_rows) == 1, "Expected 1 row with NULL user_id"
        assert "user_id" in user_null_rows[0]["null_columns"], "Expected 'user_id' in null_columns"

        timestamp_null_rows = result_df.filter(F.col("timestamp").isNull()).collect()
        assert len(timestamp_null_rows) == 1, "Expected 1 row with NULL timestamp"
        assert "timestamp" in timestamp_null_rows[0]["null_columns"], (
            "Expected 'timestamp' in null_columns"
        )


class TestDetectOutliers:
    """Tests for detect_outliers() function."""

    def test_detect_outliers_iqr(self, spark):
        """
        GIVEN:
            - 100 values: 1-100
            - Q1=25, Q3=75, IQR=50
            - Lower bound: 25 - 1.5*50 = -50
            - Upper bound: 75 + 1.5*50 = 150
            - Plus outliers: [-100, 200]
        WHEN: detect_outliers(method="iqr") is called
        THEN:
            - Returns 2 rows (outliers: -100, 200)
            - outlier_reason: ["iqr_outlier", "iqr_outlier"]
            - outlier_value: [-100, 200]
        """
        # Arrange
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("value", IntegerType(), nullable=False),
            ]
        )

        # Create 100 normal values (1-100) plus 2 outliers (-100, 200)
        normal_data = [(i, i) for i in range(1, 101)]
        outlier_data = [(101, -100), (102, 200)]
        all_data = normal_data + outlier_data

        df = spark.createDataFrame(all_data, schema=schema)

        # Act
        result_df = detect_outliers(df, column="value", method="iqr", iqr_multiplier=1.5)

        # Assert
        assert result_df.count() == 2, "Expected 2 outlier rows"

        # Verify schema includes outlier columns
        assert "outlier_reason" in result_df.columns, "Expected 'outlier_reason' column"
        assert "outlier_value" in result_df.columns, "Expected 'outlier_value' column"

        # Collect and verify results
        results = sorted(result_df.collect(), key=lambda r: r["outlier_value"])

        # First outlier: -100 (below lower bound)
        assert results[0]["outlier_value"] == -100.0
        assert "outlier" in results[0]["outlier_reason"].lower()

        # Second outlier: 200 (above upper bound)
        assert results[1]["outlier_value"] == 200.0
        assert "outlier" in results[1]["outlier_reason"].lower()

    def test_detect_outliers_threshold(self, spark):
        """
        GIVEN:
            - Values: [100, 200, 1000, 29000000 (>8 hours)]
            - threshold_max = 28800000 (8 hours in milliseconds)
        WHEN: detect_outliers(method="threshold", threshold_max=28800000) is called
        THEN:
            - Returns 1 row with duration_ms = 29000000
            - outlier_reason = "above_max"
        """
        # Arrange
        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            (1, 100),
            (2, 200),
            (3, 1000),
            (4, 29000000),  # > 8 hours (outlier)
        ]
        df = spark.createDataFrame(data, schema=schema)

        threshold_max = 28800000  # 8 hours in ms

        # Act
        result_df = detect_outliers(
            df, column="duration_ms", method="threshold", threshold_max=threshold_max
        )

        # Assert
        assert result_df.count() == 1, "Expected 1 outlier row"

        # Verify columns
        assert "outlier_reason" in result_df.columns
        assert "outlier_value" in result_df.columns

        # Verify the outlier
        result = result_df.collect()[0]
        assert result["outlier_value"] == 29000000.0
        assert result["outlier_reason"] == "above_max"
