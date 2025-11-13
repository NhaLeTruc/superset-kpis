"""
Unit tests for data quality validation functions.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 5 (Data Quality Specifications)
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from src.utils.data_quality import validate_schema


class TestValidateSchema:
    """Tests for validate_schema() function."""

    def test_validate_schema_valid(self, spark):
        """
        GIVEN: DataFrame with exact schema match
        WHEN: validate_schema() is called
        THEN: Returns (True, [])
        """
        # Arrange
        expected_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", IntegerType(), nullable=False)
        ])

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
        expected_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", IntegerType(), nullable=False),
            StructField("app_version", StringType(), nullable=False)
        ])

        # DataFrame with only user_id and count (missing app_version)
        actual_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", IntegerType(), nullable=False)
        ])

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
        expected_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        # DataFrame with duration_ms as IntegerType (wrong!)
        actual_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("duration_ms", IntegerType(), nullable=False)  # Wrong type
        ])

        data = [("u001", 5000), ("u002", 3000)]
        df = spark.createDataFrame(data, schema=actual_schema)

        # Act
        is_valid, errors = validate_schema(df, expected_schema, strict=True)

        # Assert
        assert is_valid is False
        assert len(errors) == 1
        assert "duration_ms" in errors[0]
        assert "IntegerType" in errors[0] or "int" in errors[0].lower()
        assert "LongType" in errors[0] or "long" in errors[0].lower()
