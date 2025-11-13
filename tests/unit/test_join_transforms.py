"""
Unit tests for join optimization transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 1 (Join Optimization Specifications)
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.transforms.join_transforms import identify_hot_keys


class TestIdentifyHotKeys:
    """Tests for identify_hot_keys() function."""

    def test_identify_hot_keys_basic(self, spark):
        """
        GIVEN: DataFrame with 100 users, u001 has 10,000 interactions, others have 100
        WHEN: identify_hot_keys() is called with threshold_percentile=0.99
        THEN:
            - Returns DataFrame with 1 row (u001)
            - u001 has count=10,000
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        # Create u001 with 10,000 interactions
        hot_key_data = [(f"u001", i) for i in range(10000)]

        # Create 99 other users with 100 interactions each
        normal_data = []
        for user_num in range(2, 101):
            user_id = f"u{user_num:03d}"
            for interaction_num in range(100):
                normal_data.append((user_id, 10000 + user_num * 100 + interaction_num))

        all_data = hot_key_data + normal_data
        df = spark.createDataFrame(all_data, schema=schema)

        # Act
        result_df = identify_hot_keys(df, key_column="user_id", threshold_percentile=0.99)

        # Assert
        assert result_df.count() == 1, "Expected 1 hot key"

        # Verify the hot key
        result = result_df.collect()[0]
        assert result["user_id"] == "u001"
        assert result["count"] == 10000

    def test_identify_hot_keys_uniform_distribution(self, spark):
        """
        GIVEN: DataFrame with uniform distribution (all users have 100 interactions)
        WHEN: identify_hot_keys() is called
        THEN: Returns empty DataFrame
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        # Create 100 users with exactly 100 interactions each (uniform)
        data = []
        for user_num in range(1, 101):
            user_id = f"u{user_num:03d}"
            for interaction_num in range(100):
                data.append((user_id, user_num * 100 + interaction_num))

        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = identify_hot_keys(df, key_column="user_id", threshold_percentile=0.99)

        # Assert
        assert result_df.count() == 0, "Expected no hot keys with uniform distribution"

    def test_identify_hot_keys_multiple(self, spark):
        """
        GIVEN: DataFrame with 100 users, top 5 have 5,000 interactions each
        WHEN: identify_hot_keys() is called with threshold_percentile=0.95
        THEN: Returns DataFrame with 5 rows (top 5 users)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        # Create top 5 users with 5,000 interactions each
        hot_keys_data = []
        for user_num in range(1, 6):
            user_id = f"u{user_num:03d}"
            for interaction_num in range(5000):
                hot_keys_data.append((user_id, user_num * 10000 + interaction_num))

        # Create 95 other users with 100 interactions each
        normal_data = []
        for user_num in range(6, 101):
            user_id = f"u{user_num:03d}"
            for interaction_num in range(100):
                normal_data.append((user_id, user_num * 1000 + interaction_num))

        all_data = hot_keys_data + normal_data
        df = spark.createDataFrame(all_data, schema=schema)

        # Act
        result_df = identify_hot_keys(df, key_column="user_id", threshold_percentile=0.95)

        # Assert
        assert result_df.count() == 5, "Expected 5 hot keys"

        # Verify all hot keys have count=5000
        results = result_df.collect()
        for row in results:
            assert row["count"] == 5000
            assert row["user_id"] in ["u001", "u002", "u003", "u004", "u005"]

    def test_identify_hot_keys_invalid_column(self, spark):
        """
        GIVEN: DataFrame without 'nonexistent_column'
        WHEN: identify_hot_keys() is called with key_column='nonexistent_column'
        THEN: Raises ValueError with message "Column 'nonexistent_column' not found"
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", IntegerType(), nullable=False)
        ])

        data = [("u001", 100), ("u002", 200)]
        df = spark.createDataFrame(data, schema=schema)

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            identify_hot_keys(df, key_column="nonexistent_column")

        assert "nonexistent_column" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()
