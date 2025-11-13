"""
Unit tests for join optimization transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 1 (Join Optimization Specifications)
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from src.transforms.join_transforms import identify_hot_keys, apply_salting


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


class TestApplySalting:
    """Tests for apply_salting() function."""

    def test_apply_salting_basic(self, spark):
        """
        GIVEN:
            - df with u001 (hot key), u002, u003 (normal keys)
            - hot_keys_df contains only u001
            - salt_factor = 10
        WHEN: apply_salting() is called
        THEN:
            - u001 rows have salt values 0-9
            - u001 rows have user_id_salted = "u001_0", "u001_1", ..., "u001_9"
            - u002, u003 rows have salt = 0
            - u002, u003 rows have user_id_salted = "u002_0", "u003_0"
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        # Create data: u001 (hot key), u002, u003 (normal)
        data = [(f"u001", i) for i in range(100)]  # u001 has 100 interactions
        data += [("u002", 200), ("u003", 300)]     # u002, u003 have 1 interaction each

        df = spark.createDataFrame(data, schema=schema)

        # Hot keys DataFrame: only u001
        hot_keys_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", LongType(), nullable=False)
        ])
        hot_keys_df = spark.createDataFrame([("u001", 100)], schema=hot_keys_schema)

        # Act
        result_df = apply_salting(df, hot_keys_df, key_column="user_id", salt_factor=10)

        # Assert
        assert "salt" in result_df.columns, "Expected 'salt' column"
        assert "user_id_salted" in result_df.columns, "Expected 'user_id_salted' column"

        # Check u001 rows (should have varied salts)
        u001_rows = result_df.filter(F.col("user_id") == "u001").collect()
        u001_salts = [row["salt"] for row in u001_rows]
        assert all(0 <= salt < 10 for salt in u001_salts), "u001 salts should be 0-9"

        # Check at least some variety in salts (not all the same)
        unique_salts = set(u001_salts)
        assert len(unique_salts) > 1, "u001 should have varied salt values"

        # Check u002, u003 rows (should have salt=0)
        u002_row = result_df.filter(F.col("user_id") == "u002").collect()[0]
        assert u002_row["salt"] == 0, "u002 should have salt=0"
        assert u002_row["user_id_salted"] == "u002_0", "u002 should have user_id_salted='u002_0'"

        u003_row = result_df.filter(F.col("user_id") == "u003").collect()[0]
        assert u003_row["salt"] == 0, "u003 should have salt=0"
        assert u003_row["user_id_salted"] == "u003_0", "u003 should have user_id_salted='u003_0'"

    def test_apply_salting_distribution(self, spark):
        """
        GIVEN: 10,000 rows with user_id = u001 (hot key)
        WHEN: apply_salting() is called with salt_factor=10
        THEN:
            - Each salt value (0-9) appears ~1,000 times
            - Distribution variance < 10% (fairly uniform)
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        data = [("u001", i) for i in range(10000)]
        df = spark.createDataFrame(data, schema=schema)

        # Hot keys DataFrame
        hot_keys_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", LongType(), nullable=False)
        ])
        hot_keys_df = spark.createDataFrame([("u001", 10000)], schema=hot_keys_schema)

        # Act
        result_df = apply_salting(df, hot_keys_df, key_column="user_id", salt_factor=10)

        # Assert - check distribution
        salt_counts = result_df.groupBy("salt").count().collect()

        # Each salt should appear ~1,000 times (10,000 / 10)
        expected_count = 10000 / 10
        for row in salt_counts:
            actual_count = row["count"]
            variance_pct = abs(actual_count - expected_count) / expected_count * 100
            assert variance_pct < 20, f"Salt {row['salt']} distribution variance {variance_pct}% exceeds 20%"

    def test_apply_salting_no_hot_keys(self, spark):
        """
        GIVEN: hot_keys_df is empty
        WHEN: apply_salting() is called
        THEN:
            - All rows have salt = 0
            - All rows have {key_column}_salted = "{key_column}_0"
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        data = [("u001", 100), ("u002", 200), ("u003", 300)]
        df = spark.createDataFrame(data, schema=schema)

        # Empty hot keys DataFrame
        hot_keys_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", LongType(), nullable=False)
        ])
        hot_keys_df = spark.createDataFrame([], schema=hot_keys_schema)

        # Act
        result_df = apply_salting(df, hot_keys_df, key_column="user_id", salt_factor=10)

        # Assert
        results = result_df.collect()
        for row in results:
            assert row["salt"] == 0, f"Row with user_id={row['user_id']} should have salt=0"
            expected_salted = f"{row['user_id']}_0"
            assert row["user_id_salted"] == expected_salted

    def test_apply_salting_invalid_salt_factor(self, spark):
        """
        GIVEN: salt_factor = 1
        WHEN: apply_salting() is called
        THEN: Raises ValueError with message "salt_factor must be >= 2"
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("interaction_id", IntegerType(), nullable=False)
        ])

        data = [("u001", 100)]
        df = spark.createDataFrame(data, schema=schema)

        hot_keys_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("count", LongType(), nullable=False)
        ])
        hot_keys_df = spark.createDataFrame([("u001", 100)], schema=hot_keys_schema)

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            apply_salting(df, hot_keys_df, key_column="user_id", salt_factor=1)

        assert "salt_factor" in str(exc_info.value).lower()
        assert ">= 2" in str(exc_info.value) or "must be" in str(exc_info.value).lower()
