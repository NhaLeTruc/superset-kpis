"""
Unit tests for optimized join execution.

Tests optimized_join() function from join transforms.
"""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.transforms.join import optimized_join


class TestOptimizedJoin:
    """Tests for optimized_join() function."""

    def test_optimized_join_broadcast(self, spark):
        """
        GIVEN:
            - large_df: 1000 interactions
            - small_df: 10 metadata (small enough to broadcast)
        WHEN: optimized_join() is called with enable_broadcast=True
        THEN:
            - Join completes successfully
            - All rows joined correctly
        """
        # Arrange - large DataFrame
        large_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("interaction_id", IntegerType(), nullable=False),
            ]
        )

        large_data = [(f"u{i % 10:03d}", i) for i in range(1000)]
        large_df = spark.createDataFrame(large_data, schema=large_schema)

        # Arrange - small DataFrame (metadata)
        small_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
            ]
        )

        small_data = [(f"u{i:03d}", f"Country{i}") for i in range(10)]
        small_df = spark.createDataFrame(small_data, schema=small_schema)

        # Act
        result_df = optimized_join(
            large_df, small_df, join_key="user_id", join_type="inner", enable_broadcast=True
        )

        # Assert
        assert result_df.count() == 1000, "All 1000 interactions should be joined"
        assert "country" in result_df.columns, "Metadata column should be present"
        assert "salt" not in result_df.columns, "Salt column should be removed"
        assert "user_id_salted" not in result_df.columns, "Salted column should be removed"

    def test_optimized_join_with_salting(self, spark):
        """
        GIVEN:
            - large_df: 1000 interactions with u001 having 500 (50% skew)
            - small_df: 10 metadata
            - enable_salting=True
        WHEN: optimized_join() is called
        THEN:
            - Join completes successfully
            - No salt columns in output
        """
        # Arrange - large DataFrame with skew
        large_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("interaction_id", IntegerType(), nullable=False),
            ]
        )

        # 500 interactions for u001 (hot key) + 500 for others
        hot_key_data = [("u001", i) for i in range(500)]
        other_data = [(f"u{i:03d}", 500 + i) for i in range(2, 11) for _ in range(56)]
        large_data = hot_key_data + other_data[:500]  # Total 1000 rows
        large_df = spark.createDataFrame(large_data, schema=large_schema)

        # Small DataFrame (metadata)
        small_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
            ]
        )

        small_data = [(f"u{i:03d}", f"Country{i}") for i in range(1, 11)]
        small_df = spark.createDataFrame(small_data, schema=small_schema)

        # Act
        result_df = optimized_join(
            large_df,
            small_df,
            join_key="user_id",
            join_type="inner",
            enable_broadcast=False,
            enable_salting=True,
        )

        # Assert
        assert result_df.count() == 1000, "All interactions should be joined"
        assert "salt" not in result_df.columns, "Salt column should be cleaned up"
        assert "user_id_salted" not in result_df.columns, "Salted column should be cleaned up"

    def test_optimized_join_no_skew(self, spark):
        """
        GIVEN:
            - large_df: 1000 interactions, uniformly distributed
            - small_df: 10 metadata
        WHEN: optimized_join() is called
        THEN:
            - No hot keys detected
            - Join completes successfully
        """
        # Arrange - uniformly distributed large DataFrame
        large_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("interaction_id", IntegerType(), nullable=False),
            ]
        )

        large_data = [(f"u{i % 10:03d}", i) for i in range(1000)]  # Uniform distribution
        large_df = spark.createDataFrame(large_data, schema=large_schema)

        # Small DataFrame
        small_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
            ]
        )

        small_data = [(f"u{i:03d}", f"Country{i}") for i in range(10)]
        small_df = spark.createDataFrame(small_data, schema=small_schema)

        # Act
        result_df = optimized_join(
            large_df, small_df, join_key="user_id", join_type="inner", enable_salting=True
        )

        # Assert
        assert result_df.count() == 1000, "All interactions should be joined"

    def test_optimized_join_left_join(self, spark):
        """
        GIVEN:
            - large_df: interactions with users u001, u002, u003
            - small_df: metadata with only u001, u002 (u003 missing)
            - join_type="left"
        WHEN: optimized_join() is called
        THEN:
            - All 3 users present in result
            - u003 has NULL values for metadata columns
        """
        # Arrange - large DataFrame
        large_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("interaction_id", IntegerType(), nullable=False),
            ]
        )

        large_data = [("u001", 1), ("u002", 2), ("u003", 3)]
        large_df = spark.createDataFrame(large_data, schema=large_schema)

        # Small DataFrame (missing u003)
        small_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
            ]
        )

        small_data = [("u001", "US"), ("u002", "UK")]
        small_df = spark.createDataFrame(small_data, schema=small_schema)

        # Act
        result_df = optimized_join(large_df, small_df, join_key="user_id", join_type="left")

        # Assert using chispa
        expected_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("interaction_id", IntegerType(), nullable=False),
                StructField("country", StringType(), nullable=True),
            ]
        )
        expected_df = spark.createDataFrame(
            [("u001", 1, "US"), ("u002", 2, "UK"), ("u003", 3, None)], schema=expected_schema
        )

        assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
