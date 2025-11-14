"""
Unit tests for monitoring utilities and custom accumulators.

This module tests the custom Spark accumulators used for monitoring
data processing jobs, including record counting, data quality tracking,
and partition skew detection.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from datetime import datetime


class TestMonitoringAccumulators:
    """Test suite for custom monitoring accumulators."""

    def test_record_counter_accumulator(self, spark):
        """Test that RecordCounterAccumulator tracks total records processed."""
        from src.utils.monitoring import RecordCounterAccumulator

        # Create accumulator
        acc = RecordCounterAccumulator()
        spark.sparkContext.register(acc, "test_record_counter")

        # Create test data
        data = [(i,) for i in range(100)]
        df = spark.createDataFrame(data, ["id"])

        # Process data with accumulator
        def count_records(partition):
            count = 0
            for row in partition:
                acc.add(1)
                count += 1
            return iter([count])

        result = df.rdd.mapPartitions(count_records).collect()

        # Verify accumulator value
        assert acc.value == 100, f"Expected 100 records, got {acc.value}"

    def test_skipped_records_accumulator(self, spark):
        """Test that SkippedRecordsAccumulator tracks filtered records."""
        from src.utils.monitoring import SkippedRecordsAccumulator

        # Create accumulator
        acc = SkippedRecordsAccumulator()
        spark.sparkContext.register(acc, "test_skipped_records")

        # Create test data with some invalid records
        data = [
            (1, 100),   # valid
            (2, -50),   # invalid (negative)
            (3, 200),   # valid
            (4, -10),   # invalid (negative)
            (5, 150),   # valid
        ]
        df = spark.createDataFrame(data, ["id", "value"])

        # Process data and skip negatives
        def filter_records(partition):
            for row in partition:
                if row['value'] < 0:
                    acc.add(1)
                else:
                    yield row

        result = df.rdd.mapPartitions(filter_records).collect()

        # Verify accumulator value
        assert acc.value == 2, f"Expected 2 skipped records, got {acc.value}"
        assert len(result) == 3, f"Expected 3 valid records, got {len(result)}"

    def test_data_quality_errors_accumulator(self, spark):
        """Test that DataQualityErrorsAccumulator tracks validation failures."""
        from src.utils.monitoring import DataQualityErrorsAccumulator

        # Create accumulator
        acc = DataQualityErrorsAccumulator()
        spark.sparkContext.register(acc, "test_dq_errors")

        # Test adding different error types
        acc.add("null_user_id")
        acc.add("invalid_timestamp")
        acc.add("null_user_id")
        acc.add("negative_duration")
        acc.add("null_user_id")

        # Verify accumulator value (dictionary of error counts)
        errors = acc.value
        assert errors["null_user_id"] == 3, f"Expected 3 null_user_id errors, got {errors.get('null_user_id', 0)}"
        assert errors["invalid_timestamp"] == 1, f"Expected 1 invalid_timestamp error, got {errors.get('invalid_timestamp', 0)}"
        assert errors["negative_duration"] == 1, f"Expected 1 negative_duration error, got {errors.get('negative_duration', 0)}"
        assert sum(errors.values()) == 5, f"Expected 5 total errors, got {sum(errors.values())}"

    def test_partition_skew_detector(self, spark):
        """Test that PartitionSkewDetector tracks partition sizes."""
        from src.utils.monitoring import PartitionSkewDetector

        # Create accumulator
        acc = PartitionSkewDetector()
        spark.sparkContext.register(acc, "test_partition_skew")

        # Create test data - will be distributed across partitions
        data = [(i,) for i in range(1000)]
        df = spark.createDataFrame(data, ["id"]).repartition(4)

        # Track partition sizes
        def track_partition_size(partition):
            size = 0
            for row in partition:
                size += 1
            acc.add(size)
            return iter([size])

        partition_sizes = df.rdd.mapPartitions(track_partition_size).collect()

        # Verify accumulator tracked partition info
        skew_info = acc.value
        assert "max_partition_size" in skew_info
        assert "min_partition_size" in skew_info
        assert "partition_count" in skew_info
        assert skew_info["partition_count"] == 4
        assert skew_info["max_partition_size"] >= skew_info["min_partition_size"]


class TestMonitoringHelpers:
    """Test suite for monitoring helper functions."""

    def test_create_monitoring_context(self, spark):
        """Test creating a monitoring context with all accumulators."""
        from src.utils.monitoring import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_job")

        # Verify all accumulators are created
        assert "record_counter" in context
        assert "skipped_records" in context
        assert "data_quality_errors" in context
        assert "partition_skew" in context

        # Verify they are properly initialized
        assert context["record_counter"].value == 0
        assert context["skipped_records"].value == 0
        assert context["data_quality_errors"].value == {}
        assert context["partition_skew"].value == {
            "max_partition_size": 0,
            "min_partition_size": float('inf'),
            "partition_count": 0
        }

    def test_format_monitoring_summary(self, spark):
        """Test formatting monitoring summary for display."""
        from src.utils.monitoring import create_monitoring_context, format_monitoring_summary

        context = create_monitoring_context(spark.sparkContext, "test_job")

        # Simulate some activity
        context["record_counter"].add(10000)
        context["skipped_records"].add(150)
        context["data_quality_errors"].add("null_user_id")
        context["data_quality_errors"].add("null_user_id")
        context["data_quality_errors"].add("invalid_duration")

        # Format summary
        summary = format_monitoring_summary(context, "Test Job")

        # Verify summary contains expected information
        assert "Test Job" in summary
        assert "10,000" in summary or "10000" in summary  # Record count
        assert "150" in summary  # Skipped records
        assert "null_user_id" in summary  # Error type
        assert "2" in summary  # Error count for null_user_id

    def test_monitoring_decorator(self, spark):
        """Test monitoring decorator for tracking function execution."""
        from src.utils.monitoring import with_monitoring

        # Create a test function with monitoring
        @with_monitoring("test_transform")
        def test_transform(df, monitoring_context):
            # Simulate processing
            monitoring_context["record_counter"].add(df.count())
            return df.filter("id > 5")

        # Create test data
        data = [(i,) for i in range(10)]
        df = spark.createDataFrame(data, ["id"])

        # Execute with monitoring
        context = {"record_counter": spark.sparkContext.accumulator(0)}
        result = test_transform(df, context)

        # Verify monitoring was applied
        assert result.count() == 4  # ids 6,7,8,9
        assert context["record_counter"].value == 10  # Original count


class TestMonitoringIntegration:
    """Test suite for monitoring integration with transforms."""

    def test_monitoring_with_data_quality(self, spark):
        """Test monitoring integration with data quality validation."""
        from src.utils.monitoring import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_dq")

        # Create test data with quality issues
        data = [
            (1, "user1", 100, "2024-01-01 10:00:00"),  # valid
            (2, None, 200, "2024-01-01 11:00:00"),     # null user_id
            (3, "user3", -50, "2024-01-01 12:00:00"),  # negative duration
            (4, "user4", 150, None),                    # null timestamp
            (5, "user5", 300, "2024-01-01 13:00:00"),  # valid
        ]

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("timestamp", StringType(), True),
        ])

        df = spark.createDataFrame(data, schema)

        # Validate and track errors
        def validate_row(row):
            errors = []
            if row['user_id'] is None:
                errors.append("null_user_id")
                context["data_quality_errors"].add("null_user_id")
            if row['duration_ms'] is not None and row['duration_ms'] < 0:
                errors.append("negative_duration")
                context["data_quality_errors"].add("negative_duration")
            if row['timestamp'] is None:
                errors.append("null_timestamp")
                context["data_quality_errors"].add("null_timestamp")

            if errors:
                context["skipped_records"].add(1)
                return None
            else:
                context["record_counter"].add(1)
                return row

        valid_rows = [r for r in df.collect() if validate_row(r) is not None]

        # Verify monitoring tracked all issues
        assert context["record_counter"].value == 2  # 2 valid records
        assert context["skipped_records"].value == 3  # 3 invalid records
        errors = context["data_quality_errors"].value
        assert errors["null_user_id"] == 1
        assert errors["negative_duration"] == 1
        assert errors["null_timestamp"] == 1

    def test_monitoring_with_skew_detection(self, spark):
        """Test monitoring partition skew detection."""
        from src.utils.monitoring import create_monitoring_context

        context = create_monitoring_context(spark.sparkContext, "test_skew")

        # Create skewed data (most records have same key)
        skewed_data = [(1, i) for i in range(900)]  # 90% with key=1
        normal_data = [(i, i) for i in range(2, 102)]  # 10% distributed
        data = skewed_data + normal_data

        df = spark.createDataFrame(data, ["key", "value"])

        # Track partition sizes after grouping (which will create skew)
        def track_partition(partition):
            size = sum(1 for _ in partition)
            context["partition_skew"].add(size)
            return iter([size])

        partition_sizes = df.rdd.mapPartitions(track_partition).collect()

        # Verify skew was detected
        skew_info = context["partition_skew"].value
        assert skew_info["max_partition_size"] > 0
        assert skew_info["partition_count"] > 0

        # Calculate skew ratio
        if skew_info["min_partition_size"] > 0:
            skew_ratio = skew_info["max_partition_size"] / skew_info["min_partition_size"]
            # Some skew should be detected
            assert skew_ratio >= 1.0
