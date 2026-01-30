"""
Unit tests for custom Spark accumulators.

Tests accumulator classes for tracking metrics across Spark partitions.
"""

import sys


class TestRecordCounterAccumulator:
    """Test suite for RecordCounterAccumulator."""

    def test_zero_returns_zero(self):
        """Test that zero() returns 0."""
        from src.utils.monitoring.accumulators import RecordCounterAccumulator

        acc = RecordCounterAccumulator()
        assert acc.zero(0) == 0
        assert acc.zero(100) == 0  # Initial value should be ignored

    def test_add_in_place(self):
        """Test that addInPlace correctly adds two values."""
        from src.utils.monitoring.accumulators import RecordCounterAccumulator

        acc = RecordCounterAccumulator()
        assert acc.addInPlace(5, 3) == 8
        assert acc.addInPlace(0, 10) == 10
        assert acc.addInPlace(100, 0) == 100

    def test_accumulator_with_spark(self, spark):
        """Test accumulator works with Spark context."""
        from src.utils.monitoring.accumulators import RecordCounterAccumulator

        acc = spark.sparkContext.accumulator(0, RecordCounterAccumulator())

        # Simulate adding counts
        acc.add(10)
        acc.add(20)

        assert acc.value == 30


class TestSkippedRecordsAccumulator:
    """Test suite for SkippedRecordsAccumulator."""

    def test_zero_returns_zero(self):
        """Test that zero() returns 0."""
        from src.utils.monitoring.accumulators import SkippedRecordsAccumulator

        acc = SkippedRecordsAccumulator()
        assert acc.zero(0) == 0

    def test_add_in_place(self):
        """Test that addInPlace correctly adds two values."""
        from src.utils.monitoring.accumulators import SkippedRecordsAccumulator

        acc = SkippedRecordsAccumulator()
        assert acc.addInPlace(5, 3) == 8

    def test_accumulator_with_spark(self, spark):
        """Test accumulator works with Spark context."""
        from src.utils.monitoring.accumulators import SkippedRecordsAccumulator

        acc = spark.sparkContext.accumulator(0, SkippedRecordsAccumulator())

        acc.add(5)
        acc.add(15)

        assert acc.value == 20


class TestDataQualityErrorsAccumulator:
    """Test suite for DataQualityErrorsAccumulator."""

    def test_zero_returns_empty_dict(self):
        """Test that zero() returns empty dict."""
        from src.utils.monitoring.accumulators import DataQualityErrorsAccumulator

        acc = DataQualityErrorsAccumulator()
        assert acc.zero({}) == {}
        assert acc.zero({"existing": 1}) == {}

    def test_add_in_place_merges_dicts(self):
        """Test that addInPlace correctly merges error dictionaries."""
        from src.utils.monitoring.accumulators import DataQualityErrorsAccumulator

        acc = DataQualityErrorsAccumulator()

        v1 = {"null_user_id": 5, "invalid_timestamp": 2}
        v2 = {"null_user_id": 3, "negative_duration": 1}

        result = acc.addInPlace(v1, v2)

        assert result["null_user_id"] == 8  # 5 + 3
        assert result["invalid_timestamp"] == 2
        assert result["negative_duration"] == 1

    def test_add_in_place_empty_dicts(self):
        """Test addInPlace with empty dictionaries."""
        from src.utils.monitoring.accumulators import DataQualityErrorsAccumulator

        acc = DataQualityErrorsAccumulator()

        assert acc.addInPlace({}, {}) == {}
        assert acc.addInPlace({"a": 1}, {}) == {"a": 1}
        assert acc.addInPlace({}, {"b": 2}) == {"b": 2}

    def test_accumulator_with_spark(self, spark):
        """Test accumulator works with Spark context."""
        from src.utils.monitoring.accumulators import DataQualityErrorsAccumulator

        acc = spark.sparkContext.accumulator({}, DataQualityErrorsAccumulator())

        acc.add({"null_user_id": 5})
        acc.add({"null_user_id": 3, "invalid_timestamp": 2})

        assert acc.value == {"null_user_id": 8, "invalid_timestamp": 2}


class TestPartitionSkewDetector:
    """Test suite for PartitionSkewDetector."""

    def test_zero_returns_initial_state(self):
        """Test that zero() returns correct initial state."""
        from src.utils.monitoring.accumulators import (
            _MIN_PARTITION_SENTINEL,
            PartitionSkewDetector,
        )

        acc = PartitionSkewDetector()
        result = acc.zero({})

        assert result["max_partition_size"] == 0
        assert result["min_partition_size"] == _MIN_PARTITION_SENTINEL
        assert result["partition_count"] == 0

    def test_add_in_place_tracks_max(self):
        """Test that addInPlace correctly tracks maximum partition size."""
        from src.utils.monitoring.accumulators import PartitionSkewDetector

        acc = PartitionSkewDetector()

        v1 = {"max_partition_size": 100, "min_partition_size": 50, "partition_count": 1}
        v2 = {"max_partition_size": 200, "min_partition_size": 75, "partition_count": 1}

        result = acc.addInPlace(v1, v2)

        assert result["max_partition_size"] == 200

    def test_add_in_place_tracks_min(self):
        """Test that addInPlace correctly tracks minimum partition size."""
        from src.utils.monitoring.accumulators import PartitionSkewDetector

        acc = PartitionSkewDetector()

        v1 = {"max_partition_size": 100, "min_partition_size": 50, "partition_count": 1}
        v2 = {"max_partition_size": 200, "min_partition_size": 25, "partition_count": 1}

        result = acc.addInPlace(v1, v2)

        assert result["min_partition_size"] == 25

    def test_add_in_place_sums_partition_count(self):
        """Test that addInPlace correctly sums partition counts."""
        from src.utils.monitoring.accumulators import PartitionSkewDetector

        acc = PartitionSkewDetector()

        v1 = {"max_partition_size": 100, "min_partition_size": 50, "partition_count": 3}
        v2 = {"max_partition_size": 200, "min_partition_size": 25, "partition_count": 5}

        result = acc.addInPlace(v1, v2)

        assert result["partition_count"] == 8

    def test_sentinel_value_is_sys_maxsize(self):
        """Test that sentinel value uses sys.maxsize."""
        from src.utils.monitoring.accumulators import _MIN_PARTITION_SENTINEL

        assert sys.maxsize == _MIN_PARTITION_SENTINEL

    def test_accumulator_with_spark(self, spark):
        """Test accumulator works with Spark context."""
        from src.utils.monitoring.accumulators import (
            _MIN_PARTITION_SENTINEL,
            PartitionSkewDetector,
        )

        initial = {
            "max_partition_size": 0,
            "min_partition_size": _MIN_PARTITION_SENTINEL,
            "partition_count": 0,
        }
        acc = spark.sparkContext.accumulator(initial, PartitionSkewDetector())

        acc.add({"max_partition_size": 100, "min_partition_size": 100, "partition_count": 1})
        acc.add({"max_partition_size": 50, "min_partition_size": 50, "partition_count": 1})

        assert acc.value["max_partition_size"] == 100
        assert acc.value["min_partition_size"] == 50
        assert acc.value["partition_count"] == 2
