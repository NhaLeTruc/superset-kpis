"""
Custom Spark Accumulators

Provides specialized accumulators for tracking metrics across Spark partitions:
- Record counting
- Skipped records tracking
- Data quality error categorization
- Partition skew detection
"""

from __future__ import annotations

import sys
from typing import Any

from pyspark import AccumulatorParam


# Sentinel value for uninitialized min_partition_size
# Using sys.maxsize instead of float("inf") to avoid serialization issues
_MIN_PARTITION_SENTINEL = sys.maxsize


class RecordCounterAccumulator(AccumulatorParam):
    """
    Accumulator for counting total records processed.

    Tracks the total number of records that pass through the pipeline
    across all partitions.
    """

    def zero(self, initial_value: int) -> int:
        """Return the zero value for this accumulator."""
        return 0

    def addInPlace(self, v1: int, v2: int) -> int:
        """Add two values together."""
        return v1 + v2


class SkippedRecordsAccumulator(AccumulatorParam):
    """
    Accumulator for counting skipped/filtered records.

    Tracks records that are filtered out due to outliers,
    quality issues, or business rules.
    """

    def zero(self, initial_value: int) -> int:
        """Return the zero value for this accumulator."""
        return 0

    def addInPlace(self, v1: int, v2: int) -> int:
        """Add two values together."""
        return v1 + v2


class DataQualityErrorsAccumulator(AccumulatorParam):
    """
    Accumulator for tracking data quality errors by type.

    Maintains a dictionary of error types and their counts:
    - null_user_id: Records with missing user IDs
    - invalid_timestamp: Records with invalid timestamps
    - negative_duration: Records with negative duration values
    - invalid_action_type: Records with invalid action types
    - etc.
    """

    def zero(self, initial_value: dict[str, int]) -> dict[str, int]:
        """Return the zero value (empty dict) for this accumulator."""
        return {}

    def addInPlace(self, v1: dict[str, int], v2: dict[str, int]) -> dict[str, int]:
        """Merge two error dictionaries."""
        result = v1.copy()
        for error_type, count in v2.items():
            result[error_type] = result.get(error_type, 0) + count
        return result


class PartitionSkewDetector(AccumulatorParam):
    """
    Accumulator for detecting partition skew.

    Tracks partition sizes to identify data skew issues:
    - max_partition_size: Largest partition size seen
    - min_partition_size: Smallest partition size seen
    - partition_count: Number of partitions processed
    """

    def zero(self, initial_value: dict[str, Any]) -> dict[str, Any]:
        """Return the zero value for this accumulator."""
        return {
            "max_partition_size": 0,
            "min_partition_size": _MIN_PARTITION_SENTINEL,
            "partition_count": 0,
        }

    def addInPlace(self, v1: dict[str, Any], v2: dict[str, Any]) -> dict[str, Any]:
        """Merge two partition info dictionaries."""
        return {
            "max_partition_size": max(v1["max_partition_size"], v2["max_partition_size"]),
            "min_partition_size": min(v1["min_partition_size"], v2["min_partition_size"]),
            "partition_count": v1["partition_count"] + v2["partition_count"],
        }
