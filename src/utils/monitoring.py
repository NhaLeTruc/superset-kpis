"""
Monitoring utilities and custom Spark accumulators.

This module provides custom accumulators for tracking data processing metrics,
data quality issues, and partition skew across Spark jobs.

Custom Accumulators:
- RecordCounterAccumulator: Tracks total records processed
- SkippedRecordsAccumulator: Counts records filtered/skipped
- DataQualityErrorsAccumulator: Tracks validation failures by type
- PartitionSkewDetector: Monitors partition size distribution

Helper Functions:
- create_monitoring_context: Initialize all accumulators for a job
- format_monitoring_summary: Format monitoring output
- with_monitoring: Decorator for adding monitoring to functions
"""
from pyspark import AccumulatorParam
from typing import Dict, Any, Callable
from functools import wraps
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Custom Accumulators
# ============================================================================

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

    def zero(self, initial_value: Dict[str, int]) -> Dict[str, int]:
        """Return the zero value (empty dict) for this accumulator."""
        return {}

    def addInPlace(self, v1: Dict[str, int], v2: Dict[str, int]) -> Dict[str, int]:
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

    def zero(self, initial_value: Dict[str, Any]) -> Dict[str, Any]:
        """Return the zero value for this accumulator."""
        return {
            "max_partition_size": 0,
            "min_partition_size": float('inf'),
            "partition_count": 0
        }

    def addInPlace(self, v1: Dict[str, Any], v2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two partition info dictionaries."""
        return {
            "max_partition_size": max(v1["max_partition_size"], v2["max_partition_size"]),
            "min_partition_size": min(v1["min_partition_size"], v2["min_partition_size"]),
            "partition_count": v1["partition_count"] + v2["partition_count"]
        }


# ============================================================================
# Helper Functions
# ============================================================================

def create_monitoring_context(spark_context, job_name: str) -> Dict[str, Any]:
    """
    Create a monitoring context with all accumulators initialized.

    Args:
        spark_context: Spark context to register accumulators with
        job_name: Name of the job (used for accumulator naming)

    Returns:
        Dictionary containing all initialized accumulators

    Example:
        >>> context = create_monitoring_context(spark.sparkContext, "data_processing")
        >>> context["record_counter"].add(1)
        >>> print(context["record_counter"].value)
        1
    """
    context = {}

    # Record counter - accumulator auto-registers in PySpark 3.x
    context["record_counter"] = spark_context.accumulator(0, RecordCounterAccumulator())

    # Skipped records counter
    context["skipped_records"] = spark_context.accumulator(0, SkippedRecordsAccumulator())

    # Data quality errors
    context["data_quality_errors"] = spark_context.accumulator({}, DataQualityErrorsAccumulator())

    # Partition skew detector
    context["partition_skew"] = spark_context.accumulator(
        {"max_partition_size": 0, "min_partition_size": float('inf'), "partition_count": 0},
        PartitionSkewDetector()
    )

    logger.info(f"📊 Monitoring context created for job: {job_name}")
    return context


def format_monitoring_summary(context: Dict[str, Any], job_name: str) -> str:
    """
    Format monitoring metrics into a human-readable summary.

    Args:
        context: Monitoring context dictionary with accumulators
        job_name: Name of the job for the report header

    Returns:
        Formatted string with monitoring summary

    Example:
        >>> summary = format_monitoring_summary(context, "Data Processing")
        >>> print(summary)
        ========================================
        Monitoring Summary: Data Processing
        ========================================
        Records Processed: 1,000,000
        Records Skipped: 1,250 (0.13%)
        ...
    """
    record_count = context["record_counter"].value
    skipped_count = context["skipped_records"].value
    dq_errors = context["data_quality_errors"].value
    skew_info = context["partition_skew"].value

    # Calculate percentages
    total_records = record_count + skipped_count
    skipped_pct = (skipped_count / total_records * 100) if total_records > 0 else 0

    # Build summary
    lines = [
        "=" * 60,
        f"📊 Monitoring Summary: {job_name}",
        "=" * 60,
        "",
        "📈 Record Processing:",
        f"   ✅ Records Processed: {record_count:,}",
        f"   ⏭️  Records Skipped: {skipped_count:,} ({skipped_pct:.2f}%)",
        f"   📊 Total Records: {total_records:,}",
        "",
    ]

    # Data quality errors
    if dq_errors:
        lines.append("⚠️  Data Quality Errors:")
        for error_type, count in sorted(dq_errors.items(), key=lambda x: -x[1]):
            error_pct = (count / total_records * 100) if total_records > 0 else 0
            lines.append(f"   - {error_type}: {count:,} ({error_pct:.2f}%)")
        lines.append("")

    # Partition skew
    if skew_info["partition_count"] > 0:
        max_size = skew_info["max_partition_size"]
        min_size = skew_info["min_partition_size"]
        if min_size != float('inf') and min_size > 0:
            skew_ratio = max_size / min_size
        else:
            skew_ratio = 0

        lines.append("📦 Partition Distribution:")
        lines.append(f"   - Partitions Processed: {skew_info['partition_count']}")
        lines.append(f"   - Max Partition Size: {max_size:,}")
        lines.append(f"   - Min Partition Size: {min_size if min_size != float('inf') else 0:,}")

        if skew_ratio > 0:
            lines.append(f"   - Skew Ratio: {skew_ratio:.2f}x")
            if skew_ratio > 3.0:
                lines.append(f"   ⚠️  WARNING: High partition skew detected! (>{skew_ratio:.1f}x)")
        lines.append("")

    lines.append("=" * 60)

    return "\n".join(lines)


def with_monitoring(operation_name: str) -> Callable:
    """
    Decorator to add monitoring to transformation functions.

    Args:
        operation_name: Name of the operation for logging

    Returns:
        Decorated function with monitoring

    Example:
        >>> @with_monitoring("calculate_dau")
        ... def calculate_dau(df, monitoring_context):
        ...     monitoring_context["record_counter"].add(df.count())
        ...     return df.groupBy("date").agg(...)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"🔄 Starting operation: {operation_name}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"✅ Completed operation: {operation_name}")
                return result
            except Exception as e:
                logger.error(f"❌ Failed operation: {operation_name} - {str(e)}")
                raise
        return wrapper
    return decorator


# ============================================================================
# Integration Helper Functions
# ============================================================================

def track_data_quality_errors(row: Dict[str, Any], context: Dict[str, Any],
                              non_nullable_columns: list = None,
                              numeric_columns: list = None) -> bool:
    """
    Track data quality errors for a row and update monitoring context.

    Args:
        row: Row to validate (as dictionary)
        context: Monitoring context with accumulators
        non_nullable_columns: List of columns that should not be null
        numeric_columns: List of numeric columns to check for negatives

    Returns:
        True if row is valid, False if it has errors

    Example:
        >>> is_valid = track_data_quality_errors(
        ...     row, context,
        ...     non_nullable_columns=["user_id", "timestamp"],
        ...     numeric_columns=["duration_ms"]
        ... )
    """
    has_errors = False

    # Check for null values
    if non_nullable_columns:
        for col in non_nullable_columns:
            if col in row and row[col] is None:
                context["data_quality_errors"].add({f"null_{col}": 1})
                has_errors = True

    # Check for negative numeric values
    if numeric_columns:
        for col in numeric_columns:
            if col in row and row[col] is not None and row[col] < 0:
                context["data_quality_errors"].add({f"negative_{col}": 1})
                has_errors = True

    if has_errors:
        context["skipped_records"].add(1)
        return False
    else:
        context["record_counter"].add(1)
        return True


def track_partition_size(partition, context: Dict[str, Any]):
    """
    Track the size of a partition for skew detection.

    Args:
        partition: Iterator of rows in the partition
        context: Monitoring context with accumulators

    Yields:
        Rows from the partition

    Example:
        >>> df.rdd.mapPartitions(
        ...     lambda part: track_partition_size(part, context)
        ... )
    """
    size = 0
    rows = []
    for row in partition:
        size += 1
        rows.append(row)

    # Update partition skew detector
    context["partition_skew"].add({
        "max_partition_size": size,
        "min_partition_size": size,
        "partition_count": 1
    })

    # Yield all rows
    for row in rows:
        yield row


def log_monitoring_summary(context: Dict[str, Any], job_name: str):
    """
    Log the monitoring summary to logger.

    Args:
        context: Monitoring context with accumulators
        job_name: Name of the job for the report header
    """
    summary = format_monitoring_summary(context, job_name)
    print(summary)
    logger.info(f"\n{summary}")


# ============================================================================
# Pre-configured Monitoring Profiles
# ============================================================================

def get_monitoring_profile(profile_name: str) -> Dict[str, Any]:
    """
    Get a pre-configured monitoring profile.

    Args:
        profile_name: Name of the profile (e.g., 'strict', 'standard', 'minimal')

    Returns:
        Dictionary with monitoring configuration

    Profiles:
        - strict: Track all metrics, fail on any data quality issues
        - standard: Track all metrics, log warnings for issues
        - minimal: Track only record counts
    """
    profiles = {
        "strict": {
            "track_records": True,
            "track_skipped": True,
            "track_dq_errors": True,
            "track_partition_skew": True,
            "fail_on_errors": True,
            "skew_threshold": 3.0,
        },
        "standard": {
            "track_records": True,
            "track_skipped": True,
            "track_dq_errors": True,
            "track_partition_skew": True,
            "fail_on_errors": False,
            "skew_threshold": 5.0,
        },
        "minimal": {
            "track_records": True,
            "track_skipped": False,
            "track_dq_errors": False,
            "track_partition_skew": False,
            "fail_on_errors": False,
            "skew_threshold": 10.0,
        }
    }

    return profiles.get(profile_name, profiles["standard"])
