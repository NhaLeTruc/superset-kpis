"""
Monitoring Reporting and Formatting

Provides functions for formatting monitoring metrics and logging summaries.
"""

import logging
from collections.abc import Callable
from functools import wraps
from typing import Any


logger = logging.getLogger(__name__)


def format_monitoring_summary(context: dict[str, Any], job_name: str) -> str:
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
        skew_ratio = max_size / min_size if min_size != float("inf") and min_size > 0 else 0

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


def log_monitoring_summary(context: dict[str, Any], job_name: str):
    """
    Log the monitoring summary to logger.

    Args:
        context: Monitoring context with accumulators
        job_name: Name of the job for the report header
    """
    summary = format_monitoring_summary(context, job_name)
    print(summary)
    logger.info(f"\n{summary}")


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
                logger.error(f"❌ Failed operation: {operation_name} - {e!s}")
                raise

        return wrapper

    return decorator
