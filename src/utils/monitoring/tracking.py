"""
Data Quality Tracking and Monitoring Profiles

Provides integration helpers for tracking data quality and partition metrics,
plus pre-configured monitoring profiles.
"""

from typing import Any


def track_data_quality_errors(
    row: dict[str, Any],
    context: dict[str, Any],
    non_nullable_columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
) -> bool:
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


def track_partition_size(partition, context: dict[str, Any]):
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
    context["partition_skew"].add(
        {"max_partition_size": size, "min_partition_size": size, "partition_count": 1}
    )

    # Yield all rows
    for row in rows:
        yield row


def get_monitoring_profile(profile_name: str) -> dict[str, Any]:
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
        },
    }

    return profiles.get(profile_name, profiles["standard"])
