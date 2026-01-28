"""
Monitoring Package

Provides comprehensive monitoring infrastructure for Spark jobs:
- Custom accumulators for tracking metrics
- Context management for job monitoring
- Reporting and formatting utilities
- Data quality tracking helpers
- Pre-configured monitoring profiles

All public APIs are re-exported for backward compatibility.
"""

from .accumulators import (
    DataQualityErrorsAccumulator,
    PartitionSkewDetector,
    RecordCounterAccumulator,
    SkippedRecordsAccumulator,
)
from .context import create_monitoring_context
from .reporting import format_monitoring_summary, log_monitoring_summary, with_monitoring
from .tracking import get_monitoring_profile, track_data_quality_errors, track_partition_size


__all__ = [
    "DataQualityErrorsAccumulator",
    "PartitionSkewDetector",
    # Accumulators
    "RecordCounterAccumulator",
    "SkippedRecordsAccumulator",
    # Context
    "create_monitoring_context",
    # Reporting
    "format_monitoring_summary",
    "get_monitoring_profile",
    "log_monitoring_summary",
    # Tracking
    "track_data_quality_errors",
    "track_partition_size",
    "with_monitoring",
]
