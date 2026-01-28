"""
Spark Configuration (Compatibility Shim)

This module maintains backward compatibility by re-exporting all functions
from the new spark module structure.

New code should import from spark_session and spark_tuning directly.
"""
from .spark_session import create_spark_session, get_spark_config_summary
from .spark_tuning import (
    configure_job_specific_settings,
    apply_performance_profile,
    calculate_optimal_partitions,
)

__all__ = [
    "create_spark_session",
    "get_spark_config_summary",
    "configure_job_specific_settings",
    "apply_performance_profile",
    "calculate_optimal_partitions",
]
