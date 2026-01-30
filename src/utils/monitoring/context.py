"""
Monitoring Context Management

Provides factory function for creating monitoring contexts with all accumulators initialized.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .accumulators import (
    _MIN_PARTITION_SENTINEL,
    DataQualityErrorsAccumulator,
    PartitionSkewDetector,
    RecordCounterAccumulator,
    SkippedRecordsAccumulator,
)


if TYPE_CHECKING:
    from pyspark import SparkContext


logger = logging.getLogger(__name__)


def create_monitoring_context(spark_context: SparkContext, job_name: str) -> dict[str, Any]:
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
        {
            "max_partition_size": 0,
            "min_partition_size": _MIN_PARTITION_SENTINEL,
            "partition_count": 0,
        },
        PartitionSkewDetector(),
    )

    logger.info(f"📊 Monitoring context created for job: {job_name}")
    return context
