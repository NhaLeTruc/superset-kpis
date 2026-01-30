"""
Job-Specific Spark Tuning

Provides configuration profiles optimized for different workload types:
- ETL: Large data processing with more partitions
- Analytics: Query optimization with more broadcasting
- ML: Machine learning with more memory caching
- Streaming: Micro-batch optimization
"""

from pyspark.sql import SparkSession


def calculate_optimal_partitions(data_size_gb: float, partition_size_mb: int = 128) -> int:
    """
    Calculate optimal partition count based on data size.

    Uses the rule of thumb: ~128MB per partition for balanced parallelism.

    Args:
        data_size_gb: Estimated data size in GB
        partition_size_mb: Target partition size in MB (default: 128)

    Returns:
        Optimal partition count (minimum 200)
    """
    return max(200, int((data_size_gb * 1024) / partition_size_mb))


def configure_job_specific_settings(
    spark: SparkSession, job_type: str, data_size_gb: float = 0.0
) -> None:
    """
    Apply job-specific Spark configurations.

    Args:
        spark: Active SparkSession
        job_type: Type of job - 'etl', 'analytics', 'ml', 'streaming'
        data_size_gb: Optional estimated data size in GB for dynamic partition calculation.
                      If provided, overrides default partition counts using 128MB/partition rule.

    Job Types:
        - 'etl': ETL/data processing jobs (more shuffle partitions)
        - 'analytics': Analytics queries (fewer partitions, more broadcast)
        - 'ml': Machine learning jobs (more memory, fewer partitions)
        - 'streaming': Streaming jobs (micro-batch optimization)

    Example:
        >>> spark = create_spark_session()
        >>> configure_job_specific_settings(spark, 'etl')
        >>> # With dynamic partitioning for 500GB dataset:
        >>> configure_job_specific_settings(spark, 'etl', data_size_gb=500)
    """
    if job_type == "etl":
        # ETL jobs: more partitions for large data processing
        if data_size_gb:
            partitions = calculate_optimal_partitions(data_size_gb, partition_size_mb=128)
        else:
            partitions = 400  # Default fallback
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
        print(f"📊 Configured for ETL workload ({partitions} partitions)")

    elif job_type == "analytics":
        # Analytics: fewer partitions, more broadcasting
        if data_size_gb:
            # For analytics, use smaller partitions (256MB) for faster queries
            partitions = calculate_optimal_partitions(data_size_gb, partition_size_mb=256)
        else:
            partitions = 20  # Default fallback
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")
        print(f"📈 Configured for Analytics workload ({partitions} partitions, 200MB broadcast)")

    elif job_type == "ml":
        # ML: more memory for caching, fewer partitions
        if data_size_gb:
            # For ML, use larger partitions (512MB) to reduce overhead
            partitions = calculate_optimal_partitions(data_size_gb, partition_size_mb=512)
        else:
            partitions = 50  # Default fallback
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.memory.storageFraction", "0.5")
        print(f"🤖 Configured for ML workload ({partitions} partitions, 50% storage)")

    elif job_type == "streaming":
        # Streaming: micro-batch optimization
        spark.conf.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        print("🌊 Configured for Streaming workload")

    else:
        print(f"⚠️  Unknown job type: {job_type}. Using default settings.")
