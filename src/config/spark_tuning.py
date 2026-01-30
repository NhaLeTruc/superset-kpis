"""
Job-Specific Spark Tuning

Provides configuration profiles optimized for different workload types:
- ETL: Large data processing with more partitions
- Analytics: Query optimization with more broadcasting
- ML: Machine learning with more memory caching
- Streaming: Micro-batch optimization
"""

from pyspark.sql import SparkSession


def calculate_optimal_partitions(
    spark: SparkSession,
    data_size_gb: float,
    partition_size_mb: int = 128,
    partitions_per_core: int = 2,
) -> int:
    """
    Calculate optimal partition count based on data size AND cluster resources.

    Uses two approaches and takes the maximum:
    1. Data-based: ~128MB per partition for balanced I/O
    2. Parallelism-based: 2-4 partitions per CPU core for full cluster utilization

    Args:
        spark: Active SparkSession (used to query cluster configuration)
        data_size_gb: Estimated data size in GB
        partition_size_mb: Target partition size in MB (default: 128)
        partitions_per_core: Multiplier for parallelism (default: 2, recommended: 2-4)

    Returns:
        Optimal partition count (minimum equals total cluster cores)

    Note on Spark 3.x Adaptive Query Execution (AQE):
        For Spark 3.0+, consider enabling AQE which dynamically optimizes partition
        counts at runtime based on actual data statistics. This can make manual
        tuning less critical:

            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

        With AQE enabled, this function provides a reasonable initial estimate,
        and Spark will auto-coalesce small partitions or split large ones as needed.
        For Spark 2.x or when AQE is disabled, this function's output is more critical.

    Example:
        >>> spark = create_spark_session()
        >>> # For a 500GB dataset on a cluster with 100 cores:
        >>> partitions = calculate_optimal_partitions(spark, data_size_gb=500)
        >>> # Returns max(100 * 2, 500 * 1024 / 128) = max(200, 4000) = 4000
    """
    # Query cluster resources from SparkSession
    try:
        # Try to get executor configuration
        num_executors = int(spark.conf.get("spark.executor.instances", "0"))
        cores_per_executor = int(spark.conf.get("spark.executor.cores", "0"))

        if num_executors > 0 and cores_per_executor > 0:
            total_cores = num_executors * cores_per_executor
        else:
            # Fallback: use SparkContext's defaultParallelism (works in all modes)
            total_cores = spark.sparkContext.defaultParallelism
    except Exception:
        # Conservative fallback for edge cases (e.g., config not yet initialized)
        total_cores = 4

    # Ensure minimum sensible core count
    total_cores = max(total_cores, 2)

    # Calculate based on parallelism (2-4 partitions per core recommended)
    parallelism_based = total_cores * partitions_per_core

    # Calculate based on data size (~128MB per partition)
    data_based = int((data_size_gb * 1024) / partition_size_mb) if data_size_gb > 0 else 0

    # Take the maximum of both approaches, with floor at total_cores
    return max(total_cores, parallelism_based, data_based)


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
            partitions = calculate_optimal_partitions(spark, data_size_gb, partition_size_mb=128)
        else:
            partitions = calculate_optimal_partitions(spark, data_size_gb=0)
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
        print(f"📊 Configured for ETL workload ({partitions} partitions)")

    elif job_type == "analytics":
        # Analytics: fewer partitions, more broadcasting
        if data_size_gb:
            # For analytics, use larger partitions (256MB) for faster queries
            partitions = calculate_optimal_partitions(spark, data_size_gb, partition_size_mb=256)
        else:
            partitions = calculate_optimal_partitions(spark, data_size_gb=0, partition_size_mb=256)
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")
        print(f"📈 Configured for Analytics workload ({partitions} partitions, 200MB broadcast)")

    elif job_type == "ml":
        # ML: more memory for caching, fewer partitions
        if data_size_gb:
            # For ML, use larger partitions (512MB) to reduce overhead
            partitions = calculate_optimal_partitions(spark, data_size_gb, partition_size_mb=512)
        else:
            partitions = calculate_optimal_partitions(spark, data_size_gb=0, partition_size_mb=512)
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
