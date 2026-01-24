"""
Job-Specific Spark Tuning

Provides configuration profiles optimized for different workload types:
- ETL: Large data processing with more partitions
- Analytics: Query optimization with more broadcasting
- ML: Machine learning with more memory caching
- Streaming: Micro-batch optimization
"""
from pyspark.sql import SparkSession


def configure_job_specific_settings(
    spark: SparkSession,
    job_type: str
) -> None:
    """
    Apply job-specific Spark configurations.

    Args:
        spark: Active SparkSession
        job_type: Type of job - 'etl', 'analytics', 'ml', 'streaming'

    Job Types:
        - 'etl': ETL/data processing jobs (more shuffle partitions)
        - 'analytics': Analytics queries (fewer partitions, more broadcast)
        - 'ml': Machine learning jobs (more memory, fewer partitions)
        - 'streaming': Streaming jobs (micro-batch optimization)

    Example:
        >>> spark = create_spark_session()
        >>> configure_job_specific_settings(spark, 'etl')
    """
    if job_type == "etl":
        # ETL jobs: more partitions for large data processing
        spark.conf.set("spark.sql.shuffle.partitions", "400")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
        print("📊 Configured for ETL workload (400 partitions)")

    elif job_type == "analytics":
        # Analytics: fewer partitions, more broadcasting
        spark.conf.set("spark.sql.shuffle.partitions", "20")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")
        print("📈 Configured for Analytics workload (20 partitions, 200MB broadcast)")

    elif job_type == "ml":
        # ML: more memory for caching, fewer partitions
        spark.conf.set("spark.sql.shuffle.partitions", "50")
        spark.conf.set("spark.memory.storageFraction", "0.5")
        print("🤖 Configured for ML workload (50 partitions, 50% storage)")

    elif job_type == "streaming":
        # Streaming: micro-batch optimization
        spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                      "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        print("🌊 Configured for Streaming workload")

    else:
        print(f"⚠️  Unknown job type: {job_type}. Using default settings.")


def apply_performance_profile(spark: SparkSession, profile_name: str) -> None:
    """
    Apply a pre-configured performance profile.

    Args:
        spark: Active SparkSession
        profile_name: Profile name - 'high_throughput', 'low_latency', 'memory_intensive'

    Profiles:
        - 'high_throughput': Maximize data processing throughput
        - 'low_latency': Optimize for query response time
        - 'memory_intensive': Optimize for in-memory operations

    Example:
        >>> apply_performance_profile(spark, 'high_throughput')
    """
    if profile_name == "high_throughput":
        spark.conf.set("spark.sql.shuffle.partitions", "600")
        spark.conf.set("spark.default.parallelism", "600")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "30MB")
        print("🚀 Applied high_throughput profile (600 partitions, 30MB broadcast)")

    elif profile_name == "low_latency":
        spark.conf.set("spark.sql.shuffle.partitions", "50")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "500MB")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        print("⚡ Applied low_latency profile (50 partitions, 500MB broadcast)")

    elif profile_name == "memory_intensive":
        spark.conf.set("spark.sql.shuffle.partitions", "100")
        spark.conf.set("spark.memory.storageFraction", "0.6")
        spark.conf.set("spark.memory.fraction", "0.9")
        print("💾 Applied memory_intensive profile (60% storage, 90% memory)")

    else:
        print(f"⚠️  Unknown profile: {profile_name}")
