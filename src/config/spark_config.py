"""
Spark Session Configuration

Provides optimized Spark session creation with production-ready settings.
Configures Adaptive Query Execution (AQE), memory management, and performance tuning.
"""
import os
from typing import Optional
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "GoodNote Analytics",
    master: Optional[str] = None,
    enable_hive: bool = False,
    log_level: str = "WARN"
) -> SparkSession:
    """
    Create optimized Spark session with production-ready configurations.

    Features:
    - Adaptive Query Execution (AQE) enabled
    - Optimized memory management
    - Broadcast join threshold tuning
    - Shuffle partition optimization
    - Compression and serialization tuning

    Args:
        app_name: Application name for Spark UI
        master: Spark master URL (default: from SPARK_MASTER env or local[*])
        enable_hive: Enable Hive support (default: False)
        log_level: Logging level - ERROR, WARN, INFO, DEBUG (default: WARN)

    Returns:
        Configured SparkSession

    Environment Variables:
        - SPARK_MASTER: Spark master URL (e.g., spark://host:7077, local[4])
        - SPARK_DRIVER_MEMORY: Driver memory (default: 4g)
        - SPARK_EXECUTOR_MEMORY: Executor memory (default: 4g)
        - SPARK_EXECUTOR_CORES: Cores per executor (default: 4)

    Example:
        >>> spark = create_spark_session(app_name="Data Processing")
        >>> df = spark.read.parquet("data/interactions.parquet")
    """
    # Get configuration from environment or defaults
    if master is None:
        master = os.getenv("SPARK_MASTER", "local[*]")

    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "4")

    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)

    # ============================================
    # 1. ADAPTIVE QUERY EXECUTION (AQE)
    # ============================================
    # AQE optimizes query execution dynamically
    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

    # ============================================
    # 2. MEMORY MANAGEMENT
    # ============================================
    builder = builder \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3")

    # ============================================
    # 3. SHUFFLE OPTIMIZATION
    # ============================================
    # Dynamic partition allocation based on data size
    builder = builder \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.openCostInBytes", "4MB")

    # ============================================
    # 4. BROADCAST JOIN OPTIMIZATION
    # ============================================
    # Increase threshold for broadcast joins (small table optimization)
    builder = builder \
        .config("spark.sql.autoBroadcastJoinThreshold", "100MB")

    # ============================================
    # 5. COMPRESSION & SERIALIZATION
    # ============================================
    builder = builder \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m")

    # ============================================
    # 6. SQL OPTIMIZATIONS
    # ============================================
    builder = builder \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.cbo.enabled", "true")  # Cost-based optimization

    # ============================================
    # 7. WAREHOUSE & CHECKPOINTING
    # ============================================
    warehouse_dir = os.getenv("SPARK_WAREHOUSE_DIR", "/tmp/spark-warehouse")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint")

    builder = builder \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)

    # ============================================
    # 8. UI & MONITORING
    # ============================================
    builder = builder \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events")

    # ============================================
    # 9. HIVE SUPPORT (Optional)
    # ============================================
    if enable_hive:
        builder = builder.enableHiveSupport()

    # Create session
    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel(log_level)

    # Print configuration summary
    print("=" * 60)
    print(f"✅ Spark Session Created: {app_name}")
    print("=" * 60)
    print(f"Master: {master}")
    print(f"Driver Memory: {driver_memory}")
    print(f"Executor Memory: {executor_memory}")
    print(f"Executor Cores: {executor_cores}")
    print(f"AQE Enabled: True")
    print(f"Broadcast Join Threshold: 100MB")
    print(f"Shuffle Partitions: 200")
    print("=" * 60)

    return spark


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
        spark.conf.set("spark.sql.shuffle.partitions", "100")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")
        print("📈 Configured for Analytics workload (100 partitions, 200MB broadcast)")

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


def get_spark_config_summary(spark: SparkSession) -> dict:
    """
    Get summary of current Spark configuration.

    Args:
        spark: Active SparkSession

    Returns:
        Dictionary with key configuration values

    Example:
        >>> spark = create_spark_session()
        >>> config = get_spark_config_summary(spark)
        >>> print(f"AQE Enabled: {config['aqe_enabled']}")
    """
    conf = spark.sparkContext.getConf()

    summary = {
        "app_name": spark.sparkContext.appName,
        "master": spark.sparkContext.master,
        "driver_memory": conf.get("spark.driver.memory", "N/A"),
        "executor_memory": conf.get("spark.executor.memory", "N/A"),
        "executor_cores": conf.get("spark.executor.cores", "N/A"),
        "aqe_enabled": conf.get("spark.sql.adaptive.enabled", "false"),
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "N/A"),
        "broadcast_threshold": conf.get("spark.sql.autoBroadcastJoinThreshold", "N/A"),
    }

    return summary


if __name__ == "__main__":
    # Test configuration
    print("Testing Spark configuration...")
    spark = create_spark_session(app_name="Config Test")

    print("\n" + "=" * 60)
    print("Configuration Summary:")
    print("=" * 60)
    config = get_spark_config_summary(spark)
    for key, value in config.items():
        print(f"{key}: {value}")

    print("\n" + "=" * 60)
    print("Testing job-specific configurations:")
    print("=" * 60)
    configure_job_specific_settings(spark, "etl")
    configure_job_specific_settings(spark, "analytics")

    spark.stop()
    print("\n✅ Configuration test complete")
