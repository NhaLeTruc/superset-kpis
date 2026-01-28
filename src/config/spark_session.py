"""
Spark Session Creation

Provides factory function for creating optimized Spark sessions with
production-ready configurations including AQE, memory management, and performance tuning.
"""

import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "GoodNote Analytics",
    master: str | None = None,
    enable_hive: bool = False,
    log_level: str = "WARN",
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
    builder = SparkSession.builder.appName(app_name).master(master)

    # Adaptive Query Execution (AQE)
    builder = (
        builder.config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    )

    # Memory Management
    builder = (
        builder.config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
    )

    # Shuffle Optimization
    builder = (
        builder.config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.files.openCostInBytes", "4MB")
    )

    # Broadcast Join Optimization
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "100MB")

    # Compression & Serialization
    builder = (
        builder.config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
    )

    # SQL Optimizations
    builder = (
        builder.config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .config("spark.sql.join.preferSortMergeJoin", "true")
        .config("spark.sql.cbo.enabled", "true")
    )

    # Warehouse & Checkpointing
    warehouse_dir = os.getenv("SPARK_WAREHOUSE_DIR", "/tmp/spark-warehouse")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint")
    event_log_dir = os.getenv("SPARK_EVENT_LOG_DIR", "/tmp/spark-events")
    os.makedirs(event_log_dir, exist_ok=True)

    builder = builder.config("spark.sql.warehouse.dir", warehouse_dir).config(
        "spark.sql.streaming.checkpointLocation", checkpoint_dir
    )

    # UI & Monitoring
    builder = (
        builder.config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
    )

    # Hive Support (Optional)
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
    print("AQE Enabled: True")
    print("Broadcast Join Threshold: 100MB")
    print("Shuffle Partitions: 200")
    print("=" * 60)

    return spark


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
