#!/usr/bin/env python3
"""
Job 3: Performance Metrics

Calculates performance metrics:
- Percentiles (P50, P95, P99) by app version
- Device performance correlation
- Statistical anomaly detection

Writes results to PostgreSQL for monitoring dashboards.

Usage:
    python src/jobs/03_performance_metrics.py \
        --enriched-path data/processed/enriched_interactions.parquet \
        --write-to-db
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.spark_config import create_spark_session, configure_job_specific_settings
from src.config.database_config import write_to_postgres
from src.utils.monitoring import create_monitoring_context, log_monitoring_summary
from src.transforms.performance_transforms import (
    calculate_percentiles,
    calculate_device_correlation,
    detect_anomalies_statistical
)


def read_enriched_data(spark, path: str) -> DataFrame:
    """Read enriched interactions from Parquet."""
    print(f"📖 Reading enriched data from: {path}")
    df = spark.read.parquet(path)
    print(f"   ✅ Loaded {df.count():,} enriched interactions")
    return df


def compute_performance_metrics(enriched_df: DataFrame) -> dict:
    """
    Compute all performance metrics.

    Returns:
        Dictionary with DataFrames for each metric
    """
    metrics = {}

    # Add date column for time-series analysis
    enriched_df = enriched_df.withColumn("metric_date", F.to_date("timestamp"))

    # 1. Percentiles by App Version
    print("\n📊 Calculating Performance Percentiles by App Version...")
    version_perf_df = calculate_percentiles(
        df=enriched_df,
        value_column="duration_ms",
        group_by_columns=["app_version", "metric_date"],
        percentiles=[0.5, 0.95, 0.99]
    )

    # Rename percentile columns to match database schema
    version_perf_df = version_perf_df \
        .withColumnRenamed("percentile_0.5", "p50_duration_ms") \
        .withColumnRenamed("percentile_0.95", "p95_duration_ms") \
        .withColumnRenamed("percentile_0.99", "p99_duration_ms")

    # Add additional statistics
    version_stats = enriched_df.groupBy("app_version", "metric_date").agg(
        F.count("*").alias("total_interactions"),
        F.avg("duration_ms").alias("avg_duration_ms"),
        F.min("duration_ms").alias("min_duration_ms"),
        F.max("duration_ms").alias("max_duration_ms")
    )

    version_perf_df = version_perf_df.join(version_stats, ["app_version", "metric_date"])

    version_count = version_perf_df.count()
    print(f"   ✅ Computed percentiles for {version_count} app version-date combinations")
    metrics["performance_by_version"] = version_perf_df

    # 2. Device Performance
    print("\n📊 Calculating Device Performance Correlation...")
    device_perf_df = calculate_device_correlation(
        enriched_df,
        enriched_df.select("user_id", "device_type").distinct()
    )

    # Add date dimension
    device_perf_with_date = enriched_df.groupBy("device_type", "metric_date").agg(
        F.countDistinct("user_id").alias("user_count"),
        F.count("*").alias("total_interactions"),
        F.avg("duration_ms").alias("avg_duration_ms")
    )

    # Calculate percentiles by device and date
    device_percentiles = calculate_percentiles(
        df=enriched_df,
        value_column="duration_ms",
        group_by_columns=["device_type", "metric_date"],
        percentiles=[0.5, 0.95, 0.99]
    )

    device_percentiles = device_percentiles \
        .withColumnRenamed("percentile_0.5", "p50_duration_ms") \
        .withColumnRenamed("percentile_0.95", "p95_duration_ms") \
        .withColumnRenamed("percentile_0.99", "p99_duration_ms")

    device_perf_final = device_perf_with_date.join(
        device_percentiles,
        ["device_type", "metric_date"]
    )

    device_count = device_perf_final.count()
    print(f"   ✅ Computed device performance for {device_count} device-date combinations")
    metrics["device_performance"] = device_perf_final

    # 3. Statistical Anomaly Detection
    print("\n📊 Detecting Performance Anomalies...")
    anomalies_df = detect_anomalies_statistical(
        df=enriched_df,
        value_column="duration_ms",
        z_threshold=3.0,
        group_by_columns=["app_version", "metric_date"]
    )

    if anomalies_df.count() > 0:
        # Rename duration_ms to metric_value for database schema
        anomalies_df = anomalies_df.withColumnRenamed("duration_ms", "metric_value")

        # Enrich anomalies with severity classification
        anomalies_df = anomalies_df.withColumn(
            "severity",
            F.when(F.abs(F.col("z_score")) >= 4.0, "critical")
             .when(F.abs(F.col("z_score")) >= 3.5, "high")
             .when(F.abs(F.col("z_score")) >= 3.0, "medium")
             .otherwise("low")
        )

        # Add detection timestamp and description
        anomalies_df = anomalies_df \
            .withColumn("detected_at", F.lit(datetime.now())) \
            .withColumn("metric_name", F.lit("duration_ms")) \
            .withColumn(
                "description",
                F.concat(
                    F.lit("Performance anomaly detected for "),
                    F.col("app_version"),
                    F.lit(" on "),
                    F.col("metric_date").cast("string"),
                    F.lit(": "),
                    F.round("metric_value", 2).cast("string"),
                    F.lit("ms ("),
                    F.round(F.abs("z_score"), 2).cast("string"),
                    F.lit(" std deviations, "),
                    F.col("anomaly_type"),
                    F.lit(")")
                )
            )

        anomaly_count = anomalies_df.count()
        print(f"   ⚠️  Detected {anomaly_count} performance anomalies")

        # Show critical anomalies
        critical_anomalies = anomalies_df.filter("severity = 'critical'").count()
        if critical_anomalies > 0:
            print(f"   🚨 {critical_anomalies} CRITICAL anomalies require immediate attention!")

    else:
        print("   ✅ No anomalies detected")

    metrics["performance_anomalies"] = anomalies_df

    return metrics


def write_metrics_to_db(metrics: dict) -> None:
    """Write all metrics to PostgreSQL."""
    print("\n💾 Writing performance metrics to PostgreSQL...")

    # Table mapping
    table_mapping = {
        "performance_by_version": "performance_by_version",
        "device_performance": "device_performance",
        "performance_anomalies": "performance_anomalies"
    }

    for metric_name, df in metrics.items():
        table_name = table_mapping[metric_name]
        print(f"\n   📝 Writing {metric_name} to {table_name}...")

        try:
            # Special handling for anomalies (append mode)
            mode = "append" if metric_name == "performance_anomalies" else "overwrite"

            write_to_postgres(
                df=df,
                table_name=table_name,
                mode=mode,
                batch_size=10000,
                num_partitions=4
            )
            print(f"   ✅ Successfully wrote {df.count():,} rows")

        except Exception as e:
            print(f"   ❌ Failed to write {metric_name}: {str(e)}")
            raise


def print_metrics_summary(metrics: dict) -> None:
    """Print summary of computed metrics."""
    print("\n" + "=" * 60)
    print("📊 Performance Metrics Summary")
    print("=" * 60)

    # Version Performance Summary
    version_df = metrics["performance_by_version"]
    print(f"\nPerformance by Version:")
    print(f"  Total Combinations: {version_df.count():,}")

    version_summary = version_df.agg({
        "p50_duration_ms": "avg",
        "p95_duration_ms": "avg",
        "p99_duration_ms": "avg"
    }).collect()[0]

    print(f"  Average P50: {version_summary[0]:,.0f}ms")
    print(f"  Average P95: {version_summary[1]:,.0f}ms")
    print(f"  Average P99: {version_summary[2]:,.0f}ms")

    # Device Performance Summary
    device_df = metrics["device_performance"]
    print(f"\nDevice Performance:")
    print(f"  Total Combinations: {device_df.count():,}")

    device_summary = device_df.groupBy("device_type").agg(
        F.avg("avg_duration_ms").alias("avg_duration")
    ).orderBy(F.desc("avg_duration"))

    print("  Average Duration by Device:")
    for row in device_summary.collect():
        print(f"    {row['device_type']}: {row['avg_duration']:,.0f}ms")

    # Anomalies Summary
    anomalies_df = metrics["performance_anomalies"]
    anomaly_count = anomalies_df.count()
    print(f"\nPerformance Anomalies:")
    print(f"  Total Detected: {anomaly_count}")

    if anomaly_count > 0:
        severity_dist = anomalies_df.groupBy("severity").count().orderBy(F.desc("count"))
        print("  By Severity:")
        for row in severity_dist.collect():
            print(f"    {row['severity']}: {row['count']}")

    print("=" * 60)


def main():
    """Main job execution."""
    parser = argparse.ArgumentParser(description="GoodNote Performance Metrics")
    parser.add_argument("--enriched-path", required=True, help="Path to enriched interactions Parquet")
    parser.add_argument("--write-to-db", action="store_true", help="Write results to PostgreSQL")
    parser.add_argument("--output-path", help="Optional: Write results to Parquet")

    args = parser.parse_args()

    print("=" * 60)
    print("🚀 GoodNote Performance Metrics")
    print("=" * 60)
    print(f"Start Time: {datetime.now()}")
    print(f"Enriched Data Path: {args.enriched_path}")
    print(f"Write to Database: {args.write_to_db}")
    print("=" * 60)

    # Create Spark session
    spark = create_spark_session(app_name="GoodNote - Performance Metrics")
    configure_job_specific_settings(spark, job_type="analytics")

    # Create monitoring context
    monitoring_ctx = create_monitoring_context(spark.sparkContext, "performance_metrics")

    try:
        # Read enriched data
        enriched_df = read_enriched_data(spark, args.enriched_path)

        # Track record count
        record_count = enriched_df.count()
        monitoring_ctx["record_counter"].add(record_count)

        # Compute all performance metrics
        metrics = compute_performance_metrics(enriched_df)

        # Print summary
        print_metrics_summary(metrics)

        # Write to PostgreSQL if requested
        if args.write_to_db:
            write_metrics_to_db(metrics)

        # Optionally write to Parquet
        if args.output_path:
            print(f"\n💾 Writing metrics to Parquet: {args.output_path}")
            for metric_name, df in metrics.items():
                output_file = f"{args.output_path}/{metric_name}"
                df.write.mode("overwrite").parquet(output_file)
                print(f"   ✅ Wrote {metric_name}")

        # Log monitoring summary
        print("\n")
        log_monitoring_summary(monitoring_ctx, "Performance Metrics Job")

        print("\n" + "=" * 60)
        print("✅ Job completed successfully!")
        print(f"End Time: {datetime.now()}")
        print("=" * 60)

        sys.exit(0)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Job failed with error: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
