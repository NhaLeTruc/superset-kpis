#!/usr/bin/env python3
"""
Job 3: Performance Metrics

Calculates performance metrics:
- Percentiles (P50, P95, P99) by app version
- Device performance correlation
- Statistical anomaly detection

Writes results to PostgreSQL for monitoring dashboards.

Usage (via helper script):
    ./scripts/run_spark_job.sh src/jobs/03_performance_metrics.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --dev-mode \
        --write-to-db

Usage (direct spark-submit):
    docker exec goodnote-spark-master bash -c '/opt/spark/bin/spark-submit \
        --master "$(SPARK_MASTER_URL)" \
        /opt/spark-apps/src/jobs/03_performance_metrics.py \
        --enriched-path /app/data/processed/enriched_interactions.parquet \
        --dev-mode \
        --write-to-db'
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.constants import (
    ANOMALY_SEVERITY_CRITICAL,
    ANOMALY_SEVERITY_HIGH,
    ANOMALY_SEVERITY_MEDIUM,
    DEFAULT_PERCENTILES,
    TABLE_DEVICE_CORRELATION,
    TABLE_DEVICE_PERFORMANCE,
    TABLE_PERFORMANCE_ANOMALIES,
    TABLE_PERFORMANCE_BY_VERSION,
    Z_SCORE_ANOMALY_THRESHOLD,
)
from src.jobs.base_job import BaseAnalyticsJob
from src.schemas.columns import (
    COL_ANOMALY_TYPE,
    COL_APP_VERSION,
    COL_AVG_DURATION_MS,
    COL_BASELINE_MEAN,
    COL_DESCRIPTION,
    COL_DETECTED_AT,
    COL_DEVICE_TYPE,
    COL_DURATION_MS,
    COL_ETA_SQUARED,
    COL_EXPECTED_VALUE,
    COL_F_STATISTIC,
    COL_METRIC_DATE,
    COL_METRIC_NAME,
    COL_METRIC_VALUE,
    COL_P50,
    COL_P50_DURATION_MS,
    COL_P95,
    COL_P95_DURATION_MS,
    COL_P99,
    COL_P99_DURATION_MS,
    COL_SEVERITY,
    COL_TIMESTAMP,
    COL_USER_ID,
    COL_Z_SCORE,
)
from src.transforms.performance import (
    calculate_device_correlation,
    calculate_percentiles,
    detect_anomalies_statistical,
)


class PerformanceMetricsJob(BaseAnalyticsJob):
    """Performance Metrics Analytics Job."""

    def __init__(self):
        super().__init__(job_name="Performance Metrics", job_type="analytics")
        self._counts: dict[str, int] = {}

    @staticmethod
    def _rename_percentile_columns(df: DataFrame) -> DataFrame:
        """Rename raw p50/p95/p99 output from calculate_percentiles to *_duration_ms names."""
        return (
            df.withColumnRenamed(COL_P50, COL_P50_DURATION_MS)
            .withColumnRenamed(COL_P95, COL_P95_DURATION_MS)
            .withColumnRenamed(COL_P99, COL_P99_DURATION_MS)
        )

    def get_argument_parser(self) -> argparse.ArgumentParser:
        """Configure job-specific arguments."""
        parser = argparse.ArgumentParser(description="GoodNote Performance Metrics")
        parser.add_argument(
            "--enriched-path", required=True, help="Path to enriched interactions Parquet"
        )
        parser.add_argument(
            "--write-to-db", action="store_true", help="Write results to PostgreSQL"
        )
        parser.add_argument("--output-path", help="Optional: Write results to Parquet")
        parser.add_argument(
            "--dev-mode", action="store_true", help="Enable development mode with summary stats"
        )
        return parser

    def compute_metrics(self) -> dict[str, DataFrame]:
        """
        Compute all performance metrics.

        Returns:
            Dictionary with DataFrames for each metric
        """
        # Read enriched data
        enriched_df = self.read_parquet(self.args.enriched_path, "enriched interactions")

        # Validate required columns exist
        required_columns = [
            COL_APP_VERSION,
            COL_DEVICE_TYPE,
            COL_DURATION_MS,
            COL_TIMESTAMP,
            COL_USER_ID,
        ]
        warnings = self.validate_dataframe(
            enriched_df, required_columns=required_columns, name="enriched_df"
        )
        for warning in warnings:
            print(f"   ⚠️  {warning}")

        # Add date column for time-series analysis
        enriched_df = enriched_df.withColumn(COL_METRIC_DATE, F.to_date(COL_TIMESTAMP))

        metrics = {}

        # 1. Percentiles by App Version
        print("\n📊 Calculating Performance Percentiles by App Version...")
        version_perf_df = calculate_percentiles(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            group_by_columns=[COL_APP_VERSION, COL_METRIC_DATE],
            percentiles=DEFAULT_PERCENTILES,
        )

        # Rename percentile columns to match database schema
        version_perf_df = self._rename_percentile_columns(version_perf_df).persist()

        version_count = version_perf_df.count()
        print(f"   ✅ Computed percentiles for {version_count} app version-date combinations")
        metrics["performance_by_version"] = version_perf_df

        # 2. Device Performance
        print("\n📊 Calculating Device Performance...")
        device_percentiles = calculate_percentiles(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            group_by_columns=[COL_DEVICE_TYPE, COL_METRIC_DATE],
            percentiles=DEFAULT_PERCENTILES,
        )

        device_percentiles = self._rename_percentile_columns(device_percentiles)

        # Add user count
        device_perf_with_users = enriched_df.groupBy(COL_DEVICE_TYPE, COL_METRIC_DATE).agg(
            F.countDistinct(COL_USER_ID).alias("user_count")
        )

        device_perf_final = device_percentiles.join(
            device_perf_with_users, [COL_DEVICE_TYPE, COL_METRIC_DATE], "left"
        ).persist()

        device_count = device_perf_final.count()
        print(f"   ✅ Computed device performance for {device_count} device-date combinations")
        metrics["device_performance"] = device_perf_final

        # 2b. Device-Performance Correlation (ANOVA)
        print("\n📊 Calculating Device-Performance Correlation (ANOVA)...")
        # enriched_df already has device_type, so no need to pass metadata_df
        device_correlation_df = calculate_device_correlation(
            interactions_df=enriched_df,
        ).persist()
        correlation_count = device_correlation_df.count()
        print(f"   ✅ Computed correlation for {correlation_count} device types")

        # Show ANOVA results
        anova_stats = device_correlation_df.select(COL_F_STATISTIC, COL_ETA_SQUARED).first()
        if anova_stats and anova_stats[COL_F_STATISTIC] is not None:
            print(f"   📈 F-statistic: {anova_stats[COL_F_STATISTIC]:.2f}")
            print(f"   📈 Eta-squared (effect size): {anova_stats[COL_ETA_SQUARED]:.4f}")
        metrics["device_correlation"] = device_correlation_df

        # 3. Statistical Anomaly Detection
        print("\n📊 Detecting Performance Anomalies...")
        anomalies_df = detect_anomalies_statistical(
            df=enriched_df,
            value_column=COL_DURATION_MS,
            z_threshold=Z_SCORE_ANOMALY_THRESHOLD,
            group_by_columns=[COL_APP_VERSION, COL_METRIC_DATE],
        )

        # Rename duration_ms to metric_value for database schema
        anomalies_df = anomalies_df.withColumnRenamed(COL_DURATION_MS, COL_METRIC_VALUE)

        # Enrich anomalies with severity classification
        anomalies_df = anomalies_df.withColumn(
            COL_SEVERITY,
            F.when(F.abs(F.col(COL_Z_SCORE)) >= ANOMALY_SEVERITY_CRITICAL, "critical")
            .when(F.abs(F.col(COL_Z_SCORE)) >= ANOMALY_SEVERITY_HIGH, "high")
            .when(F.abs(F.col(COL_Z_SCORE)) >= ANOMALY_SEVERITY_MEDIUM, "medium")
            .otherwise("low"),
        )

        # Rename baseline_mean → expected_value to match the DB schema column definition
        anomalies_df = anomalies_df.withColumnRenamed(COL_BASELINE_MEAN, COL_EXPECTED_VALUE)

        # Add detection timestamp and description
        anomalies_df = (
            anomalies_df.withColumn(COL_DETECTED_AT, F.current_timestamp())
            .withColumn(COL_METRIC_NAME, F.lit(COL_DURATION_MS))
            .withColumn(
                COL_DESCRIPTION,
                F.concat(
                    F.lit("Performance anomaly detected for "),
                    F.col(COL_APP_VERSION),
                    F.lit(" on "),
                    F.col(COL_METRIC_DATE).cast("string"),
                    F.lit(": "),
                    F.round(F.col(COL_METRIC_VALUE), 2).cast("string"),
                    F.lit("ms ("),
                    F.round(F.abs(F.col(COL_Z_SCORE)), 2).cast("string"),
                    F.lit(" std deviations, "),
                    F.col(COL_ANOMALY_TYPE),
                    F.lit(")"),
                ),
            )
        )

        # Drop internal computation columns and irrelevant interaction fields.
        # detect_anomalies_statistical() returns the full enriched_df row for each
        # anomaly, including intermediate stats (initial_mean, initial_stddev,
        # initial_z_score, baseline_stddev) and raw interaction fields that belong
        # only in the source data, not in the anomalies output table.
        anomalies_df = anomalies_df.select(
            COL_APP_VERSION,
            COL_DEVICE_TYPE,
            COL_METRIC_DATE,
            COL_METRIC_NAME,
            COL_METRIC_VALUE,
            COL_EXPECTED_VALUE,
            COL_Z_SCORE,
            COL_SEVERITY,
            COL_DETECTED_AT,
            COL_DESCRIPTION,
        )

        # Cache and count once
        anomalies_df = anomalies_df.persist()
        anomaly_count = anomalies_df.count()

        if anomaly_count > 0:
            print(f"   ⚠️  Detected {anomaly_count} performance anomalies")

            # Show critical anomalies
            critical_count = anomalies_df.filter(F.col(COL_SEVERITY) == "critical").count()
            if critical_count > 0:
                print(f"   🚨 {critical_count} CRITICAL anomalies require immediate attention!")
        else:
            print("   ✅ No anomalies detected")

        metrics["performance_anomalies"] = anomalies_df

        # Store counts for print_summary() to avoid redundant count() calls
        self._counts = {
            "version": version_count,
            "device": device_count,
            "correlation": correlation_count,
            "anomaly": anomaly_count,
        }

        # Unpersist intermediate DataFrames (metrics DataFrames will be unpersisted after write)
        enriched_df.unpersist()

        return metrics

    def print_summary(self, metrics: dict[str, DataFrame]) -> None:  # noqa: PLR0915
        """Print summary of computed metrics."""
        print("\n" + "=" * 60)
        print("📊 Performance Metrics Summary")
        print("=" * 60)

        # Version Performance Summary
        version_df = metrics["performance_by_version"]
        version_count = self._counts.get("version")
        if version_count is None:
            version_count = version_df.count()
        print("\nPerformance by Version:")
        print(f"  Total Combinations: {version_count:,}")

        version_summary = version_df.agg(
            F.avg(COL_P50_DURATION_MS).alias("avg_p50"),
            F.avg(COL_P95_DURATION_MS).alias("avg_p95"),
            F.avg(COL_P99_DURATION_MS).alias("avg_p99"),
        ).collect()[0]

        avg_p50 = version_summary["avg_p50"] if version_summary["avg_p50"] is not None else 0.0
        avg_p95 = version_summary["avg_p95"] if version_summary["avg_p95"] is not None else 0.0
        avg_p99 = version_summary["avg_p99"] if version_summary["avg_p99"] is not None else 0.0
        print(f"  Average P50: {avg_p50:,.0f}ms")
        print(f"  Average P95: {avg_p95:,.0f}ms")
        print(f"  Average P99: {avg_p99:,.0f}ms")

        # Device Performance Summary
        device_df = metrics["device_performance"]
        device_count = self._counts.get("device")
        if device_count is None:
            device_count = device_df.count()
        print("\nDevice Performance:")
        print(f"  Total Combinations: {device_count:,}")

        device_summary = (
            device_df.groupBy(COL_DEVICE_TYPE)
            .agg(F.avg(COL_AVG_DURATION_MS).alias("avg_duration"))
            .orderBy(F.desc("avg_duration"))
        )

        print("  Average Duration by Device:")
        for row in device_summary.collect():
            avg_duration = row["avg_duration"] if row["avg_duration"] is not None else 0.0
            print(f"    {row[COL_DEVICE_TYPE]}: {avg_duration:,.0f}ms")

        # Device Correlation Summary (ANOVA)
        correlation_df = metrics["device_correlation"]
        print("\nDevice-Performance Correlation (ANOVA):")
        anova_row = correlation_df.select(COL_F_STATISTIC, COL_ETA_SQUARED).first()
        if anova_row and anova_row[COL_F_STATISTIC] is not None:
            f_stat = anova_row[COL_F_STATISTIC]
            eta_sq = anova_row[COL_ETA_SQUARED]
            print(f"  F-statistic: {f_stat:.2f}")
            print(f"  Eta-squared: {eta_sq:.4f} ({eta_sq * 100:.1f}% variance explained)")
            # Interpret effect size
            if eta_sq >= 0.14:
                effect = "large"
            elif eta_sq >= 0.06:
                effect = "medium"
            else:
                effect = "small"
            print(f"  Effect size: {effect}")
        else:
            print("  Insufficient data for correlation analysis")

        # Anomalies Summary
        anomalies_df = metrics["performance_anomalies"]
        anomaly_count = self._counts.get("anomaly")
        if anomaly_count is None:
            anomaly_count = anomalies_df.count()
        print("\nPerformance Anomalies:")
        print(f"  Total Detected: {anomaly_count}")

        if anomaly_count > 0:
            severity_dist = anomalies_df.groupBy(COL_SEVERITY).count().orderBy(F.desc("count"))
            print("  By Severity:")
            for row in severity_dist.collect():
                print(f"    {row[COL_SEVERITY]}: {row['count']}")

        print("=" * 60)

    def get_table_mapping(self) -> dict[str, str]:
        """Get database table mapping with special mode for anomalies."""
        return {
            "performance_by_version": TABLE_PERFORMANCE_BY_VERSION,
            "device_performance": TABLE_DEVICE_PERFORMANCE,
            "device_correlation": TABLE_DEVICE_CORRELATION,
            "performance_anomalies": TABLE_PERFORMANCE_ANOMALIES,
        }


def main():
    """Main job execution."""
    job = PerformanceMetricsJob()
    sys.exit(job.run())


if __name__ == "__main__":
    main()
