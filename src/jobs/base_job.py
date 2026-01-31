"""
Base Analytics Job Class

Provides common infrastructure for all analytics jobs:
- Spark session management
- Monitoring context setup
- Argument parsing
- Error handling and logging
- Template method pattern for job execution

Jobs should inherit from BaseAnalyticsJob and implement:
- compute_metrics() - core computation logic
- print_summary() - custom reporting
- get_argument_parser() - job-specific arguments
"""

from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import argparse

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

from src.config.database_config import write_to_postgres
from src.config.spark_config import (
    configure_job_specific_settings,
    create_spark_session,
    get_spark_config_summary,
)
from src.utils.data_quality import detect_nulls
from src.utils.monitoring import create_monitoring_context, log_monitoring_summary


class BaseAnalyticsJob(ABC):
    """
    Base class for all analytics jobs.

    Provides common infrastructure and template method pattern.
    Subclasses implement job-specific logic.
    """

    def __init__(self, job_name: str, job_type: str = "analytics", data_size_gb: int = 1):
        """
        Initialize base job.

        Args:
            job_name: Human-readable name (e.g., "User Engagement Analytics")
            job_type: Type for Spark tuning - "etl", "analytics", or "ml"
            data_size_gb: Estimated data size in GB for dynamic partition calculation.
                          If > 0, partitions are calculated as max(200, data_size_gb * 1024 / 128).
                          If 0, uses default partition counts per job_type.
        """
        self.job_name = job_name
        self.job_type = job_type
        self.data_size_gb = data_size_gb
        self.spark: SparkSession | None = None
        self.monitoring_ctx: dict | None = None
        self.args = None
        self.partition_by: list[str] = ["date"]  # Default partition columns for parquet output

    def read_parquet(self, path: str, name: str = "data") -> DataFrame:
        """
        Generic method to read any Parquet file.

        Args:
            path: Path to Parquet file/directory
            name: Display name for logging

        Returns:
            DataFrame
        """
        print(f"📖 Reading {name} from: {path}")
        df = self.spark.read.parquet(path).persist()
        record_count = df.count()

        # Track in monitoring
        if self.monitoring_ctx:
            self.monitoring_ctx["record_counter"].add(record_count)

        print(f"   ✅ Loaded {record_count:,} records. Please unpersist when done.")
        return df

    def read_csv(
        self,
        path: str,
        name: str = "data",
        schema: StructType | None = None,
        num_partitions: int = 1,
    ) -> DataFrame:
        """
        Generic method to read any CSV file.

        Args:
            path: Path to CSV file
            name: Display name for logging
            schema: Optional StructType schema for reading
            num_partitions: Number of partitions to repartition DataFrame into

        Returns:
            DataFrame
        """
        print(f"📖 Reading {name} from: {path}")

        if schema:
            df = self.spark.read.csv(path, header=True, schema=schema)
        else:
            df = self.spark.read.csv(path, header=True, inferSchema=True)

        df = df.repartition(num_partitions).persist()
        record_count = df.count()

        # Track in monitoring
        if self.monitoring_ctx:
            self.monitoring_ctx["record_counter"].add(record_count)

        print(f"   ✅ Loaded {record_count:,} records")
        return df

    def validate_dataframe(
        self,
        df: DataFrame,
        required_columns: list[str] | None = None,
        not_null_columns: list[str] | None = None,
        name: str = "DataFrame",
        schema: StructType | None = None,
    ) -> list[str]:
        """
        Validate a DataFrame's structure and data quality.

        Args:
            df: DataFrame to validate
            required_columns: Columns that must exist (ignored if schema provided)
            not_null_columns: Columns to check for NULLs (ignored if schema provided)
            name: Display name for error messages
            schema: Optional StructType schema - if provided, derives required_columns,
                    not_null_columns, and validates datatypes automatically.
                    Type validation uses schema metadata only (O(1), no data scan).

        Returns:
            List of warning messages (empty if no issues)

        Raises:
            ValueError: If required columns are missing or no validation criteria provided
        """
        warnings = []

        # If schema provided, derive columns from it
        if schema:
            required_columns = [f.name for f in schema.fields]
            not_null_columns = [f.name for f in schema.fields if not f.nullable]
        elif required_columns is None:
            raise ValueError("Must provide either 'schema' or 'required_columns'")

        # Check for required columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in {name}: {missing_cols}")

        # Validate datatypes (metadata only - O(1), no data scan)
        if schema:
            for field in schema.fields:
                if field.name in df.columns:
                    actual_type = df.schema[field.name].dataType
                    if actual_type != field.dataType:
                        warnings.append(
                            f"Type mismatch in {name}.{field.name}: "
                            f"expected {field.dataType.simpleString()}, "
                            f"got {actual_type.simpleString()}"
                        )

        # Detect nulls in non-nullable columns
        if not_null_columns:
            null_rows_df = detect_nulls(df, not_null_columns)
            null_count = null_rows_df.count()
            if null_count > 0:
                warnings.append(f"NULL values found in {name}: {null_count} rows affected")

        return warnings

    def write_to_database(
        self,
        metrics: dict[str, DataFrame],
        table_mapping: dict[str, str],
        mode: str = "overwrite",
        batch_size: int = 10000,
        num_partitions: int | None = None,
    ) -> None:
        """
        Write multiple DataFrames to PostgreSQL.

        Args:
            metrics: Dictionary mapping metric names to DataFrames
            table_mapping: Dictionary mapping metric names to table names
            mode: Write mode - "append", "overwrite", "error", "ignore"
            batch_size: Rows per batch for JDBC
            num_partitions: Number of parallel write partitions
        """
        print("\n💾 Writing metrics to PostgreSQL...")

        for metric_name, df in metrics.items():
            if metric_name not in table_mapping:
                print(f"   ⚠️  Skipping {metric_name} - no table mapping")
                continue

            table_name = table_mapping[metric_name]
            print(f"\n   📝 Writing {metric_name} to {table_name}...")

            try:
                write_to_postgres(
                    df=df,
                    table_name=table_name,
                    mode=mode,
                    batch_size=batch_size,
                    num_partitions=num_partitions,
                )

            except Exception as e:
                print(f"   ❌ Failed to write {metric_name}: {e!s}")
                raise

    def write_to_parquet(
        self,
        metrics: dict[str, DataFrame],
        output_path: str,
        partition_by: list[str] | None = None,
        coalesce_partitions: int | None = None,
    ) -> None:
        """
        Write multiple DataFrames to Parquet files.

        Args:
            metrics: Dictionary mapping metric names to DataFrames
            output_path: Base directory path for outputs
            partition_by: Optional list of column names to partition by
        """
        print(f"\n💾 Writing metrics to Parquet: {output_path}")

        for metric_name, df in metrics.items():
            output_file = f"{output_path}/{metric_name}"
            if coalesce_partitions:
                df = df.coalesce(coalesce_partitions)
            writer = df.write.mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.parquet(output_file)
            print(f"   ✅ Wrote {metric_name}")

    def setup_spark(self) -> None:
        """Create and configure Spark session."""
        self.spark = create_spark_session(app_name=f"GoodNote - {self.job_name}")
        configure_job_specific_settings(
            self.spark, job_type=self.job_type, data_size_gb=self.data_size_gb
        )

        # Print final config after all tuning applied
        config = get_spark_config_summary(self.spark)
        print("=" * 60)
        print(f"Spark Session: {config['app_name']} ({self.job_type})")
        print("=" * 60)
        print(f"Master: {config['master']}")
        print(f"Driver Memory: {config['driver_memory']}")
        print(f"Executor Memory: {config['executor_memory']}")
        print(f"Shuffle Partitions: {config['shuffle_partitions']}")
        print(f"Broadcast Threshold: {config['broadcast_threshold']}")
        print(f"AQE Enabled: {config['aqe_enabled']}")
        print("=" * 60)

    def setup_monitoring(self, context_name: str) -> None:
        """
        Create monitoring context.

        Args:
            context_name: Name for monitoring context (e.g., "user_engagement")
        """
        self.monitoring_ctx = create_monitoring_context(self.spark.sparkContext, context_name)

    def print_job_header(self) -> None:
        """Print job startup header."""
        print("=" * 60)
        print(f"🚀 GoodNote {self.job_name}")
        print("=" * 60)
        print(f"Start Time: {datetime.now()}")

        # Print key arguments
        if self.args:
            for key, value in vars(self.args).items():
                if value and key not in ["func"]:  # Skip empty and internal args
                    print(f"{key.replace('_', ' ').title()}: {value}")

        print("=" * 60)

    def print_job_footer(self, success: bool = True) -> None:
        """
        Print job completion footer.

        Args:
            success: Whether job completed successfully
        """
        print("\n" + "=" * 60)
        if success:
            print("✅ Job completed successfully!")
        else:
            print("❌ Job failed!")
        print(f"End Time: {datetime.now()}")
        print("=" * 60)

    @abstractmethod
    def get_argument_parser(self) -> argparse.ArgumentParser:
        """
        Create argument parser with job-specific arguments.

        Returns:
            Configured ArgumentParser

        Example:
            parser = argparse.ArgumentParser(description="My Job")
            parser.add_argument("--input-path", required=True)
            return parser
        """
        pass

    @abstractmethod
    def compute_metrics(self) -> dict[str, DataFrame]:
        """
        Core job computation logic.

        Returns:
            Dictionary mapping metric names to DataFrames

        Example:
            return {
                "dau": dau_df,
                "mau": mau_df
            }
        """
        pass

    @abstractmethod
    def print_summary(self, metrics: dict[str, DataFrame]) -> None:
        """
        Print job-specific summary.

        Args:
            metrics: Computed metrics dictionary

        Example:
            print("DAU Average:", metrics["dau"].agg({"dau": "avg"}).collect()[0][0])
        """
        pass

    @abstractmethod
    def get_table_mapping(self) -> dict[str, str] | None:
        """
        Get mapping from metric names to database table names.

        Override this if job writes to database.

        Returns:
            Dictionary mapping metric names to table names, or None
        """
        return None

    def run(self) -> int:
        """
        Execute complete job pipeline.

        Template method that orchestrates:
        1. Argument parsing
        2. Spark setup
        3. Monitoring setup
        4. Core computation
        5. Output writing
        6. Summary reporting
        7. Cleanup

        Returns:
            Exit code (0 for success, 1 for failure)
        """
        try:
            # Parse arguments
            parser = self.get_argument_parser()
            self.args = parser.parse_args()

            # Print header
            self.print_job_header()

            # Setup Spark
            self.setup_spark()

            # Setup monitoring
            context_name = self.job_name.lower().replace(" ", "_").replace("-", "_")
            self.setup_monitoring(context_name)

            # Execute core computation
            metrics = self.compute_metrics()

            # Print summary
            self.print_summary(metrics)

            # Write to database if requested
            if hasattr(self.args, "write_to_db") and self.args.write_to_db:
                table_mapping = self.get_table_mapping()
                if table_mapping:
                    self.write_to_database(metrics, table_mapping)

            # Write to Parquet if requested
            if hasattr(self.args, "output_path") and self.args.output_path:
                self.write_to_parquet(
                    metrics, self.args.output_path, partition_by=self.partition_by
                )

            # Log monitoring summary
            if self.monitoring_ctx:
                print("\n")
                log_monitoring_summary(self.monitoring_ctx, self.job_name)

            # Print footer
            self.print_job_footer(success=True)

            return 0

        except Exception as e:
            print("\n" + "=" * 60)
            print(f"❌ Job failed with error: {e!s}")
            print("=" * 60)
            traceback.print_exc()

            self.print_job_footer(success=False)

            return 1

        finally:
            if self.spark:
                self.spark.stop()
