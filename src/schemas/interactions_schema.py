"""
Schema definition for interactions data.

This module defines the schema for user interaction events.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


INTERACTIONS_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("action_type", StringType(), False),
    StructField("page_id", StringType(), False),
    StructField("duration_ms", IntegerType(), False),
    StructField("app_version", StringType(), False),
])

INTERACTIONS_REQUIRED_COLUMNS = [f.name for f in INTERACTIONS_SCHEMA.fields]

INTERACTIONS_NOT_NULL_COLUMNS = [f.name for f in INTERACTIONS_SCHEMA.fields if not f.nullable]