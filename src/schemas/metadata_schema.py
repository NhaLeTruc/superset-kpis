"""
Schema definition for user metadata.

This module defines the schema for user metadata/profile information.
"""
from pyspark.sql.types import StructType, StructField, StringType, DateType


METADATA_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("country", StringType(), False),
    StructField("subscription_type", StringType(), False),
    StructField("registration_date", DateType(), True),
])

METADATA_REQUIRED_COLUMNS = [f.name for f in METADATA_SCHEMA.fields]

METADATA_NOT_NULL_COLUMNS = [f.name for f in METADATA_SCHEMA.fields if not f.nullable]