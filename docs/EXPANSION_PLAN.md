# Expansion Plan: Unused Functions

This document lists utility functions that exist in the codebase but are not currently used by the main processing jobs. These functions are available for future expansion and integration.

## Data Quality (`src/utils/data_quality.py`)

| Function | Description |
|----------|-------------|
| `validate_schema()` | DataFrame schema validation against expected structure |
| `detect_outliers()` | Outlier detection using IQR or threshold methods |

## PostgreSQL Reader (`src/utils/postgres_reader.py`)

| Function | Description |
|----------|-------------|
| `read_from_postgres()` | Read a PostgreSQL table into a Spark DataFrame |
| `execute_sql()` | Execute arbitrary SQL queries against PostgreSQL |
| `get_table_row_count()` | Get the row count from a PostgreSQL table |

## PostgreSQL Connection (`src/utils/postgres_connection.py`)

| Function | Description |
|----------|-------------|
| `create_connection_string()` | Build a JDBC connection string from config |
| `validate_database_config()` | Validate database configuration parameters |

## Monitoring (`src/utils/monitoring.py`)

| Function | Description |
|----------|-------------|
| `track_data_quality_errors()` | Track and log data quality errors |
| `track_partition_size()` | Track partition sizes to detect skew |
| `get_monitoring_profile()` | Get pre-configured monitoring profiles |
| `with_monitoring()` | Decorator to add monitoring to functions |

## Summary

**Total unused functions: 11**

- Data Quality: 2 functions
- PostgreSQL Reader: 3 functions
- PostgreSQL Connection: 2 functions
- Monitoring: 4 functions

These functions provide ready-to-use capabilities for:
- Data validation and quality checks
- Direct PostgreSQL database access
- Enhanced monitoring and observability

---

## Analysis: validate_schema() vs validate_dataframe()

**Question:** Does `validate_schema()` make `validate_dataframe()` redundant?

**Answer:** No, they are NOT redundant. They serve different purposes.

### Comparison

| Feature | `validate_schema()` | `validate_dataframe()` |
|---------|---------------------|------------------------|
| Location | `src/utils/data_quality.py:19` | `src/jobs/base_job.py:126` |
| Purpose | Pure schema comparison | Broad data validation |
| Extra columns check | Yes (strict mode) | No |
| NULL detection | No | Yes |
| Return type | `(bool, list[str])` | `list[str]` |
| Data scan required | No (metadata only) | Yes (for NULLs) |

### Recommendation: Keep both separate

1. **Different responsibilities** - Schema validation is O(1) metadata check; NULL detection requires data scan
2. **Different return semantics** - `validate_schema()` returns errors; `validate_dataframe()` raises on critical issues
3. **Different layers** - Utility function vs. BaseAnalyticsJob method

### Future Refactor Option

If reducing duplication is desired, have `validate_dataframe()` call `validate_schema()` internally:

```python
if schema:
    is_valid, schema_errors = validate_schema(df, schema, strict=False)
    warnings.extend(schema_errors)
```
