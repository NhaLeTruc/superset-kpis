# Custom Accumulators and Optimizations

This document evaluates the project's implementation of custom Spark accumulators and optimizations.

---

## A. Custom Accumulators

### Implementation

The project implements **4 custom accumulators** in `src/utils/monitoring/accumulators.py`:

| Accumulator | Purpose | Insights Provided |
|-------------|---------|-------------------|
| `RecordCounterAccumulator` | Counts total records processed | Pipeline throughput tracking |
| `SkippedRecordsAccumulator` | Counts filtered/skipped records | Data filtering rate, quality loss |
| `DataQualityErrorsAccumulator` | Tracks errors by category (dict) | Error type breakdown (null_user_id, invalid_timestamp, etc.) |
| `PartitionSkewDetector` | Tracks min/max/count partition sizes | Skew ratio detection (max/min) |

### Usage

Accumulators are registered via `create_monitoring_context()` in `src/utils/monitoring/context.py`:

```python
context["record_counter"] = spark_context.accumulator(0, RecordCounterAccumulator())
context["skipped_records"] = spark_context.accumulator(0, SkippedRecordsAccumulator())
context["data_quality_errors"] = spark_context.accumulator({}, DataQualityErrorsAccumulator())
context["partition_skew"] = spark_context.accumulator({...}, PartitionSkewDetector())
```

### Real-time Monitoring and Alerting

The accumulators enable real-time monitoring through several mechanisms:

#### 1. Throughput Tracking
`record_counter` updates during job execution, allowing dashboards to display records/second metrics.

#### 2. Data Quality Dashboards
`data_quality_errors` maintains a dictionary of error types and counts:
```python
{
    "null_user_id": 150,
    "invalid_timestamp": 42,
    "negative_duration": 8
}
```
This enables drill-down analysis by error category.

#### 3. Skew Detection with Alerting
`partition_skew` calculates the skew ratio (max/min partition size) and triggers warnings:

```python
# From src/utils/monitoring/reporting.py:91-92
if skew_ratio > 3.0:
    lines.append(f"   WARNING: High partition skew detected! (>{skew_ratio:.1f}x)")
```

#### 4. Skip Rate Monitoring
The ratio `skipped_records / (record_counter + skipped_records)` provides the filtering percentage, useful for alerting on unexpected data loss.

---

## B. Optimizations

### 1. Join Optimization (3-Tier Strategy)

**Location:** `src/transforms/join/execution.py:60-152`

The `optimized_join()` function implements a cascading optimization strategy:

| Strategy | When Used | Why Required |
|----------|-----------|--------------|
| **Broadcast Join** | Small table < threshold | Avoids shuffle - O(n) instead of O(n log n). Each executor gets a copy of the small table. |
| **Salted Join** | Hot keys detected (top 1%) | Distributes skewed keys across multiple partitions to prevent straggler tasks. |
| **Standard Join** | Fallback | Sort-merge join when neither optimization applies. |

#### Hot Key Detection

**Location:** `src/transforms/join/optimization.py:14-46`

```python
# Uses approxQuantile to identify top 1% keys
threshold_value = key_counts.approxQuantile("count", [threshold_percentile], 0.001)[0]
hot_keys = key_counts.filter(F.col("count") > threshold_value)
```

**Why required:** In real-world data, some keys (e.g., popular users) have disproportionately more records. Without detection, these keys cause partition skew.

#### Salting Strategy

**Location:** `src/transforms/join/optimization.py:49-109`

```python
# For hot keys: random salt 0 to (salt_factor-1)
# For non-hot keys: salt = 0
F.when(
    F.col("is_hot_key").isNotNull(),
    F.floor(F.rand() * salt_factor).cast("int"),
).otherwise(F.lit(0))
```

**Why required:** Without salting, one executor handles all records for a hot key, becoming a bottleneck. Salting distributes the load across 10 partitions (default salt_factor), reducing wall-clock time by up to 10x for skewed joins.

### 2. Dynamic Partition Tuning

**Location:** `src/config/spark_tuning.py:14-79`

```python
def calculate_optimal_partitions(spark, data_size_gb, partition_size_mb=128, partitions_per_core=2):
    # Two-pronged calculation:
    parallelism_based = total_cores * partitions_per_core  # CPU utilization
    data_based = (data_size_gb * 1024) / partition_size_mb  # ~128MB per partition
    return max(total_cores, parallelism_based, data_based)
```

**Why required:**
- **Too few partitions:** CPU underutilization, memory pressure (OOM risk)
- **Too many partitions:** Scheduling overhead, small file problem, excessive task serialization
- **128MB target:** Balances HDFS block size alignment, I/O throughput, and memory efficiency

### 3. Job-Type Specific Configuration

**Location:** `src/config/spark_tuning.py:82-147`

| Job Type | Partition Size | Broadcast Threshold | Rationale |
|----------|---------------|---------------------|-----------|
| ETL | 128MB | 50MB | More partitions for large shuffle-heavy writes; conservative broadcast to avoid driver OOM |
| Analytics | 256MB | 200MB | Fewer partitions reduce overhead; aggressive broadcast for fast interactive queries |
| ML | 512MB | Default | Large partitions reduce task overhead; 50% storage fraction for iterative algorithm caching |
| Streaming | N/A | N/A | HDFS-backed state store for fault tolerance |

**Why required:** Different workloads have different bottlenecks:
- ETL is I/O bound (needs parallelism)
- Analytics is latency sensitive (needs fewer tasks)
- ML is memory bound (needs caching)

### 4. Caching/Persistence Strategy

**Example from** `src/jobs/01_data_processing.py:76-99`:

```python
hot_keys_df = identify_hot_keys(...).persist()  # Cache for reuse in join
# ... use hot_keys_df in optimized_join() ...
hot_keys_df.unpersist()  # Free memory immediately after use
```

**Pattern used throughout:**
1. `persist()` DataFrames used multiple times
2. `unpersist()` immediately after last use
3. Explicit cleanup in `finally` blocks

**Why required:** Spark's lazy evaluation means without persistence, expensive operations (aggregations, joins) are recomputed each time the DataFrame is accessed. Strategic caching prevents redundant computation while explicit unpersist prevents memory leaks.

### 5. Adaptive Query Execution (AQE)

**Location:** `src/config/spark_session.py:62-68`

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Why required:** AQE dynamically optimizes query plans at runtime based on actual data statistics, complementing the static optimizations above. It can:
- Coalesce small partitions automatically
- Split skewed partitions
- Switch join strategies based on runtime size estimates

---

## Summary

| Requirement | Status | Evidence |
|-------------|--------|----------|
| At least 3 custom accumulators | **Met** | 4 implemented (RecordCounter, SkippedRecords, DataQualityErrors, PartitionSkewDetector) |
| Accumulators provide job progress insights | **Met** | Record counter tracks throughput; skipped records tracks filtering |
| Accumulators provide data quality insights | **Met** | DataQualityErrorsAccumulator categorizes errors by type |
| Explanation for real-time monitoring/alerting | **Met** | Skew ratio > 3x triggers warnings; skip rate enables alerting |
| Optimizations implemented | **Met** | Broadcast joins, salting, dynamic partitioning, caching, AQE |
| Optimizations explained in detail | **Met** | Each optimization includes rationale for why it's required |
