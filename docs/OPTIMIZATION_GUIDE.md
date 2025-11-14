# Spark Optimization Guide

Comprehensive guide to Apache Spark optimizations implemented in the GoodNote Analytics Platform.

---

## Overview

This platform implements **7 major optimization techniques** to handle TB-scale data efficiently:

1. **Broadcast Joins** - Avoid shuffle for small tables
2. **Data Skew Handling (Salting)** - Eliminate straggler tasks
3. **Adaptive Query Execution (AQE)** - Automatic runtime optimization
4. **Predicate Pushdown** - Filter early, process less
5. **Column Pruning** - Select only needed columns
6. **Optimal Partitioning** - Balance parallelism and overhead
7. **Efficient Caching** - Reuse DataFrames strategically

**Expected Impact:** 30-60% performance improvement over baseline

---

## Spark-Specific Optimizations

### Summary Table

| Technique | Impact | Use Case | Implementation |
|-----------|--------|----------|----------------|
| **Broadcast Join** | 50-70% faster | Small tables (<10GB) | `spark.sql.autoBroadcastJoinThreshold` |
| **Salting** | Eliminates stragglers | Skewed join keys | `identify_hot_keys()` + `apply_salting()` |
| **AQE** | 10-30% improvement | Automatic optimization | `spark.sql.adaptive.enabled=true` |
| **Predicate Pushdown** | 30-50% less data | Early filtering | Filter before joins/aggregations |
| **Column Pruning** | 20-40% less I/O | Select only needed | `.select()` specific columns |
| **Optimal Partitions** | 20-40% faster shuffle | 128MB per partition | `spark.sql.shuffle.partitions` |
| **Efficient Caching** | 2-5x speedup | Reused DataFrames | `MEMORY_AND_DISK_SER` mode |
| **Off-Heap Memory** | 10-20% improvement | Reduce GC pressure | `spark.memory.offHeap.enabled=true` |

---

## 1. Broadcast Join Optimization

### Problem
Joining large tables with small tables causes expensive shuffle operations.

### Solution
Broadcast small tables (<10GB) to all executors, avoiding shuffle entirely.

### Configuration
```python
# In spark_config.py
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB default
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10737418240")  # 10GB with AQE
```

### Implementation
```python
from pyspark.sql.functions import broadcast

# Automatic (recommended with AQE)
enriched_df = interactions_df.join(metadata_df, "user_id")

# Manual broadcast hint
enriched_df = interactions_df.join(broadcast(metadata_df), "user_id")
```

### Expected Impact
- **50-70% faster** joins for small dimension tables
- Eliminates shuffle for small table
- Reduces network I/O

---

## 2. Data Skew Handling (Salting)

### Problem
Power users create data skew - one user with 100x more interactions causes straggler tasks.

### Solution
Use **salting** to distribute skewed keys across multiple partitions.

### Implementation

#### Step 1: Identify Hot Keys
```python
from src.transforms.join_transforms import identify_hot_keys

# Detect users with >99th percentile interactions
hot_keys_df = identify_hot_keys(
    interactions_df,
    key_column="user_id",
    threshold_percentile=0.99
)

# Output: DataFrame with hot keys and their counts
# | user_id | count  |
# |---------|--------|
# | u000042 | 150000 |
# | u001337 | 120000 |
```

#### Step 2: Apply Salting to Large Table
```python
from src.transforms.join_transforms import apply_salting

# Add random salt (0-9) to hot keys, 0 to normal keys
interactions_salted = apply_salting(
    interactions_df,
    hot_keys_df,
    key_column="user_id",
    salt_factor=10
)

# New columns added:
# - salt: random int [0-9] for hot keys, 0 for normal
# - user_id_salted: "user_id_salt" (e.g., "u000042_7")
```

#### Step 3: Explode Small Table
```python
from src.transforms.join_transforms import explode_for_salting

# Replicate metadata 10x for hot keys, 1x for normal
metadata_exploded = explode_for_salting(
    metadata_df,
    hot_keys_df,
    key_column="user_id",
    salt_factor=10
)

# Hot key "u000042" becomes:
# u000042_0, u000042_1, ..., u000042_9
```

#### Step 4: Join on Salted Keys
```python
# Join using salted keys
joined_df = interactions_salted.join(
    metadata_exploded,
    on="user_id_salted",
    how="inner"
)

# Clean up temporary columns
final_df = joined_df.drop("salt", "user_id_salted")
```

### Complete Example
```python
from src.transforms.join_transforms import optimized_join

# All-in-one function handles detection and salting automatically
enriched_df = optimized_join(
    large_df=interactions_df,
    small_df=metadata_df,
    join_key="user_id",
    join_type="inner",
    enable_broadcast=True,      # Try broadcast first
    enable_salting=True,         # Apply salting if skew detected
    skew_threshold=0.99,         # Top 1% users
    salt_factor=10
)
```

### Expected Impact
- **70-80% reduction** in max task time
- Eliminates straggler tasks
- More balanced partition sizes

---

## 3. Adaptive Query Execution (AQE)

### Problem
Static query plans can't adapt to runtime statistics and data distribution.

### Solution
Enable AQE to dynamically optimize query execution based on runtime statistics.

### Configuration
```python
# In spark_config.py
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Skew join detection
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### Benefits
1. **Dynamic partition coalescing** - Reduces small partitions after shuffle
2. **Automatic skew join handling** - Detects and splits skewed partitions
3. **Dynamic broadcast joins** - Converts to broadcast if small enough
4. **Local shuffle reader** - Eliminates unnecessary shuffle

### Expected Impact
- **10-30% improvement** automatically
- Adapts to actual data distribution
- No code changes required

---

## 4. Predicate Pushdown

### Problem
Loading entire datasets before filtering wastes I/O and memory.

### Solution
Apply filters as early as possible in the pipeline.

### Implementation
```python
# ❌ Bad: Load all data, then filter
df = spark.read.parquet("/data/interactions")
filtered = df.filter(F.col("date") >= "2023-01-01")

# ✅ Good: Filter during read (if partitioned by date)
df = spark.read.parquet("/data/interactions") \
    .filter(F.col("date") >= "2023-01-01")

# ✅ Best: Use partition pruning
df = spark.read.parquet("/data/interactions/date=2023-01-*")
```

### Expected Impact
- **30-50% less data** read from storage
- Faster query execution
- Lower memory usage

---

## 5. Column Pruning

### Problem
Reading unnecessary columns wastes I/O (especially with columnar formats).

### Solution
Select only required columns as early as possible.

### Implementation
```python
# ❌ Bad: Read all columns
df = spark.read.parquet("/data/interactions")
result = df.select("user_id", "timestamp").groupBy("user_id").count()

# ✅ Good: Select early
df = spark.read.parquet("/data/interactions") \
    .select("user_id", "timestamp")
result = df.groupBy("user_id").count()
```

### Expected Impact
- **20-40% less I/O** with Parquet/ORC
- Faster query execution
- Lower memory usage

---

## 6. Optimal Partitioning

### Problem
Too few partitions = low parallelism. Too many partitions = high overhead.

### Solution
Target **128MB per partition** (rule of thumb).

### Configuration
```python
# Calculate optimal partitions
data_size_gb = 1000  # 1TB
partition_size_mb = 128
optimal_partitions = (data_size_gb * 1024) / partition_size_mb
# Result: 8000 partitions

spark.conf.set("spark.sql.shuffle.partitions", str(int(optimal_partitions)))
```

### Dynamic Adjustment
```python
# Job-specific configuration
def create_spark_session(job_name, dataset_size_gb=100):
    optimal_partitions = max(200, int((dataset_size_gb * 1024) / 128))

    spark = SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.shuffle.partitions", optimal_partitions) \
        .getOrCreate()

    return spark
```

### Expected Impact
- **20-40% faster** shuffle operations
- Balanced task execution
- Better resource utilization

---

## 7. Efficient Caching

### Problem
Re-computing expensive DataFrames multiple times wastes resources.

### Solution
Cache strategically using appropriate storage levels.

### Storage Levels
```python
from pyspark import StorageLevel

# MEMORY_ONLY - Fast but can cause OOM
df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK - Safe, falls back to disk
df.persist(StorageLevel.MEMORY_AND_DISK)

# MEMORY_AND_DISK_SER - Recommended (serialized, saves memory)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Implementation
```python
# Cache expensive transformations that are reused
enriched_df = optimized_join(interactions_df, metadata_df, "user_id")
enriched_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
enriched_df.count()  # Trigger caching

# Use cached DataFrame multiple times
dau_df = calculate_dau(enriched_df)
power_users_df = identify_power_users(enriched_df)

# Unpersist when done
enriched_df.unpersist()
```

### Expected Impact
- **2-5x speedup** for reused DataFrames
- Avoids re-computing expensive operations
- Reduced disk I/O

---

## Performance Benchmarks

### Expected Job Runtimes (1TB Dataset)

**Status:** To be validated during Phase 11 execution

| Job | Baseline | Optimized | Improvement |
|-----|----------|-----------|-------------|
| **01_data_processing.py** | ~45 min | ~30 min | **33% ⬇️** |
| **02_user_engagement.py** | ~30 min | ~20 min | **33% ⬇️** |
| **03_performance_metrics.py** | ~20 min | ~15 min | **25% ⬇️** |
| **04_session_analysis.py** | ~35 min | ~25 min | **29% ⬇️** |
| **Total Pipeline** | **~130 min** | **~90 min** | **31% ⬇️** |

### Target Spark UI Metrics

**Optimized Configuration:**

| Metric | Baseline | Target | Improvement |
|--------|----------|--------|-------------|
| **Shuffle Volume** | 2.5 TB | 1.2 TB | **52% ⬇️** |
| **Max Task / Median** | 15x | <3x | **80% ⬇️** |
| **GC Time %** | 18% | <8% | **56% ⬇️** |
| **Memory Spill** | 500 GB | 0 GB | **100% ⬇️** |

---

## Expected Bottlenecks & Solutions

### Bottleneck #1: Data Skew on Join

**Observation:** Max task >>15x median (power users cause skew)

**Root Cause:** Skewed `user_id` distribution in joins

**Solution Implemented:**
- `identify_hot_keys()` - Detects top 1% users
- `apply_salting()` - Random salting with factor=10
- `explode_for_salting()` - Metadata replication

**Expected Impact:** Max task time reduced by 70-80%

**Code Location:** `src/transforms/join_transforms.py`

---

### Bottleneck #2: Excessive Shuffle

**Observation:** Shuffle write >2x input data

**Root Cause:**
- Missing predicate pushdown
- Inefficient partition count
- Wide transformations without optimization

**Solution Implemented:**
- Early filtering in all jobs
- Optimal shuffle partitions (2000+)
- Column pruning before shuffle
- AQE dynamic coalescing

**Expected Impact:** Shuffle volume reduced by 40-50%

**Code Location:** All jobs in `src/jobs/`

---

### Bottleneck #3: GC Pressure

**Observation:** GC time >10-15% of execution time

**Root Cause:**
- Insufficient executor memory
- MEMORY_ONLY caching causing OOM
- Too many small objects

**Solution Implemented:**
- Executor memory tuning (16GB per executor)
- MEMORY_AND_DISK_SER for cached DataFrames
- Off-heap memory enabled
- Serialized storage for cached data

**Expected Impact:** GC time reduced to <8%

**Code Location:** `src/config/spark_config.py`

---

## Configuration Reference

### Production Spark Configuration

See `src/config/spark_config.py` for complete configuration.

**Key Settings:**
```python
# Memory
spark.executor.memory = 16g
spark.driver.memory = 8g
spark.memory.fraction = 0.8
spark.memory.storageFraction = 0.3
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 4g

# Parallelism
spark.default.parallelism = 400
spark.sql.shuffle.partitions = 2000

# AQE
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true

# Broadcasting
spark.sql.autoBroadcastJoinThreshold = 10485760  # 10MB
spark.sql.adaptive.autoBroadcastJoinThreshold = 10737418240  # 10GB
```

---

## Validation Guide

### Spark UI Analysis Checklist

**Phase 11 will validate these metrics:**

1. **Jobs Tab**
   - [ ] Total job duration
   - [ ] Number of stages
   - [ ] Task count per stage

2. **Stages Tab**
   - [ ] Task distribution (min/median/max)
   - [ ] Shuffle read/write volumes
   - [ ] Input/output sizes

3. **Storage Tab**
   - [ ] Cached RDD sizes
   - [ ] Memory vs disk storage
   - [ ] Unpersisted DataFrames

4. **Executors Tab**
   - [ ] GC time percentage
   - [ ] Memory usage patterns
   - [ ] Task failures/retries

5. **SQL Tab**
   - [ ] Query execution plans
   - [ ] Adaptive plan changes
   - [ ] Broadcast vs shuffle joins

**Access Spark UI:** http://localhost:4040 (during job execution)

---

## Tools & Scripts

### Automated Analysis
```bash
# Generate sample data with configurable skew
python scripts/generate_sample_data.py --size medium --seed 42

# Run baseline vs optimized comparison
./scripts/run_optimization_analysis.sh --size medium --iterations 2

# Output: Performance comparison report
```

### Screenshot Guide
See [SPARK_UI_SCREENSHOT_GUIDE.md](./SPARK_UI_SCREENSHOT_GUIDE.md) for:
- What screenshots to capture
- Before/after comparison strategy
- Naming conventions
- Analysis checklist

---

## References

- **Spark Configuration:** `src/config/spark_config.py`
- **Join Optimizations:** `src/transforms/join_transforms.py`
- **Job Implementations:** `src/jobs/*.py`
- **Spark Tuning Guide:** https://spark.apache.org/docs/latest/tuning.html
- **AQE Documentation:** https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution

---

**Document Version:** 1.0
**Last Updated:** 2025-11-14
**Status:** Framework complete, validation pending (Phase 11)
