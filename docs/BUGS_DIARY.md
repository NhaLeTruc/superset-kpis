# Bugs Diary

This document tracks bugs discovered and fixed during development and debugging sessions.

---

## 2026-01-31: Spark Configuration Issues

### Bug 1: Config API Mismatch (Fixed)

**Location:** `src/config/spark_tuning.py` - `get_spark_config_summary()`

**Problem:** The function used the wrong API to read SQL configuration values. It was using `sparkContext.getConf().get()` which only works for SparkConf properties set at session creation, not for SQL configs set via `spark.conf.set()`.

**Symptom:** SQL configs like `spark.sql.shuffle.partitions` returned `None` or default values instead of the actual configured values.

**Root Cause:** Two different configuration namespaces in Spark:
- `sparkContext.getConf()` - SparkConf properties (core Spark settings)
- `spark.conf` - Runtime SQL configurations

**Fix:** Changed from:
```python
spark.sparkContext.getConf().get("spark.sql.shuffle.partitions")
```
To:
```python
spark.conf.get("spark.sql.shuffle.partitions")
```

---

### Bug 2: Hardcoded Parallelism (Fixed)

**Location:** `src/config/spark_tuning.py` - `apply_spark_tuning()`

**Problem:** `spark.default.parallelism` was hardcoded to 200, which was too high for local development environments.

**Symptom:** Excessive task overhead and potential performance degradation on machines with fewer cores.

**Root Cause:** The value was set assuming a cluster environment without considering local mode.

**Fix:** Commented out the hardcoded setting to let Spark auto-detect the appropriate parallelism based on the available cores:
```python
# Let Spark auto-detect based on available cores
# spark.conf.set("spark.default.parallelism", "200")
```

---

### Observation: Cluster Timing Issue

**Context:** When running in cluster mode, `spark.sparkContext.defaultParallelism` may return inaccurate values.

**Reason:** If executors haven't fully registered with the driver yet, the parallelism value reflects only the driver's local cores rather than the total cluster capacity.

**Implication:** Code that relies on `defaultParallelism` for partition calculations should either:
1. Wait for executor registration
2. Use explicit configuration values
3. Accept that early reads may be conservative estimates

---
