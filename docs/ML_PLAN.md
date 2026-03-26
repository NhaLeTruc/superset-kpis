# ML Feasibility Plan: Extensions 1–4

## Using Jupyter + Spark MLlib (No New Dependencies)

---

## Overview

This plan covers four machine learning extensions demonstrable as Jupyter notebooks
inside the existing Docker stack. All models use **Spark MLlib**, which ships with
PySpark 3.5.0 and is therefore already present in every container. No new Python
packages are required.

| # | Extension | MLlib API | Source Tables |
| --- | --- | --- | --- |
| 1 | Churn Prediction | `GBTClassifier`, `Pipeline` | `session_metrics`, `user_metadata`, `daily_active_users` |
| 2 | Behavioral Segmentation | `KMeans`, `BisectingKMeans` | `session_metrics`, `power_users`, `user_metadata` |
| 3 | ML Anomaly Detection | `KMeans` + distance scoring | `performance_by_version`, `device_performance` |
| 4 | Retention Forecasting | `GBTRegressor`, `LinearRegression` | `cohort_retention` |

---

## Part 1 — Infrastructure Change Required

One service block must be added to `docker-compose.yml`. The Jupyter container
uses the official `jupyter/pyspark-notebook:spark-3.5.0` image, which includes
PySpark, pandas, numpy, and matplotlib. psycopg2-binary is installed on first
startup by mounting the existing `requirements.txt`.

### Add to `docker-compose.yml` (after the `spark-history` block)

```yaml
  # Jupyter Notebook — ML feasibility environment
  jupyter:
    image: jupyter/pyspark-notebook:spark-3.5.0
    container_name: goodnote-jupyter
    ports:
      - "8888:8888"
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - JUPYTER_TOKEN=${JUPYTER_TOKEN:-goodnote}
      # Postgres — same env vars already used by all other services
      - POSTGRES_HOST=${POSTGRES_HOST:-postgres}
      - POSTGRES_PORT=${POSTGRES_PORT:-5432}
      - POSTGRES_DB=${POSTGRES_DB:-analytics}
      - POSTGRES_USER=${POSTGRES_USER:-analytics_user}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-analytics_pass}
      # Spark cluster connection
      - SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}
    volumes:
      - ./src:/home/jovyan/work/src
      - ./data:/home/jovyan/work/data
      - ./requirements.txt:/tmp/requirements.txt
      - ./notebooks:/home/jovyan/work/notebooks
    command: >
      bash -c "pip install --quiet psycopg2-binary python-dotenv &&
               start-notebook.sh --NotebookApp.token='${JUPYTER_TOKEN:-goodnote}'"
    networks:
      - goodnote-network
    depends_on:
      postgres:
        condition: service_healthy
      spark-master:
        condition: service_healthy
```

### Create the notebooks directory

```bash
mkdir -p notebooks
```

### Start Jupyter

```bash
docker compose up -d jupyter
# Access at http://localhost:8888  (token: goodnote)
```

---

## Part 2 — Shared Notebook Boilerplate

Every notebook begins with the same setup block. Save it once and reuse.

```python
# ── Cell 1: Setup ─────────────────────────────────────────────────────────────
import os
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    dbname=os.getenv("POSTGRES_DB", "analytics"),
    user=os.getenv("POSTGRES_USER", "analytics_user"),
    password=os.getenv("POSTGRES_PASSWORD", "analytics_pass"),
)

def pg_query(sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    with psycopg2.connect(**PG) as conn:
        return pd.read_sql(sql, conn)

# Connect to the existing Spark cluster.
# spark.driver.host must be set to the Jupyter container hostname so that
# spark-worker-1 and spark-worker-2 can open the callback channel to the driver.
# spark.driver.bindAddress=0.0.0.0 lets the driver listen on all interfaces.
spark = (
    SparkSession.builder
    .appName("ML Feasibility")
    .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.driver.host", "goodnote-jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")  # small datasets in notebooks
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "master:", spark.sparkContext.master)
```

**Why `spark.driver.host=goodnote-jupyter`:**
In client mode (used by all notebooks) the Spark driver runs inside the Jupyter
container. Workers open a TCP connection back to the driver to send task results.
Within `goodnote-network`, every container is reachable by its Docker service name,
so setting `spark.driver.host` to the Jupyter service name is all that is needed.
The JDBC jar (`postgresql-42.7.1.jar`) is not required here because data is loaded
from Postgres via `psycopg2`, not via `spark.read.jdbc()`.

---

## Part 3 — Extension 1: Churn Prediction

**Goal:** Predict which active users will stop using the app within 30 days.

### Ext-1 Data Sources

| Table | Columns Used | Role |
| --- | --- | --- |
| `session_metrics` | `user_id`, `session_duration_ms`, `actions_count`, `is_bounce` | Behavioral features |
| `user_metadata` | `user_id`, `device_type`, `country`, `subscription_type`, `registration_date` | Profile features |
| `daily_active_users` | `date`, `dau` | Date range reference |

### Ext-1 Feature Engineering

```sql
-- Per-user session aggregates (run via pg_query)
SELECT
    s.user_id,
    COUNT(*)                                   AS total_sessions,
    AVG(s.session_duration_ms) / 1000.0        AS avg_session_sec,
    AVG(s.actions_count)                       AS avg_actions_per_session,
    SUM(s.is_bounce)::float / COUNT(*)         AS personal_bounce_rate,
    MAX(s.session_start_time)                  AS last_seen,
    m.device_type,
    m.country,
    m.subscription_type,
    m.registration_date
FROM session_metrics s
JOIN user_metadata m USING (user_id)
GROUP BY s.user_id, m.device_type, m.country, m.subscription_type, m.registration_date
```

**Label definition (derived in notebook):**

```python
CHURN_WINDOW_DAYS = 30
reference_date = df["last_seen"].max()

df["churned"] = (
    (reference_date - df["last_seen"]).dt.days > CHURN_WINDOW_DAYS
).astype(int)
```

### Ext-1 MLlib Pipeline

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

# Convert pandas → Spark
sdf = spark.createDataFrame(df.fillna({"avg_session_sec": 0, "personal_bounce_rate": 0}))

# Encode categoricals
device_idx  = StringIndexer(inputCol="device_type",       outputCol="device_idx",  handleInvalid="keep")
country_idx = StringIndexer(inputCol="country",           outputCol="country_idx", handleInvalid="keep")
sub_idx     = StringIndexer(inputCol="subscription_type", outputCol="sub_idx",     handleInvalid="keep")

assembler = VectorAssembler(
    inputCols=[
        "total_sessions", "avg_session_sec", "avg_actions_per_session",
        "personal_bounce_rate", "device_idx", "country_idx", "sub_idx",
    ],
    outputCol="features_raw",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True)

gbt = GBTClassifier(labelCol="churned", featuresCol="features", maxIter=50, maxDepth=5, seed=42)

pipeline = Pipeline(stages=[device_idx, country_idx, sub_idx, assembler, scaler, gbt])

train, test = sdf.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

evaluator = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderROC")
auc = evaluator.evaluate(model.transform(test))
print(f"AUC-ROC: {auc:.4f}")
```

### Ext-1 Feasibility Signal

An AUC-ROC above 0.70 confirms the existing session + metadata features carry
meaningful churn signal. With the `--small` dataset (1K users, 10K interactions)
the full notebook runs in under 2 minutes on the cluster.

---

## Part 4 — Extension 2: Behavioral Segmentation

**Goal:** Discover natural user behavioral clusters beyond the binary "power user / not"
threshold already in `power_users`.

### Ext-2 Data Sources

| Table | Columns Used | Role |
| --- | --- | --- |
| `session_metrics` | `user_id`, `session_duration_ms`, `actions_count`, `is_bounce` | Session behavior |
| `power_users` | `user_id`, `total_duration_hours`, `total_interactions` | Engagement intensity |
| `user_metadata` | `user_id`, `subscription_type`, `device_type` | Profile context |

### Ext-2 Feature Engineering

```sql
-- Per-user behavioral profile (run via pg_query)
SELECT
    s.user_id,
    COUNT(*)                                           AS total_sessions,
    AVG(s.session_duration_ms) / 1000.0                AS avg_session_sec,
    AVG(s.actions_count)                               AS avg_actions,
    SUM(s.is_bounce)::float / COUNT(*)                 AS bounce_rate,
    COALESCE(p.total_duration_hours, 0)                AS lifetime_hours,
    COALESCE(p.total_interactions, 0)                  AS lifetime_interactions,
    m.subscription_type
FROM session_metrics s
LEFT JOIN power_users p   USING (user_id)
JOIN      user_metadata m USING (user_id)
GROUP BY s.user_id, p.total_duration_hours, p.total_interactions, m.subscription_type
```

### Ext-2 MLlib Pipeline

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler

assembler = VectorAssembler(
    inputCols=["total_sessions", "avg_session_sec", "avg_actions", "bounce_rate", "lifetime_hours"],
    outputCol="features_raw",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True)

evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
results = []

for k in range(3, 8):
    kmeans = KMeans(featuresCol="features", k=k, seed=42)
    pipe   = Pipeline(stages=[assembler, scaler, kmeans])
    m      = pipe.fit(sdf)
    score  = evaluator.evaluate(m.transform(sdf))
    results.append((k, score))
    print(f"k={k}  silhouette={score:.4f}")

best_k = max(results, key=lambda x: x[1])[0]
print(f"\nBest k: {best_k}")
```

### Ext-2 Cluster Profiling

```python
from pyspark.sql import functions as F

predictions = best_model.transform(sdf)
(
    predictions
    .groupBy("prediction")
    .agg(
        F.mean("avg_session_sec").alias("avg_session_sec"),
        F.mean("avg_actions").alias("avg_actions"),
        F.mean("bounce_rate").alias("bounce_rate"),
        F.mean("lifetime_hours").alias("lifetime_hours"),
        F.count("*").alias("user_count"),
    )
    .orderBy("prediction")
    .toPandas()
)
```

Expected segments with realistic data: *heavy editors*, *casual browsers*,
*occasional viewers*, *enterprise collaborators*, *dormant users*.

---

## Part 5 — Extension 3: ML-Based Anomaly Detection

**Goal:** Evaluate whether a KMeans distance-based approach detects more or fewer
anomalies than the existing Z-score method, by running both independently and
comparing results side-by-side.

The notebook does **not** modify `src/transforms/performance/anomalies.py`. It
reads the Z-score detections already written to the `performance_anomalies` table
by Job 3, then runs KMeans against the same raw `performance_by_version` data.
Any decision to replace or augment the production code belongs in Part 10.

### Why KMeans for Anomaly Detection

KMeans anomaly detection flags rows whose distance to their nearest cluster centroid
exceeds a configurable percentile threshold. Unlike Z-score:

- It handles multivariate data (p50 + p95 + p99 simultaneously) without assuming
  independence between columns.
- It does not assume a Gaussian distribution.
- The 99th-percentile threshold adapts to the actual data shape rather than a fixed σ.

### Ext-3 Data Sources

| Table | Columns Used |
| --- | --- |
| `performance_by_version` | `app_version`, `metric_date`, `p50_duration_ms`, `p95_duration_ms`, `p99_duration_ms`, `total_interactions` |
| `device_performance` | `device_type`, `metric_date`, `p50_duration_ms`, `p95_duration_ms`, `p99_duration_ms` |
| `performance_anomalies` | All columns — existing Z-score detections for comparison |

### Ext-3 MLlib Pipeline

```python
import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import functions as F

perf_pd = pg_query("""
    SELECT app_version, metric_date,
           p50_duration_ms, p95_duration_ms, p99_duration_ms,
           total_interactions
    FROM performance_by_version
    ORDER BY metric_date
""")
sdf = spark.createDataFrame(perf_pd.fillna(0))

PERF_COLS = ["p50_duration_ms", "p95_duration_ms", "p99_duration_ms", "total_interactions"]
assembler = VectorAssembler(inputCols=PERF_COLS, outputCol="features_raw")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True)
kmeans    = KMeans(featuresCol="features", k=4, seed=42)
pipeline  = Pipeline(stages=[assembler, scaler, kmeans])
model     = pipeline.fit(sdf)

# Distance from each scaled feature vector to its assigned centroid
centers = model.stages[-1].clusterCenters()

@F.udf("double")
def centroid_dist(features, cluster_idx):
    return float(np.linalg.norm(np.array(features.toArray()) - centers[cluster_idx]))

predictions = model.transform(sdf).withColumn(
    "centroid_distance", centroid_dist(F.col("features"), F.col("prediction"))
)

# Flag as anomaly if distance exceeds 99th percentile
p99_dist    = predictions.approxQuantile("centroid_distance", [0.99], 0.01)[0]
ml_anomalies = predictions.filter(F.col("centroid_distance") > p99_dist)
print(f"ML anomalies detected: {ml_anomalies.count()}")
```

### Ext-3 Comparison with Existing Z-Score Results

```python
zscore_pd = pg_query("""
    SELECT metric_date, app_version, severity, z_score
    FROM performance_anomalies
    ORDER BY metric_date
""")

ml_pd = ml_anomalies.select("metric_date", "app_version", "centroid_distance").toPandas()

print("Z-score anomalies:", len(zscore_pd))
print("KMeans anomalies :", len(ml_pd))

overlap = pd.merge(
    zscore_pd[["metric_date", "app_version"]],
    ml_pd[["metric_date", "app_version"]],
    on=["metric_date", "app_version"],
)
print("Detected by both :", len(overlap))
```

---

## Part 6 — Extension 4: Cohort Retention Forecasting

**Goal:** Given the first two weeks of a new cohort's retention data, predict their
week-8 and week-12 retention before those dates arrive.

### Ext-4 Data Sources

| Table | Columns Used |
| --- | --- |
| `cohort_retention` | `cohort_week`, `week_number`, `cohort_size`, `retained_users`, `retention_rate` |

### Ext-4 Feature Engineering

Each training sample is one cohort. Features are early-week retention rates;
the target is a later-week retention rate.

```python
cohort_pd = pg_query("""
    SELECT cohort_week, week_number, cohort_size, retention_rate
    FROM cohort_retention
    ORDER BY cohort_week, week_number
""")

pivot = (
    cohort_pd
    .pivot_table(index="cohort_week", columns="week_number", values="retention_rate")
    .reset_index()
)
pivot.columns = [f"week_{c}" if isinstance(c, int) else c for c in pivot.columns]

sizes = cohort_pd[cohort_pd.week_number == 0][["cohort_week", "cohort_size"]]
pivot = pivot.merge(sizes, on="cohort_week")

TARGET = "week_12"  # or "week_8"
pivot  = pivot.dropna(subset=["week_0", "week_1", "week_2", "week_4", TARGET])
```

### Ext-4 MLlib Pipeline

```python
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

FEATURE_COLS = ["week_0", "week_1", "week_2", "week_4", "cohort_size"]

sdf = spark.createDataFrame(pivot[FEATURE_COLS + [TARGET]].rename(columns={TARGET: "label"}))

assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
evaluator = RegressionEvaluator(labelCol="label", metricName="rmse")
train, test = sdf.randomSplit([0.8, 0.2], seed=42)

for name, regressor in [
    ("LinearRegression", LinearRegression(featuresCol="features", labelCol="label")),
    ("GBTRegressor",     GBTRegressor(featuresCol="features", labelCol="label", maxIter=50, maxDepth=4, seed=42)),
]:
    pipe  = Pipeline(stages=[assembler, regressor])
    model = pipe.fit(train)
    preds = model.transform(test)
    rmse  = evaluator.evaluate(preds)
    r2    = RegressionEvaluator(labelCol="label", metricName="r2").evaluate(preds)
    print(f"{name:22s}  RMSE={rmse:.4f}  R²={r2:.4f}")
```

### Ext-4 Forecasting New Cohorts

```python
new_cohorts_pd = pg_query("""
    SELECT cohort_week, week_number, cohort_size, retention_rate
    FROM cohort_retention
    WHERE week_number <= 4
""")

new_pivot = (
    new_cohorts_pd
    .pivot_table(index="cohort_week", columns="week_number", values="retention_rate")
    .reset_index()
)
new_pivot.columns = [f"week_{c}" if isinstance(c, int) else c for c in new_pivot.columns]
new_pivot = new_pivot.merge(
    new_cohorts_pd[new_cohorts_pd.week_number == 0][["cohort_week", "cohort_size"]],
    on="cohort_week",
)
new_pivot = new_pivot.dropna(subset=FEATURE_COLS)

forecasts = best_model.transform(spark.createDataFrame(new_pivot[FEATURE_COLS]))
forecasts.select("cohort_week", "prediction").toPandas()
```

---

## Part 7 — Generating Sample Data

If the database is empty, run the sample data generator and ETL jobs first:

```bash
# 1. Generate sample CSVs (1K users, 10K interactions)
docker exec goodnote-spark-dev \
  python3 scripts/generate_sample_data.py --small --realistic-all

# 2. Run ETL pipeline to populate all 13 PostgreSQL tables
docker exec goodnote-spark-master bash -c '
  /opt/spark/bin/spark-submit /opt/spark-apps/src/jobs/01_data_processing.py \
    --interactions-path /app/data/raw/user_interactions.csv \
    --metadata-path /app/data/raw/user_metadata.csv \
    --output-path /app/data/processed/enriched_interactions.parquet --dev-mode &&
  /opt/spark/bin/spark-submit /opt/spark-apps/src/jobs/02_user_engagement.py \
    --enriched-path /app/data/processed/enriched_interactions.parquet --write-to-db --dev-mode &&
  /opt/spark/bin/spark-submit /opt/spark-apps/src/jobs/03_performance_metrics.py \
    --enriched-path /app/data/processed/enriched_interactions.parquet --write-to-db --dev-mode &&
  /opt/spark/bin/spark-submit /opt/spark-apps/src/jobs/04_session_analysis.py \
    --enriched-path /app/data/processed/enriched_interactions.parquet --write-to-db --dev-mode
'
```

---

## Part 8 — Notebook File Naming

```text
notebooks/
├── 01_churn_prediction.ipynb
├── 02_behavioral_segmentation.ipynb
├── 03_ml_anomaly_detection.ipynb
└── 04_retention_forecasting.ipynb
```

Each notebook loads data via psycopg2, submits ML work to the cluster via
`spark://spark-master:7077`, and renders inline charts using pandas `.plot()`
(matplotlib is pre-installed in `jupyter/pyspark-notebook`).

---

## Part 9 — Constraints and Limitations

| Constraint | Impact |
| --- | --- |
| No Isolation Forest in Spark MLlib 3.5 | Extension 3 uses KMeans distance instead; produces useful comparison but is not a full IF replacement |
| `cohort_retention` is cohort-level, not user-level | Extension 1 churn label is derived from `session_metrics.session_start_time`, not the cohort table |
| JDBC jar not in `jupyter/pyspark-notebook` image | Mitigated by using psycopg2 for all Postgres reads; the Spark cluster session itself does not need it |
| `--small` dataset (1K users) | Feature distributions may be narrower than production; feasibility results will directionally hold |

---

## Part 10 — Path to Production

Each notebook establishes feasibility. Promoting a model to production follows the
existing job pattern:

1. Create `src/jobs/05_ml_churn.py` inheriting from `BaseAnalyticsJob`
   (`job_type="ml"` is already supported by `configure_job_specific_settings`
   in `src/config/spark_tuning.py`)
2. Add output tables to `database/01_schema.sql`
   (e.g., `user_churn_scores`, `user_segments`, `ml_anomalies`)
3. Add indexes to `database/02_indexes.sql` following existing patterns
4. Add a Superset chart to the relevant dashboard using the new table as source
5. Add a `make run-ml-jobs` target mirroring the existing `run-jobs` target

The `BaseAnalyticsJob.get_table_mapping()` → `write_to_postgres()` pattern used by
Jobs 2–4 works identically for MLlib output DataFrames.
