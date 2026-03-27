# ML Operations Plan — GoodNote Analytics Platform

This document covers the four ML pipelines demonstrated in `notebooks/` and specifies
how each is promoted to daily production operation. Each pipeline becomes a Spark job
(`src/jobs/05_*` through `08_*`) inheriting from `BaseAnalyticsJob`, scheduled alongside
the existing ETL pipeline, and writing results to PostgreSQL for Superset dashboards.

---

## Table of Contents

1. [Pipeline 5 — Churn Prediction](#pipeline-5--churn-prediction)
2. [Pipeline 6 — Behavioral Segmentation](#pipeline-6--behavioral-segmentation)
3. [Pipeline 7 — ML Anomaly Detection](#pipeline-7--ml-anomaly-detection)
4. [Pipeline 8 — Retention Forecasting](#pipeline-8--retention-forecasting)
5. [Shared Infrastructure Changes](#shared-infrastructure-changes)

---

## Pipeline 5 — Churn Prediction

**Source notebook:** `notebooks/01_churn_prediction.ipynb`
**Schedule:** Daily, after Job 4 completes (reads its output)
**Output table:** `user_churn_scores`

---

### System Design

#### Data Flow

```
session_metrics   ──┐
user_metadata     ──┼──► PostgreSQL feature query ──► pandas ──► Spark DataFrame
daily_active_users ─┘
       │
       ├──► StringIndexer (device_type, country, subscription_type)
       ├──► VectorAssembler (9 features)
       └──► GBTClassifier (maxIter=50, maxDepth=5)
                │
                ├──► user_churn_scores  (PostgreSQL)
                └──► model artifact     (data/models/churn_pipeline/)
```

#### Model

`GBTClassifier` from Spark MLlib with a five-stage `Pipeline`:

| Stage | Class | Config |
|-------|-------|--------|
| 1–3 | `StringIndexer` | `handleInvalid="keep"` for device_type, country, subscription_type |
| 4 | `VectorAssembler` | 9 input columns → `features` (6 numeric + 3 encoded categoricals) |
| 5 | `GBTClassifier` | `maxIter=50`, `maxDepth=5`, `labelCol="churned"`, `seed=42` |

`StandardScaler` is intentionally absent: GBT is invariant to monotone feature
rescaling, so scaling adds compute cost without accuracy benefit.

#### Output Schema — `user_churn_scores`

```sql
CREATE TABLE user_churn_scores (
    user_id            TEXT        NOT NULL,
    churn_probability  DOUBLE PRECISION NOT NULL,  -- raw model probability [0, 1]
    churn_predicted    SMALLINT    NOT NULL,        -- 0 or 1 at default 0.5 threshold
    score_date         DATE        NOT NULL,
    model_version      TEXT        NOT NULL,        -- git SHA of the job that scored
    PRIMARY KEY (user_id, score_date)
);
```

#### Model Persistence

The fitted `PipelineModel` is saved to `data/models/churn_pipeline/YYYY-MM-DD/` using
`model.save(path)` (MLlib native format). On retraining, the new model is written to a
date-stamped directory; the symlink `data/models/churn_pipeline/current` is atomically
updated to point at the new version. Inference-only runs load from `current` without
re-fitting.

#### Tech Picks

| Decision | Choice | Reason |
|----------|--------|--------|
| Classifier | `GBTClassifier` | Strong AUC-ROC on tabular data without feature engineering overhead |
| Categorical encoding | `StringIndexer` | Native to MLlib; handles unseen categories via `handleInvalid="keep"` |
| Churn label | Recency proxy (> 30 days inactive) | No ground truth labels; interpretable to stakeholders |
| Model format | MLlib native (`PipelineModel.save`) | No external dependency; re-loadable in any Spark session |
| Retraining trigger | Weekly, Sunday midnight | Balances freshness with compute cost; daily is overkill for a 30-day churn window |

---

### Operations Runbook

#### Running the Job

```bash
# Full run: retrain + score all users
make run-job-5

# Score only (load existing model, skip fit)
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/05_churn_prediction.py \
  --enriched-path data/processed/enriched_interactions.parquet \
  --model-path data/models/churn_pipeline/current \
  --write-to-db \
  --score-only

# Retrain only (fit and save model, skip DB write)
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/05_churn_prediction.py \
  --enriched-path data/processed/enriched_interactions.parquet \
  --model-path data/models/churn_pipeline \
  --save-model
```

#### Schedule

Job 5 runs daily at **02:00 UTC** in score-only mode. It retrains every Sunday at
**00:00 UTC**. The ETL pipeline (Jobs 1–4) must complete before Job 5 starts; use
`depends_on` in the orchestrator or check `etl_job_runs` for a successful run on
the same date.

```sql
-- Health check: confirm ETL completed before launching Job 5
SELECT status FROM etl_job_runs
WHERE job_name = 'Session Analysis'
  AND run_date = CURRENT_DATE
  AND status = 'SUCCESS';
```

**ETL dependency failure:** If the health check returns 0 rows (Job 4 did not
complete successfully), Job 5 must **abort** — it must not score from yesterday's
data silently, because stale churn scores would trigger interventions based on
outdated session activity. Log a `SKIPPED` entry to `etl_job_runs` with
`job_metadata = {"reason": "upstream Job 4 did not complete"}` and page on-call
as a P3 event. Do not retry automatically on the same day.

#### Monitoring

Key signals to alert on:

| Signal | Threshold | Action |
|--------|-----------|--------|
| AUC-ROC on held-out split | < 0.70 | Block model promotion; investigate feature drift |
| Churn rate in `user_churn_scores` | > 60% or < 5% | Label drift — check `session_metrics` recency distribution |
| Job wall time | > 30 min (medium dataset) | Executor OOM or Postgres read bottleneck |
| Null `churn_probability` rows | > 0 | `handleInvalid="keep"` failure; check new categorical values |

Log all metrics to `etl_job_runs` via the existing `BaseAnalyticsJob` monitoring hook.

#### Retraining

**Training data window:** The weekly refit trains on a **rolling 180-day window** of
session data (not all history). This keeps the model sensitive to recent behaviour
changes while providing enough data for stable GBT fits. If fewer than 500 labelled
users exist in the window, fall back to all available history.

1. Confirm `user_churn_scores` row count is stable and AUC has not degraded.
2. Run the Sunday full retrain job.
3. Compare new model AUC against the previous week's (logged in `etl_job_runs`).
4. If AUC regresses more than 0.03, rollback: update `current` symlink to the
   previous date-stamped directory.

#### Debugging

```bash
# Inspect feature distribution before fit
make db-query Q="
  SELECT AVG(total_sessions), AVG(avg_session_sec),
         SUM(churned)::float / COUNT(*) AS churn_rate
  FROM (
    SELECT s.user_id,
           COUNT(*) AS total_sessions,
           AVG(s.session_duration_ms) / 1000.0 AS avg_session_sec,
           MAX(s.session_start_time) AS last_seen,
           (EXTRACT(EPOCH FROM (NOW() - MAX(s.session_start_time))) / 86400 > 30)::int AS churned
    FROM session_metrics s GROUP BY s.user_id
  ) t"

# Check if model artifact exists
docker exec goodnote-spark-master ls -la /app/data/models/churn_pipeline/
```

---

### Performance

#### Throughput

| Dataset | Users | Feature extraction | Fit (GBT) | Inference | DB write | Total |
|---------|-------|--------------------|-----------|-----------|----------|-------|
| Small   | 1K    | 2 s                | 15 s      | 5 s       | 3 s      | ~25 s |
| Medium  | 10K   | 5 s                | 45 s      | 10 s      | 5 s      | ~65 s |
| Large   | 100K  | 20 s               | 4 min     | 45 s      | 20 s     | ~6 min |

Fit time scales with `maxIter × maxDepth × n_partitions`. The 100K dataset triggers
automatic partition scaling via `BaseAnalyticsJob(data_size_gb=2)`.

#### Resource Requirements

| Resource | Score-only | Retrain |
|----------|-----------|---------|
| Driver memory | 2 GB | 4 GB |
| Executor memory | 2 GB × 2 workers | 4 GB × 2 workers |
| Shuffle partitions | 8 (small/medium) / 200 (large) | same |
| Peak heap (GBT fit, 100K rows) | ~1.2 GB driver | ~1.2 GB driver |

GBT collects tree statistics to the driver. For > 500K rows, increase driver memory
to 8 GB or reduce `maxIter` to 20 and `maxDepth` to 3.

#### Service Level Objectives

| Metric | Target | Rationale |
|--------|--------|-----------|
| Daily scoring latency (medium dataset) | < 5 min wall time | Churn scores feed downstream retention campaigns. A 5-min budget fits within a nightly batch window without delaying dependent jobs. |
| Score freshness | Scores in `user_churn_scores` must reflect the previous day's session data | Stale scores lead to wrong intervention timing — targeting a user who churned yesterday with a "come back" offer wastes spend and looks broken. |
| Model AUC-ROC | ≥ 0.70 on weekly held-out validation split | 0.70 is the conventional threshold separating a useful classifier from near-random. Below this, the model produces too many false positives to act on safely. |
| Score coverage | 100% of users with ≥ 1 session in the last 90 days must have a score | Any gap means a user with churn risk goes unscored and unactioned — the 90-day window is the business definition of "active". |

#### Terminology

| Term | Definition |
|------|------------|
| AUC-ROC | Area Under the Receiver Operating Characteristic curve. Measures a classifier's ability to distinguish between classes across all thresholds; 1.0 is perfect, 0.5 is random. |
| Churn | A user who has stopped engaging with the app, proxied here as > 30 days of inactivity. |
| GBT / GBTClassifier | Gradient Boosted Trees classifier. An ensemble method that builds decision trees sequentially, each correcting the errors of the prior one. Strong on tabular data without feature engineering. |
| handleInvalid="keep" | A `StringIndexer` option that assigns unseen categorical values (e.g. a new device type) to a reserved index rather than raising an error during inference. |
| Held-out validation split | A portion of data withheld from training and used exclusively to evaluate model quality, giving an unbiased estimate of real-world performance. |
| MLlib | Spark's built-in machine learning library. Provides distributed implementations of common algorithms (GBT, KMeans, LinearRegression, etc.) that run natively on a Spark cluster. |
| PipelineModel | MLlib's abstraction for chaining preprocessing and model stages into a single serialisable object. Fit once, apply consistently to new data. |
| Score-only mode | Running inference with an already-trained model without refitting. Faster and used for daily scoring between weekly retraining runs. |
| StringIndexer | MLlib transformer that encodes a categorical string column to a numeric index. Required before feeding categoricals into a `VectorAssembler`. |
| VectorAssembler | MLlib transformer that combines multiple numeric columns into a single dense feature vector, which is the required input format for all MLlib estimators. |
| Wall time | Elapsed real-world clock time from job start to finish, as opposed to CPU time. The relevant latency measure for pipeline scheduling. |

---

## Pipeline 6 — Behavioral Segmentation

**Source notebook:** `notebooks/02_behavioral_segmentation.ipynb`
**Schedule:** Weekly, Sunday after Job 5 retraining completes
**Output tables:** `user_segments`, `segment_profiles`

---

### System Design

#### Data Flow

```
session_metrics ──┐
power_users     ──┼──► PostgreSQL feature query ──► pandas ──► Spark DataFrame
user_metadata   ──┘
       │
       ├──► VectorAssembler (6 features)
       ├──► StandardScaler (withMean=True, withStd=True)
       └──► KMeans (k=best_k from silhouette sweep k=3..7)
                │
                ├──► user_segments      (PostgreSQL) — per-user cluster assignment
                ├──► segment_profiles   (PostgreSQL) — per-cluster mean feature values
                └──► model artifact     (data/models/segmentation_pipeline/)
```

#### Model

`KMeans` from Spark MLlib with silhouette-driven `k` selection:

| Stage | Class | Config |
|-------|-------|--------|
| 1 | `VectorAssembler` | 6 numeric cols → `features_raw` |
| 2 | `StandardScaler` | `withMean=True`, `withStd=True` → `features` |
| 3 | `KMeans` | `k` from silhouette sweep (3–7), `seed=42` |

`StandardScaler` is mandatory here: KMeans uses Euclidean distance, and the raw
features have incompatible scales (e.g. `lifetime_hours` ∈ [0, 2000] vs
`bounce_rate` ∈ [0, 1]).

#### Output Schemas

```sql
CREATE TABLE user_segments (
    user_id        TEXT     NOT NULL,
    segment_id     SMALLINT NOT NULL,   -- cluster index (0-based)
    segment_name   TEXT     NOT NULL,   -- human-readable label (see naming below)
    segment_date   DATE     NOT NULL,
    model_version  TEXT     NOT NULL,
    PRIMARY KEY (user_id, segment_date)
);

CREATE TABLE segment_profiles (
    segment_id              SMALLINT         NOT NULL,
    segment_name            TEXT             NOT NULL,
    user_count              INTEGER          NOT NULL,
    avg_session_sec         DOUBLE PRECISION NOT NULL,
    avg_actions             DOUBLE PRECISION NOT NULL,
    avg_bounce_rate         DOUBLE PRECISION NOT NULL,
    avg_lifetime_hours      DOUBLE PRECISION NOT NULL,
    avg_lifetime_interactions DOUBLE PRECISION NOT NULL,
    avg_total_sessions      DOUBLE PRECISION NOT NULL,
    profile_date            DATE             NOT NULL,
    PRIMARY KEY (segment_id, profile_date)
);
```

#### Segment Naming

Cluster indices are mapped to names after each fit by ranking clusters on
`avg_lifetime_hours` (descending):

| Rank | Name |
|------|------|
| 0 (highest hours) | `heavy_editor` |
| 1 | `engaged_browser` |
| 2 | `casual_viewer` |
| 3 | `occasional_user` |
| 4 (lowest hours) | `dormant` |

Ranks beyond 4 fall back to `segment_{idx}`. The mapping is recomputed every
weekly refit so names remain semantically stable across k changes.

#### Tech Picks

| Decision | Choice | Reason |
|----------|--------|--------|
| Algorithm | `KMeans` | Standard baseline; interpretable centroids; supported natively in MLlib |
| k selection | Silhouette score sweep (3–7) | Objective metric; no domain assumption on number of segments |
| Scaling | `StandardScaler(withMean=True)` | Required for Euclidean KMeans; centering prevents origin bias |
| Schedule | Weekly | Segments shift slowly; daily refit would generate churn in segment assignments |
| Naming | Rank-by-lifetime-hours | Stable across k changes; maps to existing `power_users` concept |

---

### Operations Runbook

#### Running the Job

```bash
# Full weekly run: refit + reassign + write profiles
make run-job-6

# Reassign only (load existing model centroids)
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/06_behavioral_segmentation.py \
  --enriched-path data/processed/enriched_interactions.parquet \
  --model-path data/models/segmentation_pipeline/current \
  --write-to-db \
  --assign-only
```

#### Schedule

Weekly refit runs Sunday **01:30 UTC** (after Job 5 retraining at 00:00 UTC). The daily
reassignment run (score-only, using last Sunday's model) is skipped for
segmentation — weekly resolution is sufficient.

> **Schedule note:** Job 7 (ML Anomaly Detection) also runs daily at 01:30 UTC. On
> Sundays both Job 6 and Job 7 start simultaneously. Job 6's k-sweep can take up to
> 10 min on a large dataset. Ensure the orchestrator assigns them to separate executor
> resource pools, or stagger Job 7 to 01:45 UTC on Sundays to avoid memory contention.

#### Monitoring

| Signal | Threshold | Action |
|--------|-----------|--------|
| Best silhouette score | < 0.25 | Segments poorly defined; investigate feature distribution |
| Segment size imbalance | Any cluster > 70% of users | Degenerate solution; force k=3 fallback |
| Segment churn (users moving between weeks) | > 25% of users changing segment | Model instability; check for data anomalies |
| `heavy_editor` cluster shrinking week-over-week | > 10% decline | Possible data quality issue in `power_users` table |

#### Retraining

Segmentation models are refit every Sunday regardless of metric gating.
Store the silhouette score in `etl_job_runs.job_metadata` (JSON) each run:

```json
{
  "best_k": 5,
  "silhouette": 0.38,
  "cluster_sizes": [2100, 1840, 1600, 1200, 960]
}
```

If `silhouette < 0.20` on two consecutive Sundays, open an investigation ticket —
this signals that the feature space no longer contains natural groupings, likely
due to a change in app behaviour.

#### Debugging

```bash
# Check segment distribution
make db-query Q="
  SELECT segment_name, COUNT(*) AS users
  FROM user_segments
  WHERE segment_date = CURRENT_DATE
  GROUP BY segment_name ORDER BY users DESC"

# Check silhouette trend
make db-query Q="
  SELECT run_date, job_metadata->>'silhouette' AS silhouette
  FROM etl_job_runs
  WHERE job_name = 'Behavioral Segmentation'
  ORDER BY run_date DESC LIMIT 10"
```

---

### Performance

#### Throughput

| Dataset | Users | Feature extraction | Preprocessing | k sweep (3–7) | Reassign + write | Total |
|---------|----|---|---|---|---|---|
| Small | 1K | 2 s | 3 s | 30 s | 5 s | ~40 s |
| Medium | 10K | 5 s | 8 s | 90 s | 10 s | ~2 min |
| Large | 100K | 20 s | 30 s | 8 min | 40 s | ~10 min |

The k sweep (5 KMeans fits) dominates cost. If runtime is a constraint, restrict
the sweep to k=3..5 or run a single fit at the previously best k from `etl_job_runs`.

#### Resource Requirements

| Resource | Weekly refit | Daily assign-only |
|----------|------------|----------|
| Driver memory | 2 GB | 2 GB |
| Executor memory | 2 GB × 2 workers | 2 GB × 2 workers |
| Shuffle partitions | 8 (small/medium) / 200 (large) | same |

KMeans centroid convergence is CPU-bound, not memory-bound. The 100K dataset
fits comfortably in 2 GB executor memory; scaling memory does not improve runtime.

#### Service Level Objectives

| Metric | Target | Rationale |
|--------|--------|-----------|
| Weekly refit wall time (medium dataset) | < 10 min | Segmentation refits weekly (slower cadence than churn), so a 10-min budget is generous but still bounded to avoid resource contention with other weekend jobs. |
| Silhouette score | ≥ 0.25 (acceptable) / ≥ 0.35 (good) | Below 0.25 the clusters overlap so heavily that segment labels carry no signal. 0.35 is the "good" bar where segments are meaningfully distinct for product decisions. |
| Segment coverage | 100% of users with activity in last 90 days | An uncovered user is invisible to any segment-based feature or campaign — the 90-day window is the business definition of "active". |
| Segment name stability | ≤ 10% of users changing segment week-over-week under normal conditions | If segments relabel massively each week, dashboards become uninterpretable and A/B tests are invalidated. Stability ≤ 10% means the model is converging, not drifting. |

#### Terminology

| Term | Definition |
|------|------------|
| Centroid | The geometric center of a cluster, computed as the mean of all member points in feature space. Used both to assign new points to clusters and to describe each segment's "typical" user. |
| Euclidean distance | Straight-line distance between two points in multi-dimensional feature space. KMeans minimises the sum of squared Euclidean distances from each point to its assigned centroid. |
| k | The number of clusters in a KMeans model. Chosen here by sweeping k=3..7 and picking the value with the highest silhouette score. |
| KMeans | An unsupervised clustering algorithm that partitions data into k groups by iteratively assigning points to their nearest centroid and recomputing centroids until convergence. |
| Segment churn | The fraction of users assigned to a different segment compared to the previous week. High churn signals model instability, not user behaviour change. |
| Silhouette score | A cluster quality metric ranging from -1 to 1. Measures how similar a point is to its own cluster versus neighbouring clusters. Higher is better; ≥ 0.25 indicates meaningful separation. |
| StandardScaler | MLlib transformer that normalises each feature to zero mean and unit variance. Mandatory for KMeans because Euclidean distance is sensitive to feature scale. |
| withMean / withStd | `StandardScaler` parameters. `withMean=True` subtracts the column mean (centering); `withStd=True` divides by standard deviation (scaling). Both are required to prevent large-range features dominating distance calculations. |

---

## Pipeline 7 — ML Anomaly Detection

**Source notebook:** `notebooks/03_ml_anomaly_detection.ipynb`
**Schedule:** Daily, after Job 3 (performance metrics) completes
**Output tables:** `ml_performance_anomalies`, `device_ml_anomalies`

---

### System Design

#### Data Flow

```
performance_by_version ──┐
device_performance      ──┴──► PostgreSQL queries ──► pandas ──► two Spark DataFrames
        │
        ├── App-version pipeline ───────────────────────────────────┐
        │    VectorAssembler (4 cols) → StandardScaler → KMeans(k=4)│
        │    centroid_distance UDF → approxQuantile p99 → anomalies ┤──► ml_performance_anomalies
        │                                                            │
        └── Device pipeline ─────────────────────────────────────── ┤
             VectorAssembler (3 cols) → StandardScaler → KMeans(k=3)│
             centroid_distance UDF → approxQuantile p99 → anomalies ──► device_ml_anomalies
```

Note: The existing Z-score pipeline in `src/transforms/performance/anomalies.py`
continues to run unchanged. Pipeline 7 produces supplementary detections written to
separate tables. The overlap analysis from the notebook informs whether to eventually
consolidate the two approaches.

#### Model

Two independent `KMeans` pipelines share the same structural pattern but use different
feature sets and k values:

| Pipeline | Features | k | Rationale |
|----------|----------|---|-----------|
| App-version | p50, p95, p99, total_interactions | 4 | Captures: low-latency, mid-latency, high-latency, high-volume |
| Device | p50, p95, p99 | 3 | Fewer device types justify a smaller k |

Anomaly threshold: 99th percentile of centroid distances, computed with
`approxQuantile(..., 0.99, 0.01)` (max 1% relative error).

#### Output Schemas

```sql
CREATE TABLE ml_performance_anomalies (
    app_version        TEXT             NOT NULL,
    metric_date        DATE             NOT NULL,
    centroid_distance  DOUBLE PRECISION NOT NULL,
    cluster_id         SMALLINT         NOT NULL,
    p50_duration_ms    DOUBLE PRECISION,
    p95_duration_ms    DOUBLE PRECISION,
    p99_duration_ms    DOUBLE PRECISION,
    p99_distance_threshold DOUBLE PRECISION NOT NULL,
    detected_date      DATE             NOT NULL,
    model_version      TEXT             NOT NULL,
    PRIMARY KEY (app_version, metric_date, detected_date)
);

CREATE TABLE device_ml_anomalies (
    device_type        TEXT             NOT NULL,
    metric_date        DATE             NOT NULL,
    centroid_distance  DOUBLE PRECISION NOT NULL,
    cluster_id         SMALLINT         NOT NULL,
    p50_duration_ms    DOUBLE PRECISION,
    p95_duration_ms    DOUBLE PRECISION,
    p99_duration_ms    DOUBLE PRECISION,
    p99_distance_threshold DOUBLE PRECISION NOT NULL,
    detected_date      DATE             NOT NULL,
    model_version      TEXT             NOT NULL,
    PRIMARY KEY (device_type, metric_date, detected_date)
);
```

#### Tech Picks

| Decision | Choice | Reason |
|----------|--------|--------|
| Algorithm | KMeans + centroid distance | No labelled anomalies available; handles multivariate correlations; does not assume Gaussian distribution |
| Threshold | p99 of centroid distances | Adaptive to actual data shape; always flags exactly ~1% of rows |
| Two separate models | Separate fit per table | App-version and device feature spaces are incompatible; sharing centroids would be meaningless |
| Degenerate guard | Warn if any cluster > 90% of rows | Signals k is too large or data has no natural multivariate structure |
| Relationship to Z-score | Additive, not replacement | Both methods run independently; overlap analysis guides future consolidation |

#### Coexistence with Z-Score

Pipeline 7 does not touch `performance_anomalies`. Superset can surface both:

- `performance_anomalies` — Z-score detections from Job 3 (univariate, per-metric)
- `ml_performance_anomalies` — KMeans detections from Job 7 (multivariate)

A reconciliation view can join both:

```sql
CREATE VIEW anomaly_consensus AS
SELECT app_version, metric_date, 'z_score' AS method, severity AS signal FROM performance_anomalies
UNION ALL
SELECT app_version, metric_date, 'kmeans', ROUND(centroid_distance::numeric, 2)::text FROM ml_performance_anomalies;
```

---

### Operations Runbook

#### Running the Job

```bash
# Daily run: refit KMeans on latest data and write new anomalies
make run-job-7

# Dry-run: print anomaly counts without writing to DB
# Note: Pipeline 7 reads from PostgreSQL (performance_by_version, device_performance),
# not from Parquet. No --enriched-path flag is needed.
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/07_ml_anomaly_detection.py \
  --dry-run
```

#### Schedule

Daily at **01:30 UTC**, after Job 3 completes. Unlike churn and segmentation, the
KMeans model is refit daily — performance anomaly patterns can shift with each app
release, so yesterday's centroids may not represent today's baseline.

#### Monitoring

| Signal | Threshold | Action |
|--------|-----------|--------|
| ML anomaly count, app-version | > 10% of app-version rows | Threshold inflation; investigate data distribution shift |
| ML anomaly count | 0 rows | Query returned no data or `approxQuantile` failed; check `performance_by_version` row count |
| Degenerate cluster warning in logs | Any occurrence | Set k=3 fallback for app-version or k=2 for device |
| Overlap with Z-score anomalies | Declining week-over-week | Feature drift in one or both methods; run comparison notebook manually |

#### Debugging

```bash
# Confirm both anomaly tables were populated
make db-query Q="
  SELECT 'ml_app_version' AS source, COUNT(*), MAX(detected_date)
  FROM ml_performance_anomalies
  UNION ALL
  SELECT 'ml_device', COUNT(*), MAX(detected_date)
  FROM device_ml_anomalies"

# Compare with Z-score anomalies for the same date
make db-query Q="
  SELECT a.app_version, a.metric_date, a.severity AS zscore_severity,
         m.centroid_distance
  FROM performance_anomalies a
  LEFT JOIN ml_performance_anomalies m
    ON a.app_version = m.app_version AND a.metric_date = m.metric_date
  ORDER BY a.metric_date DESC LIMIT 20"
```

#### Model Refresh Policy

Because each daily run refits from scratch, there is no `current` symlink or
version tracking for Pipeline 7. The centroid geometry is ephemeral — only
the anomaly flags written to PostgreSQL persist. `model_version` in the output
table stores the git SHA for auditability.

---

### Performance

#### Throughput

Pipeline 7 processes `performance_by_version` rows (one row per app-version per day),
not individual users. The input is small regardless of dataset size.

| Dataset | performance_by_version rows | KMeans fit (k=4) | Distance scoring | DB write | Total |
|---------|---------------------------|-----------------|-----------------|---------|-------|
| Small | ~50 | 3 s | 2 s | 2 s | ~10 s |
| Medium | ~500 | 5 s | 3 s | 3 s | ~15 s |
| Large | ~5,000 | 10 s | 5 s | 5 s | ~25 s |

This is the fastest of the four pipelines because the input cardinality is
determined by the number of app versions × calendar days, not by user count.

#### Resource Requirements

| Resource | Value |
|----------|-------|
| Driver memory | 1 GB |
| Executor memory | 1 GB × 2 workers |
| Shuffle partitions | 8 (sufficient for all dataset sizes) |
| Python UDF overhead | ~10% of scoring time (centroid_distance uses NumPy; JVM→Python serialisation) |

The Python UDF (`centroid_dist`) is the primary performance sensitivity. Replace it
with a Spark native column expression if Python UDF overhead becomes material at
larger scales:

```python
# Native replacement (no Python UDF serialisation overhead):
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F

center_vectors = [Vectors.dense(c) for c in centers]
# Use broadcast + map for centroid lookup instead of UDF
```

#### Service Level Objectives

| Metric | Target | Rationale |
|--------|--------|-----------|
| Daily job wall time | < 5 min for all dataset sizes | This runs after Job 3 in the daily pipeline; a tight budget keeps the total ETL chain within the overnight window. |
| Anomaly detection latency | Results in `ml_performance_anomalies` within 30 min of Job 3 completing | Performance anomalies (crashes, slowdowns) need to surface before the morning engineering review, not the next day. |
| False positive rate (vs Z-score) | < 30% KMeans-only detections that are not corroborated by Z-score (review quarterly) | KMeans distance is less interpretable than Z-score. A high rate of KMeans-only flags suggests the model is detecting noise. Quarterly review allows calibration without over-tuning. |
| Coverage | Both app-version and device anomaly tables written on every run | A silent missing table is worse than a failed job — downstream dashboards would show stale data with no error signal. |

#### Terminology

| Term | Definition |
|------|------------|
| approxQuantile | Spark's approximate percentile function. Uses a bounded-error streaming algorithm (Greenwald-Khanna) to compute percentiles without sorting the full dataset. The second argument (0.01) is the maximum relative error. |
| Centroid distance | The Euclidean distance from a data point to its assigned cluster centroid. Used as an anomaly score: points far from any centroid are unusual relative to the normal performance clusters. |
| Degenerate cluster | A cluster that absorbs the vast majority of data points (> 90% here), leaving other clusters nearly empty. Indicates k is too large for the data's natural structure. |
| False positive rate | The proportion of flagged anomalies that are not genuine anomalies. Here measured as KMeans-only detections not corroborated by the Z-score method, reviewed quarterly. |
| Multivariate | Involving multiple variables simultaneously. KMeans anomaly detection is multivariate — it considers p50, p95, p99, and interaction count together, catching anomalies invisible in any single metric. |
| p50 / p95 / p99 | Percentile latency metrics. p50 is the median; p95 means 95% of requests completed faster than this value; p99 is the 99th percentile (worst 1% of requests). |
| UDF (User-Defined Function) | A custom Python function registered in Spark to operate on DataFrame columns. Incurs JVM-to-Python serialisation overhead compared to native Spark column expressions. |
| Univariate | Involving a single variable in isolation. The existing Z-score pipeline is univariate — it flags each metric (p50, p95, p99) independently, without considering their joint behaviour. |
| Z-score | Number of standard deviations a value lies from the mean of its distribution. The existing anomaly pipeline flags readings with \|Z\| ≥ 3.0 as anomalous. |

---

## Pipeline 8 — Retention Forecasting

**Source notebook:** `notebooks/04_retention_forecasting.ipynb`
**Schedule:** Weekly, Monday after new cohort data from Job 2 is available
**Output tables:** `cohort_retention_forecasts`

---

### System Design

#### Data Flow

```
cohort_retention ──► PostgreSQL query ──► pandas ──► pivot (long → wide)
       │
       ├── Training cohorts (first 80% by date)
       │    VectorAssembler(week_0..4, cohort_size)
       │    ├──► LinearRegression
       │    └──► GBTRegressor (maxIter=50, maxDepth=4)
       │         └── best model by RMSE on temporal test split
       │
       └── Inference: new cohorts with data through week 4 only
                └──► GBTRegressor (or LinearRegression if GBT overfits)
                          └──► cohort_retention_forecasts (PostgreSQL)
```

#### Model

Two regressors are evaluated every weekly refit; the lower-RMSE model is promoted:

| Model | Config | When preferred |
|-------|--------|---------------|
| `LinearRegression` | default regularisation | Fewer than 20 training cohorts; interpretability required |
| `GBTRegressor` | `maxIter=50`, `maxDepth=4`, `seed=42` | More than 20 cohorts; non-linear decay patterns |

Features are the first four observed retention checkpoints plus cohort size:
`week_0`, `week_1`, `week_2`, `week_4`, `cohort_size`.

Predictions are clipped to `[0.0, 1.0]` — regression can extrapolate outside
valid retention bounds. Clipped predictions are logged as a warning metric.

#### Output Schema — `cohort_retention_forecasts`

```sql
CREATE TABLE cohort_retention_forecasts (
    cohort_week        TEXT             NOT NULL,
    target_week        SMALLINT         NOT NULL,  -- 8 or 12
    predicted_retention DOUBLE PRECISION NOT NULL, -- clipped to [0, 1]
    model_type         TEXT             NOT NULL,  -- 'GBTRegressor' or 'LinearRegression'
    model_rmse         DOUBLE PRECISION NOT NULL,  -- test-set RMSE at time of this forecast
    forecast_date      DATE             NOT NULL,
    model_version      TEXT             NOT NULL,
    PRIMARY KEY (cohort_week, target_week, forecast_date)
);
```

#### Temporal Split Design

The train/test split is strictly temporal (not random) to prevent data leakage:

```
Cohorts sorted chronologically:
[Jan W1][Jan W2]...[Aug W4][Sep W1]...[Sep W4]
|──────── 80% training ────────|── 20% test ──|
```

The GBTRegressor is trained on historical cohorts and evaluated on the most recent
cohorts. A random split would allow future cohorts' retention curves to influence the
training of predictions about past cohorts — an invalid setup for time-ordered data.

#### Fallback Logic

| Condition | Fallback |
|-----------|----------|
| `week_12` not yet observed in any cohort | Target = `week_8` |
| `week_8` also unavailable | Raise `ValueError`; log to `etl_job_runs`, skip forecast |
| < 4 training cohorts | Linear regression only (GBT cannot generalise on tiny samples) |
| Best RMSE ≥ baseline (predict mean) | Log warning; write forecasts anyway; escalate for investigation |

#### Tech Picks

| Decision | Choice | Reason |
|----------|--------|--------|
| Regressors | `LinearRegression` + `GBTRegressor` | Baseline + non-linear model; winner determined objectively each week |
| Split | Temporal (oldest 80% train) | Prevents data leakage; mirrors actual production use |
| Output clipping | `clip(0.0, 1.0)` | Retention is a fraction; regression has no built-in domain constraints |
| Feature set | Weeks 0, 1, 2, 4 + cohort_size | Early weeks provide the retention decay rate; week 3 omitted (minimal information gain) |
| Target | `week_12` (fallback to `week_8`) | Business value: 12-week forecast gives the product team a 2-month lead time |

---

### Operations Runbook

#### Running the Job

```bash
# Full weekly run: refit model + write forecasts
make run-job-8

# Forecast only (use saved model, do not refit)
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/08_retention_forecasting.py \
  --model-path data/models/retention_pipeline/current \
  --write-to-db \
  --forecast-only

# Evaluate only: print RMSE and R², skip DB write
docker exec goodnote-spark-master bash scripts/run_spark_job.sh \
  src/jobs/08_retention_forecasting.py \
  --evaluate-only
```

#### Schedule

Weekly refit runs Monday **03:00 UTC** after Job 2 (user engagement) completes and
new cohort data is available in `cohort_retention`. Forecasts are for cohorts that
have only reached week 4 of observation.

#### Monitoring

| Signal | Threshold | Action |
|--------|-----------|--------|
| Weekly RMSE | > 0.05 (5 percentage points) | Model error is too large for actionable forecasting |
| Forecast clipping rate | > 5% of forecasts | Model is extrapolating; check cohort_size outliers or extreme early retention |
| Training cohort count | < 4 | Insufficient training data; skip GBTRegressor, use LinearRegression only |
| Baseline comparison | Best RMSE ≥ baseline RMSE | No better than predicting the mean; do not surface forecasts in Superset |

Log RMSE and model type to `etl_job_runs.job_metadata`:

```json
{
  "model_type": "GBTRegressor",
  "test_rmse": 0.028,
  "test_r2": 0.81,
  "baseline_rmse": 0.047,
  "train_cohorts": 32,
  "test_cohorts": 8,
  "target_week": 12
}
```

#### Retraining

1. Every Monday, both models are refit from scratch on all available complete cohorts.
2. The winner (lower RMSE) is saved to `data/models/retention_pipeline/YYYY-MM-DD/`.
3. `current` symlink is updated atomically.
4. If the new RMSE exceeds last week's by more than 20%, trigger a Slack alert
   and retain both models for comparison.

#### Debugging

```bash
# Check cohort data availability
make db-query Q="
  SELECT MAX(week_number) AS max_week, COUNT(DISTINCT cohort_week) AS cohorts
  FROM cohort_retention"

# Inspect forecast accuracy for cohorts where week_12 is now known
make db-query Q="
  SELECT f.cohort_week,
         f.predicted_retention,
         r.retention_rate AS actual_retention,
         ABS(f.predicted_retention - r.retention_rate) AS abs_error
  FROM cohort_retention_forecasts f
  JOIN cohort_retention r
    ON f.cohort_week = r.cohort_week::text
   AND r.week_number = f.target_week
  WHERE f.target_week = 12
  ORDER BY abs_error DESC"
```

---

### Performance

#### Throughput

Input to Pipeline 8 is cohort-level (one row per cohort per week), making it the
smallest input of all four pipelines.

| Dataset | Cohorts | Pivot + feature prep | Model fit (both) | Inference | DB write | Total |
|---------|---------|---------------------|-----------------|-----------|---------|-------|
| Small (1K users, 40 wks) | ~40 | 1 s | 10 s | 2 s | 2 s | ~15 s |
| Medium (10K users, 52 wks) | ~52 | 2 s | 15 s | 3 s | 3 s | ~25 s |
| Large (100K users, 52 wks) | ~52 | 2 s | 15 s | 3 s | 3 s | ~25 s |

Cohort count is bounded by the number of calendar weeks in the dataset, not by user
count. Pipeline 8 does not scale with the large dataset — it is always fast.

#### Resource Requirements

| Resource | Value |
|----------|-------|
| Driver memory | 1 GB |
| Executor memory | 1 GB × 2 workers |
| Shuffle partitions | 8 (all dataset sizes) |
| Peak memory (pandas pivot) | < 50 MB for ≤ 200 cohorts |

GBTRegressor trains on a matrix that is at most `n_cohorts × 5` (52 rows × 5
columns for a one-year dataset). This fits in a single partition and uses minimal
executor resources.

#### Service Level Objectives

| Metric | Target | Rationale |
|--------|--------|-----------|
| Weekly job wall time | < 5 min for all dataset sizes | The training matrix is tiny (≤ 52 rows × 5 columns), so a 5-min budget is ample and any overrun signals a pipeline problem, not a scaling problem. |
| Model RMSE | < 0.05 (5 pp error on retention rate) | Retention rates typically range 0.1–0.9, so a 5 percentage-point error is acceptable for planning but not so loose it renders forecasts useless for headcount or capacity decisions. |
| Forecast lead time | Forecasts for new cohorts must be available by Monday 06:00 UTC (3 hours after job start) | Product and growth teams review weekly metrics Monday morning. Forecasts must be ready before that review, so the 3-hour window (job starts ~03:00 UTC) is the constraint. |
| Forecast coverage | All cohorts with data through week 4 must have a week-12 (or week-8) forecast | Week 4 is the minimum data needed for the model to extrapolate to week 12. Cohorts without it are genuinely unforecastable; those with it must not be silently skipped. |
| Backtesting accuracy | When week_12 is eventually observed, MAE of prior forecasts ≤ 0.04 | RMSE on held-out data measures fit; backtesting MAE on eventually-observed actuals measures real-world forecast quality. The tighter 0.04 target (vs 0.05 RMSE) reflects that systematic bias is worse than in-sample noise. |

#### Terminology

| Term | Definition |
|------|------------|
| Backtesting | Evaluating past forecasts against ground truth that has since been observed. Here, comparing week-12 predictions made weeks earlier against the actual week-12 retention once it is recorded. |
| Baseline RMSE | The error achieved by the simplest possible model: always predicting the training mean. A model whose RMSE does not beat baseline is no better than guessing the average. |
| Clipping | Capping model output to a valid range — here `[0.0, 1.0]` — because retention is a fraction and regression has no built-in domain constraints. |
| Cohort | A group of users who made their first interaction in the same calendar week. Cohort analysis tracks what fraction remain active at weeks 1, 2, 4, 8, and 12 after acquisition. |
| Data leakage | When information from outside the training period influences model training, producing overly optimistic evaluation metrics. Prevented here by using a strict temporal split. |
| GBTRegressor | Gradient Boosted Trees regressor. The same ensemble method as GBTClassifier but predicts a continuous value (retention rate) instead of a class label. |
| LinearRegression | A baseline linear model that predicts the target as a weighted sum of input features. Preferred when training cohorts are few (< 20) or when interpretability is required. |
| MAE (Mean Absolute Error) | Average absolute difference between predicted and actual values. Less sensitive to large errors than RMSE; used here for backtesting because retention errors are generally small. |
| R² (R-squared) | Coefficient of determination. The fraction of variance in the target explained by the model; 1.0 is a perfect fit, 0.0 means the model explains nothing beyond the mean. |
| Retention rate | The fraction of a cohort still active at a given week after their first interaction. A week-12 retention of 0.35 means 35% of the cohort is still using the app 12 weeks later. |
| RMSE (Root Mean Squared Error) | Square root of the average squared difference between predicted and actual values. Penalises large errors more than MAE; the primary model selection metric here. |
| Temporal split | A train/test split that respects time order — older cohorts train, newer cohorts test. Required for time-series data to prevent data leakage from future cohorts. |

---

## Shared Infrastructure Changes

### New Jobs

| File | Job | `job_type` | Schedule |
|------|-----|-----------|----------|
| `src/jobs/05_churn_prediction.py` | Churn Prediction | `"ml"` | Daily 02:00 UTC |
| `src/jobs/06_behavioral_segmentation.py` | Behavioral Segmentation | `"ml"` | Weekly Sun 01:30 UTC |
| `src/jobs/07_ml_anomaly_detection.py` | ML Anomaly Detection | `"ml"` | Daily 01:30 UTC |
| `src/jobs/08_retention_forecasting.py` | Retention Forecasting | `"ml"` | Weekly Mon 03:00 UTC |

All four inherit from `BaseAnalyticsJob` and use `job_type="ml"` so
`configure_job_specific_settings` applies the ML-tuned Spark configuration.

### Makefile Targets

```makefile
run-job-5:   ## Churn prediction: retrain + score
    docker exec $(SPARK_MASTER) bash scripts/run_spark_job.sh src/jobs/05_churn_prediction.py ...

run-job-6:   ## Behavioral segmentation: refit + reassign
    docker exec $(SPARK_MASTER) bash scripts/run_spark_job.sh src/jobs/06_behavioral_segmentation.py ...

run-job-7:   ## ML anomaly detection: daily refit + flag
    docker exec $(SPARK_MASTER) bash scripts/run_spark_job.sh src/jobs/07_ml_anomaly_detection.py ...

run-job-8:   ## Retention forecasting: weekly refit + forecast
    docker exec $(SPARK_MASTER) bash scripts/run_spark_job.sh src/jobs/08_retention_forecasting.py ...

run-ml-jobs: run-job-5 run-job-6 run-job-7 run-job-8
    ## All 4 ML jobs sequentially
```

### Database Schema Additions

Add to `database/01_schema.sql`:

```sql
-- Pipeline 5 — production scoring output
CREATE TABLE IF NOT EXISTS user_churn_scores ( ... );

-- Pipeline 5 — ground-truth actuals for calibration and evaluation queries
-- Populated by a scheduled query that labels users as churned once 30+ days
-- of inactivity have elapsed after their score_date.
CREATE TABLE IF NOT EXISTS user_churn_actuals (
    user_id        TEXT    NOT NULL,
    score_date     DATE    NOT NULL,
    actually_churned SMALLINT NOT NULL,  -- 1 if churned, 0 if not
    labelled_date  DATE    NOT NULL,     -- date the label was finalised (score_date + 31 days)
    PRIMARY KEY (user_id, score_date)
);

-- Pipeline 5 — shadow scoring table for champion-challenger comparison
-- PRIMARY KEY differs from user_churn_scores to allow both models to score
-- the same user on the same date.
CREATE TABLE IF NOT EXISTS user_churn_scores_shadow (
    user_id            TEXT             NOT NULL,
    churn_probability  DOUBLE PRECISION NOT NULL,
    churn_predicted    SMALLINT         NOT NULL,
    score_date         DATE             NOT NULL,
    model_version      TEXT             NOT NULL,
    challenger_version TEXT             NOT NULL,
    PRIMARY KEY (user_id, score_date, challenger_version)
);

-- Pipeline 6
CREATE TABLE IF NOT EXISTS user_segments ( ... );
CREATE TABLE IF NOT EXISTS segment_profiles ( ... );

-- Pipeline 7
CREATE TABLE IF NOT EXISTS ml_performance_anomalies ( ... );
CREATE TABLE IF NOT EXISTS device_ml_anomalies ( ... );

-- Pipeline 8
CREATE TABLE IF NOT EXISTS cohort_retention_forecasts ( ... );
```

Add indexes to `database/02_indexes.sql`:

```sql
CREATE INDEX IF NOT EXISTS idx_churn_scores_date       ON user_churn_scores (score_date);
CREATE INDEX IF NOT EXISTS idx_churn_scores_prob        ON user_churn_scores (churn_probability DESC);
CREATE INDEX IF NOT EXISTS idx_user_segments_date       ON user_segments (segment_date);
CREATE INDEX IF NOT EXISTS idx_user_segments_name       ON user_segments (segment_name, segment_date);
CREATE INDEX IF NOT EXISTS idx_ml_anomalies_date        ON ml_performance_anomalies (detected_date);
CREATE INDEX IF NOT EXISTS idx_device_anomalies_date    ON device_ml_anomalies (detected_date);
CREATE INDEX IF NOT EXISTS idx_retention_forecasts_date ON cohort_retention_forecasts (forecast_date);
```

### Model Storage Layout

```text
data/models/
├── churn_pipeline/
│   ├── 2026-03-24/        ← MLlib PipelineModel (Parquet-based)
│   ├── 2026-03-31/
│   └── current -> 2026-03-31/   ← atomic symlink
├── segmentation_pipeline/
│   ├── 2026-03-24/
│   └── current -> 2026-03-24/
└── retention_pipeline/
    ├── 2026-03-24/
    └── current -> 2026-03-24/
```

Pipeline 7 (anomaly detection) does not persist models — it refits daily from scratch.

#### Model Artifact Retention Policy

Each MLlib `PipelineModel` directory is Parquet-based and can reach 50–200 MB per
version. Without cleanup, `data/models/` grows indefinitely.

**Retention rule:** Keep the last **8 weekly versions** for churn and segmentation,
and the last **8 weekly versions** for retention forecasting. On each successful
retrain, delete the oldest directory beyond the retention window:

```bash
# Run after atomically updating the current symlink
cd /app/data/models/churn_pipeline
ls -d 20*/ | sort | head -n -8 | xargs -r rm -rf
```

Never delete the directory pointed to by `current`, even if it falls outside the
count (the symlink check must precede any deletion).

### Execution Order

```
[Job 1] Data Processing
    └──► [Job 2] User Engagement  ──► [Job 8] Retention Forecasting (Monday)
    │                             └──► [Job 6] Behavioral Segmentation (Sunday) ◄─┐
    └──► [Job 3] Performance      ──► [Job 7] ML Anomaly Detection  (daily)       │
    └──► [Job 4] Session Analysis ──► [Job 5] Churn Prediction       (daily)      │
                                  └──────────────────────────────────────────────┘
```
Job 6 depends on both Job 2 (for `power_users`) and Job 4 (for `session_metrics`).

Jobs 5–8 read only from PostgreSQL tables written by Jobs 2–4. They do not need
the raw Parquet output of Job 1 directly (feature engineering is done via SQL joins).
