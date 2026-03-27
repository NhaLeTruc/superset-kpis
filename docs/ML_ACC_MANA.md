# ML Model Accuracy Measurement and Management in Production

This document covers production-grade accuracy management for the four ML pipelines
defined in `docs/ML_PLAN.md`:

| Pipeline | Notebook | Model |
|----------|----------|-------|
| 5 — Churn Prediction | `01_churn_prediction.ipynb` | `GBTClassifier` |
| 6 — Behavioral Segmentation | `02_behavioral_segmentation.ipynb` | `KMeans` |
| 7 — ML Anomaly Detection | `03_ml_anomaly_detection.ipynb` | `KMeans` + centroid distance |
| 8 — Retention Forecasting | `04_retention_forecasting.ipynb` | `GBTRegressor` / `LinearRegression` |

Each section identifies the problem, explains why it matters for these specific models,
and provides concrete implementation guidance.

---

## 1. Drift Detection

### What It Is

Drift is when the statistical properties of data change after a model is trained,
causing its predictions to silently degrade without any code change. Three types apply
to these pipelines:

| Type | What changes | Detection method |
|------|-------------|-----------------|
| Feature drift | Input distributions shift | Statistical tests on feature stats per run |
| Label drift | Target distribution shifts | Monitor output score/rate distributions |
| Concept drift | Feature → target relationship changes | Backtesting MAE trends over time |

### Per-Pipeline Guidance

**Pipeline 5 — Churn Prediction**

Feature drift is most likely after app releases that change session behaviour
(`avg_session_sec`, `total_sessions`). Label drift appears as a churn rate outside
expected bounds in `user_churn_scores`.

Compute and log feature statistics on each scoring run:

```python
from pyspark.sql import functions as F

feature_cols = ["total_sessions", "avg_session_sec", "avg_actions_per_session",
                "days_since_last_session", "lifetime_hours", "bounce_rate"]

stats = features_df.select(
    *[F.mean(c).alias(f"{c}_mean") for c in feature_cols],
    *[F.stddev(c).alias(f"{c}_stddev") for c in feature_cols],
)
# Append to etl_job_runs.job_metadata as {"feature_stats": {...}}
```

Alert when any feature mean deviates more than 2 standard deviations from its
30-day rolling average. Concept drift is detected by comparing the AUC trend in
`etl_job_runs` week-over-week — a sustained decline without a data quality explanation
indicates the model is no longer learning the same relationship.

> **Limitation:** The rolling-σ rule detects point-in-time spikes in the feature
> mean; it will miss gradual monotonic drift (e.g. `avg_session_sec` rising 1% per
> week for months) and will fire on normal seasonal variation. A more rigorous
> approach is to compute the **Population Stability Index (PSI)** on the full feature
> distribution each week. PSI > 0.2 conventionally signals a significant distributional
> shift requiring investigation. This can be added as a follow-on improvement once the
> rolling-σ baseline is in place.

**Pipeline 6 — Behavioral Segmentation**

Segment drift manifests as silhouette score decline or the `heavy_editor` cluster
shrinking consistently. Because KMeans has no fixed target label, concept drift
takes the form of the feature space losing its natural cluster structure.

Track the silhouette score trend in `etl_job_runs.job_metadata`:

```sql
SELECT run_date,
       (job_metadata->>'silhouette')::float AS silhouette,
       (job_metadata->>'best_k')::int       AS best_k
FROM etl_job_runs
WHERE job_name = 'Behavioral Segmentation'
ORDER BY run_date DESC
LIMIT 12;
```

If silhouette declines for three consecutive weeks, inspect whether any feature's
distribution has changed materially (e.g. `bounce_rate` shifting after a UX change).

**Pipeline 7 — ML Anomaly Detection**

Because Pipeline 7 refits daily, feature drift is self-correcting — the model adapts
its centroids to the new baseline each day. The risk is the opposite: the model may
absorb a genuine degradation into its "normal" cluster, masking a real incident.
Monitor the 30-day rolling mean of `centroid_distance` for the normal cluster. If
it trends upward, the definition of "normal" performance is drifting higher.

**Pipeline 8 — Retention Forecasting**

The most vulnerable to concept drift. If the app introduces a feature that
fundamentally changes how early retention predicts long-term retention (e.g.
a new onboarding flow), `week_1` and `week_2` will no longer carry the same
predictive signal for `week_12`. Detect this by tracking the backtesting MAE
trend in `etl_job_runs.job_metadata` for cohorts where `week_12` has been
observed:

```sql
SELECT run_date,
       (job_metadata->>'test_rmse')::float    AS test_rmse,
       (job_metadata->>'baseline_rmse')::float AS baseline_rmse
FROM etl_job_runs
WHERE job_name = 'Retention Forecasting'
ORDER BY run_date DESC
LIMIT 20;
```

A rising gap between `test_rmse` and `baseline_rmse` means the model is losing
its advantage over simply predicting the mean.

---

## 2. Model Calibration

### What It Is

A model is **calibrated** when its output probabilities match empirical frequencies.
A calibrated churn model that outputs `churn_probability = 0.8` should be right
~80% of the time. An uncalibrated model may rank users correctly (high AUC-ROC)
while its absolute probabilities are systematically wrong — making threshold-based
decisions unreliable.

### Applies To

Pipeline 5 (GBTClassifier) only. KMeans produces distances, not probabilities.
GBTRegressor predictions are clipped retention rates, not confidence scores.

### Why GBT Needs Calibration

Gradient boosted trees are known to produce poorly calibrated probabilities —
they tend to push scores toward 0 and 1 due to the ensemble averaging effect.
If churn interventions are triggered at `churn_probability > 0.6`, the model's
threshold must reflect real-world churn rates, not just relative ranking.

### Implementation

**Step 1 — Measure calibration at each weekly refit** using a reliability diagram.
Bin predictions into deciles and compare mean predicted probability against
observed churn rate in each bin:

```sql
-- After a week where actuals are known (users who churned vs. didn't)
SELECT width_bucket(churn_probability, 0, 1, 10)  AS decile,
       COUNT(*)                                    AS users,
       AVG(churn_probability)                      AS avg_predicted,
       AVG(actually_churned::int)                  AS avg_actual
FROM user_churn_scores s
JOIN user_churn_actuals a USING (user_id, score_date)
GROUP BY decile
ORDER BY decile;
```

**Step 2 — Apply Platt scaling if calibration error exceeds 0.05** (mean absolute
difference between `avg_predicted` and `avg_actual` across deciles).

Platt scaling fits a logistic regression on raw GBT scores using a held-out
calibration set. In MLlib, use the `.rawPrediction` column (the log-odds vector
before sigmoid squashing), not the `.probability` column, as input to the
calibration `LogisticRegression`. Fitting on already-squashed probabilities
produces a poorly-conditioned calibration because the GBT ensemble pushes values
toward 0 and 1, leaving little gradient signal for a second logistic layer.

```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

# raw_gbt_df has columns: rawPrediction (DenseVector[neg_score, pos_score]),
# churned (label), from running the GBT model on a held-out calibration set.
assembler = VectorAssembler(inputCols=["raw_score"], outputCol="cal_features")
cal_df = raw_gbt_df.withColumn("raw_score", F.vector_to_array(F.col("rawPrediction"))[1])
cal_df = assembler.transform(cal_df)
platt = LogisticRegression(featuresCol="cal_features", labelCol="churned", maxIter=100)
calibration_model = platt.fit(cal_df)
```

**Step 3 — Log the calibration error** to `etl_job_runs.job_metadata`:

```json
{
  "calibration_mae": 0.042,
  "calibration_status": "acceptable",
  "platt_scaling_applied": false
}
```

Alert if `calibration_mae > 0.10` — at that point the probability values are
misleading enough to require recalibration before the model is used for targeting.

---

## 3. Champion-Challenger / Shadow Mode

### What It Is

Before promoting a newly retrained model to production, run it in **shadow mode**:
score users with both the old model (champion) and the new model (challenger) but
only act on the champion. Compare challenger predictions against champion and
against eventual outcomes before deciding to promote.

### Why the Current Approach Falls Short

The existing retraining flow in Pipelines 5 and 8 promotes the new model
immediately after a single RMSE or AUC comparison on a held-out split. This
provides no safety net if the new model behaves unexpectedly on real production
traffic (e.g. scoring a completely different distribution of users as high-risk).

### Implementation

**Promotion gate — Pipeline 5 (Churn)**

Before updating the `current` symlink, check three conditions:

```python
PROMOTION_RULES = {
    "auc_not_regressed": lambda new, old: new["auc"] >= old["auc"] - 0.03,
    "score_dist_stable": lambda new, old: abs(new["churn_rate"] - old["churn_rate"]) < 0.10,
    "coverage_met":      lambda new, _:   new["scored_users"] / new["active_users"] >= 1.0,
}
```

If any rule fails, keep the champion, log the challenger metrics, and alert.

**Shadow scoring — Pipeline 5 (Churn)**

Add an optional `--shadow` flag to `05_churn_prediction.py`. In shadow mode the
job writes to `user_churn_scores_shadow` instead of `user_churn_scores`:

```sql
-- Do NOT use LIKE ... INCLUDING ALL here: that would copy the parent's
-- PRIMARY KEY (user_id, score_date), preventing both champion and challenger
-- from storing a score for the same user on the same date.
CREATE TABLE user_churn_scores_shadow (
    user_id            TEXT             NOT NULL,
    churn_probability  DOUBLE PRECISION NOT NULL,
    churn_predicted    SMALLINT         NOT NULL,
    score_date         DATE             NOT NULL,
    model_version      TEXT             NOT NULL,
    challenger_version TEXT             NOT NULL,
    PRIMARY KEY (user_id, score_date, challenger_version)
);
```

Run shadow mode for one week before each major retraining event (e.g. after
schema changes or feature additions). Compare distributions:

```sql
SELECT AVG(c.churn_probability)  AS champion_avg,
       AVG(s.churn_probability)  AS challenger_avg,
       CORR(c.churn_probability, s.churn_probability) AS rank_correlation
FROM user_churn_scores c
JOIN user_churn_scores_shadow s USING (user_id, score_date);
```

A `rank_correlation < 0.90` is a red flag — the two models are ranking users
substantially differently and manual investigation is required before promotion.

**Promotion gate — Pipeline 8 (Retention Forecasting)**

Compare new model RMSE against last week's RMSE from `etl_job_runs`. If the new
RMSE exceeds last week's by more than 20%, retain both models and surface both
sets of forecasts in `cohort_retention_forecasts` with distinct `model_type` values
for side-by-side review in Superset.

---

## 4. Feedback Loop Contamination

### What It Is

When model predictions drive interventions (e.g. retention emails, in-app nudges),
those interventions change user behaviour. If the same users are then used to
generate training labels for the next model, the labels reflect intervened behaviour,
not natural churn. Over time the model trains on increasingly distorted data and
its true accuracy becomes unobservable.

### Applies To

**Pipeline 5 (Churn Prediction) — primary risk.** If churn scores feed a CRM or
push-notification system, users who receive a retention intervention and subsequently
stay active will be labelled as "not churned" — even though they would have churned
without the intervention.

**Pipeline 8 (Retention Forecasting) — secondary risk.** If forecasts trigger
cohort-level interventions (e.g. a special onboarding flow offered to cohorts
identified as at-risk), those cohorts' observed `week_12` retention will reflect
the intervened behaviour. They then enter the training set for future forecasts,
causing the model to learn a retention relationship that only holds when
interventions are applied. The effect is subtler than in Pipeline 5 (cohort-level
rather than user-level) but accumulates over time. Track the fraction of training
cohorts that received an intervention; if it exceeds 30%, the training signal is
materially contaminated.

### Concrete Effects

| Scenario | Effect on training data | Effect on model |
|----------|------------------------|-----------------|
| Retention campaign targets top 20% churn-risk users | Their `churned` label is suppressed | Model underestimates true churn risk for similar future users |
| Campaign is highly effective | Churn rate in training data drops | Model threshold drift — calibrated probability no longer reflects true risk |
| Campaign targets randomly | No bias | Baseline for counterfactual comparison |

### Mitigations

**1 — Holdout group:** Randomly withhold 5% of high-risk users from all
interventions each cycle. These users provide uncontaminated labels for model
evaluation. Never use the holdout group for training — only for evaluation.

**2 — Label flagging:** Add a `received_intervention` column to the label
construction query so contaminated training examples can be filtered or
down-weighted:

```sql
-- Label construction query for Pipeline 5 training
SELECT s.user_id,
       (EXTRACT(EPOCH FROM (NOW() - MAX(s.session_start_time))) / 86400 > 30)::int AS churned,
       EXISTS (
           SELECT 1 FROM crm_events c
           WHERE c.user_id = s.user_id
             AND c.event_type = 'retention_campaign'
             AND c.event_date >= NOW() - INTERVAL '30 days'
       ) AS received_intervention
FROM session_metrics s
GROUP BY s.user_id;
```

Exclude `received_intervention = true` rows from training until a counterfactual
estimation strategy is in place.

**3 — Log intervention exposure:** Record which users received interventions and
when in an `interventions` table. This enables uplift modelling in future — estimating
the causal effect of the model's recommendations independent of selection bias.

---

## 5. Feature Importance Tracking

### What It Is

GBT models expose a `featureImportances` vector after fitting — a per-feature measure
of how much each feature contributed to split decisions across all trees. Tracking this
over time reveals when the model's reliance on specific features shifts, which is often
a leading indicator of concept drift or data quality problems before RMSE degrades.

### Applies To

Pipeline 5 (GBTClassifier) and Pipeline 8 (GBTRegressor).

### Implementation

After each refit, extract and log feature importances to `etl_job_runs.job_metadata`:

```python
feature_names = ["total_sessions", "avg_session_sec", "avg_actions_per_session",
                 "days_since_last_session", "lifetime_hours", "bounce_rate",
                 "device_type_idx", "country_idx", "subscription_type_idx"]

importances = dict(zip(feature_names, model.stages[-1].featureImportances.toArray()))
# importances = {"total_sessions": 0.31, "days_since_last_session": 0.28, ...}
```

Store as JSON in `etl_job_runs.job_metadata` under `"feature_importances"`.

**Drift alert rule:** If any feature's importance changes by more than 0.10 (absolute)
compared to the previous week, log a warning. This threshold is intentionally loose —
small weekly variation is normal; a jump from 0.05 to 0.25 for a previously minor
feature is not.

```sql
-- Compare current week vs previous week importances (Pipeline 5)
WITH ranked AS (
    SELECT run_date,
           job_metadata->'feature_importances' AS fi,
           LAG(job_metadata->'feature_importances') OVER (ORDER BY run_date) AS fi_prev
    FROM etl_job_runs
    WHERE job_name = 'Churn Prediction'
)
SELECT run_date, fi, fi_prev
FROM ranked
WHERE fi IS NOT NULL AND fi_prev IS NOT NULL
ORDER BY run_date DESC
LIMIT 1;
```

Parse and compare in Python post-query. A visualisation of importance trends
over 12 weeks in Superset provides an at-a-glance signal for model health reviews.

---

## 6. Explainability at Inference Time

### What It Is

Storing only `churn_probability` tells an operator *that* a user is at risk, not
*why*. Explainability at inference time means persisting a simplified explanation
alongside each prediction so that product managers, support teams, and engineers
can understand and act on individual scores without re-running the model.

### Applies To

Pipeline 5 (Churn Prediction) is the highest priority — its output directly drives
user-facing interventions. Pipeline 8 forecasts are cohort-level and less
actionable at the individual level.

### Approach: Top Contributing Features

Full SHAP values are not natively available in Spark MLlib without additional
libraries. A practical approximation is to identify the top two features that
pushed a user's score above the population mean, using the product of feature
value deviation and feature importance:

**`population_means` source:** Compute and freeze the feature means at training
time from the training split, then persist them to `etl_job_runs.job_metadata`
under `"population_means"` at each weekly refit. Load the saved dict at inference
time. Do **not** recompute means from the scoring population each run — that would
shift the reference point and make week-over-week contribution scores incomparable.

**Categorical exclusion:** Apply this approximation to the six **numeric** features
only (`total_sessions`, `avg_session_sec`, `avg_actions_per_session`,
`days_since_last_session`, `lifetime_hours`, `bounce_rate`). The three encoded
categoricals (`device_type_idx`, `country_idx`, `subscription_type_idx`) use
arbitrary integer indices assigned by `StringIndexer` — arithmetic distance from
their mean is not meaningful and should not be surfaced as a churn reason.

```python
from pyspark.sql import functions as F

NUMERIC_FEATURES = [
    "total_sessions", "avg_session_sec", "avg_actions_per_session",
    "days_since_last_session", "lifetime_hours", "bounce_rate",
]

# feature_importances: dict from fitted model (all 9 features)
# population_means: dict persisted at training time (numeric features only)

for feat in NUMERIC_FEATURES:
    importance = feature_importances.get(feat, 0.0)
    scored_df = scored_df.withColumn(
        f"contrib_{feat}",
        F.abs((F.col(feat) - population_means[feat]) * importance)
    )

# Identify top 2 contributing features per user
# Store as "churn_reason_1", "churn_reason_2" in user_churn_scores
```

Extend `user_churn_scores` with two columns:

```sql
ALTER TABLE user_churn_scores
    ADD COLUMN churn_reason_1 TEXT,   -- e.g. 'days_since_last_session'
    ADD COLUMN churn_reason_2 TEXT;   -- e.g. 'bounce_rate'
```

This does not require a schema migration for existing rows — `NULL` is a valid
historical state. New rows from the next scoring run will populate both columns.

### Limitations to Document

- This is an approximation, not true SHAP. It is directionally correct but should
  not be presented as a precise attribution.
- Only the six numeric features produce meaningful contribution scores. The three
  encoded categorical features are deliberately excluded (see above).
- For users near the decision boundary (`churn_probability` 0.45–0.55), feature
  contributions are noisy. Consider suppressing reasons for near-boundary scores.

---

## 7. Ground Truth Collection and Label Quality

### What It Is

Every supervised ML model depends on the quality of its labels. Pipeline 5 uses a
**proxy label** (> 30 days inactive = churned) rather than a verified ground truth
(e.g. account cancellation). The proxy's validity degrades as the app's usage
patterns evolve.

### Applies To

Pipeline 5 (Churn Prediction) primarily. Pipeline 8's retention rate is a direct
observation from `cohort_retention`, not a proxy, making it more reliable.

### Proxy Label Risks

| Risk | Scenario | Effect |
|------|----------|--------|
| Threshold staleness | Users now have longer natural gaps due to new seasonal content | True churners labelled as active; false negatives increase |
| Hibernate-state users | App introduces a "pause subscription" feature | Paused users labelled churned; model conflates pause with churn |
| Multi-device users | A user inactive on mobile but active on desktop | Incorrectly labelled churned if `session_metrics` is device-scoped |
| Reactivation | A user labelled churned reactivates within 31 days | Historical label is wrong but never corrected |

### Mitigations

**1 — Validate proxy against hard signals** when available. If `user_metadata`
contains a `subscription_status` or `cancellation_date` column, compute the
agreement rate between the proxy label and the hard signal:

```sql
SELECT COUNT(*) FILTER (WHERE proxy_churned = true  AND actually_cancelled = true)  AS true_positive,
       COUNT(*) FILTER (WHERE proxy_churned = true  AND actually_cancelled = false) AS false_positive,
       COUNT(*) FILTER (WHERE proxy_churned = false AND actually_cancelled = true)  AS false_negative,
       COUNT(*) FILTER (WHERE proxy_churned = false AND actually_cancelled = false) AS true_negative
FROM (
    SELECT u.user_id,
           (EXTRACT(EPOCH FROM (NOW() - MAX(s.session_start_time))) / 86400 > 30)::int::bool AS proxy_churned,
           (u.subscription_status = 'cancelled') AS actually_cancelled
    FROM session_metrics s
    JOIN user_metadata u USING (user_id)
    GROUP BY u.user_id, u.subscription_status
) t;
```

**2 — Version the churn threshold** in `src/config/constants.py`. If the threshold
is changed from 30 days to 45 days, old model versions trained at 30 days are no
longer comparable. Record the threshold value in `etl_job_runs.job_metadata` so
historical AUC comparisons remain meaningful:

```json
{
  "churn_threshold_days": 30,
  "auc": 0.74,
  "model_version": "abc1234"
}
```

**3 — Add a label review step** to the quarterly model review checklist. Verify
that the distribution of `churned=1` rows in the training query is consistent
with business expectations and that no structural changes to `session_metrics`
have silently altered the label logic.

---

## 8. Evaluation Beyond Held-Out RMSE / AUC

### What It Is

A single aggregate metric (AUC, RMSE, silhouette) summarises model quality across
all users or all cohorts. It can hide systematic failure modes that matter
operationally: a model may have acceptable aggregate AUC while performing poorly
on the users most likely to be targeted, or have acceptable RMSE while producing
large errors for small cohorts that need the forecast most.

### Pipeline 5 — Churn Prediction: Additional Metrics

| Metric | How to compute | Why it matters |
|--------|---------------|----------------|
| Precision at k | Among top-k highest-scored users, what fraction actually churned? | Interventions typically target the top N% — precision at that cutoff is the operational accuracy |
| Recall at k | Among all churned users, what fraction are in the top-k? | Measures how many at-risk users the targeting campaign reaches |
| F1 at operating threshold | Harmonic mean of precision and recall at the deployed threshold (default 0.5) | Single number for threshold-specific performance |
| Lift at top 20% | Churn rate in top-quintile score vs. population average | Quantifies how much better the model is than random targeting |

```sql
-- Precision at top 20% (requires actuals from a holdout group)
WITH ranked AS (
    SELECT user_id,
           churn_probability,
           actually_churned,
           NTILE(5) OVER (ORDER BY churn_probability DESC) AS quintile
    FROM user_churn_scores s
    JOIN user_churn_actuals a USING (user_id, score_date)
    WHERE score_date = CURRENT_DATE - 7
)
SELECT quintile,
       COUNT(*)                          AS users,
       AVG(actually_churned::int)        AS churn_rate,
       AVG(AVG(actually_churned::int)) OVER () AS population_churn_rate
FROM ranked
GROUP BY quintile
ORDER BY quintile;
```

### Pipeline 6 — Behavioral Segmentation: Additional Metrics

| Metric | How to compute | Why it matters |
|--------|---------------|----------------|
| Inter-cluster distance | Pairwise Euclidean distance between centroids | Segments too close together are not meaningfully distinct |
| Intra-cluster variance | Feature standard deviation within each cluster | High within-cluster variance means the segment is internally heterogeneous |
| Segment size balance | Gini coefficient of cluster sizes | Extreme imbalance (one cluster > 60% of users) often signals a degenerate solution |
| Centroid drift | Euclidean distance between this week's centroid and last week's | Measures semantic stability of each segment over time |

### Pipeline 7 — ML Anomaly Detection: Additional Metrics

| Metric | How to compute | Why it matters |
|--------|---------------|----------------|
| Precision vs Z-score (corroborated detections) | Fraction of KMeans flags also flagged by Z-score | Primary false-positive proxy given no labelled anomalies |
| Detection rate per app version | Anomalies / total rows per version | Ensures no version is systematically missed or over-flagged |
| Distance distribution stability | Compare p50/p95/p99 of `centroid_distance` week-over-week | A widening distribution means the "normal" cluster is becoming less cohesive |

### Pipeline 8 — Retention Forecasting: Additional Metrics

| Metric | How to compute | Why it matters |
|--------|---------------|----------------|
| RMSE by cohort size | Stratify errors into small/medium/large cohorts | Small cohorts often have noisier retention curves and higher errors |
| Directional accuracy | Fraction of forecasts where predicted > actual iff true > mean | Checks if the model correctly predicts above/below-average retention |
| Bias (mean signed error) | Mean of (predicted − actual) | A consistently positive or negative bias is more actionable than raw RMSE |
| Improvement over baseline | `(baseline_rmse − model_rmse) / baseline_rmse` | Relative gain; a model that is 5% better than predicting the mean is barely useful |

---

## 9. Incident Response Playbook

### What It Is

When an SLO is breached, there must be a defined escalation path: who is paged,
what the response SLA is, what immediate actions are taken, and how affected
downstream systems are protected while the fix is deployed. Without this,
a model failure surfaces as corrupted Superset dashboards or incorrect CRM
targeting with no clear owner.

### Severity Levels

| Severity | Condition | Response SLA |
|----------|-----------|-------------|
| P1 — Critical | Job fails to write any output; table missing | 30 min |
| P2 — High | SLO metric breached (AUC < 0.70, RMSE > 0.05, coverage < 100%) | 2 hours |
| P3 — Medium | Metric degrading but not yet breached; drift alert triggered | Next business day |
| P4 — Low | Calibration drift; feature importance shift; segment instability | Weekly review |

### Per-Pipeline Runbooks

**Pipeline 5 — Churn Prediction**

| Trigger | Immediate action | Resolution path |
|---------|-----------------|-----------------|
| `user_churn_scores` has 0 rows for today | Pause CRM campaign triggers; alert on-call | Check `etl_job_runs` for failure; re-run job manually |
| AUC < 0.70 on weekly validation | Block model promotion; keep champion | Investigate feature drift; inspect `churn_rate` in recent scoring runs |
| Churn rate > 60% or < 5% | Pause automated interventions | Check `session_metrics` recency distribution; verify label construction query |
| Scoring latency > 30 min | Alert on-call | Check executor OOM in Spark UI; check Postgres read latency |

**Pipeline 6 — Behavioral Segmentation**

| Trigger | Immediate action | Resolution path |
|---------|-----------------|-----------------|
| `user_segments` not updated for 48h | Alert on-call | Check `etl_job_runs`; re-run `make run-job-6` |
| Silhouette < 0.20 for 2 consecutive Sundays | Open investigation ticket | Inspect feature distribution; check for schema changes in `session_metrics` |
| Any cluster > 70% of users | Force k=3 fallback; write to `user_segments` with warning flag | Investigate feature scaling; check `StandardScaler` fit output |

**Pipeline 7 — ML Anomaly Detection**

| Trigger | Immediate action | Resolution path |
|---------|-----------------|-----------------|
| Either anomaly table missing | Alert on-call; surface last known-good data in Superset | Check `etl_job_runs`; re-run `make run-job-7` |
| 0 anomalies detected | Verify `performance_by_version` has rows for today | Check Job 3 completion status; check `approxQuantile` input |
| Anomaly count > 10% of rows | Suppress Superset alerts; flag as potentially unreliable | Check for data distribution shift; lower p99 threshold temporarily to p95 |

**Pipeline 8 — Retention Forecasting**

| Trigger | Immediate action | Resolution path |
|---------|-----------------|-----------------|
| `cohort_retention_forecasts` not updated by Monday 06:00 UTC | Alert product team; mark Monday report as pending | Check `etl_job_runs`; re-run `make run-job-8` |
| New model RMSE exceeds previous by > 20% | Rollback `current` symlink to previous model version | Run comparison notebook; check if new cohort data is structurally different |
| Forecast clipping rate > 5% | Log warning; write forecasts with `clipped=true` flag | Inspect cohort size outliers; check early retention values for anomalies |

### Rollback Procedure (Pipelines 5 and 8)

```bash
# Identify previous model version
ls -lt /app/data/models/churn_pipeline/

# Roll back to previous date-stamped version
docker exec goodnote-spark-master \
    ln -sfn /app/data/models/churn_pipeline/YYYY-MM-DD \
             /app/data/models/churn_pipeline/current

# Verify rollback
docker exec goodnote-spark-master \
    readlink /app/data/models/churn_pipeline/current
```

### Communication Template

When a P1 or P2 incident is declared, post to the team channel with:

```
[ML INCIDENT - P{severity}] {Pipeline name}
Impact: {What is broken or degraded}
Detected: {timestamp}
Current state: {Champion model rolled back / Job re-running / Investigation ongoing}
ETA: {When normal service is expected}
Owner: {On-call engineer}
```

---

## 10. Fairness and Subgroup Accuracy

### What It Is

Aggregate accuracy metrics (AUC, RMSE, silhouette) can hide systematic underperformance
for specific user subgroups. If the churn model performs well overall but poorly for
users on a specific device type or subscription tier, interventions targeting those
groups will be ineffective or misleading. Beyond product quality, systematic accuracy
gaps across demographic or geographic subgroups may carry compliance implications.

### Subgroups to Evaluate

The following subgroup dimensions are available via `user_metadata` and the existing
feature set:

| Dimension | Column | Relevant pipelines |
|-----------|--------|--------------------|
| Device type | `device_type` | 5, 6, 7 |
| Country | `country` | 5, 6 |
| Subscription type | `subscription_type` | 5, 6 |
| Cohort size | derived | 8 |
| App version | `app_version` | 7 |

### Per-Pipeline Guidance

**Pipeline 5 — Churn Prediction**

Compute AUC and precision-at-k separately for each subgroup. Flag any subgroup
whose AUC is more than 0.05 below the global AUC:

```sql
-- Churn rate by device type (proxy for subgroup performance)
SELECT m.device_type,
       COUNT(*)                             AS users,
       AVG(s.churn_probability)             AS avg_predicted_churn,
       AVG(a.actually_churned::int)         AS avg_actual_churn,
       AVG(s.churn_probability) -
           AVG(a.actually_churned::int)     AS calibration_error
FROM user_churn_scores s
JOIN user_churn_actuals a  USING (user_id, score_date)
JOIN user_metadata m        USING (user_id)
WHERE s.score_date = CURRENT_DATE - 7
GROUP BY m.device_type
ORDER BY calibration_error DESC;
```

**Pipeline 6 — Behavioral Segmentation**

Check whether any segment is dominated by a single device type or country —
a segment that is 90% iOS users is not a behavioural segment, it is a device
artifact. Compute segment composition after each weekly refit:

```sql
SELECT s.segment_name,
       m.device_type,
       COUNT(*)::float / SUM(COUNT(*)) OVER (PARTITION BY s.segment_name) AS fraction
FROM user_segments s
JOIN user_metadata m USING (user_id)
WHERE s.segment_date = CURRENT_DATE
GROUP BY s.segment_name, m.device_type
ORDER BY s.segment_name, fraction DESC;
```

If any device type exceeds 70% of a segment's membership, investigate whether
`device_type` is leaking into the feature set through a correlated feature.

**Pipeline 7 — ML Anomaly Detection**

The anomaly models already partition by `app_version` and `device_type`.
Fairness here means ensuring no version or device category is systematically
over-represented in anomaly flags:

```sql
SELECT app_version,
       COUNT(*)                                    AS anomaly_count,
       COUNT(*)::float / SUM(COUNT(*)) OVER ()    AS share_of_all_anomalies
FROM ml_performance_anomalies
WHERE detected_date >= CURRENT_DATE - 30
GROUP BY app_version
ORDER BY share_of_all_anomalies DESC;
```

An older app version with 2 rows in the dataset that produces 15% of all anomalies
is likely a data sparsity artifact, not a real signal.

**Pipeline 8 — Retention Forecasting**

Stratify RMSE by cohort size to confirm that forecasts are not systematically
less accurate for small cohorts (which need the forecast most, as they have fewer
observed data points):

```sql
SELECT CASE
           WHEN cohort_size < 100  THEN 'small'
           WHEN cohort_size < 1000 THEN 'medium'
           ELSE 'large'
       END AS cohort_size_bucket,
       COUNT(*)                                                     AS cohorts,
       SQRT(AVG(POWER(f.predicted_retention - r.retention_rate, 2))) AS rmse
FROM cohort_retention_forecasts f
JOIN cohort_retention r
  ON f.cohort_week = r.cohort_week::text
 AND r.week_number  = f.target_week
WHERE f.target_week = 12
GROUP BY cohort_size_bucket
ORDER BY rmse DESC;
```

### Remediation Principles

- Do not suppress or correct subgroup-level predictions without understanding the root cause.
- Underperformance for a subgroup usually signals a data quality issue (sparse training
  examples, proxy label mismatch) rather than a model architecture issue.
- If a subgroup consistently receives lower-quality predictions and is subject to
  automated interventions, consider a separate model or a subgroup-specific recalibration
  rather than a global model fix that may degrade overall performance.
- Record subgroup accuracy metrics in `etl_job_runs.job_metadata` at each refit and
  include them in the quarterly model review.

---

## 11. Model Artifact Retention

### What It Is

Each MLlib `PipelineModel` is stored as a directory of Parquet files. Without a
cleanup policy, `data/models/` accumulates model versions indefinitely. At 50–200 MB
per version, a year of weekly retraining for three pipelines (Pipelines 5, 6, 8)
produces 3–10 GB of model data with no upper bound.

### Retention Rules

| Pipeline | Cadence | Versions to keep |
|----------|---------|-----------------|
| 5 — Churn Prediction | Weekly | Last 8 (≈ 2 months) |
| 6 — Behavioral Segmentation | Weekly | Last 8 |
| 8 — Retention Forecasting | Weekly | Last 8 |
| 7 — ML Anomaly Detection | Daily refit | No persistence; not applicable |

Eight versions provides enough history to roll back through any recent regression
and to reconstruct scores for a calibration audit without requiring indefinite storage.

### Cleanup Procedure

Run after each successful retrain and symlink update:

```bash
# Example for churn pipeline (run inside goodnote-spark-master)
MODEL_DIR=/app/data/models/churn_pipeline
CURRENT=$(readlink "$MODEL_DIR/current")

ls -d "$MODEL_DIR"/20*/  | sort | while read dir; do
    # Never delete the version currently pointed to by 'current'
    if [ "$dir" != "$CURRENT/" ] && [ "$dir" != "$CURRENT" ]; then
        echo "$dir"
    fi
done | head -n -8 | xargs -r rm -rf
```

Apply the same pattern for `segmentation_pipeline` and `retention_pipeline`.

### What to Log

Record the deleted version directories in `etl_job_runs.job_metadata` under
`"pruned_model_versions"` so there is an audit trail if a deleted version is
later needed for a compliance or backtesting request.
