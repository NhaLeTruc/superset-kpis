# Retention Analysis — GoodNote Analytics Platform

**Dataset:** 10,000 users · 131,708 interactions · 2024-01-01 to 2024-12-31
**Cohort definition:** Week of user registration (Monday-anchored ISO weeks)
**Retention metric:** At least one interaction in the target calendar week
**Analysis date:** 2026-03-26

---

## 1. Dataset Overview

| Metric | Value |
|---|---|
| Total users | 10,000 |
| Users with at least one interaction | 9,709 (97.1%) |
| Users never active | 291 (2.9%) |
| Total interactions | 131,708 |
| Cohort weeks | 40 (2024-01-01 → 2024-09-30) |
| Observation window | Up to 52 weeks per cohort |

### Registration-to-First-Interaction Gap

Users do not interact immediately upon registering. The distribution of days between
registration and first interaction reveals a slow-activation pattern:

| Percentile | Days to first interaction |
|---|---|
| Minimum | 0 days |
| Median (P50) | 64 days |
| Mean | 87 days |
| P75 | 135 days |
| P90 | 210 days |
| Maximum | 359 days |

| Activation window | % of all users activated |
|---|---|
| Within 7 days | 11.8% |
| Within 30 days | 33.2% |
| Within 90 days | 60.6% |

**Interpretation:** Most users take weeks to months before their first interaction.
Only ~12% activate within their registration week, which suppresses W0 retention figures.
Onboarding improvements targeting faster time-to-first-value are the highest-leverage retention lever.

---

## 2. Weekly Cohort Retention Matrix

Each cell shows the percentage of that cohort's users who were active in week N
relative to their registration week (W0). `--` means the observation window had not
elapsed when the analysis was run.

> **Reading the table:** A user registered in the week of 2024-01-01 has a 8.8% chance
> of being active in that same week (W0), and a 7.0% chance of being active the following
> week (W1).

```
Cohort        N     W00    W01    W02    W03    W04    W05    W06    W07    W08    W09    W10    W11    W12
──────────────────────────────────────────────────────────────────────────────────────────────────────────
2024-01-01   272    8.8%   7.0%   4.8%   5.5%   4.8%   4.0%   3.3%   2.2%   3.3%   2.6%   3.7%   1.5%   1.5%
2024-01-08   257    4.3%   2.7%   7.8%   4.3%   4.7%   3.5%   2.3%   3.1%   0.8%   3.1%   1.6%   0.8%   3.5%
2024-01-15   260    6.9%   7.7%   7.3%   2.3%   4.2%   4.6%   4.2%   1.9%   1.9%   1.5%   2.3%   1.2%   1.5%
2024-01-22   267    4.1%   5.6%   4.1%   3.0%   2.6%   6.0%   1.5%   4.5%   1.1%   0.7%   2.6%   1.5%   2.2%
2024-01-29   258    3.5%   5.4%   7.4%   4.3%   5.4%   4.3%   2.3%   1.9%   2.3%   1.9%   1.2%   3.5%   2.7%
2024-02-05   235    5.5%   9.8%   5.5%   3.8%   2.1%   5.5%   3.0%   1.7%   3.0%   3.0%   0.9%   2.1%   0.9%
2024-02-12   256    3.5%   6.6%   7.8%   4.3%   7.8%   5.5%   3.5%   1.2%   4.3%   1.2%   1.2%   2.0%   2.0%
2024-02-19   264    5.3%   9.8%   9.1%   8.0%   4.5%   8.0%   4.5%   3.0%   3.0%   3.4%   3.4%   2.3%   4.2%
2024-02-26   240    3.8%   5.4%   9.2%   2.9%   5.0%   4.2%   1.7%   2.1%   2.9%   3.3%   2.1%   0.8%   0.8%
2024-03-04   257    5.8%  13.2%   7.4%   5.8%   5.8%   2.7%   1.9%   1.9%   1.2%   1.6%   2.7%   2.3%   3.9%
2024-03-11   266    7.1%   8.6%   6.0%   4.9%   7.5%   4.1%   3.8%   1.9%   2.3%   3.0%   3.4%   4.1%   4.5%
2024-03-18   249    4.4%   5.6%   5.2%   2.8%   7.2%   1.6%   3.6%   2.0%   2.8%   1.6%   4.0%   1.2%   2.8%
2024-03-25   258    2.7%  11.6%   9.7%   7.8%   5.0%   3.9%   4.7%   5.0%   4.7%   4.3%   3.5%   2.3%   3.1%
2024-04-01   234    5.6%   7.7%   6.4%   4.3%   5.1%   2.6%   3.4%   2.1%   5.1%   1.7%   2.6%   4.7%   2.6%
2024-04-08   235    4.3%   7.7%   6.0%   3.0%   4.3%   2.1%   3.4%   3.0%   3.8%   2.1%   2.6%   1.7%   3.0%
2024-04-15   245    4.5%  10.6%   6.5%   2.4%   3.7%   2.9%   4.1%   2.0%   2.9%   1.6%   3.7%   2.9%   1.2%
2024-04-22   246    3.3%   9.3%   5.7%   4.5%   3.3%   6.9%   2.4%   4.1%   3.3%   4.1%   2.4%   2.0%   2.0%
2024-04-29   266    4.9%   9.8%   7.9%   6.0%   9.4%   3.0%   3.8%   1.5%   3.0%   3.8%   2.6%   3.4%   2.3%
2024-05-06   273    4.0%   7.0%   6.6%  10.6%   2.6%   4.0%   3.7%   2.2%   2.6%   1.8%   1.8%   2.2%   2.2%
2024-05-13   260    5.4%  10.0%   8.5%   3.1%   5.8%   4.2%   3.5%   1.5%   3.1%   2.3%   1.9%   3.8%   3.8%
2024-05-20   272    5.9%  16.2%   6.2%   5.1%   5.5%   2.9%   4.4%   1.1%   3.3%   1.8%   4.4%   2.6%   2.9%
2024-05-27   271   13.3%   7.4%   8.9%   5.5%   5.9%   5.2%   1.5%   4.1%   3.0%   2.2%   2.6%   2.2%   3.7%
2024-06-03   261    6.1%  10.3%   7.3%   5.4%   5.4%   5.7%   5.4%   4.2%   1.9%   4.6%   5.4%   3.8%   5.0%
2024-06-10   278    5.8%   8.3%   6.5%   5.4%   5.4%   3.6%   2.9%   2.2%   6.1%   4.0%   3.2%   6.8%   5.0%
2024-06-17   255    7.5%   7.8%   6.7%   7.1%   2.7%   4.3%   5.1%   3.9%   3.1%   6.3%   4.7%   3.5%   5.5%
2024-06-24   242    9.1%   7.4%   4.1%   6.6%   3.7%   5.0%   5.4%   5.0%   2.9%   2.5%   3.3%   4.1%   2.9%
2024-07-01   283    5.3%   9.2%   7.4%   5.7%   5.7%   4.9%   3.9%   5.3%   3.2%   4.6%   3.5%   4.9%   2.5%
2024-07-08   228    5.3%   8.8%   7.0%   7.5%   5.3%   6.6%   6.1%   3.5%   4.4%   5.3%   3.5%   3.5%   3.9%
2024-07-15   249    7.2%   8.8%   8.4%   5.6%   6.8%   9.2%   5.2%   6.0%   6.4%   2.8%   3.6%   3.2%   9.2%
2024-07-22   263    5.7%  10.3%   8.7%   7.2%   5.3%   7.6%   5.7%   4.6%   4.6%   3.8%   3.4%   6.5%   4.2%
2024-07-29   261    4.6%  10.7%  10.3%   7.7%   6.5%   7.3%   6.5%   2.3%   2.7%   4.6%   8.0%   4.6%   2.7%
2024-08-05   255    7.1%   9.8%  10.6%   8.2%   6.3%   8.2%   5.5%   2.4%   6.7%   7.8%   4.7%   4.3%   4.7%
2024-08-12   254    5.9%  19.3%  13.0%   6.3%  11.4%   7.9%   5.9%   3.5%   4.7%   5.9%   4.7%   3.9%   3.1%
2024-08-19   240   10.8%  15.0%  10.0%   7.1%   7.9%   6.2%   5.4%   4.6%   7.1%   3.8%   4.2%   2.9%   4.6%
2024-08-26   268    6.3%  11.6%  13.1%   6.0%   4.5%   7.1%  10.1%   5.6%   2.6%   4.5%   4.1%   4.9%   3.7%
2024-09-02   265    7.5%  15.8%   9.8%   2.6%   8.7%  12.1%   6.4%   3.4%   3.4%   4.9%   4.5%   4.9%   3.8%
2024-09-09   253   13.4%  12.3%  10.3%   6.7%  11.5%   8.3%   4.0%   1.6%   5.5%   2.0%   2.0%   3.2%   4.3%
2024-09-16   192    8.9%  17.7%   6.8%  10.4%   6.8%   7.8%   6.8%   3.6%   1.6%   4.2%   3.1%   4.2%   5.7%
2024-09-23   276    7.6%  13.0%  17.8%   9.1%   6.5%   8.7%   5.1%   5.1%   5.1%   6.5%   5.8%   4.0%   4.3%
2024-09-30    36   11.1%  33.3%   8.3%   8.3%   5.6%   2.8%   2.8%   8.3%   2.8%   5.6%   0.0%   2.8%   2.8%
```

---

## 3. Average Retention Curve

Averaged across all 40 mature cohorts (each having a full 12-week observation window).

```
Week  Retention  Chart
────────────────────────────────────────────────
W00     6.3%     ██████░░░░░░░░░░░░░░░░░░░░░░░░
W01    10.4%     ██████████░░░░░░░░░░░░░░░░░░░░  ← peak
W02     8.0%     ████████░░░░░░░░░░░░░░░░░░░░░░
W03     5.7%     ██████░░░░░░░░░░░░░░░░░░░░░░░░
W04     5.7%     ██████░░░░░░░░░░░░░░░░░░░░░░░░
W05     5.4%     █████░░░░░░░░░░░░░░░░░░░░░░░░░
W06     4.2%     ████░░░░░░░░░░░░░░░░░░░░░░░░░░
W07     3.2%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
W08     3.5%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
W09     3.4%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
W10     3.2%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
W11     3.2%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
W12     3.4%     ███░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

### Key observations

- **W0 < W1 (6.3% → 10.4%):** Retention *rises* from the registration week to the following
  week. This is expected given the 64-day median activation gap — users who register early
  in the week often make their first interaction the following week. W1 is the true "first
  active week" for most users.
- **Steep cliff W1→W4 (10.4% → 5.7%):** A 45% relative drop in the first month. This is
  the highest-risk churn window — users who do engage quickly drop off within 4 weeks.
- **Plateau from W7 onward (~3.2–3.5%):** Retention stabilises around 3–3.5% from week 7
  through week 12. This represents the loyal core user base — roughly 1 in 30 registered
  users remains active three months after registration.

### Week-over-week drop-off

| Transition | Rate | Relative drop |
|---|---|---|
| W01 → W02 | 10.4% → 8.0% | −23% |
| W02 → W03 | 8.0% → 5.7% | −29% |
| W03 → W04 | 5.7% → 5.7% | −1% |
| W04 → W05 | 5.7% → 5.4% | −6% |
| W05 → W06 | 5.4% → 4.2% | −22% |
| W06 → W07 | 4.2% → 3.2% | −23% |
| W07 → W12 | 3.2% → 3.4% | flat |

The sharpest drops are in W2–W3 (first month) and W5–W7 (second month). After W7 the curve
flattens — users who survive past 7 weeks are significantly more likely to remain engaged.

---

## 4. Retention by Subscription Type

| Type | W00 | W01 | W02 | W04 | W08 | W12 |
|---|---|---|---|---|---|---|
| free | 6.6% | 10.4% | 8.0% | 5.5% | 3.4% | 3.4% |
| premium | 6.3% | 11.0% | 8.3% | 5.8% | 3.7% | 3.4% |
| enterprise | 5.0% | 8.2% | 7.1% | 6.6% | 2.7% | 3.2% |

**Findings:**

- **Premium users** have the highest W1 retention (11.0%) and remain the strongest through
  W4 (5.8%), reflecting higher engagement from paying users.
- **Enterprise users** start lower (W1: 8.2%) but show the *slowest decay*: they reach
  parity with premium by W4 (6.6%) and converge with all tiers by W12 (~3%). Enterprise
  users may have delayed onboarding (procurement, IT setup) but convert to loyal users once
  activated.
- **Free users** closely track premium through W4 then stabilise at the same long-term rate
  (3.4% at W12), suggesting the gap is in early engagement rather than long-term value.

---

## 5. Retention by Device Type

| Device | W01 | W04 | W08 | W12 |
|---|---|---|---|---|
| iPhone | 10.3% | 5.7% | 3.5% | 3.8% |
| iPad | 10.5% | 5.9% | 3.2% | 3.3% |
| Android Phone | 10.9% | 5.2% | 3.4% | 2.8% |
| Android Tablet | 9.7% | 6.1% | 3.2% | 3.2% |

**Findings:**

- Device type shows minimal differentiation — all four devices converge to ~3–4% by W12.
- **Android Phone** has the highest W1 rate (10.9%) but the steepest fall to W12 (2.8%),
  suggesting higher but less durable engagement from smartphone users.
- **Android Tablet** has the lowest W1 but the strongest W4 (6.1%), indicating tablet
  users take longer to activate but retain slightly better in the first month.
- No device type stands out as a strong retention predictor; device is not a differentiating
  factor for long-term loyalty.

---

## 6. Retention by Country (Top 5)

| Country | Users | W01 | W04 | W08 |
|---|---|---|---|---|
| US | 3,397 | 10.4% | 6.0% | 3.3% |
| UK | 1,081 | 10.1% | 5.4% | 3.0% |
| JP | 1,027 | 9.8% | 4.4% | 4.0% |
| DE | 816 | 11.0% | 6.9% | 4.3% |
| CN | 809 | 10.3% | 5.6% | 3.6% |

**Findings:**

- **Germany (DE)** has the strongest retention across all measured weeks (W01: 11.0%,
  W04: 6.9%, W08: 4.3%) — the highest W4 and W8 rates of any country.
- **Japan (JP)** shows an unusual pattern: relatively lower W4 (4.4%) but recovers to 4.0%
  at W8, suggesting a re-engagement pattern or different usage cadence.
- **UK** trails all others at W08 (3.0%), indicating higher post-month-one churn than
  comparable markets.
- Country differences are modest (within ~2pp at each checkpoint) but may warrant
  localised onboarding experiments in the UK and JP markets.

---

## 7. Cohort Trend: Early vs Later Cohorts

Comparing the average W01, W04, and W08 retention for cohorts registered in each quarter:

| Registration quarter | Cohort weeks | Avg W01 | Avg W04 | Avg W08 |
|---|---|---|---|---|
| Q1 2024 (Jan–Mar) | 13 cohorts | 7.5% | 4.9% | 2.4% |
| Q2 2024 (Apr–Jun) | 13 cohorts | 9.5% | 5.0% | 3.3% |
| Q3 2024 (Jul–Sep) | 14 cohorts | 11.6% | 6.8% | 4.7% |

**Trend:** Later cohorts show meaningfully higher retention at every checkpoint. Q3 cohorts
retain at W01 at 55% higher rates than Q1 cohorts (11.6% vs 7.5%), and at W08 at nearly
double (4.7% vs 2.4%). Two explanations are plausible:

1. **Survivorship / maturation effect:** Q1 cohorts have had more time for dormant users
   to be counted as inactive; Q3 cohorts still include users in their active/settling phase.
2. **Product improvements:** If the app improved over the year, later cohorts genuinely
   experience a better product and retain better.

Distinguishing these requires comparing cohorts at the same age in a future analysis.

---

## 8. Summary and Recommendations

| Insight | Metric | Recommendation |
|---|---|---|
| Most users take 2+ months to first interact | Median activation gap: 64 days | Add in-app prompts, email nudges, or push notifications in the first 7–14 days to capture the 88% who don't activate in their registration week |
| Steepest churn is in weeks 1–3 | W1→W3 loses ~45% of active users | Prioritise onboarding flows targeting users who interact once then disappear; trigger re-engagement after 7 days of inactivity |
| Loyal core stabilises at ~3.3% from W7 | W7–W12 flat at 3.2–3.5% | This cohort is high-LTV; identify its behavioural traits (action type, session depth) to build a predictive churn model |
| Enterprise users have delayed but durable activation | Enterprise W1: 8.2% but converges by W12 | Consider a dedicated enterprise onboarding track with longer nurture sequences |
| Germany over-indexes on long-term retention | DE W08: 4.3% vs US 3.3% | Use DE user behaviour as a model for engagement patterns to replicate in other markets |
| Later cohorts retain better at every checkpoint | Q3 W08: 4.7% vs Q1 W08: 2.4% | Confirm whether this is a product improvement signal by running controlled cohort comparison at equal ages |

---

*Analysis performed on data generated by `scripts/generate_sample_data.py` (realistic mode,
medium dataset). Raw numbers saved to `data/raw/retention_analysis.json`.*

---

## 9. Recreating This Analysis in Superset

### Prerequisites

- Superset running at `http://localhost:8088` (start with `make up`)
- ETL jobs 1–2 completed (`make run-job-1 run-job-2`) — this populates `cohort_retention`
  and `cohort_retention_by_segment` in the `analytics` database

### Step 1 — Register the Datasets

Go to **Datasets → + Dataset**, select database `analytics`, schema `public`.
Register these two tables:

| Table | Used for |
|---|---|
| `cohort_retention` | overall retention curve, heatmap, quarterly trend |
| `cohort_retention_by_segment` | subscription / device / country breakdowns |

For the aggregated views (Charts 3–7 below), use **Datasets → + Dataset → Virtual (SQL)**
and paste the SQL provided in each section.

### Step 2 — Build the Charts

#### Chart 1: Cohort Retention Heatmap (→ Section 2)

Reproduces the full cohort × week matrix as a colour-coded grid.

**Chart type:** Heatmap
**Dataset:** `cohort_retention`

```sql
SELECT
    cohort_week::text      AS cohort_week,
    week_number,
    ROUND(retention_rate * 100, 1) AS retention_pct
FROM cohort_retention
ORDER BY cohort_week, week_number
```

| Field | Value |
|---|---|
| X-axis | `week_number` |
| Y-axis | `cohort_week` |
| Metric | `MAX(retention_pct)` |
| Color scheme | Sequential (e.g. Blues) — darker = higher retention |

---

#### Chart 2: Average Retention Curve (→ Section 3)

Reproduces the W0–W12 average curve showing the W1 peak and W7+ plateau.

**Chart type:** Line Chart
**Virtual dataset SQL:**

```sql
SELECT
    week_number,
    ROUND(AVG(retention_rate) * 100, 1) AS avg_retention_pct
FROM cohort_retention
GROUP BY week_number
ORDER BY week_number
```

| Field | Value |
|---|---|
| X-axis | `week_number` |
| Metric | `MAX(avg_retention_pct)` |
| X-axis label | "Weeks since registration" |

Add a **Marker** annotation at `week_number = 1` labelled "W1 peak (10.4%)" and a
**Range** annotation from `week_number = 7` to `12` labelled "Plateau ~3.3%".

---

#### Chart 3: Retention by Subscription Type (→ Section 4)

**Chart type:** Line Chart
**Virtual dataset SQL:**

```sql
SELECT
    week_number,
    segment_value              AS subscription_type,
    ROUND(AVG(retention_rate) * 100, 1) AS avg_retention_pct
FROM cohort_retention_by_segment
WHERE segment_type = 'subscription_type'
GROUP BY week_number, segment_value
ORDER BY week_number
```

| Field | Value |
|---|---|
| X-axis | `week_number` |
| Metric | `MAX(avg_retention_pct)` |
| Series (Group By) | `subscription_type` |

---

#### Chart 4: Retention by Device Type (→ Section 5)

**Chart type:** Line Chart
**Virtual dataset SQL:**

```sql
SELECT
    week_number,
    segment_value              AS device_type,
    ROUND(AVG(retention_rate) * 100, 1) AS avg_retention_pct
FROM cohort_retention_by_segment
WHERE segment_type = 'device_type'
GROUP BY week_number, segment_value
ORDER BY week_number
```

Same configuration as Chart 3 — set Series to `device_type`.

---

#### Chart 5: Retention by Country — Top 5 (→ Section 6)

**Chart type:** Line Chart
**Virtual dataset SQL:**

```sql
SELECT
    week_number,
    segment_value              AS country,
    ROUND(AVG(retention_rate) * 100, 1) AS avg_retention_pct
FROM cohort_retention_by_segment
WHERE segment_type = 'country'
  AND segment_value IN ('US', 'UK', 'JP', 'DE', 'CN')
GROUP BY week_number, segment_value
ORDER BY week_number
```

Same configuration as Chart 3 — set Series to `country`.

---

#### Chart 6: Cohort Quarter Trend (→ Section 7)

Shows how W1, W4, and W8 retention compare across Q1/Q2/Q3 cohorts.

**Chart type:** Bar Chart (grouped)
**Virtual dataset SQL:**

```sql
SELECT
    CASE
        WHEN cohort_week BETWEEN '2024-01-01' AND '2024-03-31' THEN 'Q1 2024'
        WHEN cohort_week BETWEEN '2024-04-01' AND '2024-06-30' THEN 'Q2 2024'
        WHEN cohort_week BETWEEN '2024-07-01' AND '2024-09-30' THEN 'Q3 2024'
    END                        AS registration_quarter,
    week_number,
    ROUND(AVG(retention_rate) * 100, 1) AS avg_retention_pct
FROM cohort_retention
WHERE week_number IN (1, 4, 8)
GROUP BY 1, 2
ORDER BY registration_quarter, week_number
```

| Field | Value |
|---|---|
| X-axis | `registration_quarter` |
| Metric | `MAX(avg_retention_pct)` |
| Series (Group By) | `week_number` |
| Bar mode | Grouped |

---

#### Charts 7–9: KPI Big Numbers (→ Section 3 key stats)

Create three **Big Number with Trendline** charts, all from the `cohort_retention` table:

| Chart | Filter | Metric | Expected value |
|---|---|---|---|
| W0 avg retention | `week_number = 0` | `AVG(retention_rate * 100)` | 6.3% |
| W1 avg retention (peak) | `week_number = 1` | `AVG(retention_rate * 100)` | 10.4% |
| W7–W12 plateau avg | `week_number >= 7` | `AVG(retention_rate * 100)` | ~3.3% |

Apply filters via **Filters → Custom SQL**: e.g. `week_number = 1`.

---

### Step 3 — Assemble the Dashboard

1. **Dashboards → + Dashboard**, name it `Retention Analysis`
2. Drag charts into this recommended layout:

```
┌─────────────────────────────────────────────────┐
│  [KPI: W0]   [KPI: W1 peak]   [KPI: W7+ plateau]│  ← row 1: headline numbers
├─────────────────────────────────────────────────┤
│         Cohort Retention Heatmap (Chart 1)       │  ← row 2: full width
├──────────────────────┬──────────────────────────┤
│  Avg Retention Curve │  Quarter Trend Bar Chart  │  ← row 3: half/half
│      (Chart 2)       │       (Chart 6)           │
├──────────────────────┴──────────────────────────┤
│  By Subscription │  By Device  │  By Country     │  ← row 4: thirds
│    (Chart 3)     │  (Chart 4)  │  (Chart 5)      │
└─────────────────────────────────────────────────┘
```

3. Add a **Dashboard Filter** on `cohort_week` (date range picker) wired to all charts
   that use `cohort_retention` or the virtual datasets derived from it — this lets you
   restrict the view to a specific registration period.

4. Save and publish.
