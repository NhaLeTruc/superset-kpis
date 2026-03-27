"""Retention analysis script for GoodNote Analytics data."""

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta


# ── Load ──────────────────────────────────────────────────────────────────────
meta = {}
with open("data/raw/user_metadata.csv") as f:
    for row in csv.DictReader(f):
        meta[row["user_id"]] = {
            "reg_date": datetime.strptime(row["registration_date"], "%Y-%m-%d"),
            "subscription": row["subscription_type"],
            "device": row["device_type"],
            "country": row["country"],
        }

interactions = defaultdict(list)
with open("data/raw/user_interactions.csv") as f:
    for row in csv.DictReader(f):
        uid = row["user_id"]
        if uid in meta:
            ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
            interactions[uid].append(ts)

print(f"Loaded {len(meta)} users, {sum(len(v) for v in interactions.values())} interactions")


def week_start(dt):
    """Return the Monday midnight of the week containing dt."""
    d = dt.date() if hasattr(dt, "date") else dt
    monday = d - timedelta(days=d.weekday())
    return datetime(monday.year, monday.month, monday.day)


MAX_DATE = datetime(2024, 12, 31)
MAX_WEEKS = 12

# ── Cohort assignment (registration week) ─────────────────────────────────────
user_cohort = {uid: week_start(info["reg_date"]) for uid, info in meta.items()}

user_active_weeks = {
    uid: {week_start(ts) for ts in ts_list} for uid, ts_list in interactions.items()
}

# ── Cohort retention matrix ───────────────────────────────────────────────────
cohort_users = defaultdict(list)
for uid in meta:
    cohort_users[user_cohort[uid]].append(uid)

all_cohorts = sorted(cohort_users.keys())
mature_cohorts = [c for c in all_cohorts if c + timedelta(weeks=MAX_WEEKS) <= MAX_DATE]

print(f"All cohort weeks   : {len(all_cohorts)}")
print(f"Mature cohorts     : {len(mature_cohorts)}")
print(f"Cohort range       : {all_cohorts[0].date()} -> {all_cohorts[-1].date()}")
print(f"Mature range       : {mature_cohorts[0].date()} -> {mature_cohorts[-1].date()}")

ret = {}
sizes = {}
for c in all_cohorts:
    users = cohort_users[c]
    n = len(users)
    sizes[c] = n
    ret[c] = {}
    for w in range(MAX_WEEKS + 1):
        target = c + timedelta(weeks=w)
        if target > MAX_DATE:
            ret[c][w] = None
        else:
            active = sum(1 for uid in users if target in user_active_weeks.get(uid, set()))
            ret[c][w] = active / n if n > 0 else 0.0

# ── Print full matrix ─────────────────────────────────────────────────────────
print("\n--- WEEKLY COHORT RETENTION MATRIX ---")
header = "Cohort         N    " + "  ".join(f"W{w:02d}" for w in range(MAX_WEEKS + 1))
print(header)
print("-" * len(header))
for c in all_cohorts:
    n = sizes[c]
    row = f"{c.date()!s:<12}  {n:>4}  "
    for w in range(MAX_WEEKS + 1):
        v = ret[c][w]
        if v is None:
            row += "  -- "
        else:
            row += f"{v * 100:4.1f}%"
        if w < MAX_WEEKS:
            row += "  "
    print(row)

# ── Average retention (mature cohorts) ───────────────────────────────────────
print("\n--- AVERAGE RETENTION CURVE (mature cohorts only) ---")
avg_ret = {}
for w in range(MAX_WEEKS + 1):
    vals = [ret[c][w] for c in mature_cohorts if ret[c][w] is not None]
    avg_ret[w] = sum(vals) / len(vals) if vals else 0.0
    bar = "|" * int(avg_ret[w] * 50)
    print(f"  W{w:02d}: {avg_ret[w] * 100:5.1f}%  {bar}")

# ── Week-over-week drop-off ───────────────────────────────────────────────────
print("\n--- WEEK-OVER-WEEK DROP-OFF ---")
for w in range(1, MAX_WEEKS + 1):
    prev = avg_ret[w - 1]
    curr = avg_ret[w]
    drop = (prev - curr) / prev * 100 if prev > 0 else 0
    print(f"  W{w - 1:02d}->W{w:02d}: {prev * 100:.1f}% -> {curr * 100:.1f}%  (drop {drop:.1f}%)")

# ── Retention by subscription ─────────────────────────────────────────────────
print("\n--- RETENTION BY SUBSCRIPTION TYPE (mature cohorts avg) ---")
sub_avg = {}
for sub in ["free", "premium", "enterprise"]:
    week_rates = defaultdict(list)
    for c in mature_cohorts:
        cu = [u for u in cohort_users[c] if meta[u]["subscription"] == sub]
        if not cu:
            continue
        for w in range(MAX_WEEKS + 1):
            target = c + timedelta(weeks=w)
            active = sum(1 for u in cu if target in user_active_weeks.get(u, set()))
            week_rates[w].append(active / len(cu))
    sub_avg[sub] = {
        w: (sum(week_rates[w]) / len(week_rates[w]) if week_rates[w] else 0.0)
        for w in range(MAX_WEEKS + 1)
    }
    spot = "  ".join(f"W{w:02d}={sub_avg[sub][w] * 100:4.1f}%" for w in [0, 1, 2, 4, 8, 12])
    print(f"  {sub:<12}: {spot}")

# ── Retention by device ───────────────────────────────────────────────────────
print("\n--- RETENTION BY DEVICE TYPE (W01/W04/W08/W12 avg) ---")
device_avg = {}
for device in ["iPhone", "iPad", "Android Phone", "Android Tablet"]:
    week_rates = defaultdict(list)
    for c in mature_cohorts:
        cu = [u for u in cohort_users[c] if meta[u]["device"] == device]
        if not cu:
            continue
        for w in [1, 4, 8, 12]:
            target = c + timedelta(weeks=w)
            active = sum(1 for u in cu if target in user_active_weeks.get(u, set()))
            week_rates[w].append(active / len(cu))
    device_avg[device] = {
        w: (sum(week_rates[w]) / len(week_rates[w]) if week_rates[w] else 0.0)
        for w in [1, 4, 8, 12]
    }
    vals = device_avg[device]
    print(
        f"  {device:<18}: W01={vals[1] * 100:4.1f}%  W04={vals[4] * 100:4.1f}%  W08={vals[8] * 100:4.1f}%  W12={vals[12] * 100:4.1f}%"
    )

# ── Registration-to-first-interaction gap ─────────────────────────────────────
print("\n--- REGISTRATION TO FIRST INTERACTION GAP ---")
gaps_days = []
never_active = 0
for uid, info in meta.items():
    ts_list = interactions.get(uid, [])
    if not ts_list:
        never_active += 1
        continue
    first_ts = min(ts_list)
    gap = (first_ts - info["reg_date"]).days
    gaps_days.append(gap)

gaps_days.sort()
n = len(gaps_days)
print(f"  Users with interactions : {n}")
print(f"  Users never active      : {never_active}")
print(f"  Min gap (days)          : {gaps_days[0]}")
print(f"  Median gap (days)       : {gaps_days[n // 2]}")
print(f"  Mean gap (days)         : {sum(gaps_days) / n:.1f}")
print(f"  P75 gap (days)          : {gaps_days[int(n * 0.75)]}")
print(f"  P90 gap (days)          : {gaps_days[int(n * 0.90)]}")
print(f"  Max gap (days)          : {gaps_days[-1]}")
print(f"  Activated within 7d     : {sum(1 for g in gaps_days if g <= 7) / n * 100:.1f}%")
print(f"  Activated within 30d    : {sum(1 for g in gaps_days if g <= 30) / n * 100:.1f}%")
print(f"  Activated within 90d    : {sum(1 for g in gaps_days if g <= 90) / n * 100:.1f}%")

# ── Country retention ─────────────────────────────────────────────────────────
print("\n--- W01/W04/W08 RETENTION BY COUNTRY (top 5) ---")
country_counts = Counter(meta[uid]["country"] for uid in meta)
top_countries = [c for c, _ in country_counts.most_common(5)]
country_avg = {}
for country in top_countries:
    week_rates = defaultdict(list)
    for c in mature_cohorts:
        cu = [u for u in cohort_users[c] if meta[u]["country"] == country]
        if not cu:
            continue
        for w in [1, 4, 8]:
            target = c + timedelta(weeks=w)
            active = sum(1 for u in cu if target in user_active_weeks.get(u, set()))
            week_rates[w].append(active / len(cu))
    country_avg[country] = {
        w: (sum(week_rates[w]) / len(week_rates[w]) if week_rates[w] else 0.0) for w in [1, 4, 8]
    }
    vals = country_avg[country]
    n_country = country_counts[country]
    print(
        f"  {country:<4} (n={n_country:>4}): W01={vals[1] * 100:4.1f}%  W04={vals[4] * 100:4.1f}%  W08={vals[8] * 100:4.1f}%"
    )

# ── Save results ──────────────────────────────────────────────────────────────
out = {
    "all_cohorts": [str(c.date()) for c in all_cohorts],
    "mature_cohorts": [str(c.date()) for c in mature_cohorts],
    "sizes": {str(k.date()): v for k, v in sizes.items()},
    "retention": {str(k.date()): {str(wk): vv for wk, vv in v.items()} for k, v in ret.items()},
    "avg_ret": {str(w): v for w, v in avg_ret.items()},
    "sub_avg": {sub: {str(w): v for w, v in d.items()} for sub, d in sub_avg.items()},
    "device_avg": {dev: {str(w): v for w, v in d.items()} for dev, d in device_avg.items()},
    "country_avg": {ctry: {str(w): v for w, v in d.items()} for ctry, d in country_avg.items()},
    "gaps_summary": {
        "min": gaps_days[0],
        "median": gaps_days[n // 2],
        "mean": round(sum(gaps_days) / n, 1),
        "p75": gaps_days[int(n * 0.75)],
        "p90": gaps_days[int(n * 0.90)],
        "max": gaps_days[-1],
        "within_7d_pct": round(sum(1 for g in gaps_days if g <= 7) / n * 100, 1),
        "within_30d_pct": round(sum(1 for g in gaps_days if g <= 30) / n * 100, 1),
        "within_90d_pct": round(sum(1 for g in gaps_days if g <= 90) / n * 100, 1),
    },
    "never_active": never_active,
    "total_users": len(meta),
}

with open("data/raw/retention_analysis.json", "w") as f:
    json.dump(out, f, indent=2)
print("\nSaved -> data/raw/retention_analysis.json")
