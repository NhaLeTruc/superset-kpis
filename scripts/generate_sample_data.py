#!/usr/bin/env python3
"""
Sample Data Generator for GoodNote Analytics Platform

Generates realistic sample data for testing Spark jobs and optimization analysis.
Creates two CSV files:
- user_interactions.csv: User interaction events
- user_metadata.csv: User demographic and device information

Usage:
    python3 scripts/generate_sample_data.py --num-users 1000 --num-interactions 10000
    python3 scripts/generate_sample_data.py --small   # 1K users, 10K interactions
    python3 scripts/generate_sample_data.py --medium  # 10K users, 100K interactions
    python3 scripts/generate_sample_data.py --large   # 100K users, 1M interactions

    # Realistic patterns (recommended):
    python3 scripts/generate_sample_data.py --small --realistic-all

    # Backward compatible uniform distribution:
    python3 scripts/generate_sample_data.py --small --uniform
"""
import argparse
import csv
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# =============================================================================
# DATE RANGES
# =============================================================================
ACT_START_DATE = datetime(2024, 1, 1)
ACT_END_DATE = datetime(2025, 1, 1)
REG_START_DATE = datetime(2023, 1, 1)
REG_END_DATE = datetime(2023, 12, 31)

# Session timeout must match Spark job constant
SESSION_TIMEOUT_SECONDS = 1800

# =============================================================================
# BASIC DISTRIBUTIONS (used by both uniform and realistic modes)
# =============================================================================
ACTION_DISTRIBUTION = {
    "VIEW": 0.50,
    "EDIT": 0.30,
    "SHARE": 0.15,
    "EXPORT": 0.05
}

DEVICE_DISTRIBUTION = {
    "iPhone": 0.40,
    "iPad": 0.25,
    "Android Phone": 0.25,
    "Android Tablet": 0.10
}

COUNTRY_DISTRIBUTION = {
    "US": 0.35,
    "UK": 0.10,
    "CA": 0.08,
    "DE": 0.08,
    "FR": 0.07,
    "JP": 0.10,
    "AU": 0.05,
    "CN": 0.08,
    "IN": 0.06,
    "BR": 0.03
}

SUBSCRIPTION_DISTRIBUTION = {
    "free": 0.60,
    "premium": 0.30,
    "enterprise": 0.10
}

VERSION_DISTRIBUTION = {
    "3.0.0": 0.10,
    "3.0.1": 0.15,
    "3.1.0": 0.25,
    "3.1.1": 0.30,
    "3.2.0": 0.20
}

# =============================================================================
# TEMPORAL PATTERNS (for realistic mode)
# =============================================================================
# Hour weights by subscription type (index 0-23)
HOUR_WEIGHTS = {
    # Enterprise: peaks at 9-11am and 2-4pm (work hours)
    "enterprise": [
        0.05, 0.03, 0.02, 0.02, 0.02, 0.05, 0.15, 0.40,
        0.80, 1.00, 1.00, 0.90, 0.70, 0.85, 0.95, 0.90,
        0.70, 0.50, 0.30, 0.20, 0.15, 0.10, 0.08, 0.06
    ],
    # Free: peaks at 8-10am and 7-10pm (before/after work)
    "free": [
        0.08, 0.05, 0.03, 0.02, 0.02, 0.05, 0.20, 0.50,
        0.90, 1.00, 0.85, 0.60, 0.50, 0.50, 0.45, 0.50,
        0.55, 0.65, 0.80, 1.00, 1.10, 0.90, 0.50, 0.20
    ],
    # Premium: blend of both patterns
    "premium": [
        0.06, 0.04, 0.03, 0.02, 0.02, 0.05, 0.18, 0.45,
        0.85, 1.00, 0.92, 0.75, 0.60, 0.68, 0.70, 0.70,
        0.62, 0.58, 0.55, 0.60, 0.62, 0.50, 0.30, 0.12
    ],
}

# Day-of-week weights (Monday=0 through Sunday=6)
DAY_WEIGHTS = {
    "enterprise": [1.20, 1.20, 1.20, 1.20, 1.10, 0.40, 0.30],
    "free": [0.85, 0.85, 0.85, 0.90, 1.00, 1.30, 1.35],
    "premium": [1.00, 1.00, 1.00, 1.00, 1.00, 1.10, 1.10],
}

# Seasonal events: (start_month, start_day, end_month, end_day, multiplier)
SEASONAL_EVENTS = [
    (8, 15, 9, 15, 1.30),   # Back to school
    (12, 20, 12, 31, 0.55), # Holiday dip
    (1, 1, 1, 7, 1.40),     # New Year surge
]

# Burst events: (month, day, duration_days, multiplier)
DEFAULT_BURST_EVENTS = [
    (3, 15, 3, 3.0),   # Spring marketing campaign
    (6, 1, 2, 4.0),    # Product launch
    (10, 10, 5, 2.5),  # Fall promotion
]

# =============================================================================
# ACTION SEQUENCE MODEL (for realistic sessions)
# =============================================================================
# Transition probabilities: current_action -> {next_action: probability}
ACTION_TRANSITIONS = {
    "VIEW": {"VIEW": 0.55, "EDIT": 0.35, "SHARE": 0.08, "EXPORT": 0.02},
    "EDIT": {"VIEW": 0.15, "EDIT": 0.45, "SHARE": 0.30, "EXPORT": 0.10},
    "SHARE": {"VIEW": 0.50, "EDIT": 0.20, "SHARE": 0.25, "EXPORT": 0.05},
    "EXPORT": {"VIEW": 0.65, "EDIT": 0.20, "SHARE": 0.10, "EXPORT": 0.05},
}

# Duration log-normal parameters by action type (mu, sigma for ln(ms))
ACTION_DURATION_PARAMS = {
    "VIEW": (7.5, 1.2),    # median ~1800ms, range ~200ms-30s
    "EDIT": (9.2, 1.4),    # median ~10000ms, range ~1s-3min
    "SHARE": (8.0, 1.0),   # median ~3000ms, range ~500ms-20s
    "EXPORT": (8.5, 0.9),  # median ~5000ms, range ~1s-25s
}

# Intra-session gap parameters (log-normal, in seconds)
INTRA_SESSION_GAP_PARAMS = (2.8, 1.3)  # median ~16s, range ~2s-10min

# Page popularity follows Zipf distribution
PAGE_ZIPF_EXPONENT = 1.3
NUM_PAGES = 100

# =============================================================================
# USER LIFECYCLE MODEL
# =============================================================================
LIFECYCLE_STAGES = {
    "onboarding": {"max_days": 14, "activity_multiplier": 2.5},
    "settling": {"max_days": 42, "activity_multiplier": 1.5},
    "active": {"max_days": 365, "activity_multiplier": 1.0},
    "dormant": {"max_days": 90, "activity_multiplier": 0.1},
    "churned": {"max_days": float('inf'), "activity_multiplier": 0.0},
}


# =============================================================================
# DISTRIBUTION UTILITIES
# =============================================================================
def weighted_random_choice(distribution: dict) -> str:
    """Choose item based on weighted distribution."""
    items = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(items, weights=weights, k=1)[0]


def pareto_sample(alpha: float, x_min: float = 1.0) -> float:
    """
    Generate Pareto-distributed value.
    Lower alpha = more skewed (heavier tail).
    """
    u = random.random()
    return x_min / (u ** (1.0 / alpha))


def log_normal_sample(mu: float, sigma: float) -> float:
    """Generate log-normal distributed value."""
    return math.exp(random.gauss(mu, sigma))


def poisson_sample(lam: float) -> int:
    """Generate Poisson-distributed integer."""
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def zipf_sample(n: int, s: float) -> int:
    """
    Generate Zipf-distributed integer in range [1, n].
    Uses rejection sampling.
    """
    # Precompute normalization constant
    h_n = sum(1.0 / (k ** s) for k in range(1, n + 1))
    while True:
        u = random.random()
        v = random.random()
        x = int((n ** (1 - s) - 1) * u + 1) ** (1 / (1 - s))
        k = max(1, min(n, int(x + 0.5)))
        # Rejection test
        if v * k ** s <= 1.0:
            return k


def build_zipf_cdf(n: int, s: float) -> list:
    """Pre-compute Zipf CDF for efficient sampling."""
    weights = [1.0 / (k ** s) for k in range(1, n + 1)]
    total = sum(weights)
    cdf = []
    cumulative = 0.0
    for w in weights:
        cumulative += w / total
        cdf.append(cumulative)
    return cdf


def sample_from_cdf(cdf: list) -> int:
    """Sample from pre-computed CDF using binary search."""
    u = random.random()
    lo, hi = 0, len(cdf) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if cdf[mid] < u:
            lo = mid + 1
        else:
            hi = mid
    return lo + 1  # 1-indexed


# =============================================================================
# TEMPORAL UTILITIES
# =============================================================================
def get_seasonal_multiplier(date: datetime) -> float:
    """Get seasonal activity multiplier for a date."""
    month, day = date.month, date.day
    for start_m, start_d, end_m, end_d, mult in SEASONAL_EVENTS:
        # Handle year wrap (e.g., Dec 20 - Jan 7)
        if start_m <= end_m:
            if (month > start_m or (month == start_m and day >= start_d)) and \
               (month < end_m or (month == end_m and day <= end_d)):
                return mult
        else:
            if (month > start_m or (month == start_m and day >= start_d)) or \
               (month < end_m or (month == end_m and day <= end_d)):
                return mult
    return 1.0


def get_burst_multiplier(date: datetime, burst_events: list) -> float:
    """Check if date falls within a burst event."""
    for month, day, duration, mult in burst_events:
        event_start = datetime(date.year, month, day)
        event_end = event_start + timedelta(days=duration)
        if event_start <= date < event_end:
            return mult
    return 1.0


def sample_hour_weighted(subscription_type: str) -> int:
    """Sample hour of day based on subscription-specific weights."""
    weights = HOUR_WEIGHTS.get(subscription_type, HOUR_WEIGHTS["free"])
    return random.choices(range(24), weights=weights, k=1)[0]


# =============================================================================
# USER PROFILE MODEL
# =============================================================================
@dataclass
class UserProfile:
    """Model user behavior and lifecycle."""
    user_id: str
    subscription_type: str
    registration_date: datetime
    device_type: str
    country: str

    # Lifecycle parameters (set during initialization)
    base_activity_level: float = 1.0
    churn_week: Optional[int] = None
    avg_sessions_per_active_day: float = 2.0
    avg_actions_per_session: float = 4.0

    # Precomputed Zipf CDF for page sampling (class-level)
    _page_cdf: list = field(default_factory=list, repr=False)

    def get_lifecycle_stage(self, current_date: datetime) -> str:
        """Determine lifecycle stage based on days since registration."""
        days_since_reg = (current_date - self.registration_date).days
        if days_since_reg < 0:
            return "churned"  # Not yet registered

        weeks_since_reg = days_since_reg // 7
        if self.churn_week is not None and weeks_since_reg >= self.churn_week:
            return "churned"

        if days_since_reg <= LIFECYCLE_STAGES["onboarding"]["max_days"]:
            return "onboarding"
        elif days_since_reg <= LIFECYCLE_STAGES["settling"]["max_days"]:
            return "settling"
        else:
            return "active"

    def get_activity_multiplier(self, current_date: datetime) -> float:
        """Calculate total activity multiplier for the date."""
        stage = self.get_lifecycle_stage(current_date)
        stage_mult = LIFECYCLE_STAGES[stage]["activity_multiplier"]

        # Day of week multiplier
        day_of_week = current_date.weekday()
        dow_mult = DAY_WEIGHTS.get(
            self.subscription_type, DAY_WEIGHTS["free"]
        )[day_of_week]

        return self.base_activity_level * stage_mult * dow_mult

    def should_be_active(self, current_date: datetime, seasonal_mult: float,
                         burst_mult: float) -> bool:
        """Probabilistically determine if user is active on this date."""
        if current_date < self.registration_date:
            return False

        activity = self.get_activity_multiplier(current_date)
        activity *= seasonal_mult * burst_mult

        # Base daily activity probability (calibrated so avg user ~30% days active)
        base_prob = 0.15
        prob = min(1.0, base_prob * activity)
        return random.random() < prob

    def generate_session_count(self) -> int:
        """Generate number of sessions for an active day."""
        lam = self.avg_sessions_per_active_day * self.base_activity_level
        return max(1, poisson_sample(lam))


# =============================================================================
# SESSION GENERATOR
# =============================================================================
class SessionGenerator:
    """Generate realistic user sessions."""

    def __init__(self, page_cdf: list):
        self.page_cdf = page_cdf

    def generate_session(self, user: UserProfile, session_start: datetime,
                         app_version: str) -> list:
        """Generate a complete session with realistic action sequence."""
        interactions = []
        current_time = session_start

        # Determine session length (log-normal distributed action count)
        num_actions = max(1, int(log_normal_sample(
            math.log(user.avg_actions_per_session), 0.8
        )))

        # First action is usually VIEW
        current_action = "VIEW" if random.random() < 0.85 else weighted_random_choice(
            ACTION_DISTRIBUTION
        )

        for i in range(num_actions):
            # Sample page using Zipf distribution
            page_num = sample_from_cdf(self.page_cdf)
            page_id = f"p{page_num:03d}"

            # Duration based on action type (log-normal)
            mu, sigma = ACTION_DURATION_PARAMS[current_action]
            duration_ms = int(min(300000, max(100, log_normal_sample(mu, sigma))))

            interaction = {
                "user_id": user.user_id,
                "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "action_type": current_action,
                "page_id": page_id,
                "duration_ms": duration_ms,
                "app_version": app_version,
            }
            interactions.append(interaction)

            # Advance time for next action (if not last)
            if i < num_actions - 1:
                gap_mu, gap_sigma = INTRA_SESSION_GAP_PARAMS
                gap_seconds = min(
                    SESSION_TIMEOUT_SECONDS - 1,
                    max(1, int(log_normal_sample(gap_mu, gap_sigma)))
                )
                current_time += timedelta(seconds=gap_seconds)

                # Transition to next action
                transitions = ACTION_TRANSITIONS[current_action]
                current_action = weighted_random_choice(transitions)

        return interactions


# =============================================================================
# DATA GENERATION FUNCTIONS
# =============================================================================
def generate_user_metadata(num_users: int, realistic_lifecycle: bool = False,
                           pareto_alpha: float = 1.5,
                           churn_rate: float = 0.15) -> list:
    """Generate user metadata dataset."""
    print(f"Generating metadata for {num_users:,} users...")

    users = []
    for user_id in range(1, num_users + 1):
        subscription = weighted_random_choice(SUBSCRIPTION_DISTRIBUTION)
        reg_date = REG_START_DATE + timedelta(
            days=random.randint(0, (REG_END_DATE - REG_START_DATE).days)
        )

        user_dict = {
            "user_id": f"user_{user_id:06d}",
            "device_type": weighted_random_choice(DEVICE_DISTRIBUTION),
            "country": weighted_random_choice(COUNTRY_DISTRIBUTION),
            "subscription_type": subscription,
            "registration_date": reg_date.strftime("%Y-%m-%d"),
        }
        users.append(user_dict)

    print(f"   Generated {len(users):,} users")
    return users


def create_user_profiles(users: list, pareto_alpha: float = 1.5,
                         churn_rate: float = 0.15) -> list:
    """Convert user metadata to UserProfile objects with lifecycle params."""
    profiles = []
    for u in users:
        # Pareto-distributed activity level (normalized)
        activity = pareto_sample(pareto_alpha, x_min=0.1)
        activity = min(10.0, activity)  # Cap extreme outliers

        # Random churn week (some users never churn)
        churn_week = None
        if random.random() < churn_rate:
            # Churn mostly happens in first 12 weeks
            churn_week = int(log_normal_sample(2.0, 0.8))  # median ~7 weeks

        # Session parameters vary by activity level
        avg_sessions = 1.5 + activity * 0.5
        avg_actions = 3.0 + activity * 1.0

        profile = UserProfile(
            user_id=u["user_id"],
            subscription_type=u["subscription_type"],
            registration_date=datetime.strptime(u["registration_date"], "%Y-%m-%d"),
            device_type=u["device_type"],
            country=u["country"],
            base_activity_level=activity,
            churn_week=churn_week,
            avg_sessions_per_active_day=avg_sessions,
            avg_actions_per_session=avg_actions,
        )
        profiles.append(profile)

    return profiles


def generate_user_interactions_uniform(num_interactions: int, users: list,
                                       include_skew: bool = False) -> list:
    """
    Generate interactions with UNIFORM distribution (original behavior).
    Preserved for backward compatibility.
    """
    print(f"Generating {num_interactions:,} interactions (uniform mode)...")

    user_ids = [u["user_id"] for u in users]

    if include_skew:
        num_power_users = int(len(user_ids) * 0.20)
        power_users = random.sample(user_ids, num_power_users)
        normal_users = [uid for uid in user_ids if uid not in power_users]
        power_user_interactions = int(num_interactions * 0.80)
        normal_user_interactions = num_interactions - power_user_interactions
        user_pool = (
            power_users * (power_user_interactions // num_power_users) +
            normal_users * (normal_user_interactions // len(normal_users))
        )
        random.shuffle(user_pool)
        print(f"   Data skew: {num_power_users:,} power users generate 80% of interactions")
    else:
        user_pool = user_ids * (num_interactions // len(user_ids) + 1)
        random.shuffle(user_pool)

    interactions = []
    date_range = (ACT_END_DATE - ACT_START_DATE).days

    for i in range(num_interactions):
        timestamp = ACT_START_DATE + timedelta(
            days=random.randint(0, date_range),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        duration_category = random.random()
        if duration_category < 0.70:
            duration_ms = random.randint(100, 10000)
        elif duration_category < 0.95:
            duration_ms = random.randint(10000, 60000)
        else:
            duration_ms = random.randint(60000, 300000)

        interaction = {
            "user_id": user_pool[i % len(user_pool)],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "action_type": weighted_random_choice(ACTION_DISTRIBUTION),
            "page_id": f"p{random.randint(1, 100):03d}",
            "duration_ms": duration_ms,
            "app_version": weighted_random_choice(VERSION_DISTRIBUTION)
        }
        interactions.append(interaction)

    interactions.sort(key=lambda x: x["timestamp"])
    print(f"   Generated {len(interactions):,} interactions")
    return interactions


def generate_user_interactions_realistic(
    target_interactions: int,
    users: list,
    pareto_alpha: float = 1.5,
    churn_rate: float = 0.15,
    include_burst_events: bool = True,
    burst_events: list = [],
) -> list:
    """
    Generate interactions with REALISTIC patterns:
    - Time-of-day and day-of-week variation
    - Seasonal patterns
    - Session-based activity
    - User lifecycle modeling
    - Burst events (optional)
    """
    print(f"Generating ~{target_interactions:,} interactions (realistic mode)...")

    # Create user profiles with lifecycle parameters
    profiles = create_user_profiles(users, pareto_alpha, churn_rate)
    print(f"   Created {len(profiles):,} user profiles with lifecycle modeling")

    # Precompute Zipf CDF for page sampling
    page_cdf = build_zipf_cdf(NUM_PAGES, PAGE_ZIPF_EXPONENT)
    session_gen = SessionGenerator(page_cdf)

    # Use default burst events if not specified
    if burst_events is None:
        burst_events = DEFAULT_BURST_EVENTS if include_burst_events else []

    interactions = []
    total_days = (ACT_END_DATE - ACT_START_DATE).days

    # Calculate interactions per day target
    avg_per_day = target_interactions / total_days

    # Generate day by day
    current_date = ACT_START_DATE
    day_count = 0

    while current_date < ACT_END_DATE:
        day_count += 1
        if day_count % 30 == 0:
            print(f"   Processing day {day_count}/{total_days}...")

        # Calculate multipliers for this day
        seasonal_mult = get_seasonal_multiplier(current_date)
        burst_mult = get_burst_multiplier(current_date, burst_events)

        # Process each user
        for profile in profiles:
            if not profile.should_be_active(current_date, seasonal_mult, burst_mult):
                continue

            # Generate sessions for this user on this day
            num_sessions = profile.generate_session_count()
            app_version = weighted_random_choice(VERSION_DISTRIBUTION)

            for _ in range(num_sessions):
                # Session start time (hour-weighted)
                hour = sample_hour_weighted(profile.subscription_type)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                session_start = current_date.replace(
                    hour=hour, minute=minute, second=second
                )

                # Generate session interactions
                session_interactions = session_gen.generate_session(
                    profile, session_start, app_version
                )
                interactions.extend(session_interactions)

        current_date += timedelta(days=1)

    actual_count = len(interactions)
    print(f"   Generated {actual_count:,} interactions")

    # Adjust to target if significantly off - preserve session integrity
    if actual_count > target_interactions * 1.2:
        # Group interactions by (user_id, date, hour) to approximate sessions
        from collections import defaultdict
        sessions = defaultdict(list)
        for interaction in interactions:
            ts = interaction["timestamp"]
            # Session key: user + date + hour
            session_key = (interaction["user_id"], ts[:13])  # "YYYY-MM-DD HH"
            sessions[session_key].append(interaction)

        # Randomly select sessions until we hit target
        session_keys = list(sessions.keys())
        random.shuffle(session_keys)

        sampled = []
        for key in session_keys:
            if len(sampled) >= target_interactions:
                break
            sampled.extend(sessions[key])

        interactions = sampled
        print(f"   Downsampled to {len(interactions):,} interactions (preserving sessions)")
    elif actual_count < target_interactions * 0.8:
        print(f"   Warning: Generated fewer interactions than target. "
              f"Consider adjusting --pareto-alpha or --churn-rate")

    # Sort by timestamp
    interactions.sort(key=lambda x: x["timestamp"])

    return interactions


def write_csv(data: list, filename: str, fieldnames: list) -> None:
    """Write data to CSV file."""
    filepath = Path("data/raw") / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing to {filepath}...")
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    size_mb = filepath.stat().st_size / (1024 * 1024)
    print(f"   Written {len(data):,} rows ({size_mb:.2f} MB)")


def print_distribution_stats(interactions: list) -> None:
    """Print statistics about generated data distribution."""
    from collections import Counter

    print("\n--- Distribution Statistics ---")

    # Hour distribution
    hours = Counter()
    for i in interactions:
        hour = int(i["timestamp"].split()[1].split(":")[0])
        hours[hour] += 1
    print(f"Hour distribution (sample): 6am={hours[6]}, 12pm={hours[12]}, 6pm={hours[18]}, 12am={hours[0]}")

    # Day of week distribution
    days = Counter()
    for i in interactions:
        dt = datetime.strptime(i["timestamp"], "%Y-%m-%d %H:%M:%S")
        days[dt.strftime("%a")] += 1
    print(f"Day distribution: {dict(days)}")

    # Action distribution
    actions = Counter(i["action_type"] for i in interactions)
    total = sum(actions.values())
    print(f"Action distribution: {{{', '.join(f'{k}: {v/total:.1%}' for k, v in actions.items())}}}")

    # User activity distribution
    user_counts = Counter(i["user_id"] for i in interactions)
    counts = sorted(user_counts.values(), reverse=True)
    top_10_pct = sum(counts[:len(counts)//10]) / sum(counts) * 100 if counts else 0
    print(f"Top 10% users generate {top_10_pct:.1f}% of interactions")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample data for GoodNote Analytics"
    )

    # Preset sizes
    parser.add_argument("--small", action="store_true",
                        help="Small dataset (1K users, 10K interactions)")
    parser.add_argument("--medium", action="store_true",
                        help="Medium dataset (10K users, 100K interactions)")
    parser.add_argument("--large", action="store_true",
                        help="Large dataset (100K users, 1M interactions)")

    # Custom sizes
    parser.add_argument("--num-users", type=int,
                        help="Number of users to generate")
    parser.add_argument("--num-interactions", type=int,
                        help="Number of interactions to generate")

    # Realism flags
    parser.add_argument("--realistic-temporal", action="store_true",
                        help="Enable time-of-day, day-of-week, seasonal patterns")
    parser.add_argument("--realistic-sessions", action="store_true",
                        help="Generate session-based activity")
    parser.add_argument("--realistic-lifecycle", action="store_true",
                        help="Model user onboarding, settling, churn patterns")
    parser.add_argument("--realistic-all", action="store_true",
                        help="Enable all realistic data patterns")
    parser.add_argument("--burst-events", action="store_true",
                        help="Include simulated marketing campaigns/viral moments")

    # Distribution tuning
    parser.add_argument("--pareto-alpha", type=float, default=1.5,
                        help="Pareto distribution shape parameter (default: 1.5)")
    parser.add_argument("--churn-rate", type=float, default=0.15,
                        help="Base weekly churn probability (default: 0.15)")

    # Backward compatibility
    parser.add_argument("--uniform", action="store_true",
                        help="Use original uniform distribution")
    parser.add_argument("--is-skew", action="store_true",
                        help="Enable 80/20 data skew (uniform mode only)")

    # Other options
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--stats", action="store_true",
                        help="Print distribution statistics after generation")

    args = parser.parse_args()

    # Set random seed
    random.seed(args.seed)
    print(f"Random seed: {args.seed}")

    # Determine sizes
    if args.small:
        num_users = 1000
        num_interactions = 10000
        print("Small dataset selected")
    elif args.medium:
        num_users = 10000
        num_interactions = 100000
        print("Medium dataset selected")
    elif args.large:
        num_users = 100000
        num_interactions = 1000000
        print("Large dataset selected")
    elif args.num_users and args.num_interactions:
        num_users = args.num_users
        num_interactions = args.num_interactions
        print(f"Custom dataset: {num_users:,} users, {num_interactions:,} interactions")
    else:
        num_users = 1000
        num_interactions = 10000
        print("Default (small) dataset selected")

    # Determine generation mode
    use_realistic = (
        args.realistic_all or
        args.realistic_temporal or
        args.realistic_sessions or
        args.realistic_lifecycle or
        args.burst_events
    ) and not args.uniform

    mode_name = "REALISTIC" if use_realistic else "UNIFORM"

    print(f"\n{'='*60}")
    print(f"Generating GoodNote Analytics Sample Data ({mode_name} mode)")
    print(f"{'='*60}\n")

    # Generate metadata
    users = generate_user_metadata(num_users)
    write_csv(
        users,
        "user_metadata.csv",
        ["user_id", "device_type", "country", "subscription_type", "registration_date"]
    )

    # Generate interactions
    if use_realistic:
        interactions = generate_user_interactions_realistic(
            target_interactions=num_interactions,
            users=users,
            pareto_alpha=args.pareto_alpha,
            churn_rate=args.churn_rate,
            include_burst_events=args.burst_events or args.realistic_all,
        )
    else:
        interactions = generate_user_interactions_uniform(
            num_interactions,
            users,
            include_skew=args.is_skew
        )

    write_csv(
        interactions,
        "user_interactions.csv",
        ["user_id", "timestamp", "action_type", "page_id", "duration_ms", "app_version"]
    )

    if args.stats or use_realistic:
        print_distribution_stats(interactions)

    print(f"\n{'='*60}")
    print("Sample data generation complete!")
    print(f"{'='*60}\n")

    print("Files created:")
    print("   data/raw/user_interactions.csv")
    print("   data/raw/user_metadata.csv")
    print("\nReady to run Spark jobs!")
    print("   Run: ./src/jobs/run_all_jobs.sh\n")


if __name__ == "__main__":
    main()
