# Session timeout for grouping user interactions into sessions.
# Value: 600 seconds (10 minutes)
# Rationale:
#   - Assume a user session ends after 10 minutes of inactivity
#   - Balances between being too short (splitting legitimate browsing sessions)
#     and too long (incorrectly merging distinct user visits)
#   - Used by Spark's session_window() to group consecutive interactions
#   - A new session starts when user is inactive for more than 10 minutes
SESSION_TIMEOUT_SECONDS = 600

# Percentiles for performance latency analysis.
# Values: P50 (median), P95, P99
# Rationale:
#   - P50: Represents the typical user experience (median latency)
#   - P95: Industry standard for SLA monitoring; covers 95% of user requests
#   - P99: Captures tail latency for worst-case performance scenarios
#   - This trio is recommended by Google's SRE book and is standard practice
#     in Site Reliability Engineering for monitoring service performance
DEFAULT_PERCENTILES = [0.50, 0.95, 0.99]

# Threshold for identifying "hot keys" in distributed data processing.
# Value: 0.99 (99th percentile = top 1% of keys by frequency)
# Rationale:
#   - Hot keys are highly skewed values that cause data imbalance in Spark
#   - Power users with millions of events would bottleneck a single partition
#   - Keys above the 99th percentile trigger salting (distributing across 10 partitions)
#   - The 1% threshold balances skew mitigation against additional processing overhead
#   - Without this, joins on user_id would cause severe performance degradation
HOT_KEY_THRESHOLD_PERCENTILE = 0.99

# Base threshold for statistical anomaly detection using Z-scores.
# Value: 3.0 (3 standard deviations from the mean)
# Rationale:
#   - Based on the "3-sigma rule" from statistics
#   - In a normal distribution, values beyond 3 std devs occur with ~0.27% probability
#   - Industry-standard threshold used in statistical process control and control charts
#   - Initial filtering removes values beyond this threshold to compute a refined baseline,
#     then final anomalies are detected using the cleaned baseline statistics
Z_SCORE_ANOMALY_THRESHOLD = 3.0

# Anomaly severity thresholds (z-score values)
# These classify detected anomalies into priority tiers based on statistical rarity.
# The absolute Z-score magnitude determines severity:
#
#   CRITICAL (≥4.0): ~0.003% probability, ~1 in 31,500 events
#     Indicates extreme deviation requiring immediate attention
#     Flags critical performance regressions or system failures
#
#   HIGH (≥3.5): ~0.023% probability, ~1 in 4,300 events
#     Significant deviation warranting investigation
#     Separates severe anomalies from merely unusual ones
#
#   MEDIUM (≥3.0): ~0.135% probability, ~1 in 740 events
#     Minimum threshold for anomaly classification
#     Matches the base Z_SCORE_ANOMALY_THRESHOLD
#
#   LOW (<3.0): Below anomaly threshold, considered normal variation
ANOMALY_SEVERITY_CRITICAL = 4.0
ANOMALY_SEVERITY_HIGH = 3.5
ANOMALY_SEVERITY_MEDIUM = 3.0

# Database table names
TABLE_DAILY_ACTIVE_USERS = "daily_active_users"
TABLE_MONTHLY_ACTIVE_USERS = "monthly_active_users"
TABLE_USER_STICKINESS = "user_stickiness"
TABLE_POWER_USERS = "power_users"
TABLE_COHORT_RETENTION = "cohort_retention"
TABLE_PERFORMANCE_BY_VERSION = "performance_by_version"
TABLE_DEVICE_PERFORMANCE = "device_performance"
TABLE_DEVICE_CORRELATION = "device_correlation"
TABLE_PERFORMANCE_ANOMALIES = "performance_anomalies"
TABLE_SESSION_METRICS = "session_metrics"
TABLE_SESSION_FREQUENCY = "session_frequency"
TABLE_BOUNCE_RATES = "bounce_rates"
