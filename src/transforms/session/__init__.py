"""
Session Transforms Package

Provides session analysis functions:
- Sessionization based on time gaps
- Session metrics (duration, action count, bounce detection)
- Bounce rate calculation
"""
from .sessionization import sessionize_interactions
from .metrics import calculate_session_metrics, calculate_bounce_rate

__all__ = [
    "sessionize_interactions",
    "calculate_session_metrics",
    "calculate_bounce_rate",
]
