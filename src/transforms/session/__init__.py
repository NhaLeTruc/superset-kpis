"""
Session Transforms Package

Provides session analysis functions:
- Session metrics using Spark's native session_window() (duration, action count, bounce detection)
- Session frequency analysis (sessions per user, return rate)
- Bounce rate calculation
"""

from .metrics import calculate_bounce_rate, calculate_session_frequency, calculate_session_metrics


__all__ = [
    "calculate_bounce_rate",
    "calculate_session_frequency",
    "calculate_session_metrics",
]
