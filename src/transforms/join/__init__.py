"""
Join Transforms Package

Provides join optimization functions:
- Hot key detection for identifying data skew
- Salting strategies for skew mitigation
- Optimized join execution with automatic optimization
"""

from .execution import optimized_join
from .optimization import apply_salting, explode_for_salting, identify_hot_keys


__all__ = [
    "apply_salting",
    "explode_for_salting",
    "identify_hot_keys",
    "optimized_join",
]
