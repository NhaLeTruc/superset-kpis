"""
Join Transforms Package

Provides join optimization functions:
- Hot key detection for identifying data skew
- Salting strategies for skew mitigation
- Optimized join execution with automatic optimization
"""
from .optimization import identify_hot_keys, apply_salting, explode_for_salting
from .execution import optimized_join

__all__ = [
    "identify_hot_keys",
    "apply_salting",
    "explode_for_salting",
    "optimized_join",
]
