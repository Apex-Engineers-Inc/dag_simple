"""
Exception classes for DAG Simple.
"""

from __future__ import annotations


class DAGError(Exception):
    """Base exception for DAG-related errors."""

    pass


class CycleDetectedError(DAGError):
    """Raised when a cycle is detected in the DAG."""

    pass


class ValidationError(DAGError):
    """Raised when input/output validation fails."""

    pass


class MissingDependencyError(DAGError):
    """Raised when a required dependency is not satisfied."""

    pass
