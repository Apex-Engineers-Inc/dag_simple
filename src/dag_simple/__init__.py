"""
DAG Simple - A type-safe DAG library with runtime validation and caching.

This module provides a simple yet powerful way to build and execute
Directed Acyclic Graphs (DAGs) with type safety, runtime validation,
cycle detection, and result caching. Supports both sync and async nodes.
"""

from __future__ import annotations

from dag_simple.context import ExecutionContext
from dag_simple.dag import DAG
from dag_simple.exceptions import (
    CycleDetectedError,
    DAGError,
    MissingDependencyError,
    NodeExecutionError,
    ValidationError,
)
from dag_simple.execution import (
    run_async,
    run_async_in_process,
    run_sync,
    run_sync_in_process,
)
from dag_simple.node import Node, input_node, node

__version__ = "0.1.0"
__all__ = [
    "Node",
    "node",
    "input_node",
    "DAG",
    "DAGError",
    "CycleDetectedError",
    "ValidationError",
    "MissingDependencyError",
    "NodeExecutionError",
    "ExecutionContext",
    "run_sync",
    "run_async",
    "run_sync_in_process",
    "run_async_in_process",
]
