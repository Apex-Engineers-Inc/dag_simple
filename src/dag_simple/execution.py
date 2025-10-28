"""
Execution logic for DAG nodes (sync and async).
"""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, TypeVar, cast

from dag_simple.context import ExecutionContext
from dag_simple.exceptions import MissingDependencyError
from dag_simple.validation import validate_input_types, validate_output_type

if TYPE_CHECKING:
    from dag_simple.node import Node

R = TypeVar("R")


def has_async_nodes(node: Node[Any]) -> bool:
    """
    Check if this node or any of its dependencies are async.

    Args:
        node: The node to check

    Returns:
        True if the node or any dependency is async
    """
    visited: set[str] = set()

    def check(n: Node[Any]) -> bool:
        if n.name in visited:
            return False
        visited.add(n.name)

        if n.is_async:
            return True

        return any(check(dep) for dep in n.deps)

    return check(node)


def run_sync(
    node: Node[R],
    *,
    enable_cache: bool = True,
    _context: ExecutionContext | None = None,
    **inputs: Any,
) -> R:
    """
    Execute dependencies recursively and run this node (synchronous).

    For async nodes, this will raise an error. Use run_async() instead.

    Args:
        node: The node to execute
        enable_cache: Enable result caching across the execution
        _context: Internal execution context (for recursive calls)
        **inputs: Input values for the DAG

    Returns:
        The result of executing this node

    Raises:
        RuntimeError: If this is an async node
    """
    # Check if any node in the DAG is async
    if has_async_nodes(node):
        raise RuntimeError(
            f"Node '{node.name}' or its dependencies contain async functions. "
            "Use run_async() instead."
        )

    # Initialize context for top-level call
    if _context is None:
        _context = ExecutionContext(enable_cache=enable_cache, inputs=inputs)

    # Check cache
    if node.cache_result:
        found, cached_value = _context.get_cached(node.name)
        if found:
            return cached_value  # type: ignore

    # Resolve inputs from dependencies
    resolved: dict[str, Any] = {}

    for dep in node.deps:
        dep_result = run_sync(dep, enable_cache=enable_cache, _context=_context, **inputs)
        resolved[dep.name] = dep_result

    # Merge provided inputs (override dependency outputs if same name)
    resolved.update(_context.inputs)

    # Filter down to only args accepted by this node
    accepted = {k: v for k, v in resolved.items() if k in node.sig.parameters}

    # Check for missing required parameters
    required_params = {
        name
        for name, param in node.sig.parameters.items()
        if param.default is inspect.Parameter.empty
    }
    missing = required_params - accepted.keys()
    if missing:
        raise MissingDependencyError(f"Node '{node.name}' missing required parameters: {missing}")

    # Validate input types
    validate_input_types(node, accepted, node.type_hints)

    # Execute the function
    try:
        result: R = node.fn(**accepted)  # type: ignore[return-value]
    except TypeError as e:
        raise TypeError(
            f"Failed running node '{node.name}' with args {list(accepted.keys())}: {e}"
        ) from e

    # Validate output type
    validate_output_type(node, result, node.type_hints)

    # Cache result if enabled
    if node.cache_result:
        _context.set_cached(node.name, result)

    return result


async def run_async(
    node: Node[R],
    *,
    enable_cache: bool = True,
    _context: ExecutionContext | None = None,
    **inputs: Any,
) -> R:
    """
    Execute dependencies recursively and run this node (asynchronous).

    Works with both sync and async nodes. Async nodes run concurrently
    when possible.

    Args:
        node: The node to execute
        enable_cache: Enable result caching across the execution
        _context: Internal execution context (for recursive calls)
        **inputs: Input values for the DAG

    Returns:
        The result of executing this node
    """
    # Initialize context for top-level call
    if _context is None:
        _context = ExecutionContext(enable_cache=enable_cache, inputs=inputs)

    # Handle caching with proper synchronization
    if node.cache_result:
        found, cached_value = _context.get_cached(node.name)
        if found:
            return cached_value  # type: ignore

        # If not cached, acquire lock and check again (double-checked locking pattern)
        async with _context.get_cache_lock(node.name):
            found, cached_value = _context.get_cached(node.name)
            if found:
                return cached_value  # type: ignore

            # Execute the node within the lock to ensure only one execution
            result = await _execute_node_without_cache(node, _context, inputs)

            # Cache the result
            _context.set_cached(node.name, result)
            return result
    else:
        # No caching, execute normally
        result = await _execute_node_without_cache(node, _context, inputs)
        return result


async def _execute_node_without_cache(
    node: Node[R],
    _context: ExecutionContext,
    inputs: dict[str, Any],
) -> R:
    """Execute a node without checking cache (helper function)."""
    # Resolve inputs from dependencies (run them concurrently if possible)
    resolved: dict[str, Any] = {}

    if node.deps:
        # Run all dependencies concurrently
        dep_tasks = [
            run_async(dep, enable_cache=_context.enable_cache, _context=_context, **inputs)
            for dep in node.deps
        ]
        dep_results = await asyncio.gather(*dep_tasks)

        for dep, result in zip(node.deps, dep_results, strict=True):
            resolved[dep.name] = result

    # Merge provided inputs (override dependency outputs if same name)
    resolved.update(_context.inputs)

    # Filter down to only args accepted by this node
    accepted = {k: v for k, v in resolved.items() if k in node.sig.parameters}

    # Check for missing required parameters
    required_params = {
        name
        for name, param in node.sig.parameters.items()
        if param.default is inspect.Parameter.empty
    }
    missing = required_params - accepted.keys()
    if missing:
        raise MissingDependencyError(f"Node '{node.name}' missing required parameters: {missing}")

    # Validate input types
    validate_input_types(node, accepted, node.type_hints)

    # Execute the function (async or sync)
    try:
        if node.is_async:
            result: R = await node.fn(**accepted)  # type: ignore[return-value]
        else:
            result: R = node.fn(**accepted)  # type: ignore[return-value]
    except TypeError as e:
        raise TypeError(
            f"Failed running node '{node.name}' with args {list(accepted.keys())}: {e}"
        ) from e

    # Validate output type
    validate_output_type(node, result, node.type_hints)

    return cast(R, result)
