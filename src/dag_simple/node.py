"""
Core Node class and decorators for DAG Simple.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine
from typing import Any, Generic, ParamSpec, TypeVar, get_type_hints

from dag_simple.context import ExecutionContext
from dag_simple.execution import run_async, run_sync
from dag_simple.introspection import (
    get_all_dependencies,
    graph_dict,
    to_mermaid,
    topological_sort,
    visualize,
)
from dag_simple.validation import validate_no_cycles

# Type variables
T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")


class Node(Generic[R]):
    """
    Type-safe DAG node with runtime validation.

    Supports both synchronous and asynchronous functions.
    Generic parameter R represents the return type of this node.
    """

    def __init__(
        self,
        fn: Callable[..., R] | Callable[..., Coroutine[Any, Any, R]],
        name: str | None = None,
        deps: list[Node[Any]] | None = None,
        validate_types: bool = True,
        cache_result: bool = False,
    ):
        """
        Initialize a DAG node.

        Args:
            fn: The function (sync or async) this node executes
            name: Optional name (defaults to function name)
            deps: List of dependency nodes
            validate_types: Whether to perform runtime type validation
            cache_result: Whether to cache the result of this node
        """
        self.fn = fn
        self.name = name or fn.__name__
        self.deps: list[Node[Any]] = deps or []
        self.validate_types = validate_types
        self.cache_result = cache_result
        self.is_async = inspect.iscoroutinefunction(fn)

        # Introspection
        self.sig = inspect.signature(fn)
        self.type_hints: dict[str, Any] = {}

        # Get type hints if validation is enabled
        if self.validate_types:
            try:
                self.type_hints = get_type_hints(fn)
            except Exception:
                # If we can't get type hints, disable validation for this node
                self.validate_types = False

        # Validate no cycles at construction time
        validate_no_cycles(self)

    def _validate_no_cycles(self) -> None:
        """Detect cycles in the DAG starting from this node."""
        validate_no_cycles(self)

    def run(
        self,
        *,
        enable_cache: bool = True,
        _context: ExecutionContext | None = None,
        **inputs: Any,
    ) -> R:
        """
        Execute dependencies recursively and run this node (synchronous).

        For async nodes, this will raise an error. Use run_async() instead.

        Args:
            enable_cache: Enable result caching across the execution
            _context: Internal execution context (for recursive calls)
            **inputs: Input values for the DAG

        Returns:
            The result of executing this node

        Raises:
            RuntimeError: If this is an async node
        """
        return run_sync(self, enable_cache=enable_cache, _context=_context, **inputs)

    async def run_async(
        self,
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
            enable_cache: Enable result caching across the execution
            _context: Internal execution context (for recursive calls)
            **inputs: Input values for the DAG

        Returns:
            The result of executing this node
        """
        return await run_async(self, enable_cache=enable_cache, _context=_context, **inputs)

    def topological_sort(self) -> list[str]:
        """
        Return a topological sort of nodes (leaf nodes first).

        Uses Kahn's algorithm for topological sorting.
        """
        return topological_sort(self)

    def graph_dict(self) -> dict[str, list[str]]:
        """Return {node: [dependencies]}."""
        return graph_dict(self)

    def get_all_dependencies(self) -> set[str]:
        """Get all transitive dependencies of this node."""
        return get_all_dependencies(self)

    def visualize(self, indent: int = 0, visited: set[str] | None = None) -> None:
        """Print a tree visualization of the DAG."""
        visualize(self, indent, visited)

    def to_mermaid(self) -> str:
        """Generate a Mermaid graph diagram."""
        return to_mermaid(self)

    def __repr__(self) -> str:
        dep_names = [d.name for d in self.deps]
        cache_str = ", cached" if self.cache_result else ""
        async_str = ", async" if self.is_async else ""
        return f"<Node {self.name} deps={dep_names}{cache_str}{async_str}>"


def node(
    *,
    deps: list[Node[Any]] | None = None,
    name: str | None = None,
    validate_types: bool = True,
    cache_result: bool = False,
) -> Callable[[Callable[P, R] | Callable[P, Coroutine[Any, Any, R]]], Node[R]]:
    """
    Decorator: declare a DAG node with optional dependencies.

    Works with both sync and async functions.

    Args:
        deps: List of dependency nodes
        name: Optional name for the node
        validate_types: Enable runtime type validation
        cache_result: Enable result caching

    Example:
        # Sync node
        @node(deps=[node_a, node_b])
        def my_node(a: int, b: int) -> int:
            return a + b

        # Async node
        @node(deps=[node_a])
        async def my_async_node(a: int) -> int:
            await asyncio.sleep(0.1)
            return a * 2
    """

    def wrapper(fn: Callable[P, R] | Callable[P, Coroutine[Any, Any, R]]) -> Node[R]:
        return Node(
            fn,
            name=name,
            deps=deps,
            validate_types=validate_types,
            cache_result=cache_result,
        )

    return wrapper


def input_node(name: str, type_hint: type[R] | None = None) -> Node[R]:
    """
    Create a special input node that passes through a value.

    Args:
        name: Name of the input parameter
        type_hint: Optional type hint for validation

    Example:
        x = input_node("x", int)
        y = input_node("y", int)

        @node(deps=[x, y])
        def add(x: int, y: int) -> int:
            return x + y
    """
    # Create a function with the specific parameter name
    exec(
        f"""
def identity({name}: Any) -> Any:
    return {name}
""",
        globals(),
    )

    identity_func = globals()["identity"]

    # Add type hint if provided
    if type_hint is not None:
        identity_func.__annotations__[name] = type_hint
        identity_func.__annotations__["return"] = type_hint

    return Node(identity_func, name=name, validate_types=type_hint is not None)
