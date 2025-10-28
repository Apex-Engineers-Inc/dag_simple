"""
Validation utilities for DAG nodes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_origin

from dag_simple.exceptions import CycleDetectedError, ValidationError

if TYPE_CHECKING:
    from dag_simple.node import Node


def validate_no_cycles(node: Node[Any]) -> None:
    """
    Detect cycles in the DAG starting from this node.

    Args:
        node: The node to check for cycles

    Raises:
        CycleDetectedError: If a cycle is detected
    """
    visited: set[str] = set()
    path: set[str] = set()

    def dfs(n: Node[Any]) -> None:
        if n.name in path:
            cycle = " -> ".join(list(path) + [n.name])
            raise CycleDetectedError(f"Cycle detected: {cycle}")

        if n.name in visited:
            return

        visited.add(n.name)
        path.add(n.name)

        for dep in n.deps:
            dfs(dep)

        path.remove(n.name)

    dfs(node)


def validate_input_types(
    node: Node[Any], kwargs: dict[str, Any], type_hints: dict[str, Any]
) -> None:
    """
    Validate input types match function signature.

    Args:
        node: The node being validated
        kwargs: The input arguments
        type_hints: Type hints for the function

    Raises:
        ValidationError: If type validation fails
    """
    if not node.validate_types:
        return

    for param_name, param_value in kwargs.items():
        if param_name not in type_hints:
            continue

        expected_type = type_hints[param_name]

        # Skip validation for complex generic types
        if get_origin(expected_type) is not None:
            continue

        if not isinstance(param_value, expected_type):
            raise ValidationError(
                f"Node '{node.name}': parameter '{param_name}' expected type "
                f"{expected_type.__name__}, got {type(param_value).__name__}"
            )


def validate_output_type(node: Node[Any], result: Any, type_hints: dict[str, Any]) -> None:
    """
    Validate output type matches function return annotation.

    Args:
        node: The node being validated
        result: The result value
        type_hints: Type hints for the function

    Raises:
        ValidationError: If type validation fails
    """
    if not node.validate_types:
        return

    if "return" not in type_hints:
        return

    expected_type = type_hints["return"]

    # Skip validation for complex generic types (including Coroutine)
    if get_origin(expected_type) is not None:
        return

    if not isinstance(result, expected_type):
        raise ValidationError(
            f"Node '{node.name}': return type expected {expected_type.__name__}, "
            f"got {type(result).__name__}"
        )
