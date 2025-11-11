"""
Exception classes for DAG Simple.
"""

from __future__ import annotations

import shutil
from typing import Any


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


class NodeExecutionError(DAGError):
    """Raised when a node fails during execution with full context.

    This exception provides complete diagnostic information including:
    - The node that failed
    - The execution path taken to reach the failed node
    - The inputs passed to the failed node
    - The original exception that was raised
    """

    def __init__(
        self,
        node_name: str,
        execution_path: list[str],
        node_inputs: dict[str, Any],
        original_exception: Exception,
    ) -> None:
        """Initialize a NodeExecutionError.

        Args:
            node_name: The name of the node that failed
            execution_path: The ordered list of node names executed to reach this node
            node_inputs: The inputs that were passed to the failed node
            original_exception: The original exception that was raised
        """
        self.node_name = node_name
        self.execution_path = execution_path
        self.node_inputs = node_inputs
        self.original_exception = original_exception

        # Get terminal width for proper formatting
        try:
            terminal_width = shutil.get_terminal_size().columns
        except (AttributeError, ValueError):
            terminal_width = 80  # Fallback to 80 if terminal size unavailable

        # Format the error message
        path_str = " -> ".join(execution_path)

        # Format inputs in a readable way
        inputs_str = self._format_inputs(node_inputs)

        separator = "=" * terminal_width

        message = (
            f"\n{separator}\n"
            f"Node Execution Failed: '{node_name}'\n"
            f"{separator}\n"
            f"\nExecution Path:\n  {path_str}\n"
            f"\nInputs to '{node_name}':\n{inputs_str}\n"
            f"\nOriginal Error:\n  {type(original_exception).__name__}: {original_exception}\n"
            f"{separator}"
        )

        super().__init__(message)

    def _format_inputs(self, inputs: dict[str, Any]) -> str:
        """Format inputs dictionary for display."""
        if not inputs:
            return "  (no inputs)"

        lines: list[str] = []
        for key, value in inputs.items():
            # Truncate long values
            value_str = repr(value)
            if len(value_str) > 100:
                value_str = value_str[:97] + "..."
            lines.append(f"  {key}: {value_str}")

        return "\n".join(lines)

    def __reduce__(self) -> tuple[type, tuple[str, list[str], dict[str, Any], Exception]]:
        """Support pickling for multiprocessing."""
        return (
            self.__class__,
            (self.node_name, self.execution_path, self.node_inputs, self.original_exception),
        )
