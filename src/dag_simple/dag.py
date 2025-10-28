"""
High-level DAG container and executor.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dag_simple.node import Node


class DAG:
    """
    High-level DAG container and executor.

    Useful for:
    1. Composing multiple independent DAGs
    2. Managing collections of related nodes
    3. Executing multiple targets in one call
    4. Namespace management for large projects
    """

    def __init__(self, name: str = "dag"):
        self.name = name
        self.nodes: dict[str, Node[Any]] = {}

    def add_node(self, node: Node[Any]) -> None:
        """Add a node to this DAG."""
        self.nodes[node.name] = node

    def add_nodes(self, *nodes: Node[Any]) -> None:
        """Add multiple nodes to this DAG."""
        for node in nodes:
            self.add_node(node)

    def get_node(self, name: str) -> Node[Any]:
        """Get a node by name."""
        if name not in self.nodes:
            raise KeyError(f"Node '{name}' not found in DAG")
        return self.nodes[name]

    def execute(self, target: str | Node[Any], **inputs: Any) -> Any:
        """
        Execute a specific target node (synchronous).

        Args:
            target: Node name or Node object to execute
            **inputs: Input values for the DAG

        Returns:
            Result of the target node execution
        """
        if isinstance(target, str):
            target = self.get_node(target)
        return target.run(**inputs)

    async def execute_async(self, target: str | Node[Any], **inputs: Any) -> Any:
        """
        Execute a specific target node (asynchronous).

        Args:
            target: Node name or Node object to execute
            **inputs: Input values for the DAG

        Returns:
            Result of the target node execution
        """
        if isinstance(target, str):
            target = self.get_node(target)
        return await target.run_async(**inputs)

    def execute_all(self, **inputs: Any) -> dict[str, Any]:
        """
        Execute all leaf nodes (nodes with no dependents) in the DAG.

        Args:
            **inputs: Input values for the DAG

        Returns:
            Dictionary mapping node names to their results
        """
        # Find leaf nodes (nodes that no other node depends on)
        has_dependents: set[str] = set()
        for node in self.nodes.values():
            for dep in node.deps:
                has_dependents.add(dep.name)

        leaf_nodes = [node for node in self.nodes.values() if node.name not in has_dependents]

        results: dict[str, Any] = {}
        for node in leaf_nodes:
            results[node.name] = node.run(**inputs)

        return results

    async def execute_all_async(self, **inputs: Any) -> dict[str, Any]:
        """
        Execute all leaf nodes (nodes with no dependents) concurrently.

        Args:
            **inputs: Input values for the DAG

        Returns:
            Dictionary mapping node names to their results
        """
        # Find leaf nodes
        has_dependents: set[str] = set()
        for node in self.nodes.values():
            for dep in node.deps:
                has_dependents.add(dep.name)

        leaf_nodes = [node for node in self.nodes.values() if node.name not in has_dependents]

        # Execute all leaf nodes concurrently
        tasks = [node.run_async(**inputs) for node in leaf_nodes]
        results_list = await asyncio.gather(*tasks)

        return {node.name: result for node, result in zip(leaf_nodes, results_list, strict=True)}

    def visualize_all(self) -> None:
        """Visualize all nodes in the DAG."""
        print(f"DAG: {self.name}")
        print("=" * 50)
        for node in self.nodes.values():
            if not node.deps:  # Only show root nodes
                node.visualize()
                print()

    def get_execution_order(self) -> list[str]:
        """
        Get the topological execution order for all nodes in the DAG.

        Returns:
            List of node names in execution order
        """
        # Use any leaf node to get the full topological sort
        has_dependents: set[str] = set[str]()
        for node in self.nodes.values():
            for dep in node.deps:
                has_dependents.add(dep.name)

        leaf_nodes = [node for node in self.nodes.values() if node.name not in has_dependents]

        if not leaf_nodes:
            return []

        # Get topological sort from first leaf node
        return leaf_nodes[0].topological_sort() if leaf_nodes else []
