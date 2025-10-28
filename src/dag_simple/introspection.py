"""
Introspection and visualization utilities for DAG nodes.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any

from dag_simple.exceptions import CycleDetectedError

if TYPE_CHECKING:
    from dag_simple.node import Node


def topological_sort(node: Node[Any]) -> list[str]:
    """
    Return a topological sort of nodes (leaf nodes first).

    Uses Kahn's algorithm for topological sorting.

    Args:
        node: The root node to start from

    Returns:
        List of node names in topological order
    """
    # Build adjacency list and in-degree count
    graph: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = defaultdict(int)
    all_nodes: set[str] = set()

    def build_graph(n: Node[Any]) -> None:
        if n.name in all_nodes:
            return
        all_nodes.add(n.name)

        for dep in n.deps:
            graph[dep.name].append(n.name)
            in_degree[n.name] += 1
            build_graph(dep)

        # Ensure node is in in_degree dict even if it has no dependencies
        if n.name not in in_degree:
            in_degree[n.name] = 0

    build_graph(node)

    # Kahn's algorithm
    queue = deque([n for n in all_nodes if in_degree[n] == 0])
    result: list[str] = []

    while queue:
        current = queue.popleft()
        result.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(all_nodes):
        raise CycleDetectedError("Cycle detected during topological sort")

    return result


def graph_dict(node: Node[Any]) -> dict[str, list[str]]:
    """
    Return dependency graph as {node: [dependencies]}.

    Args:
        node: The root node to start from

    Returns:
        Dictionary mapping node names to their dependency names
    """
    graph: dict[str, list[str]] = {}

    def collect(n: Node[Any]) -> None:
        if n.name in graph:
            return
        graph[n.name] = [d.name for d in n.deps]
        for d in n.deps:
            collect(d)

    collect(node)
    return graph


def get_all_dependencies(node: Node[Any]) -> set[str]:
    """
    Get all transitive dependencies of a node.

    Args:
        node: The node to get dependencies for

    Returns:
        Set of all dependency node names
    """
    deps: set[str] = set()

    def collect(n: Node[Any]) -> None:
        for dep in n.deps:
            if dep.name not in deps:
                deps.add(dep.name)
                collect(dep)

    collect(node)
    return deps


def visualize(node: Node[Any], indent: int = 0, visited: set[str] | None = None) -> None:
    """
    Print a tree visualization of the DAG.

    Args:
        node: The root node to visualize
        indent: Current indentation level
        visited: Set of already visited node names
    """
    if visited is None:
        visited = set()

    marker = "✓" if node.name in visited else "○"
    cache_marker = " [cached]" if node.cache_result else ""
    async_marker = " [async]" if node.is_async else ""

    if node.name in visited:
        print("  " * indent + f"{marker} {node.name}{cache_marker}{async_marker} (already shown)")
        return

    visited.add(node.name)
    print("  " * indent + f"{marker} {node.name}{cache_marker}{async_marker}")

    for d in node.deps:
        visualize(d, indent + 1, visited)


def to_mermaid(node: Node[Any]) -> str:
    """
    Generate a Mermaid graph diagram.

    Args:
        node: The root node to generate diagram for

    Returns:
        Mermaid diagram as a string
    """
    lines = ["graph TD"]

    graph = graph_dict(node)
    for node_name, deps in graph.items():
        node_id = node_name.replace(" ", "_")
        lines.append(f"    {node_id}[{node_name}]")
        for dep in deps:
            dep_id = dep.replace(" ", "_")
            lines.append(f"    {dep_id} --> {node_id}")

    return "\n".join(lines)
