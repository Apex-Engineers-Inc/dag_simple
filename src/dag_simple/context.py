"""
Execution context for DAG Simple.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


def _default_dict() -> dict[str, Any]:
    return {}


def _default_locks() -> dict[str, asyncio.Lock]:
    return {}


def _default_list() -> list[str]:
    return []


@dataclass
class ExecutionContext:
    """Context for DAG execution with caching support."""

    cache: dict[str, Any] = field(default_factory=_default_dict)
    inputs: dict[str, Any] = field(default_factory=_default_dict)
    enable_cache: bool = True
    _cache_locks: dict[str, asyncio.Lock] = field(default_factory=_default_locks)
    execution_path: list[str] = field(default_factory=_default_list)

    def get_cached(self, key: str) -> tuple[bool, Any]:
        """Return (found, value) tuple."""
        if self.enable_cache and key in self.cache:
            return True, self.cache[key]
        return False, None

    def set_cached(self, key: str, value: Any) -> None:
        """Cache a value."""
        if self.enable_cache:
            self.cache[key] = value

    def get_cache_lock(self, key: str) -> asyncio.Lock:
        """Get or create a lock for a cache key."""
        if key not in self._cache_locks:
            self._cache_locks[key] = asyncio.Lock()
        return self._cache_locks[key]

    def add_to_path(self, node_name: str) -> None:
        """Add a node to the execution path."""
        self.execution_path.append(node_name)
