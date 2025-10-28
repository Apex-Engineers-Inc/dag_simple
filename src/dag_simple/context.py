"""
Execution context for DAG Simple.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExecutionContext:
    """Context for DAG execution with caching support."""

    cache: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    inputs: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    enable_cache: bool = True
    _cache_locks: dict[str, asyncio.Lock] = field(default_factory=lambda: dict[str, asyncio.Lock]())

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
