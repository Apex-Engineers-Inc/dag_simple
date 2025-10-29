from __future__ import annotations

import asyncio

from dag_simple.node import Node


def _base_value() -> int:
    return 2


def _double(base_value: int) -> int:
    return base_value * 2


async def _add_async(base_value: int) -> int:
    await asyncio.sleep(0)
    return base_value + 3


def _explode(base_value: int) -> None:
    raise ValueError("boom")


async def _explode_async(base_value: int) -> None:
    await asyncio.sleep(0)
    raise RuntimeError("async boom")


base_value = Node(_base_value, name="base_value")
double = Node(_double, name="double", deps=[base_value])
add_async = Node(_add_async, name="add_async", deps=[base_value])
explode = Node(_explode, name="explode", deps=[base_value])
explode_async = Node(_explode_async, name="explode_async", deps=[base_value])
