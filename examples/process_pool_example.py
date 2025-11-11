"""Example showing how to execute DAG runs inside worker processes."""

from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor

from dag_simple import node, run_async_in_process, run_sync_in_process


@node()
def make_numbers(seed: int) -> list[int]:
    """Generate a small range of numbers from a seed."""
    return [seed + offset for offset in range(5)]


@node(deps=[make_numbers])
def total_energy(make_numbers: list[int]) -> int:
    """Pretend CPU-bound work that squares numbers and sums them."""
    total = 0
    for value in make_numbers:
        for _ in range(10_000):  # small loop to simulate work without being too slow
            total += value * value
    return total


@node()
async def fetch_multiplier() -> int:
    """Async dependency that might contact a remote service."""
    await asyncio.sleep(0.1)
    return 2


@node(deps=[total_energy, fetch_multiplier])
def scaled_total(total_energy: int, fetch_multiplier: int) -> int:
    """Combine sync and async dependencies."""
    return total_energy * fetch_multiplier


def main() -> None:
    """Show different ways to reuse worker processes for DAG execution."""
    print("Single run in a dedicated worker process:")
    print(run_sync_in_process(total_energy, seed=10))

    print("\nReusing a shared process pool for multiple runs:")
    with ProcessPoolExecutor(max_workers=2) as pool:
        parallel_runs = [
            run_sync_in_process(total_energy, executor=pool, seed=seed) for seed in range(3)
        ]
        print(parallel_runs)

        print("\nMixing sync and async DAGs in the same pool:")
        print(run_async_in_process(scaled_total, executor=pool, seed=5))


if __name__ == "__main__":
    main()
