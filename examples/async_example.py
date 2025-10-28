"""
Async node examples demonstrating concurrent execution.
"""

import asyncio
import time
from typing import Any

from dag_simple import node


async def main() -> None:
    """Run async examples."""

    print("=" * 60)
    print("Example 1: Mixing Sync and Async Nodes")
    print("=" * 60)

    @node()
    def sync_node(x: int) -> int:
        """Synchronous node."""
        print(f"  Sync node processing: {x}")
        return x * 2

    @node(deps=[sync_node])
    async def async_node(sync_node: int) -> int:
        """Asynchronous node."""
        print(f"  Async node processing: {sync_node}")
        await asyncio.sleep(0.1)  # Simulate async I/O
        return sync_node + 10

    # Must use run_async() when any node is async
    result = await async_node.run_async(x=5)
    print(f"Result: {result}\n")

    print("=" * 60)
    print("Example 2: Concurrent Execution")
    print("=" * 60)

    @node()
    async def fetch_data_1(source: str) -> dict[str, Any]:
        """Fetch data from source 1 (simulated)."""
        print(f"  Fetching from {source} (1)...")
        await asyncio.sleep(0.2)
        print(f"  Finished fetching from {source} (1)")
        return {"data": [1, 2, 3], "source": source}

    @node()
    async def fetch_data_2(source: str) -> dict[str, Any]:
        """Fetch data from source 2 (simulated)."""
        print(f"  Fetching from {source} (2)...")
        await asyncio.sleep(0.2)
        print(f"  Finished fetching from {source} (2)")
        return {"data": [4, 5, 6], "source": source}

    @node(deps=[fetch_data_1, fetch_data_2])
    async def merge_data(
        fetch_data_1: dict[str, Any], fetch_data_2: dict[str, Any]
    ) -> dict[str, Any]:
        """Merge data from both sources."""
        print("  Merging data...")
        return {
            "merged": fetch_data_1["data"] + fetch_data_2["data"],
            "sources": [fetch_data_1["source"], fetch_data_2["source"]],
        }

    # The two fetch operations run concurrently!
    start = time.time()
    result = await merge_data.run_async(source="database")
    elapsed = time.time() - start

    print(f"Result: {result}")
    print(f"Time taken: {elapsed:.2f}s (would be ~0.4s if sequential)\n")

    print("=" * 60)
    print("Example 3: Async Data Pipeline")
    print("=" * 60)

    @node(cache_result=True)
    async def load_users() -> list[dict[str, Any]]:
        """Load users from API."""
        print("  Loading users from API...")
        await asyncio.sleep(0.1)
        return [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

    @node(cache_result=True)
    async def load_orders() -> list[dict[str, Any]]:
        """Load orders from API."""
        print("  Loading orders from API...")
        await asyncio.sleep(0.1)
        return [
            {"user_id": 1, "total": 100},
            {"user_id": 2, "total": 200},
            {"user_id": 1, "total": 50},
        ]

    @node(deps=[load_users, load_orders])
    async def calculate_user_totals(
        load_users: list[dict[str, Any]], load_orders: list[dict[str, Any]]
    ) -> dict[int, dict[str, Any]]:
        """Calculate total orders per user."""
        print("  Calculating user totals...")
        await asyncio.sleep(0.05)

        user_totals: dict[int, dict[str, Any]] = {
            user["id"]: {"name": user["name"], "total": 0} for user in load_users
        }

        for order in load_orders:
            user_id: int = order["user_id"]
            if user_id in user_totals:
                user_totals[user_id]["total"] += order["total"]

        return user_totals

    @node(deps=[calculate_user_totals])
    async def generate_report(calculate_user_totals: dict[int, dict[str, Any]]) -> str:
        """Generate user report."""
        print("  Generating report...")
        await asyncio.sleep(0.05)

        report_lines: list[str] = []
        for _user_id, data in calculate_user_totals.items():
            report_lines.append(f"  {data['name']}: ${data['total']}")

        return "\n".join(report_lines)

    # Visualize the DAG
    print("\nDAG Structure:")
    generate_report.visualize()
    print()

    # Execute
    result = await generate_report.run_async()
    print("\nReport:")
    print(result)
    print()

    print("=" * 60)
    print("Example 4: Error Handling with Async")
    print("=" * 60)

    @node()
    async def might_fail(should_fail: bool) -> str:
        """Node that might fail."""
        await asyncio.sleep(0.1)
        if should_fail:
            raise ValueError("Intentional failure")
        return "Success!"

    try:
        result = await might_fail.run_async(should_fail=False)
        print(f"✓ Success case: {result}")
    except Exception as e:
        print(f"✗ Failed: {e}")

    try:
        result = await might_fail.run_async(should_fail=True)
        print(f"✓ Success case: {result}")
    except ValueError as e:
        print(f"✓ Caught expected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
