"""
Data pipeline example demonstrating ETL with dag-simple.
"""

from typing import Any

from dag_simple import node


def main() -> None:
    """Run data pipeline example."""

    print("Data Pipeline Example: ETL Process")
    print("=" * 50)

    # Extract
    @node(cache_result=True, validate_types=True)
    def extract(source: str) -> list[dict[str, Any]]:
        """Extract data from source."""
        print(f"Extracting data from {source}...")
        return [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 150},
            {"id": 4, "name": "David", "value": 50},
        ]

    # Transform
    @node(deps=[extract], validate_types=True)
    def filter_data(extract: list[dict[str, Any]], min_value: int) -> list[dict[str, Any]]:
        """Filter data by minimum value."""
        print(f"Filtering data (min_value={min_value})...")
        return [item for item in extract if item["value"] >= min_value]

    @node(deps=[filter_data], validate_types=True)
    def calculate_total(filter_data: list[dict[str, Any]]) -> int:
        """Calculate total value."""
        print("Calculating total...")
        return sum(item["value"] for item in filter_data)

    @node(deps=[filter_data], validate_types=True)
    def calculate_average(filter_data: list[dict[str, Any]]) -> float:
        """Calculate average value."""
        print("Calculating average...")
        if not filter_data:
            return 0.0
        return sum(item["value"] for item in filter_data) / len(filter_data)

    # Load
    @node(deps=[calculate_total, calculate_average], validate_types=True)
    def load(calculate_total: int, calculate_average: float) -> str:
        """Load results."""
        print("Loading results...")
        return f"Total: {calculate_total}, Average: {calculate_average:.2f}"

    # Visualize the pipeline
    print("\nPipeline Structure:")
    load.visualize()
    print()

    # Execute the pipeline
    print("\nExecuting pipeline...")
    result = load.run(source="database", min_value=100)
    print(f"\nResult: {result}")

    # Show topological order
    print("\nExecution Order:")
    print(" -> ".join(load.topological_sort()))


if __name__ == "__main__":
    main()
