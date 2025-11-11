"""
Example demonstrating enhanced error messages in dag_simple.

This shows how dag_simple provides comprehensive error information including:
- The node that failed
- The execution path taken to reach the failed node
- The inputs passed to the failed node
- The original exception
"""

from dag_simple import node


# Example 1: Simple node failure
@node()
def divide(x: int, y: int) -> float:
    """Divide x by y."""
    return x / y


# Example 2: Multi-step pipeline with failure
@node()
def load_data(source: str) -> dict[str, int]:
    """Load data from source."""
    return {"count": 100, "threshold": 50}


@node(deps=[load_data])
def process_data(load_data: dict[str, int], multiplier: int) -> int:
    """Process the loaded data."""
    return load_data["count"] * multiplier


@node(deps=[process_data])
def validate_result(process_data: int) -> int:
    """Validate the processed result."""
    if process_data > 1000:
        raise ValueError(f"Result too large: {process_data}")
    return process_data


@node(deps=[validate_result])
def save_result(validate_result: int) -> str:
    """Save the validated result."""
    return f"Saved {validate_result}"


def main() -> None:
    """Run examples showing error messages."""
    print("=" * 80)
    print("Example 1: Simple Division by Zero Error")
    print("=" * 80)
    try:
        divide.run(x=10, y=0)
    except Exception as e:
        print(f"\n{e}\n")

    print("=" * 80)
    print("Example 2: Complex Pipeline Error")
    print("=" * 80)
    try:
        # This will fail at validate_result because 100 * 20 = 2000 > 1000
        save_result.run(source="database", multiplier=20)
    except Exception as e:
        print(f"\n{e}\n")
        print("\nYou can access the original exception:")
        print(f"  Original: {e.original_exception}")  # type: ignore
        print("\nYou can access the execution path:")
        print(f"  Path: {' -> '.join(e.execution_path)}")  # type: ignore
        print("\nYou can access the node inputs:")
        print(f"  Inputs: {e.node_inputs}")  # type: ignore


if __name__ == "__main__":
    main()
