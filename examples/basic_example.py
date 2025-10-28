"""
Basic example demonstrating dag-simple fundamentals.
"""

from dag_simple import node


def main() -> None:
    """Run basic examples."""

    # Example 1: Simple nodes
    print("Example 1: Simple Nodes")
    print("=" * 50)

    @node()
    def add(x: int, y: int) -> int:
        return x + y

    @node()
    def multiply(x: int, y: int) -> int:
        return x * y

    result_add = add.run(x=5, y=3)
    result_mult = multiply.run(x=5, y=3)

    print(f"5 + 3 = {result_add}")
    print(f"5 * 3 = {result_mult}")
    print()

    # Example 2: Nodes with dependencies
    print("Example 2: Nodes with Dependencies")
    print("=" * 50)

    @node()
    def double(x: int) -> int:
        return x * 2

    @node(deps=[double])
    def add_ten(double: int) -> int:
        return double + 10

    result = add_ten.run(x=5)
    print(f"(5 * 2) + 10 = {result}")
    print()

    # Example 3: Visualize DAG
    print("Example 3: Visualize DAG")
    print("=" * 50)
    add_ten.visualize()
    print()

    # Example 4: Complex DAG
    print("Example 4: Complex DAG")
    print("=" * 50)

    @node()
    def base(x: int) -> int:
        return x

    @node(deps=[base])
    def path_a(base: int) -> int:
        return base + 1

    @node(deps=[base])
    def path_b(base: int) -> int:
        return base + 2

    @node(deps=[path_a, path_b])
    def merge(path_a: int, path_b: int) -> int:
        return path_a + path_b

    result = merge.run(x=10)
    print(f"Result: {result}")
    print("\nDAG Structure:")
    merge.visualize()


if __name__ == "__main__":
    main()
