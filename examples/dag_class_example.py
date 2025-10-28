"""
DAG class examples showing when and how to use it.

The DAG class is useful for:
1. Composing multiple independent workflows
2. Executing multiple targets at once
3. Organizing nodes into namespaces
4. Managing complex projects with many nodes
"""

import asyncio
from typing import Any

from dag_simple import DAG, node


def example_1_multiple_workflows() -> None:
    """Example 1: Managing multiple independent workflows in one DAG."""
    print("=" * 60)
    print("Example 1: Multiple Independent Workflows")
    print("=" * 60)

    dag = DAG(name="analytics_platform")

    # Workflow 1: User analytics
    @node()
    def load_users() -> list[dict[str, Any]]:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @node(deps=[load_users])
    def analyze_users(load_users: list[dict[str, Any]]) -> dict[str, Any]:
        return {"total_users": len(load_users), "user_names": [u["name"] for u in load_users]}

    # Workflow 2: Sales analytics
    @node()
    def load_sales() -> list[dict[str, Any]]:
        return [{"amount": 100}, {"amount": 200}, {"amount": 150}]

    @node(deps=[load_sales])
    def analyze_sales(load_sales: list[dict[str, Any]]) -> dict[str, Any]:
        total = sum(s["amount"] for s in load_sales)
        return {"total_sales": total, "num_transactions": len(load_sales)}

    # Workflow 3: Combined report
    @node(deps=[analyze_users, analyze_sales])
    def combined_report(analyze_users: dict[str, Any], analyze_sales: dict[str, Any]) -> str:
        return f"Users: {analyze_users['total_users']}, Sales: ${analyze_sales['total_sales']}"

    # Add all nodes to the DAG
    dag.add_nodes(load_users, analyze_users, load_sales, analyze_sales, combined_report)

    # Execute specific workflows
    print("\nExecuting user analytics:")
    user_result = dag.execute("analyze_users")
    print(f"  {user_result}")

    print("\nExecuting sales analytics:")
    sales_result = dag.execute("analyze_sales")
    print(f"  {sales_result}")

    print("\nExecuting combined report:")
    report = dag.execute("combined_report")
    print(f"  {report}")

    # Execute ALL leaf nodes at once
    print("\nExecuting all leaf nodes:")
    all_results = dag.execute_all()
    for name, result in all_results.items():
        print(f"  {name}: {result}")

    print()


async def example_2_async_workflows() -> None:
    """Example 2: Async workflows with concurrent execution."""
    print("=" * 60)
    print("Example 2: Async Workflows with Concurrent Execution")
    print("=" * 60)

    dag = DAG(name="data_pipeline")

    @node()
    async def fetch_api_1() -> dict[str, Any]:
        print("  Fetching from API 1...")
        await asyncio.sleep(0.2)
        return {"data": [1, 2, 3]}

    @node()
    async def fetch_api_2() -> dict[str, Any]:
        print("  Fetching from API 2...")
        await asyncio.sleep(0.2)
        return {"data": [4, 5, 6]}

    @node()
    async def fetch_api_3() -> dict[str, Any]:
        print("  Fetching from API 3...")
        await asyncio.sleep(0.2)
        return {"data": [7, 8, 9]}

    @node(deps=[fetch_api_1])
    async def process_1(fetch_api_1: dict[str, Any]) -> dict[str, Any]:
        print("  Processing data 1...")
        await asyncio.sleep(0.1)
        return {"processed": fetch_api_1["data"]}

    @node(deps=[fetch_api_2])
    async def process_2(fetch_api_2: dict[str, Any]) -> dict[str, Any]:
        print("  Processing data 2...")
        await asyncio.sleep(0.1)
        return {"processed": fetch_api_2["data"]}

    @node(deps=[fetch_api_3])
    async def process_3(fetch_api_3: dict[str, Any]) -> dict[str, Any]:
        print("  Processing data 3...")
        await asyncio.sleep(0.1)
        return {"processed": fetch_api_3["data"]}

    dag.add_nodes(fetch_api_1, fetch_api_2, fetch_api_3, process_1, process_2, process_3)

    # Execute all leaf nodes concurrently!
    print("\nExecuting all workflows concurrently:")
    import time

    start = time.time()
    results = await dag.execute_all_async()
    elapsed = time.time() - start

    print(f"\nCompleted in {elapsed:.2f}s (would be ~0.9s if sequential)")
    for name, result in results.items():
        print(f"  {name}: {result}")

    print()


def example_3_namespace_management() -> None:
    """Example 3: Using DAG for namespace management."""
    print("=" * 60)
    print("Example 3: Namespace Management")
    print("=" * 60)

    # Create separate DAGs for different domains
    user_dag = DAG(name="user_service")
    order_dag = DAG(name="order_service")
    notification_dag = DAG(name="notification_service")

    # User service nodes
    @node(name="get_user")
    def get_user(user_id: int) -> dict[str, Any]:
        return {"id": user_id, "name": "Alice", "email": "alice@example.com"}

    @node(name="validate_user", deps=[get_user])
    def validate_user(get_user: dict[str, Any]) -> bool:
        return "email" in get_user and "@" in get_user["email"]

    user_dag.add_nodes(get_user, validate_user)

    # Order service nodes
    @node(name="get_orders")
    def get_orders(user_id: int) -> list[dict[str, Any]]:
        return [{"id": 1, "total": 100}, {"id": 2, "total": 200}]

    @node(name="calculate_total", deps=[get_orders])
    def calculate_total(get_orders: list[dict[str, Any]]) -> int:
        return sum(order["total"] for order in get_orders)

    order_dag.add_nodes(get_orders, calculate_total)

    # Notification service nodes
    @node(name="send_email")
    def send_email(email: str, message: str) -> str:
        return f"Sent to {email}: {message}"

    notification_dag.add_nodes(send_email)

    # Execute from different namespaces
    print("\nUser Service:")
    user_valid = user_dag.execute("validate_user", user_id=1)
    print(f"  User valid: {user_valid}")

    print("\nOrder Service:")
    total = order_dag.execute("calculate_total", user_id=1)
    print(f"  Total: ${total}")

    print("\nNotification Service:")
    result = notification_dag.execute(
        "send_email", email="alice@example.com", message="Your order total is $300"
    )
    print(f"  {result}")

    print()


def example_4_visualization() -> None:
    """Example 4: Visualizing complex DAGs."""
    print("=" * 60)
    print("Example 4: DAG Visualization")
    print("=" * 60)

    dag = DAG(name="ml_pipeline")

    @node()
    def load_data(path: str) -> list[int]:
        return [1, 2, 3, 4, 5]

    @node(deps=[load_data])
    def split_data(load_data: list[int]) -> dict[str, list[int]]:
        mid = len(load_data) // 2
        return {"train": load_data[:mid], "test": load_data[mid:]}

    @node(deps=[split_data])
    def train_model(split_data: dict[str, list[int]]) -> dict[str, Any]:
        return {"weights": [0.5, 0.3], "accuracy": 0.95}

    @node(deps=[split_data, train_model])
    def evaluate(split_data: dict[str, list[int]], train_model: dict[str, Any]) -> float:
        return train_model["accuracy"]

    @node(deps=[train_model, evaluate])
    def save_model(train_model: dict[str, Any], evaluate: float) -> str:
        if evaluate > 0.9:
            return "Model saved!"
        return "Model not good enough"

    dag.add_nodes(load_data, split_data, train_model, evaluate, save_model)

    print("\nDAG visualization:")
    dag.visualize_all()

    print("Execution order:")
    order = dag.get_execution_order()
    print(f"  {' -> '.join(order)}")

    print()


async def example_5_real_world() -> None:
    """Example 5: Real-world ETL + ML pipeline."""
    print("=" * 60)
    print("Example 5: Real-World ETL + ML Pipeline")
    print("=" * 60)

    dag = DAG(name="production_pipeline")

    # ETL Phase
    @node(cache_result=True)
    async def extract_from_db() -> list[dict[str, Any]]:
        print("  Extracting from database...")
        await asyncio.sleep(0.1)
        return [{"feature1": 1, "feature2": 2, "label": 0}] * 100

    @node(deps=[extract_from_db])
    async def transform_data(extract_from_db: list[dict[str, Any]]) -> dict[str, Any]:
        print("  Transforming data...")
        await asyncio.sleep(0.1)
        return {
            "features": [[d["feature1"], d["feature2"]] for d in extract_from_db],
            "labels": [d["label"] for d in extract_from_db],
        }

    # ML Phase
    @node(deps=[transform_data], cache_result=True)
    async def train_model(transform_data: dict[str, Any]) -> dict[str, Any]:
        print("  Training model...")
        await asyncio.sleep(0.2)
        return {"model_id": "model_v1", "accuracy": 0.95}

    @node(deps=[transform_data, train_model])
    async def validate_model(
        transform_data: dict[str, Any], train_model: dict[str, Any]
    ) -> dict[str, Any]:
        print("  Validating model...")
        await asyncio.sleep(0.1)
        return {"validation_accuracy": 0.93, "model_id": train_model["model_id"]}

    # Deployment Phase
    @node(deps=[train_model, validate_model])
    async def deploy_model(train_model: dict[str, Any], validate_model: dict[str, Any]) -> str:
        print("  Deploying model...")
        await asyncio.sleep(0.1)
        if validate_model["validation_accuracy"] > 0.9:
            return f"✓ Deployed {train_model['model_id']} successfully!"
        return "✗ Model validation failed, not deployed"

    # Monitoring Phase
    @node(deps=[deploy_model])
    async def send_notifications(deploy_model: str) -> str:
        print("  Sending notifications...")
        await asyncio.sleep(0.05)
        return f"Notification sent: {deploy_model}"

    dag.add_nodes(
        extract_from_db,
        transform_data,
        train_model,
        validate_model,
        deploy_model,
        send_notifications,
    )

    print("\nPipeline structure:")
    dag.visualize_all()

    print("Executing pipeline:")
    result = await dag.execute_async("send_notifications")
    print(f"\n{result}")

    print()


def main() -> None:
    """Run all examples."""
    example_1_multiple_workflows()
    asyncio.run(example_2_async_workflows())
    example_3_namespace_management()
    example_4_visualization()
    asyncio.run(example_5_real_world())


if __name__ == "__main__":
    main()
