# DAG Simple

[![PyPI version](https://img.shields.io/pypi/v/dag-simple.svg)](https://pypi.org/project/dag-simple/)
[![Python Support](https://img.shields.io/pypi/pyversions/dag-simple.svg)](https://pypi.org/project/dag-simple/)
[![Tests](https://github.com/Apex-Engineers-Inc/dag_simple/actions/workflows/ci-publish.yml/badge.svg?branch=main)](https://github.com/Apex-Engineers-Inc/dag_simple/actions/workflows/ci-publish.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)](https://github.com/Apex-Engineers-Inc/dag_simple/actions/workflows/ci-publish.yml)
[![Linting: Ruff](https://img.shields.io/badge/linting-Ruff-46a2f1?logo=ruff&logoColor=white)](https://github.com/Apex-Engineers-Inc/dag_simple/blob/main/pyproject.toml)
[![Typing: Pyright](https://img.shields.io/badge/typing-Pyright-blue.svg)](https://github.com/Apex-Engineers-Inc/dag_simple/actions/workflows/ci-publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

![DAG Simple Logo](assets/logo.svg)

A **dead simple**, **type-safe** DAG (Directed Acyclic Graph) library for Python with runtime validation, caching, and full async support.

Perfect for building data pipelines, ML workflows, and computation graphs without the complexity of larger frameworks.

## ✨ Features

- 🎯 **Type Safe** - Full generic type support with runtime validation
- ⚡ **Async/Await** - First-class async support with concurrent execution
- 🔒 **Cycle Detection** - Automatic detection at construction time
- 💾 **Result Caching** - Memoization to avoid redundant computations
- 📊 **Topological Sorting** - Efficient execution using Kahn's algorithm
- 🐛 **Great Error Messages** - Clear, actionable error reporting
- 📈 **Visualization** - Tree view and Mermaid diagram generation
- 🪶 **Zero Dependencies** - Pure Python implementation
- 🎨 **Simple API** - Decorator-based interface

## 📦 Installation

```
pip install dag-simple
```

Or using `uv`:

```
uv add dag-simple
```

## 🚀 Quick Start

```python
from dag_simple import node

# Define your computation nodes
@node()
def load_data(source: str) -> dict[str, list[int]]:
    return {"data": [1, 2, 3, 4, 5]}

@node(deps=[load_data])
def process(load_data: dict[str, list[int]]) -> dict[str, list[int]]:
    return {"processed": [x * 2 for x in load_data["data"]]}

@node(deps=[process])
def save(process: dict[str, list[int]]) -> str:
    return f"Saved {len(process['processed'])} items"

# Execute the pipeline
result = save.run(source="database")
print(result)  # "Saved 5 items"
assert result == "Saved 5 items"

# Visualize the DAG
save.visualize()
# Output:
# ○ save
#   ○ process
#     ○ load_data
```

## 🤔 When to Use What?

### Use `node.run()` for Simple Pipelines

Perfect for straightforward, linear workflows:

```python
from dag_simple import node

@node()
def step1(x: int) -> int:
    return x + 1

@node(deps=[step1])
def step2(step1: int) -> int:
    return step1 * 2

# Simple execution
result = step2.run(x=5)  # 12
assert result == 12
```

**When to use:**

- ✅ Simple, linear pipeline (A → B → C)
- ✅ Single target to execute
- ✅ Small projects (<10 nodes)
- ✅ Quick prototypes

### Use `DAG` Class for Complex Projects

The `DAG` class excels when you need:

#### 1. Multiple Independent Workflows

```python
from dag_simple import DAG, node

dag = DAG(name="analytics")

# User analytics workflow
@node()
def load_users() -> list[dict[str, int]]:
    return [{"id": 1}, {"id": 2}]

@node(deps=[load_users])
def analyze_users(load_users: list[dict[str, int]]) -> dict[str, int]:
    return {"count": len(load_users)}

# Sales analytics workflow
@node()
def load_sales() -> list[dict[str, int]]:
    return [{"amount": 100}, {"amount": 200}]

@node(deps=[load_sales])
def analyze_sales(load_sales: list[dict[str, int]]) -> dict[str, int]:
    return {"total": sum(s["amount"] for s in load_sales)}

# Add to DAG
dag.add_nodes(load_users, analyze_users, load_sales, analyze_sales)

# Execute specific workflow
user_stats = dag.execute("analyze_users")
assert user_stats == {"count": 2}

# Or execute ALL workflows at once
all_results = dag.execute_all()
# {'analyze_users': {...}, 'analyze_sales': {...}}
assert all_results["analyze_users"] == {"count": 2}
assert all_results["analyze_sales"] == {"total": 300}
```

#### 2. Concurrent Async Execution

```python
import asyncio
from dag_simple import DAG, node

dag = DAG("data_pipeline")

@node()
async def fetch_api_1() -> dict[str, list[int]]:
    await asyncio.sleep(0.2)
    return {"data": [1, 2, 3]}

@node()
async def fetch_api_2() -> dict[str, list[int]]:
    await asyncio.sleep(0.2)
    return {"data": [4, 5, 6]}

dag.add_nodes(fetch_api_1, fetch_api_2)

# Execute both APIs concurrently! (~0.2s, not 0.4s)
async def main():
    results = await dag.execute_all_async()
    assert results["fetch_api_1"] == {"data": [1, 2, 3]}
    assert results["fetch_api_2"] == {"data": [4, 5, 6]}
    return results

# For testing purposes, we'll just create the DAG
# In real usage, you'd call: asyncio.run(main())
```

#### 3. Namespace Management

```python
from dag_simple import DAG, node

# Separate DAGs for different domains
user_service = DAG("user_service")
order_service = DAG("order_service")

# Same node name in different DAGs - no conflicts!
@node()
def get_data():
    return "user data"
user_service.add_node(get_data)

@node()
def get_data():  # Different implementation
    return "order data"
order_service.add_node(get_data)
```

**When to use DAG class:**

- ✅ Multiple independent workflows
- ✅ Need to execute multiple targets at once
- ✅ Large projects (10+ nodes)
- ✅ Want automatic concurrent async execution
- ✅ Need namespace separation

## 🧵 Run DAGs in Parallel Processes

`run_sync_in_process` and `run_async_in_process` wrap a DAG evaluation in a
`ProcessPoolExecutor`. They are useful when you want the work to run on a separate
CPU core or need to isolate long-running jobs from interactive code.

**Use a process pool when you need to:**

- ✅ Run CPU-bound workloads without blocking the main thread or event loop.
- ✅ Execute multiple DAGs side-by-side.
- ✅ Keep expensive computations isolated from user-facing code.

**Remember:**

- Functions, their inputs, and their outputs must be picklable so they can cross
  process boundaries.
- Each worker has its own `ExecutionContext`, so cached values are not shared
  across processes unless you provide external storage.
- Spawning processes is expensive. Reuse a `ProcessPoolExecutor` to amortize the
  startup cost.
- `run_sync_in_process` only accepts branches made entirely of synchronous nodes.

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor

from dag_simple import node, run_async_in_process, run_sync_in_process


@node()
def make_numbers(seed: int) -> list[int]:
    return [seed + i for i in range(5)]


@node(deps=[make_numbers])
def total_energy(make_numbers: list[int]) -> int:
    return sum(value * value for value in make_numbers)


@node()
async def fetch_multiplier() -> int:
    await asyncio.sleep(0.1)
    return 2


@node(deps=[total_energy, fetch_multiplier])
def scaled(total_energy: int, fetch_multiplier: int) -> int:
    return total_energy * fetch_multiplier


@node()
def explode() -> None:
    raise ValueError("boom")


def main() -> None:
    energy = run_sync_in_process(total_energy, seed=10)
    assert energy == 730

    with ProcessPoolExecutor() as pool:
        parallel = [
            run_sync_in_process(total_energy, executor=pool, seed=seed)
            for seed in range(3)
        ]
        assert parallel == [30, 55, 90]

        scaled_result = run_async_in_process(scaled, executor=pool, seed=5)
        assert scaled_result == 510

    try:
        run_sync_in_process(explode)
    except ValueError as exc:
        assert str(exc) == "boom"


if __name__ == "__main__":
    main()
```

The exceptions raised inside a worker are re-raised in the calling process when
you access the result, so normal error handling applies. See
[`examples/process_pool_example.py`](examples/process_pool_example.py) for a
complete runnable walkthrough, including sharing a pool across many DAG
invocations.

## 📖 Core Concepts

### Nodes

Nodes are the building blocks of your DAG. Each node wraps a function and can depend on other nodes.

```python
from dag_simple import node

@node()
def add(x: int, y: int) -> int:
    return x + y

@node()
def multiply(x: int, y: int) -> int:
    return x * y

@node(deps=[add, multiply])
def combine(add: int, multiply: int) -> int:
    return add + multiply

result = combine.run(x=2, y=3)  # Returns 11: (2+3) + (2*3)
assert result == 11
```

### Async Support

First-class support for async/await:

```python
import asyncio
from dag_simple import node

@node()
async def fetch_data(url: str) -> dict[str, list[int]]:
    # Async I/O operations
    await asyncio.sleep(0.1)
    return {"data": [1, 2, 3]}

@node(deps=[fetch_data])
async def process(fetch_data: dict[str, list[int]]) -> list[int]:
    return [x * 2 for x in fetch_data["data"]]

# Execute async DAG
async def main():
    result = await process.run_async(url="https://api.example.com")
    assert result == [2, 4, 6]
    return result

# For testing purposes, we'll just create the nodes
# In real usage, you'd call: asyncio.run(main())
```

**Concurrent execution is automatic:**

```python
import asyncio
from dag_simple import node

@node()
async def fetch1() -> dict[str, list[int]]:
    await asyncio.sleep(0.2)  # 200ms
    return {"data": [1, 2, 3]}

@node()
async def fetch2() -> dict[str, list[int]]:
    await asyncio.sleep(0.2)  # 200ms
    return {"data": [4, 5, 6]}

@node(deps=[fetch1, fetch2])
async def combine(fetch1: dict[str, list[int]], fetch2: dict[str, list[int]]) -> dict[str, list[int]]:
    return {"merged": fetch1["data"] + fetch2["data"]}

# fetch1 and fetch2 run CONCURRENTLY! (~200ms total, not 400ms)
async def main():
    result = await combine.run_async()
    assert result == {"merged": [1, 2, 3, 4, 5, 6]}
    return result

# For testing purposes, we'll just create the nodes
# In real usage, you'd call: asyncio.run(main())
```

### Type Validation

Enable runtime type checking to catch errors early:

```python
from dag_simple import node

@node(validate_types=True)
def typed_function(x: int, y: str) -> str:
    return f"{y}: {x}"

# ✓ This works
result = typed_function.run(x=42, y="Answer")
assert result == "Answer: 42"

# ✗ This raises NodeExecutionError (wrapping ValidationError)
try:
    result = typed_function.run(x="wrong", y="Answer")
except Exception as e:
    print(f"Expected error: {e}")
    assert "NodeExecutionError" in str(type(e).__name__)
```

### Error Handling

All execution errors are wrapped with comprehensive context information for easier debugging:

```python
from dag_simple import node, NodeExecutionError

@node()
def load_data(source: str) -> dict[str, int]:
    return {"count": 100, "threshold": 50}

@node(deps=[load_data])
def process_data(load_data: dict[str, int], multiplier: int) -> int:
    return load_data["count"] * multiplier

@node(deps=[process_data])
def validate_result(process_data: int) -> int:
    if process_data > 1000:
        raise ValueError(f"Result too large: {process_data}")
    return process_data

@node(deps=[validate_result])
def save_result(validate_result: int) -> str:
    return f"Saved {validate_result}"

try:
    # This will fail at validate_result because 100 * 20 = 2000 > 1000
    save_result.run(source="database", multiplier=20)
except NodeExecutionError as e:
    print(e)
```

**Error Output:**

```
================================================================================
Node Execution Failed: 'validate_result'
================================================================================

Execution Path:
  load_data -> process_data -> validate_result

Inputs to 'validate_result':
  process_data: 2000

Original Error:
  ValueError: Result too large: 2000
================================================================================
```

**Key Features:**

- 🔍 **Execution Path** - Shows which nodes executed before the error occurred
- 📝 **Input Context** - All inputs passed to the failed node are captured
- 🐛 **Original Exception** - Full error details are preserved and displayed
- 🎯 **Clear Formatting** - Well-structured, easy-to-read error messages
- ⚙️ **Async Support** - Works seamlessly with both sync and async execution
- 📦 **Multiprocessing Ready** - Exceptions are picklable for use with `ProcessPoolExecutor`
- 🔗 **Exception Chaining** - Access `e.original_exception`, `e.execution_path`, and `e.node_inputs` for programmatic handling

See [`examples/error_handling_example.py`](examples/error_handling_example.py) for more detailed examples.

### Result Caching

Cache expensive computations that are used multiple times:

```python
from dag_simple import node

@node(cache_result=True)
def expensive_computation(x: int) -> int:
    # This only runs once even if multiple nodes depend on it
    return x ** 2

@node(deps=[expensive_computation])
def use_result_1(expensive_computation: int) -> int:
    return expensive_computation + 1

@node(deps=[expensive_computation])
def use_result_2(expensive_computation: int) -> int:
    return expensive_computation + 2

@node(deps=[use_result_1, use_result_2])
def final(use_result_1: int, use_result_2: int) -> int:
    return use_result_1 + use_result_2

result = final.run(x=5)  # expensive_computation runs only once
# expensive_computation(5) = 25
# use_result_1(25) = 25 + 1 = 26
# use_result_2(25) = 25 + 2 = 27
# final(26, 27) = 26 + 27 = 53
assert result == 53
```

## 🎓 Examples

### Data Pipeline (ETL)

```python
from dag_simple import node

@node(cache_result=True, validate_types=True)
def extract(source: str) -> list[dict[str, int]]:
    """Extract data from source."""
    return [
        {"id": 1, "value": 100},
        {"id": 2, "value": 200},
        {"id": 3, "value": 150},
    ]

@node(deps=[extract], validate_types=True)
def transform(extract: list[dict[str, int]], min_value: int) -> list[dict[str, int]]:
    """Filter and transform data."""
    return [item for item in extract if item["value"] >= min_value]

@node(deps=[transform], validate_types=True)
def load(transform: list[dict[str, int]]) -> str:
    """Load transformed data."""
    total = sum(item["value"] for item in transform)
    return f"Loaded {len(transform)} items, total value: {total}"

# Execute the pipeline
result = load.run(source="database", min_value=150)
print(result)  # "Loaded 2 items, total value: 350"
assert result == "Loaded 2 items, total value: 350"
```

### Async Web Scraping

```python
import asyncio
from dag_simple import node

@node()
async def fetch_page(url: str) -> str:
    # Simulated fetch
    await asyncio.sleep(0.1)
    return "<html>...</html>"

@node(deps=[fetch_page])
async def extract_links(fetch_page: str) -> list[str]:
    # Parse HTML
    return ["link1", "link2", "link3"]

@node(deps=[extract_links])
async def fetch_all_links(extract_links: list[str]) -> list[str]:
    # Fetch all links concurrently
    async def fetch_one(link: str) -> str:
        await asyncio.sleep(0.1)
        return f"content_{link}"

    return await asyncio.gather(*[fetch_one(link) for link in extract_links])

async def main():
    result = await fetch_all_links.run_async(url="https://example.com")
    assert result == ["content_link1", "content_link2", "content_link3"]
    return result

# For testing purposes, we'll just create the nodes
# In real usage, you'd call: asyncio.run(main())
```

### Machine Learning Pipeline with DAG Class

```python
from dag_simple import DAG, node
import asyncio

ml_pipeline = DAG("ml_pipeline")

@node()
async def load_data(path: str) -> dict[str, list]:
    await asyncio.sleep(0.1)
    return {"X": [[1, 2]], "y": [0]}

@node(deps=[load_data])
async def preprocess(load_data: dict[str, list]) -> dict[str, list]:
    await asyncio.sleep(0.1)
    return {"X_processed": load_data["X"], "y": load_data["y"]}

@node(deps=[preprocess], cache_result=True)
async def train_model(preprocess: dict[str, list]) -> dict[str, str | float]:
    await asyncio.sleep(0.2)
    return {"model": "trained", "accuracy": 0.95}

@node(deps=[preprocess, train_model])
async def evaluate(preprocess: dict[str, list], train_model: dict[str, str | float]) -> float:
    await asyncio.sleep(0.1)
    return 0.93

@node(deps=[train_model, evaluate])
async def save_if_good(train_model: dict[str, str | float], evaluate: float) -> str:
    if evaluate > 0.9:
        return f"✓ Saved {train_model['model']}"
    return "✗ Model not good enough"

ml_pipeline.add_nodes(load_data, preprocess, train_model, evaluate, save_if_good)

# Visualize
ml_pipeline.visualize_all()

# Execute
async def main():
    result = await ml_pipeline.execute_async("save_if_good", path="data.csv")
    assert result == "✓ Saved trained"
    return result

# For testing purposes, we'll just create the DAG
# In real usage, you'd call: asyncio.run(main())
```

## 🔍 Introspection & Debugging

### Visualize DAG Structure

```python
from dag_simple import node

# Create a simple node for demonstration
@node()
def example_node(x: int) -> int:
    return x * 2

# Tree view
example_node.visualize()

# Get topological order
order = example_node.topological_sort()
print(" -> ".join(order))
assert order == ["example_node"]

# Get dependency graph
graph = example_node.graph_dict()
print(graph)
assert graph == {"example_node": []}

# Generate Mermaid diagram
mermaid = example_node.to_mermaid()
print(mermaid)
assert "example_node" in mermaid
```

### DAG Class Methods

```python
from dag_simple import DAG, node

# Create a simple DAG for demonstration
dag = DAG("my_pipeline")

@node()
def example_node(x: int) -> int:
    return x * 2

dag.add_node(example_node)

# Execute specific target (sync)
result = dag.execute("example_node", x=10)
assert result == 20

# Execute specific target (async)
async def async_example():
    result = await dag.execute_async("example_node", x=10)
    assert result == 20
    return result

# Execute ALL leaf nodes (sync)
results = dag.execute_all(x=10)
assert results["example_node"] == 20

# Execute ALL leaf nodes (async, concurrently!)
async def async_all_example():
    results = await dag.execute_all_async(x=10)
    assert results["example_node"] == 20
    return results

# Visualization
dag.visualize_all()

# Get execution order
order = dag.get_execution_order()
assert order == ["example_node"]
```

## 📊 API Reference

### `@node` Decorator

```text
@node(
    deps: list[Node] | None = None,       # Dependencies
    name: str | None = None,              # Custom name
    validate_types: bool = True,          # Enable type validation
    cache_result: bool = False            # Enable result caching
)
```

### `Node.run()` and `Node.run_async()`

```text
# Synchronous execution
result = node.run(
    enable_cache: bool = True,  # Enable/disable caching
    **inputs                     # Input values
)

# Asynchronous execution
result = await node.run_async(
    enable_cache: bool = True,  # Enable/disable caching
    **inputs                     # Input values
)
```

### Introspection Methods

- `node.visualize()` - Print tree visualization
- `node.topological_sort()` - Get execution order
- `node.graph_dict()` - Get dependency dictionary
- `node.get_all_dependencies()` - Get all transitive dependencies
- `node.to_mermaid()` - Generate Mermaid diagram

## 🤝 Contributing

We welcome contributions! Here's how to get started:

### Development Setup

1. **Clone the repository:**

   ```
   git clone https://github.com/yourusername/dag-simple.git
   cd dag-simple
   ```

2. **Install `uv` (recommended):**

   ```
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies:**

   ```
   uv sync --dev
   ```

4. **Run tests (includes documentation code blocks):**

   ```
   uv run pytest
   ```

5. **Run type checking:**

   ```
   uv run pyright
   ```

6. **Format code:**

   ```
   uv run ruff format .
   ```

7. **Lint code:**
   ```
   uv run ruff check .
   ```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

### Testing Documentation

This project uses `pytest-codeblocks` to ensure all code examples in the README.md continue to work correctly. This helps catch issues when:

- API changes break documentation examples
- Dependencies change behavior
- Code refactoring affects examples

**Code blocks testing is automatically enabled** - when you run `uv run pytest`, it will automatically test all Python code blocks found in README.md and CONTRIBUTING.md.

The configuration in `pyproject.toml` includes:

- `--codeblocks` flag in pytest addopts for automatic execution
- `testpaths = ["tests", "**/*.md"]` to discover markdown files

**Note:** Some code blocks may fail tests because they:

- Are missing imports (e.g., `from dag_simple import node`)
- Have `await` outside async functions (documentation examples)
- Are syntax examples rather than executable code
- Are incomplete code snippets

This is expected behavior - the important thing is that pytest-codeblocks will catch real issues when your API changes or when code examples become outdated.

## 📋 Requirements

- Python 3.10+
- No runtime dependencies!

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

Inspired by modern workflow orchestration tools but designed to be simpler and more lightweight.

## 📚 Related Projects

- **Airflow** - Production-grade workflow orchestration
- **Prefect** - Modern workflow orchestration
- **Dask** - Parallel computing library
- **Luigi** - Python workflow management

DAG Simple is perfect when you need DAG functionality without the overhead of these larger frameworks.

## 💬 Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/yourusername/dag-simple/issues)
- 💡 **Discussions**: [GitHub Discussions](https://github.com/yourusername/dag-simple/discussions)
- 📖 **Documentation**: [README](https://github.com/yourusername/dag-simple#readme)

---

Made with ❤️ by the DAG Simple contributors
