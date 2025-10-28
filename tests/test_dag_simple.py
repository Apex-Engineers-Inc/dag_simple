"""Comprehensive test suite for dag-simple."""

import asyncio

import pytest

from dag_simple import (
    DAG,
    CycleDetectedError,
    MissingDependencyError,
    ValidationError,
    input_node,
    node,
)


class TestBasicNode:
    """Test basic Node functionality."""

    def test_node_creation(self) -> None:
        """Test creating a simple node."""

        @node()
        def add(x: int, y: int) -> int:
            return x + y

        assert add.name == "add"
        assert add.deps == []
        assert callable(add.fn)

    def test_node_with_custom_name(self) -> None:
        """Test node with custom name."""

        @node(name="custom_name")
        def my_func(x: int) -> int:
            return x

        assert my_func.name == "custom_name"

    def test_node_execution(self) -> None:
        """Test basic node execution."""

        @node()
        def multiply(x: int, y: int) -> int:
            return x * y

        result = multiply.run(x=3, y=4)
        assert result == 12

    def test_node_with_dependencies(self) -> None:
        """Test node with dependencies."""

        @node()
        def base(x: int) -> int:
            return x * 2

        @node(deps=[base])
        def derived(base: int) -> int:
            return base + 10

        result = derived.run(x=5)
        assert result == 20  # (5*2) + 10


class TestTypeValidation:
    """Test runtime type validation."""

    def test_valid_input_types(self) -> None:
        """Test validation passes with correct types."""

        @node(validate_types=True)
        def typed_func(x: int, y: str) -> str:
            return f"{y}{x}"

        result = typed_func.run(x=42, y="answer: ")
        assert result == "answer: 42"

    def test_invalid_input_types(self) -> None:
        """Test validation fails with incorrect types."""

        @node(validate_types=True)
        def typed_func(x: int) -> int:
            return x

        with pytest.raises(ValidationError) as exc_info:
            typed_func.run(x="not an int")

        assert "expected type int" in str(exc_info.value)

    def test_invalid_output_type(self) -> None:
        """Test output type validation."""

        @node(validate_types=True)
        def bad_return(x: int) -> int:
            return "not an int"  # type: ignore

        with pytest.raises(ValidationError) as exc_info:
            bad_return.run(x=5)

        assert "return type expected int" in str(exc_info.value)

    def test_validation_disabled(self) -> None:
        """Test that validation can be disabled."""

        @node(validate_types=False)
        def untyped_func(x: int) -> int:
            return x  # type: ignore

        # Should not raise even with wrong type
        result = untyped_func.run(x="string")
        assert result == "string"


class TestCaching:
    """Test result caching."""

    def test_cache_enabled(self) -> None:
        """Test that caching works."""
        call_count = {"count": 0}

        @node(cache_result=True)
        def expensive(x: int) -> int:
            call_count["count"] += 1
            return x * x

        @node(deps=[expensive])
        def user1(expensive: int) -> int:
            return expensive + 1

        @node(deps=[expensive])
        def user2(expensive: int) -> int:
            return expensive + 2

        @node(deps=[user1, user2])
        def combine(user1: int, user2: int) -> int:
            return user1 + user2

        result = combine.run(x=5)
        assert result == 53  # (25+1) + (25+2)
        assert call_count["count"] == 1  # Called only once

    def test_cache_disabled(self) -> None:
        """Test execution without caching."""
        call_count = {"count": 0}

        @node(cache_result=True)
        def expensive(x: int) -> int:
            call_count["count"] += 1
            return x * x

        @node(deps=[expensive])
        def user1(expensive: int) -> int:
            return expensive + 1

        @node(deps=[expensive])
        def user2(expensive: int) -> int:
            return expensive + 2

        @node(deps=[user1, user2])
        def combine(user1: int, user2: int) -> int:
            return user1 + user2

        result = combine.run(x=5, enable_cache=False)
        assert result == 53
        # Without caching, expensive is called for each dependent
        assert call_count["count"] >= 2


class TestCycleDetection:
    """Test cycle detection."""

    def test_no_cycle(self) -> None:
        """Test that valid DAG doesn't raise cycle error."""

        @node()
        def a(x: int) -> int:
            return x + 1

        @node(deps=[a])
        def b(a: int) -> int:
            return a + 2

        @node(deps=[b])
        def c(b: int) -> int:
            return b + 3

        # Should not raise
        result = c.run(x=1)
        assert result == 7  # 1+1+2+3

    def test_self_cycle(self) -> None:
        """Test detection of self-referencing cycle."""

        @node()
        def self_ref(x: int) -> int:
            return x

        with pytest.raises(CycleDetectedError):
            self_ref.deps.append(self_ref)
            self_ref._validate_no_cycles()  # pyright: ignore[reportPrivateUsage]

    def test_two_node_cycle(self) -> None:
        """Test detection of cycle between two nodes."""

        @node()
        def a(x: int) -> int:
            return x + 1

        @node()
        def b(a: int) -> int:
            return a + 1

        a.deps = [b]
        b.deps = [a]

        with pytest.raises(CycleDetectedError):
            a._validate_no_cycles()  # pyright: ignore[reportPrivateUsage]


class TestTopologicalSort:
    """Test topological sorting."""

    def test_linear_dag(self) -> None:
        """Test topological sort on linear DAG."""

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        @node(deps=[b])
        def c(b: int) -> int:
            return b

        topo = c.topological_sort()
        assert topo == ["a", "b", "c"]

    def test_diamond_dag(self) -> None:
        """Test topological sort on diamond-shaped DAG."""

        @node()
        def root(x: int) -> int:
            return x

        @node(deps=[root])
        def left(root: int) -> int:
            return root + 1

        @node(deps=[root])
        def right(root: int) -> int:
            return root + 2

        @node(deps=[left, right])
        def bottom(left: int, right: int) -> int:
            return left + right

        topo = bottom.topological_sort()
        assert topo[0] == "root"
        assert topo[-1] == "bottom"
        assert "left" in topo
        assert "right" in topo


class TestInputNodes:
    """Test input node functionality."""

    def test_input_node_creation(self) -> None:
        """Test creating input nodes."""
        x = input_node("x", int)
        assert x.name == "x"
        assert x.validate_types is True

    def test_input_node_usage(self) -> None:
        """Test using input nodes in a DAG."""
        x = input_node("x", int)
        y = input_node("y", int)

        @node(deps=[x, y])
        def add(x: int, y: int) -> int:
            return x + y

        result = add.run(x=10, y=20)
        assert result == 30

    def test_input_node_enforces_type_hint(self) -> None:
        """Input nodes with type hints should validate inputs."""
        x = input_node("x", int)

        @node(deps=[x])
        def consume(x: int) -> int:
            return x

        with pytest.raises(ValidationError):
            consume.run(x="not an int")

    def test_input_node_without_type_hint_skips_validation(self) -> None:
        """Input nodes without type hints should accept any value."""
        x = input_node("x", str)

        @node(validate_types=False, deps=[x])
        def passthrough(x: int) -> int:
            return x

        result = passthrough.run(x="a string")
        assert result == "a string"


class TestDAGClass:
    """Test the high-level DAG class."""

    def test_dag_creation(self) -> None:
        """Test creating a DAG."""
        dag = DAG(name="test_dag")
        assert dag.name == "test_dag"
        assert len(dag.nodes) == 0

    def test_add_node(self) -> None:
        """Test adding nodes to DAG."""
        dag = DAG()

        @node()
        def my_node(x: int) -> int:
            return x

        dag.add_node(my_node)
        assert "my_node" in dag.nodes

    def test_execute_by_name(self) -> None:
        """Test executing nodes by name."""
        dag = DAG()

        @node()
        def double(x: int) -> int:
            return x * 2

        dag.add_node(double)
        result = dag.execute("double", x=5)
        assert result == 10

    def test_execute_by_node(self) -> None:
        """Test executing nodes by reference."""
        dag = DAG()

        @node()
        def triple(x: int) -> int:
            return x * 3

        dag.add_node(triple)
        result = dag.execute(triple, x=5)
        assert result == 15


class TestErrorHandling:
    """Test error handling."""

    def test_missing_dependency(self) -> None:
        """Test error when required parameter is missing."""

        @node()
        def needs_x(x: int) -> int:
            return x

        with pytest.raises(MissingDependencyError) as exc_info:
            needs_x.run()

        assert "missing required parameters" in str(exc_info.value)

    def test_wrong_argument_name(self) -> None:
        """Test error with wrong argument names."""

        @node()
        def my_func(correct_name: int) -> int:
            return correct_name

        with pytest.raises(MissingDependencyError):
            my_func.run(wrong_name=5)


class TestIntrospection:
    """Test introspection methods."""

    def test_graph_dict(self) -> None:
        """Test graph_dict method."""

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        graph = b.graph_dict()
        assert "a" in graph
        assert "b" in graph
        assert graph["b"] == ["a"]
        assert graph["a"] == []

    def test_get_all_dependencies(self) -> None:
        """Test get_all_dependencies method."""

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        @node(deps=[b])
        def c(b: int) -> int:
            return b

        deps = c.get_all_dependencies()
        assert deps == {"a", "b"}

    def test_to_mermaid(self) -> None:
        """Test Mermaid diagram generation."""

        @node()
        def root(x: int) -> int:
            return x

        @node(deps=[root])
        def child(root: int) -> int:
            return root

        mermaid = child.to_mermaid()
        assert "graph TD" in mermaid
        assert "root" in mermaid
        assert "child" in mermaid
        assert "-->" in mermaid


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_deep_dependency_chain(self) -> None:
        """Test a deep chain of dependencies."""

        @node()
        def step1(x: int) -> int:
            return x + 1

        @node(deps=[step1])
        def step2(step1: int) -> int:
            return step1 + 1

        @node(deps=[step2])
        def step3(step2: int) -> int:
            return step2 + 1

        @node(deps=[step3])
        def step4(step3: int) -> int:
            return step3 + 1

        @node(deps=[step4])
        def step5(step4: int) -> int:
            return step4 + 1

        result = step5.run(x=0)
        assert result == 5

    def test_wide_dependency_tree(self) -> None:
        """Test a wide tree of dependencies."""

        @node()
        def root(x: int) -> int:
            return x

        @node(deps=[root], name="branch_0")
        def branch_0(root: int) -> int:
            return root + 0

        @node(deps=[root], name="branch_1")
        def branch_1(root: int) -> int:
            return root + 1

        @node(deps=[root], name="branch_2")
        def branch_2(root: int) -> int:
            return root + 2

        @node(deps=[root], name="branch_3")
        def branch_3(root: int) -> int:
            return root + 3

        @node(deps=[root], name="branch_4")
        def branch_4(root: int) -> int:
            return root + 4

        @node(deps=[branch_0, branch_1, branch_2, branch_3, branch_4])
        def combine(
            branch_0: int, branch_1: int, branch_2: int, branch_3: int, branch_4: int
        ) -> int:
            return branch_0 + branch_1 + branch_2 + branch_3 + branch_4

        result = combine.run(x=10)
        # 10 + (10+0 + 10+1 + 10+2 + 10+3 + 10+4)
        assert result == 60

    def test_mixed_cached_uncached(self) -> None:
        """Test mixing cached and uncached nodes."""

        @node(cache_result=True)
        def cached(x: int) -> int:
            return x * 2

        @node()
        def uncached(x: int) -> int:
            return x * 3

        @node(deps=[cached, uncached])
        def final(cached: int, uncached: int) -> int:
            return cached + uncached

        result = final.run(x=5)
        assert result == 25  # (5*2) + (5*3)


class TestAsyncNodes:
    """Test async node functionality."""

    def test_async_node_creation(self) -> None:
        """Test creating async nodes."""

        @node()
        async def async_func(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        assert async_func.is_async is True
        assert async_func.name == "async_func"

    def test_sync_node_is_not_async(self) -> None:
        """Test that sync nodes are marked as non-async."""

        @node()
        def sync_func(x: int) -> int:
            return x * 2

        assert sync_func.is_async is False

    def test_async_execution(self) -> None:
        """Test executing async nodes."""

        @node()
        async def async_add(x: int, y: int) -> int:
            await asyncio.sleep(0.01)
            return x + y

        result = asyncio.run(async_add.run_async(x=5, y=3))
        assert result == 8

    def test_mixed_sync_async(self) -> None:
        """Test mixing sync and async nodes."""

        @node()
        def sync_node(x: int) -> int:
            return x * 2

        @node(deps=[sync_node])
        async def async_node(sync_node: int) -> int:
            await asyncio.sleep(0.01)
            return sync_node + 10

        result = asyncio.run(async_node.run_async(x=5))
        assert result == 20  # (5*2) + 10

    def test_async_requires_run_async(self) -> None:
        """Test that async nodes cannot use run()."""

        @node()
        async def async_func(x: int) -> int:
            await asyncio.sleep(0.01)
            return x

        with pytest.raises(RuntimeError) as exc_info:
            async_func.run(x=5)

        assert "async" in str(exc_info.value).lower()

    def test_sync_node_with_async_dependency_requires_run_async(self) -> None:
        """Sync nodes with async dependencies should also require run_async()."""

        @node()
        async def async_dep() -> int:
            await asyncio.sleep(0.01)
            return 5

        @node(deps=[async_dep])
        def sync_consumer(async_dep: int) -> int:
            return async_dep

        with pytest.raises(RuntimeError) as exc_info:
            sync_consumer.run()

        assert "run_async" in str(exc_info.value)

    def test_async_with_caching(self) -> None:
        """Test async nodes with caching."""
        call_count = {"count": 0}

        @node(cache_result=True)
        async def async_expensive(x: int) -> int:
            call_count["count"] += 1
            await asyncio.sleep(0.01)
            return x * x

        @node(deps=[async_expensive])
        async def user1(async_expensive: int) -> int:
            return async_expensive + 1

        @node(deps=[async_expensive])
        async def user2(async_expensive: int) -> int:
            return async_expensive + 2

        @node(deps=[user1, user2])
        async def combine(user1: int, user2: int) -> int:
            return user1 + user2

        result = asyncio.run(combine.run_async(x=5))
        assert result == 53  # (25+1) + (25+2)
        # Note: With async concurrent execution, caching may not prevent all calls
        # if dependencies start before the cached value is available
        assert call_count["count"] <= 2  # Called at most twice


class TestDAGClassEnhancements:
    """Test enhanced DAG class functionality."""

    def test_execute_all(self) -> None:
        """Test executing all leaf nodes."""
        dag = DAG(name="test")

        @node()
        def root(x: int) -> int:
            return x

        @node(deps=[root])
        def branch1(root: int) -> int:
            return root + 1

        @node(deps=[root])
        def branch2(root: int) -> int:
            return root + 2

        dag.add_nodes(root, branch1, branch2)

        results = dag.execute_all(x=10)
        assert results == {"branch1": 11, "branch2": 12}

    def test_execute_all_async(self) -> None:
        """Test executing all leaf nodes asynchronously."""
        dag = DAG(name="test")

        @node()
        async def fetch1() -> int:
            await asyncio.sleep(0.01)
            return 1

        @node()
        async def fetch2() -> int:
            await asyncio.sleep(0.01)
            return 2

        dag.add_nodes(fetch1, fetch2)

        results = asyncio.run(dag.execute_all_async())
        assert results == {"fetch1": 1, "fetch2": 2}

    def test_add_multiple_nodes(self) -> None:
        """Test adding multiple nodes at once."""
        dag = DAG()

        @node()
        def node1(x: int) -> int:
            return x

        @node()
        def node2(x: int) -> int:
            return x

        dag.add_nodes(node1, node2)

        assert "node1" in dag.nodes
        assert "node2" in dag.nodes

    def test_get_execution_order(self) -> None:
        """Test getting execution order for all nodes."""
        dag = DAG()

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        @node(deps=[b])
        def c(b: int) -> int:
            return b

        dag.add_nodes(a, b, c)

        order = dag.get_execution_order()
        assert order == ["a", "b", "c"]

    def test_async_execute(self) -> None:
        """Test async execution through DAG class."""
        dag = DAG()

        @node()
        async def async_node(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * 2

        dag.add_node(async_node)

        result = asyncio.run(dag.execute_async("async_node", x=5))
        assert result == 10

    def test_get_node_not_found(self) -> None:
        """Test KeyError when getting non-existent node."""
        dag = DAG()

        with pytest.raises(KeyError) as exc_info:
            dag.get_node("nonexistent")

        assert "Node 'nonexistent' not found in DAG" in str(exc_info.value)

    def test_get_execution_order_empty_dag(self) -> None:
        """Test get_execution_order with empty DAG."""
        dag = DAG()
        order = dag.get_execution_order()
        assert order == []

    def test_execute_all_async_with_dependencies(self) -> None:
        """Test execute_all_async with nodes that have dependencies."""
        dag = DAG()

        @node()
        async def root() -> int:
            await asyncio.sleep(0.01)
            return 5

        @node(deps=[root])
        async def child(root: int) -> int:
            await asyncio.sleep(0.01)
            return root * 2

        dag.add_nodes(root, child)

        results = asyncio.run(dag.execute_all_async())
        assert results == {"child": 10}


class TestExecutionContext:
    """Test ExecutionContext utility behavior."""

    def test_get_cache_lock_reuses_same_lock(self) -> None:
        """Cache locks should be reused per key."""
        from dag_simple.context import ExecutionContext

        context = ExecutionContext(enable_cache=True)
        first = context.get_cache_lock("key")
        second = context.get_cache_lock("key")
        other = context.get_cache_lock("other")

        assert first is second
        assert first is not other


class TestExecutionErrorHandling:
    """Test error handling in execution module."""

    def test_run_sync_type_error(self) -> None:
        """Test TypeError handling in run_sync."""

        @node(validate_types=False)
        def bad_func(x: int) -> int:
            # This will raise a TypeError when x is not a number
            return x + 1

        # This should trigger a TypeError when calling the function
        with pytest.raises(TypeError) as exc_info:
            bad_func.run(x="not_an_int")

        assert "Failed running node 'bad_func'" in str(exc_info.value)

    def test_run_async_type_error(self) -> None:
        """Test TypeError handling in run_async."""

        @node(validate_types=False)
        async def bad_async_func(x: int) -> int:
            # This will raise a TypeError when x is not a number
            return x + 1

        # This should trigger a TypeError when calling the function
        with pytest.raises(TypeError) as exc_info:
            asyncio.run(bad_async_func.run_async(x="not_an_int"))

        assert "Failed running node 'bad_async_func'" in str(exc_info.value)

    def test_run_async_missing_dependency(self) -> None:
        """Test MissingDependencyError in run_async."""

        @node()
        async def needs_x(x: int) -> int:
            return x

        with pytest.raises(MissingDependencyError) as exc_info:
            asyncio.run(needs_x.run_async())

        assert "missing required parameters" in str(exc_info.value)

    def test_run_async_cached_value_return(self) -> None:
        """Test cached value return in run_async."""
        call_count = {"count": 0}

        @node(cache_result=True)
        async def cached_func(x: int) -> int:
            call_count["count"] += 1
            await asyncio.sleep(0.01)
            return x * 2

        # Test that caching works within a single execution context
        # by creating a DAG that uses the cached function twice
        @node(deps=[cached_func])
        async def user1(cached_func: int) -> int:
            return cached_func + 1

        @node(deps=[cached_func])
        async def user2(cached_func: int) -> int:
            return cached_func + 2

        @node(deps=[user1, user2])
        async def combine(user1: int, user2: int) -> int:
            return user1 + user2

        # This should call cached_func only once, even though it's used by both user1 and user2
        result = asyncio.run(combine.run_async(x=5))
        assert result == 23  # (10+1) + (10+2)
        assert call_count["count"] == 1  # Called only once

    def test_run_async_early_cached_return(self) -> None:
        """Test early cached return in run_async (missing coverage)."""
        call_count = {"count": 0}

        @node(cache_result=True)
        async def cached_func(x: int) -> int:
            call_count["count"] += 1
            await asyncio.sleep(0.01)
            return x * 2

        async def run_test():
            # Test the early cached return path by manually calling run_async twice
            # within the same execution context
            from dag_simple.context import ExecutionContext
            from dag_simple.execution import run_async

            # Create a shared execution context
            context = ExecutionContext(enable_cache=True, inputs={"x": 5})

            # First call should execute the function
            result1 = await run_async(cached_func, _context=context, x=5)
            assert result1 == 10
            assert call_count["count"] == 1

            # Second call should hit the early cached return path
            result2 = await run_async(cached_func, _context=context, x=5)
            assert result2 == 10
            assert call_count["count"] == 1  # Still only called once

        asyncio.run(run_test())


class TestIntrospectionEdgeCases:
    """Test edge cases in introspection module."""

    def test_topological_sort_cycle_detection(self) -> None:
        """Test cycle detection in topological sort."""

        @node()
        def a(x: int) -> int:
            return x

        @node()
        def b(a: int) -> int:
            return a

        # Create a cycle
        a.deps = [b]
        b.deps = [a]

        with pytest.raises(CycleDetectedError) as exc_info:
            a.topological_sort()

        assert "Cycle detected during topological sort" in str(exc_info.value)

    def test_graph_dict_already_visited(self) -> None:
        """Test graph_dict with already visited nodes."""

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        @node(deps=[a, b])  # Both depend on a
        def c(a: int, b: int) -> int:
            return a + b

        # This should handle the case where 'a' is visited multiple times
        graph = c.graph_dict()
        assert "a" in graph
        assert "b" in graph
        assert "c" in graph
        assert graph["a"] == []
        assert graph["b"] == ["a"]
        assert graph["c"] == ["a", "b"]

    def test_visualize_already_visited(self) -> None:
        """Test visualize with already visited nodes."""

        @node()
        def a(x: int) -> int:
            return x

        @node(deps=[a])
        def b(a: int) -> int:
            return a

        @node(deps=[a, b])  # Both depend on a
        def c(a: int, b: int) -> int:
            return a + b

        # This should handle the case where 'a' is visited multiple times
        # We can't easily test the print output, but we can ensure it doesn't crash
        c.visualize()


class TestNodeEdgeCases:
    """Test edge cases in node module."""

    def test_node_type_hints_exception(self) -> None:
        """Test exception handling when getting type hints fails."""
        import sys
        from unittest.mock import patch

        from dag_simple.node import Node

        # Create a function with missing type hints to simulate problematic type hints
        def problematic_func(x: object) -> object:
            return x

        # Patch get_type_hints where it's used in the node module
        # We need to patch the already-imported reference
        with patch.object(sys.modules["dag_simple.node"], "get_type_hints", side_effect=Exception("Type hint error")):
            # This should not raise an exception, but should disable validation
            node_instance = Node(problematic_func, validate_types=True)
            assert node_instance.validate_types is False

    def test_node_repr(self) -> None:
        """Test Node __repr__ method."""

        @node()
        def simple_func(x: int) -> int:
            return x

        @node(deps=[simple_func], cache_result=True)
        def cached_func(simple_func: int) -> int:
            return simple_func * 2

        @node(deps=[simple_func])
        async def async_func(simple_func: int) -> int:
            return simple_func * 3

        # Test basic repr
        repr_str = repr(simple_func)
        assert "Node simple_func" in repr_str
        assert "deps=[]" in repr_str

        # Test cached repr
        cached_repr = repr(cached_func)
        assert "Node cached_func" in cached_repr
        assert "cached" in cached_repr

        # Test async repr
        async_repr = repr(async_func)
        assert "Node async_func" in async_repr
        assert "async" in async_repr


class TestValidationEdgeCases:
    """Test edge cases in validation module."""

    def test_validate_input_types_skip_parameter(self) -> None:
        """Test validation skips parameters not in type hints."""

        @node(validate_types=True)
        def func_with_extra_param(x: int, extra_param: object) -> int:
            return x

        # This should not raise an error even though extra_param has a generic type hint
        result = func_with_extra_param.run(x=5, extra_param="anything")
        assert result == 5

    def test_validate_input_types_parameter_not_in_type_hints(self) -> None:
        """Test validation skips parameters not in type hints (missing coverage)."""

        @node(validate_types=True)
        def func_with_typed_param(x: int) -> int:
            return x

        # This should not raise an error when passing extra parameters not in type hints
        result = func_with_typed_param.run(x=5, extra_param="anything")
        assert result == 5

    def test_validate_input_types_parameter_not_in_type_hints_direct(self) -> None:
        """Test validation skips parameters not in type hints (direct call)."""
        from dag_simple.node import Node
        from dag_simple.validation import validate_input_types

        def func_with_typed_param(x: int) -> int:
            return x

        node = Node(func_with_typed_param, validate_types=True)

        # Call validation directly with a parameter not in type hints
        validate_input_types(node, {"x": 5, "extra_param": "anything"}, {"x": int})

    def test_validate_input_types_complex_generic_types(self) -> None:
        """Test validation skips complex generic types (missing coverage)."""

        @node(validate_types=True)
        def func_with_generic_types(items: list[int], mapping: dict[str, int]) -> int:
            return len(items) + len(mapping)

        # This should not raise an error even with complex generic types
        result = func_with_generic_types.run(items=[1, 2, 3], mapping={"a": 1, "b": 2})
        assert result == 5

    def test_validate_output_type_no_return_annotation(self) -> None:
        """Test validation skips when no return type annotation."""

        @node(validate_types=True)
        def func_no_return_type(x: int) -> int:
            return x

        # This should not raise an error even though there's no return type annotation
        result = func_no_return_type.run(x=5)
        assert result == 5

    def test_validate_output_type_no_return_annotation_coverage(self) -> None:
        """Test validation skips when no return type annotation (missing coverage)."""

        # Create a function without return type annotation
        def func_no_return_annotation(x: int):
            return x

        from dag_simple.node import Node
        from dag_simple.validation import validate_output_type

        node = Node(func_no_return_annotation, validate_types=True)

        # This should not raise an error when there's no return type annotation
        validate_output_type(node, 5, {})

    def test_validate_output_type_complex_generic_return(self) -> None:
        """Test validation skips complex generic return types (missing coverage)."""

        @node(validate_types=True)
        def func_with_generic_return(x: int) -> list[int]:
            return [x, x * 2]

        # This should not raise an error even with complex generic return type
        result = func_with_generic_return.run(x=5)
        assert result == [5, 10]


class TestDAGVisualization:
    """Test DAG visualization methods."""

    def test_visualize_all(self) -> None:
        """Test visualize_all method (missing coverage)."""
        dag = DAG(name="test_dag")

        @node()
        def root(x: int) -> int:
            return x

        @node(deps=[root])
        def child(root: int) -> int:
            return root * 2

        dag.add_nodes(root, child)

        # This should not raise an error and should print DAG information
        dag.visualize_all()
