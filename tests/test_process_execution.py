from __future__ import annotations

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from rustest import raises

from dag_simple.exceptions import NodeExecutionError
from dag_simple.execution import run_async_in_process, run_sync_in_process

# Add tests directory to path for rustest compatibility with relative imports
tests_dir = Path(__file__).parent
if str(tests_dir) not in sys.path:
    sys.path.insert(0, str(tests_dir))

from process_nodes import (  # noqa: E402  # type: ignore[import-not-found]
    add_async,
    double,
    explode,
    explode_async,
)


def test_run_sync_in_process_returns_value() -> None:
    result = run_sync_in_process(double)
    assert result == 4


def test_run_async_in_process_returns_value() -> None:
    result = run_async_in_process(add_async)
    assert result == 5


def test_run_sync_in_process_with_custom_executor() -> None:
    with ProcessPoolExecutor(max_workers=1) as executor:
        result_one = run_sync_in_process(double, executor=executor)
        result_two = run_sync_in_process(double, executor=executor)

    assert result_one == 4
    assert result_two == 4


def test_run_async_in_process_with_custom_executor() -> None:
    with ProcessPoolExecutor(max_workers=1) as executor:
        result_one = run_async_in_process(add_async, executor=executor)
        result_two = run_async_in_process(add_async, executor=executor)

    assert result_one == 5
    assert result_two == 5


def test_run_sync_in_process_propagates_exceptions() -> None:
    with raises(NodeExecutionError) as exc_info:
        run_sync_in_process(explode)

    exc = exc_info.value
    assert isinstance(exc, NodeExecutionError)
    assert isinstance(exc.original_exception, ValueError)
    assert "boom" in str(exc.original_exception)


def test_run_async_in_process_propagates_exceptions() -> None:
    with raises(NodeExecutionError) as exc_info:
        run_async_in_process(explode_async)

    exc = exc_info.value
    assert isinstance(exc, NodeExecutionError)
    assert isinstance(exc.original_exception, RuntimeError)
    assert "async boom" in str(exc.original_exception)
