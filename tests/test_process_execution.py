from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import pytest

from dag_simple.execution import run_async_in_process, run_sync_in_process

from .process_nodes import add_async, double, explode, explode_async


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
    with pytest.raises(ValueError, match="boom"):
        run_sync_in_process(explode)


def test_run_async_in_process_propagates_exceptions() -> None:
    with pytest.raises(RuntimeError, match="async boom"):
        run_async_in_process(explode_async)
