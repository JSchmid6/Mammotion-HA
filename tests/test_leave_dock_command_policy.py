"""Regression tests for Mammotion leave-dock command handling."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

COORDINATOR_PATH = (
    Path(__file__).parents[1] / "custom_components" / "mammotion" / "coordinator.py"
)
LAWN_MOWER_PATH = (
    Path(__file__).parents[1] / "custom_components" / "mammotion" / "lawn_mower.py"
)


def _coordinator_tree() -> ast.Module:
    """Parse the coordinator without importing Home Assistant."""
    return ast.parse(COORDINATOR_PATH.read_text(encoding="utf-8"))


def _lawn_mower_tree() -> ast.Module:
    """Parse the lawn mower platform without importing Home Assistant."""
    return ast.parse(LAWN_MOWER_PATH.read_text(encoding="utf-8"))


def _class_def(tree: ast.Module, name: str) -> ast.ClassDef:
    """Return a class definition from the parsed coordinator source."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"{name} class not found")


def _method_def(class_node: ast.ClassDef, name: str) -> ast.AsyncFunctionDef:
    """Return an async method definition from a class node."""
    for node in class_node.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} method not found")


def _called_function_names(node: ast.AST) -> set[str]:
    """Return function and method call names used inside an AST node."""
    names: set[str] = set()
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        if isinstance(call.func, ast.Attribute):
            names.add(call.func.attr)
        elif isinstance(call.func, ast.Name):
            names.add(call.func.id)
    return names


def _constant_values(node: ast.AST) -> set[Any]:
    """Return constants used inside an AST node."""
    return {
        constant.value
        for constant in ast.walk(node)
        if isinstance(constant, ast.Constant)
    }


def test_leave_dock_queues_command_without_waiting_for_task_ack() -> None:
    """Leave-dock must not block automations on the task-control ACK."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    leave_dock = _method_def(base, "async_leave_dock")
    calls = _called_function_names(leave_dock)

    assert "async_send_command" in calls
    assert "async_start_command_report_watch" in calls
    assert "async_send_and_wait" not in calls
    assert "todev_taskctrl_ack" not in _constant_values(leave_dock)


def test_dock_waits_for_task_ack_and_starts_report_watch() -> None:
    """Dock must not silently swallow failed return-to-dock command requests."""
    tree = _lawn_mower_tree()
    mower = _class_def(tree, "MammotionLawnMowerEntity")
    dock = _method_def(mower, "async_dock")
    calls = _called_function_names(dock)
    constants = _constant_values(dock)

    assert "async_send_and_wait" in calls
    assert "async_start_command_report_watch" in calls
    assert "async_send_command" not in calls
    assert "return_to_dock" in constants
    assert "todev_taskctrl_ack" in constants


def test_command_report_watch_schedules_delayed_snapshot() -> None:
    """Command report watches also request a short delayed fresh snapshot."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    start_watch = _method_def(report, "async_start_command_report_watch")
    schedule_refresh = next(
        node
        for node in report.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_schedule_command_report_snapshot_refresh"
    )

    assert "_schedule_command_report_snapshot_refresh" in _called_function_names(
        start_watch
    )
    assert "async_call_later" in _called_function_names(schedule_refresh)
    assert "_async_request_report_snapshot_guarded" in _called_function_names(
        schedule_refresh
    )
