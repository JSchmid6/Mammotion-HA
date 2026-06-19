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


def _async_method_def(class_node: ast.ClassDef, name: str) -> ast.AsyncFunctionDef:
    """Return an async method definition from a class node."""
    for node in class_node.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} method not found")


def _method_def(class_node: ast.ClassDef, name: str) -> ast.FunctionDef:
    """Return a sync method definition from a class node."""
    for node in class_node.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
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
    leave_dock = _async_method_def(base, "async_leave_dock")
    calls = _called_function_names(leave_dock)

    assert "async_send_command" in calls
    assert "async_start_command_report_watch" in calls
    assert "async_send_and_wait" not in calls
    assert "todev_taskctrl_ack" not in _constant_values(leave_dock)


def test_dock_waits_for_task_ack_and_starts_report_watch() -> None:
    """Dock must not silently swallow failed return-to-dock command requests."""
    tree = _lawn_mower_tree()
    mower = _class_def(tree, "MammotionLawnMowerEntity")
    dock = _async_method_def(mower, "async_dock")
    calls = _called_function_names(dock)
    constants = _constant_values(dock)

    assert "async_send_and_wait" in calls
    assert "async_start_command_report_watch" in calls
    assert "async_send_command" not in calls
    assert "return_to_dock" in constants
    assert "todev_taskctrl_ack" in constants


def test_task_start_queues_single_schedule_without_wrong_plan_ack() -> None:
    """Single-run task starts must not wait for the plan-list ACK."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    start_task = _async_method_def(base, "start_task")
    calls = _called_function_names(start_task)
    constants = _constant_values(start_task)

    assert "async_send_command" in calls
    assert "async_start_command_report_watch" in calls
    assert "async_send_and_wait" not in calls
    assert "single_schedule" in constants
    assert "todev_planjob_set" not in constants


def test_command_report_watch_schedules_delayed_snapshot() -> None:
    """Command report watches also request a short delayed fresh snapshot."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    start_watch = _async_method_def(report, "async_start_command_report_watch")
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
    assert "_async_request_command_report_refresh" in _called_function_names(
        schedule_refresh
    )


def test_command_report_watch_requests_full_report_cfg() -> None:
    """Command watches should nudge the legacy full report path too."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    start_watch = _async_method_def(report, "async_start_command_report_watch")
    schedule_refresh = next(
        node
        for node in report.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_schedule_command_report_snapshot_refresh"
    )

    assert "_async_request_report_cfg_guarded" in _called_function_names(start_watch)
    assert "_async_request_command_report_refresh" in _called_function_names(
        schedule_refresh
    )


def test_report_cfg_helper_uses_device_handle_request_report_cfg() -> None:
    """The full report refresh should use PyMammotion's report_cfg helper."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    report_cfg = _async_method_def(base, "async_request_report_cfg")
    calls = _called_function_names(report_cfg)

    assert "mower" in calls
    assert "request_report_cfg" in _constant_values(report_cfg)


def test_command_report_watch_schedules_bounded_snapshot_retries() -> None:
    """Command report watches should schedule a small fixed retry set."""
    tree = _coordinator_tree()
    constants = _constant_values(tree)

    assert "COMMAND_REPORT_SNAPSHOT_DELAYS" in {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    }
    assert 2.0 in constants
    assert 10.0 in constants
    assert 25.0 in constants


def test_send_command_starts_post_command_report_refresh() -> None:
    """Successful queued commands should start the centralized report refresh."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    send_command = _async_method_def(base, "async_send_command")
    calls = _called_function_names(send_command)

    assert "_async_start_post_command_report_refresh" in calls


def test_send_and_wait_starts_post_command_report_refresh() -> None:
    """Successful ACK-based commands should start the centralized report refresh."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    send_and_wait = _async_method_def(base, "async_send_and_wait")
    calls = _called_function_names(send_and_wait)

    assert "_async_start_post_command_report_refresh" in calls


def test_post_command_refresh_policy_skips_read_commands() -> None:
    """Read-only Mammotion commands must not self-amplify report polling."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    policy = _method_def(base, "_should_refresh_report_after_command")
    constants = _constant_values(policy)
    calls = _called_function_names(policy)

    assert "POST_COMMAND_REPORT_READ_ONLY_COMMANDS" in {
        node.id for node in ast.walk(policy) if isinstance(node, ast.Name)
    }
    assert "POST_COMMAND_REPORT_READ_ONLY_PREFIXES" in {
        node.id for node in ast.walk(policy) if isinstance(node, ast.Name)
    }
    assert "read_write_device" in constants
    assert "read_and_set_sidelight" in constants
    assert "startswith" in calls


def test_post_command_refresh_policy_includes_transition_commands() -> None:
    """Robot transition commands should be followed by a report watch."""
    tree = _coordinator_tree()
    constants = _constant_values(tree)

    for command in (
        "leave_dock",
        "single_schedule",
        "start_job",
        "return_to_dock",
        "pause_execute_task",
        "cancel_return_to_dock",
        "resume_execute_task",
        "cancel_job",
    ):
        assert command in constants


def test_report_availability_uses_fresh_report_before_transport_state() -> None:
    """Fresh telemetry should keep HA state visible through transport flaps."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    is_available = _method_def(report, "is_entity_available")

    call_order = [
        call.func.attr
        for call in ast.walk(is_available)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
    ]

    assert call_order.index("has_fresh_report") < call_order.index(
        "is_entity_available"
    )


def test_report_availability_checks_true_offline_before_cached_state() -> None:
    """Only an explicit offline report should hide cached mower state."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    is_available = _method_def(report, "is_entity_available")

    call_order = [
        call.func.attr
        for call in ast.walk(is_available)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
    ]

    assert call_order.index("_device_reported_offline") < call_order.index(
        "has_fresh_report"
    )


def test_stale_report_keeps_cached_state_visible_after_first_report() -> None:
    """A stale report should not create docked->unavailable->docked flaps."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    is_available = _method_def(report, "is_entity_available")
    constants = _constant_values(is_available)

    assert "report stale; keeping cached state visible" in constants


def test_reported_offline_helper_uses_mqtt_offline_flag() -> None:
    """True offline semantics require explicit MQTT offline and no live channel."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    helper = _method_def(report, "_device_reported_offline")
    attrs = {attr.attr for attr in ast.walk(helper) if isinstance(attr, ast.Attribute)}

    assert "_has_usable_ble_transport" in _called_function_names(helper)
    assert "mqtt_transport_connected" in attrs
    assert "mqtt_reported_offline" in attrs


def test_report_update_probes_stale_state_before_base_update_short_circuit() -> None:
    """Stale report probing must run before the base coordinator returns offline data."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    update = _async_method_def(report, "_async_update_data")

    call_order = [
        call.func.attr
        for call in ast.walk(update)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
    ]

    assert call_order.index("_async_probe_stale_report_if_needed") < call_order.index(
        "_async_update_data"
    )


def test_availability_probe_keeps_last_report_visible_while_in_flight() -> None:
    """A pending probe should avoid an immediate HA unavailable flap."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    is_available = _method_def(report, "is_entity_available")
    calls = _called_function_names(is_available)

    assert "_availability_probe_active" in calls
    assert "_last_report_age" in calls
    assert "_log_stale_availability" in calls


def test_availability_probe_has_failure_backoff() -> None:
    """Failed stale-report probes must not self-amplify every refresh cycle."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    probe = _async_method_def(report, "_async_probe_stale_report_if_needed")
    calls = _called_function_names(probe)
    names = {node.id for node in ast.walk(probe) if isinstance(node, ast.Name)}

    assert "_schedule_next_availability_probe" in calls
    assert "_availability_probe_backoff_seconds" in calls
    assert "_availability_probe_failures" in {
        attr.attr for attr in ast.walk(probe) if isinstance(attr, ast.Attribute)
    }
    assert "REPORT_AVAILABILITY_PROBE_MIN_INTERVAL" in names


def test_fresh_report_wait_can_reconnect_stale_cloud_receive_path() -> None:
    """Missing report replies should restart MQTT receive before giving up."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    wait = _async_method_def(base, "async_wait_for_fresh_report")
    calls = _called_function_names(wait)

    assert "_async_request_fresh_report_snapshot" in calls
    assert "_async_wait_until_fresh_report" in calls
    assert "_async_reconnect_cloud_receive" in calls


def test_cloud_receive_reconnect_has_cooldown_and_restarts_transports() -> None:
    """Receive-path repair must be bounded and actually restart cloud loops."""
    tree = _coordinator_tree()
    base = _class_def(tree, "MammotionBaseUpdateCoordinator")
    reconnect = _async_method_def(base, "_async_reconnect_cloud_receive")
    calls = _called_function_names(reconnect)
    attrs = {attr.attr for attr in ast.walk(reconnect) if isinstance(attr, ast.Attribute)}

    assert "_cloud_receive_reconnect_requesting" in attrs
    assert "_cloud_receive_reconnect_on_cooldown" in calls
    assert "_async_disconnect_cloud_receive_transports" in calls
    assert "_async_connect_cloud_receive_transports" in calls

    cooldown = _method_def(base, "_cloud_receive_reconnect_on_cooldown")
    assert "CLOUD_RECEIVE_RECONNECT_COOLDOWN" in {
        node.id for node in ast.walk(cooldown) if isinstance(node, ast.Name)
    }

    disconnect = _async_method_def(base, "_async_disconnect_cloud_receive_transports")
    connect = _async_method_def(base, "_async_connect_cloud_receive_transports")
    assert "disconnect" in _called_function_names(disconnect)
    assert "connect" in _called_function_names(connect)


def test_manual_report_refresh_waits_for_real_fresh_report() -> None:
    """The HA request_report service path should validate a real report reply."""
    tree = _coordinator_tree()
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    refresh = _async_method_def(report, "async_request_report_refresh")
    calls = _called_function_names(refresh)

    assert "async_wait_for_fresh_report" in calls
    assert "_async_request_report_cfg_guarded" in calls
