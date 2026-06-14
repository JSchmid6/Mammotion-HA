"""Regression tests for Mammotion service target handling."""

from __future__ import annotations

import ast
from pathlib import Path

from homeassistant.helpers import config_validation as cv

SERVICES_PATH = (
    Path(__file__).parents[1] / "custom_components" / "mammotion" / "services.py"
)


def _services_tree() -> ast.Module:
    """Parse services.py without importing the Mammotion integration package."""
    return ast.parse(SERVICES_PATH.read_text(encoding="utf-8"))


def _async_function_def(node: ast.AST, name: str) -> ast.AsyncFunctionDef:
    """Return an async function definition from an AST node."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.AsyncFunctionDef) and child.name == name:
            return child
    raise AssertionError(f"{name} async function not found")


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


def _assigned_names(tree: ast.Module) -> dict[str, ast.Assign]:
    """Return top-level assignments by simple target name."""
    assignments: dict[str, ast.Assign] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                assignments[target.id] = node
    return assignments


def test_home_assistant_target_entity_validation_accepts_lists() -> None:
    """HA target service calls provide entity_id as a list."""
    entity_ids = cv.entity_ids(["lawn_mower.yuka_yvfr8e9d"])

    assert entity_ids == ["lawn_mower.yuka_yvfr8e9d"]


def test_mammotion_service_schemas_accept_target_entity_lists() -> None:
    """Mammotion services should validate HA target entity_id lists."""
    tree = _services_tree()
    assignments = _assigned_names(tree)
    attrs = {
        attr.attr
        for assignment in assignments.values()
        for attr in ast.walk(assignment)
        if isinstance(attr, ast.Attribute)
    }

    assert "entity_ids" in attrs
    assert "ENTITY_IDS_SCHEMA" in assignments
    assert "SINGLE_ENTITY_ID_SCHEMA" in assignments


def test_report_services_use_centralized_entity_list_normalization() -> None:
    """Report services should not pass raw call.data entity_id values through."""
    tree = _services_tree()

    for handler_name in ("_handle_request_report", "_handle_start_report_stream"):
        handler = _async_function_def(tree, handler_name)
        calls = _called_function_names(handler)

        assert "_get_mowers_from_call" in calls
        assert "_get_mower_by_entity_id" not in calls


def test_response_services_require_single_normalized_entity() -> None:
    """GeoJSON response services should normalize target lists to one mower."""
    tree = _services_tree()

    for handler_name in (
        "_handle_get_geojson",
        "_handle_get_mow_path_geojson",
        "_handle_get_mow_progress_geojson",
    ):
        handler = _async_function_def(tree, handler_name)
        calls = _called_function_names(handler)

        assert "_get_single_mower_from_call" in calls
        assert "_get_mower_by_entity_id" not in calls
