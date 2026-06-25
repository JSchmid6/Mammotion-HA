"""Regression tests for the real Mammotion report timestamp sensor."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).parents[1]
COORDINATOR_PATH = ROOT / "custom_components" / "mammotion" / "coordinator.py"
SENSOR_PATH = ROOT / "custom_components" / "mammotion" / "sensor.py"


def _tree(path: Path) -> ast.Module:
    """Parse a Python source file without importing Home Assistant."""
    return ast.parse(path.read_text(encoding="utf-8"))


def _class_def(tree: ast.Module, name: str) -> ast.ClassDef:
    """Return a class definition from a parsed source tree."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"{name} class not found")


def test_report_coordinator_exposes_last_real_report_timestamp() -> None:
    """The coordinator should expose only the patch-marked real report timestamp."""
    tree = _tree(COORDINATOR_PATH)
    report = _class_def(tree, "MammotionReportUpdateCoordinator")
    methods = {
        node.name
        for node in report.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    attrs = {node.attr for node in ast.walk(report) if isinstance(node, ast.Attribute)}
    names = {node.id for node in ast.walk(report) if isinstance(node, ast.Name)}

    assert "last_report_received_at" in methods
    assert "LAST_REAL_REPORT_TIME_ATTR" in names
    assert "mower" in attrs


def test_last_real_report_sensor_is_timestamp_diagnostic() -> None:
    """HA should expose a dedicated timestamp sensor for real report pulses."""
    tree = _tree(SENSOR_PATH)
    constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    attrs = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}

    assert "last_real_report" in constants
    assert "TIMESTAMP" in attrs
    assert "DIAGNOSTIC" in attrs
    assert "last_report_received_at" in attrs
