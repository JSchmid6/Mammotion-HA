"""Regression tests for Mammotion camera setup behavior."""

from __future__ import annotations

import ast
from pathlib import Path

CAMERA_PATH = (
    Path(__file__).parents[1] / "custom_components" / "mammotion" / "camera.py"
)


def _camera_tree() -> ast.Module:
    """Parse camera.py without importing the Mammotion integration package."""
    return ast.parse(CAMERA_PATH.read_text(encoding="utf-8"))


def _async_function_def(tree: ast.Module, name: str) -> ast.AsyncFunctionDef:
    """Return an async function definition from an AST module."""
    for child in tree.body:
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


def test_camera_setup_does_not_block_on_stream_token_refresh() -> None:
    """Camera setup must not fetch stream tokens before entities are added."""
    tree = _camera_tree()
    setup = _async_function_def(tree, "async_setup_entry")
    calls = _called_function_names(setup)

    assert "async_add_entities" in calls
    assert "async_create_task" in calls
    assert "_async_prefetch_camera_stream" in calls
    assert "async_check_stream_expiry" not in calls


def test_camera_stream_prefetch_owns_stream_token_refresh() -> None:
    """Stream-token prefetch should remain in a background helper."""
    tree = _camera_tree()
    prefetch = _async_function_def(tree, "_async_prefetch_camera_stream")
    calls = _called_function_names(prefetch)

    assert "async_check_stream_expiry" in calls
