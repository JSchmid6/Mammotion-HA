"""Regression tests for Mammotion firmware update state policy."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any


def _load_update_policy() -> Any:
    """Load update_policy without importing the Home Assistant integration package."""
    module_path = (
        Path(__file__).parents[1]
        / "custom_components"
        / "mammotion"
        / "update_policy.py"
    )
    spec = importlib.util.spec_from_file_location("mammotion_update_policy", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


policy = _load_update_policy()


def _check(
    *,
    current_version: str = "1.15.21.1",
    release_version: str | None = None,
    progress: int | None = 96,
    upgradeable: bool = False,
    isupgrading: bool | None = True,
) -> SimpleNamespace:
    """Build a small OTA check object with the fields used by the policy."""
    product_version_info_vo = (
        None
        if release_version is None
        else SimpleNamespace(release_version=release_version)
    )
    return SimpleNamespace(
        current_version=current_version,
        product_version_info_vo=product_version_info_vo,
        progress=progress,
        upgradeable=upgradeable,
        isupgrading=isupgrading,
    )


def test_completed_same_version_does_not_leave_update_in_progress() -> None:
    """A stale Mammotion OTA flag must not keep HA stuck in update progress."""
    check = _check(release_version="1.15.21.1")

    latest = policy.latest_firmware_version(check, "1.15.21.1")

    assert latest == "1.15.21.1"
    assert not policy.firmware_update_in_progress(check, "1.15.21.1", latest)


def test_active_different_target_keeps_update_in_progress() -> None:
    """An actual in-flight target release is still shown as running."""
    check = _check(
        current_version="1.15.21.1",
        release_version="1.15.22.0",
        upgradeable=False,
        isupgrading=True,
    )

    latest = policy.latest_firmware_version(check, "1.15.21.1")

    assert latest == "1.15.22.0"
    assert policy.firmware_update_in_progress(check, "1.15.21.1", latest)


def test_complete_progress_clears_update_in_progress() -> None:
    """A terminal progress report is not an in-progress update."""
    check = _check(
        current_version="1.15.21.1",
        release_version="1.15.22.0",
        progress=100,
        isupgrading=True,
    )

    latest = policy.latest_firmware_version(check, "1.15.21.1")

    assert not policy.firmware_update_in_progress(check, "1.15.21.1", latest)
