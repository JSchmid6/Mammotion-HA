"""Policy helpers for Mammotion firmware update entities."""

from __future__ import annotations

from typing import Any


def _version_or_none(value: Any) -> str | None:
    """Return a non-empty version string."""
    if not isinstance(value, str):
        return None
    version = value.strip()
    return version or None


def _release_version(update_check: Any) -> str | None:
    """Return the target release version from an OTA check."""
    version_info = getattr(update_check, "product_version_info_vo", None)
    return _version_or_none(getattr(version_info, "release_version", None))


def latest_firmware_version(
    update_check: Any,
    installed_version: str | None,
) -> str | None:
    """Return the latest firmware version for HA's update entity."""
    release_version = _release_version(update_check)
    if release_version and (
        bool(getattr(update_check, "upgradeable", False))
        or bool(getattr(update_check, "isupgrading", False))
    ):
        return release_version
    return installed_version


def firmware_update_in_progress(
    update_check: Any,
    installed_version: str | None,
    latest_version: str | None,
) -> bool:
    """Return whether a firmware update should be shown as running in HA."""
    if update_check is None or not bool(getattr(update_check, "isupgrading", False)):
        return False

    progress = getattr(update_check, "progress", None)
    if isinstance(progress, int | float) and progress >= 100:
        return False

    installed = _version_or_none(installed_version)
    latest = _version_or_none(latest_version)
    release = _release_version(update_check)
    current = _version_or_none(getattr(update_check, "current_version", None))

    if installed and latest and installed == latest:
        return False
    if installed and release and installed == release:
        return False

    return not bool(installed and current and installed == current and not release)
