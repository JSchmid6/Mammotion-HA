"""Regression tests for Mammotion cloud report polling policy."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


def _load_report_policy() -> Any:
    """Load report_policy without importing the Home Assistant integration package."""
    module_path = (
        Path(__file__).parents[1]
        / "custom_components"
        / "mammotion"
        / "report_policy.py"
    )
    spec = importlib.util.spec_from_file_location(
        "mammotion_report_policy", module_path
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


policy = _load_report_policy()


def make_state(**overrides: int | None) -> Any:
    """Build a policy state with idle defaults."""
    values = {
        "sys_status": int(policy.WorkMode.MODE_READY),
        "charge_state": 0,
        "battery_val": 80,
        "last_status": None,
        "bp_info": 0,
        "work_area": 0,
        "work_progress": 0,
    }
    values.update(overrides)
    return policy.ReportPolicyState(**values)


def encoded_high_word(value: int) -> int:
    """Encode a value in the high word used by Mammotion report fields."""
    return value << 16


@pytest.mark.parametrize(
    "sys_status",
    [
        int(policy.WorkMode.MODE_WORKING),
        int(policy.WorkMode.MODE_RETURNING),
        int(policy.WorkMode.MODE_CHARGING_PAUSE),
    ],
)
def test_active_states_use_report_stream_cadence(sys_status: int) -> None:
    """Active report states stay on the fast cadence."""
    state = make_state(sys_status=sys_status)

    assert policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=False,
        pause_watch_active=False,
    )
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_ACTIVE_INTERVAL
    )


def test_recharge_pause_with_unfinished_job_uses_watch_when_active() -> None:
    """A charging mower with an unfinished job remains watched once job-watch is active."""
    state = make_state(charge_state=1, bp_info=1)

    assert policy.is_recharge_pause_state(state)
    assert policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=True,
        pause_watch_active=False,
    )
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=True,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_ACTIVE_INTERVAL
    )


def test_pause_away_from_dock_only_streams_during_grace() -> None:
    """A normal pause away from the dock is watched only during the short grace window."""
    state = make_state(sys_status=int(policy.WorkMode.MODE_PAUSE), bp_info=1)

    assert policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=True,
        pause_watch_active=True,
    )
    assert not policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=True,
        pause_watch_active=False,
    )


@pytest.mark.parametrize(
    "state",
    [
        make_state(charge_state=1),
        make_state(battery_val=100),
        make_state(last_status=int(policy.WorkMode.MODE_RETURNING)),
        make_state(charge_state=None, battery_val=100),
    ],
)
def test_docked_states_use_slow_docked_cadence(state: Any) -> None:
    """Docked or probably docked states use the slow docked cadence."""
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_DOCKED_INTERVAL
    )


def test_field_error_uses_minimal_keepalive() -> None:
    """A mower stopped with an error away from charging gets the minimal keepalive."""
    state = make_state(sys_status=int(policy.WorkMode.MODE_LOCK), charge_state=0)

    assert policy.is_field_error_state(state)
    assert not policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=False,
        pause_watch_active=False,
    )
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_FIELD_ERROR_INTERVAL
    )


def test_charging_error_does_not_use_field_keepalive() -> None:
    """Error states that are already charging keep the existing idle error cadence."""
    state = make_state(sys_status=int(policy.WorkMode.MODE_LOCK), charge_state=1)

    assert not policy.is_field_error_state(state)
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_IDLE_INTERVAL
    )


@pytest.mark.parametrize(
    "state",
    [
        make_state(bp_info=7),
        make_state(work_area=encoded_high_word(50)),
        make_state(work_progress=encoded_high_word(12)),
    ],
)
def test_unfinished_job_detection_uses_report_work_fields(state: Any) -> None:
    """Any unfinished-job signal keeps recharge/job-watch logic alive."""
    assert policy.has_unfinished_mow_job(state)
