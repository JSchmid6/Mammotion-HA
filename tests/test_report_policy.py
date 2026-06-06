"""Regression tests for Mammotion cloud report polling policy."""

from __future__ import annotations

import importlib.util
import sys
from datetime import timedelta
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
def test_active_states_use_active_or_critical_cadence(sys_status: int) -> None:
    """Active report states stay on the active cadence or faster critical cadence."""
    state = make_state(sys_status=sys_status)

    assert policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=False,
        pause_watch_active=False,
    )
    expected_interval = (
        policy.CLOUD_REPORT_DOCK_ACCESS_INTERVAL
        if policy.needs_dock_access_watch(state)
        else policy.CLOUD_REPORT_ACTIVE_INTERVAL
    )
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == expected_interval
    )


@pytest.mark.parametrize(
    "state",
    [
        make_state(
            sys_status=int(policy.WorkMode.MODE_WORKING),
            work_progress=encoded_high_word(10),
        ),
        make_state(
            sys_status=int(policy.WorkMode.MODE_WORKING),
            work_area=encoded_high_word(95),
        ),
        make_state(sys_status=int(policy.WorkMode.MODE_RETURNING)),
    ],
)
def test_dock_access_watch_uses_critical_cadence(state: Any) -> None:
    """Near-finish and returning states need enough lead time for garage access."""
    assert policy.needs_dock_access_watch(state)
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_DOCK_ACCESS_INTERVAL
    )


def test_dock_access_watch_ignores_docked_charge_state() -> None:
    """Already charging states should not keep the garage-access poll alive."""
    state = make_state(
        sys_status=int(policy.WorkMode.MODE_RETURNING),
        charge_state=1,
        work_progress=encoded_high_word(5),
    )

    assert not policy.needs_dock_access_watch(state)


def test_dock_access_watch_does_not_start_too_early() -> None:
    """Normal mowing keeps the cheaper active cadence until the final window."""
    state = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        work_progress=encoded_high_word(20),
        work_area=encoded_high_word(50),
    )

    assert not policy.needs_dock_access_watch(state)
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
    state = make_state(
        charge_state=1,
        bp_info=1,
        work_area=encoded_high_word(40) | 186,
        work_progress=encoded_high_word(60) | 100,
    )

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


def test_terminal_docked_report_ignores_stale_breakpoint_info() -> None:
    """Ready+docked with no open work clears job-watch even when bp_info lingers."""
    state = make_state(charge_state=1, bp_info=7, work_area=186, work_progress=0)

    assert policy.is_terminal_docked_report_state(state)
    assert not policy.has_unfinished_mow_job(state)
    assert not policy.is_recharge_pause_state(state)
    assert not policy.needs_continuous_report_stream(
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
        == policy.CLOUD_REPORT_DOCKED_INTERVAL
    )


def test_docked_report_with_preserved_work_remains_unfinished() -> None:
    """Ready+docked reports still stay watched when real work fields remain open."""
    state = make_state(
        charge_state=1,
        bp_info=7,
        work_area=encoded_high_word(40) | 186,
        work_progress=encoded_high_word(60) | 100,
    )

    assert not policy.is_terminal_docked_report_state(state)
    assert policy.has_unfinished_mow_job(state)
    assert policy.is_recharge_pause_state(state)
    assert policy.needs_continuous_report_stream(
        state,
        continuous_watch_active=True,
        pause_watch_active=False,
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


def test_docked_cadence_is_short_enough_for_automation_freshness() -> None:
    """Docked-full reports must not wait an hour before the next poll."""
    assert timedelta(minutes=15) == policy.CLOUD_REPORT_DOCKED_INTERVAL


def test_report_stale_after_allows_two_missed_polls_with_minimum() -> None:
    """Report stale age tracks missed polls without making active states too tight."""
    assert policy.report_stale_after(timedelta(minutes=15)) == timedelta(minutes=30)
    assert policy.report_stale_after(timedelta(seconds=30)) == timedelta(minutes=5)


def test_last_returning_without_charge_is_not_docked() -> None:
    """A stale last_status returning flag alone must not mean docked."""
    state = make_state(last_status=int(policy.WorkMode.MODE_RETURNING))

    assert not policy.is_docked_report_state(state)
    assert (
        policy.cloud_report_interval(
            state,
            continuous_watch_active=False,
            pause_watch_active=False,
        )
        == policy.CLOUD_REPORT_IDLE_INTERVAL
    )


def test_implausible_recent_full_battery_jump_is_rejected() -> None:
    """A 64 -> 100 jump shortly after a field report is treated as stale."""
    previous = make_state(battery_val=64, charge_state=0)
    current = make_state(battery_val=100, charge_state=1)

    assert policy.report_transition_rejection_reason(
        previous,
        current,
        elapsed=timedelta(seconds=69),
    )


def test_normal_docking_without_battery_jump_is_accepted() -> None:
    """A fresh charge_state update is still accepted when the battery is stable."""
    previous = make_state(battery_val=64, charge_state=0)
    current = make_state(battery_val=64, charge_state=1)

    assert (
        policy.report_transition_rejection_reason(
            previous,
            current,
            elapsed=timedelta(seconds=69),
        )
        is None
    )


def test_stale_full_battery_update_is_accepted_after_sanity_window() -> None:
    """Large battery changes are allowed when the previous report is old."""
    previous = make_state(battery_val=64, charge_state=0)
    current = make_state(battery_val=100, charge_state=1)

    assert (
        policy.report_transition_rejection_reason(
            previous,
            current,
            elapsed=policy.REPORT_SANITY_RECENT_WINDOW + timedelta(seconds=1),
        )
        is None
    )


def test_stale_pause_after_docked_ready_is_rejected_without_time_window() -> None:
    """A docked mower should not resurrect an old paused job snapshot later."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=1,
        battery_val=100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_PAUSE),
        charge_state=1,
        battery_val=64,
        work_area=encoded_high_word(89) | 133,
        work_progress=encoded_high_word(7) | 96,
    )

    reason = policy.report_transition_rejection_reason(
        previous,
        current,
        elapsed=timedelta(hours=1),
    )

    assert reason is not None
    assert "stale pause" in reason


def test_pause_after_active_report_is_accepted_with_stale_charge_state() -> None:
    """A real active-to-pause transition remains valid even if charge_state is stale."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        charge_state=1,
        battery_val=70,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_PAUSE),
        charge_state=1,
        battery_val=69,
        work_area=encoded_high_word(89) | 133,
        work_progress=encoded_high_word(7) | 96,
    )

    assert (
        policy.report_transition_rejection_reason(
            previous,
            current,
            elapsed=timedelta(hours=1),
        )
        is None
    )


def test_stale_ready_after_active_job_is_rejected_without_time_window() -> None:
    """An old docked-ready snapshot must not interrupt a running mow job."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        charge_state=1,
        battery_val=90,
        work_area=encoded_high_word(15) | 186,
        work_progress=encoded_high_word(85) | 100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=1,
        battery_val=100,
        work_area=186,
        work_progress=encoded_high_word(100) | 100,
    )

    reason = policy.report_transition_rejection_reason(
        previous,
        current,
        elapsed=timedelta(minutes=12),
    )

    assert reason is not None
    assert "stale ready" in reason


def test_ready_docked_reset_after_active_job_is_rejected() -> None:
    """A docked-ready reset to empty job fields is stale after active mowing."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        charge_state=0,
        battery_val=79,
        work_area=encoded_high_word(40) | 186,
        work_progress=encoded_high_word(60) | 100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=1,
        battery_val=95,
        work_area=0,
        work_progress=0,
    )

    reason = policy.report_transition_rejection_reason(
        previous,
        current,
        elapsed=timedelta(minutes=30),
    )

    assert reason is not None
    assert "stale ready" in reason


def test_ready_docked_with_preserved_unfinished_job_is_rejected() -> None:
    """A partial ready+docked report must not publish while work remains open."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        charge_state=0,
        battery_val=79,
        work_area=encoded_high_word(40) | 186,
        work_progress=encoded_high_word(60) | 100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=1,
        battery_val=79,
        work_area=encoded_high_word(40) | 186,
        work_progress=encoded_high_word(60) | 100,
    )

    reason = policy.report_transition_rejection_reason(
        previous,
        current,
        elapsed=timedelta(minutes=30),
    )

    assert reason is not None
    assert "stale ready" in reason


def test_real_return_to_ready_after_finished_job_is_accepted() -> None:
    """A finished returning job may become ready even when dock evidence is present."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_RETURNING),
        charge_state=0,
        battery_val=43,
        work_area=encoded_high_word(100) | 186,
        work_progress=100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=1,
        battery_val=43,
        work_area=186,
        work_progress=0,
    )

    assert (
        policy.report_transition_rejection_reason(
            previous,
            current,
            elapsed=timedelta(minutes=12),
        )
        is None
    )


def test_active_to_ready_without_unfinished_current_job_is_accepted() -> None:
    """A real away-from-dock cancellation/reset without job data is not rejected."""
    previous = make_state(
        sys_status=int(policy.WorkMode.MODE_WORKING),
        charge_state=0,
        battery_val=80,
        work_area=encoded_high_word(15) | 186,
        work_progress=encoded_high_word(85) | 100,
    )
    current = make_state(
        sys_status=int(policy.WorkMode.MODE_READY),
        charge_state=0,
        battery_val=80,
        work_area=0,
        work_progress=0,
    )

    assert (
        policy.report_transition_rejection_reason(
            previous,
            current,
            elapsed=timedelta(minutes=12),
        )
        is None
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
