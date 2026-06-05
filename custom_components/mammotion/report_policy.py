"""Cloud report polling policy for Mammotion mower status updates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from pymammotion.utility.constant import WorkMode

CLOUD_REPORT_ACTIVE_INTERVAL = timedelta(seconds=30)
CLOUD_REPORT_IDLE_INTERVAL = timedelta(minutes=15)
CLOUD_REPORT_FIELD_ERROR_INTERVAL = timedelta(minutes=10)
CLOUD_REPORT_DOCKED_INTERVAL = timedelta(minutes=15)
CLOUD_REPORT_DOCK_ACCESS_INTERVAL = timedelta(seconds=15)
CLOUD_REPORT_TRANSITION_INTERVAL = timedelta(seconds=15)
REPORT_FRESHNESS_MIN_AGE = timedelta(minutes=5)
REPORT_FRESHNESS_MISSED_POLLS = 2
CLOUD_REPORT_SEND_RESERVE = 40
CLOUD_REPORT_CRITICAL_SEND_RESERVE = 10
CLOUD_REPORT_BUDGET_LOG_INTERVAL = 3600.0
CLOUD_REPORT_STREAM_DURATION = timedelta(minutes=30)
CLOUD_REPORT_STREAM_RENEW_INTERVAL = timedelta(minutes=25)
CLOUD_REPORT_STREAM_DURATION_MS = int(
    CLOUD_REPORT_STREAM_DURATION.total_seconds() * 1000
)
CLOUD_REPORT_JOB_WATCH_DURATION = timedelta(hours=4)
CLOUD_REPORT_PAUSE_GRACE = timedelta(minutes=5)
CLOUD_REPORT_DOCK_ACCESS_WATCH_DURATION = timedelta(minutes=30)
DOCK_ACCESS_LEFT_TIME_THRESHOLD = 10
DOCK_ACCESS_PROGRESS_THRESHOLD = 95
REPORT_SANITY_RECENT_WINDOW = timedelta(minutes=5)
REPORT_SANITY_BATTERY_JUMP = 20
REPORT_SANITY_FULL_BATTERY = 95
CLOUD_REPORT_STREAM_STATES = frozenset(
    {
        int(WorkMode.MODE_WORKING),
        int(WorkMode.MODE_RETURNING),
        int(WorkMode.MODE_CHARGING_PAUSE),
    }
)
CLOUD_REPORT_ERROR_STATES = frozenset(
    {
        int(WorkMode.MODE_LOCK),
        int(WorkMode.MODE_LOCATION_ERROR),
        int(WorkMode.MODE_BOUNDARY_JUMP),
    }
)
PAUSE_REPORT_SOURCE_STATES = frozenset(
    {
        int(WorkMode.MODE_WORKING),
        int(WorkMode.MODE_RETURNING),
        int(WorkMode.MODE_CHARGING_PAUSE),
        int(WorkMode.MODE_PAUSE),
    }
)


@dataclass(frozen=True, slots=True)
class ReportPolicyState:
    """Minimal mower state needed to decide cloud report freshness."""

    sys_status: int | None
    charge_state: int | None
    battery_val: int | None
    last_status: int | None
    bp_info: int | None
    work_area: int | None
    work_progress: int | None


def report_policy_state_from_device(device: Any) -> ReportPolicyState:
    """Extract report-policy state from a pymammotion device object."""
    report_data = getattr(device, "report_data", None)
    dev = getattr(report_data, "dev", None)
    work = getattr(report_data, "work", None)
    return ReportPolicyState(
        sys_status=_int_or_none(getattr(dev, "sys_status", None)),
        charge_state=_int_or_none(getattr(dev, "charge_state", None)),
        battery_val=_int_or_none(getattr(dev, "battery_val", None)),
        last_status=_int_or_none(getattr(dev, "last_status", None)),
        bp_info=_int_or_none(getattr(work, "bp_info", None)),
        work_area=_int_or_none(getattr(work, "area", None)),
        work_progress=_int_or_none(getattr(work, "progress", None)),
    )


def cloud_report_interval(
    state: ReportPolicyState,
    *,
    continuous_watch_active: bool,
    pause_watch_active: bool,
) -> timedelta:
    """Return the cloud snapshot cadence for a mower report state."""
    if needs_dock_access_watch(state):
        return CLOUD_REPORT_DOCK_ACCESS_INTERVAL

    if needs_continuous_report_stream(
        state,
        continuous_watch_active=continuous_watch_active,
        pause_watch_active=pause_watch_active,
    ):
        return CLOUD_REPORT_ACTIVE_INTERVAL

    if state.sys_status is None:
        return CLOUD_REPORT_IDLE_INTERVAL

    if state.sys_status in CLOUD_REPORT_ERROR_STATES:
        return error_report_interval(state)

    if is_docked_report_state(state):
        return CLOUD_REPORT_DOCKED_INTERVAL

    return CLOUD_REPORT_IDLE_INTERVAL


def report_stale_after(interval: timedelta) -> timedelta:
    """Return the report age after which cached state is no longer trustworthy."""
    missed_poll_window = interval * REPORT_FRESHNESS_MISSED_POLLS
    return max(REPORT_FRESHNESS_MIN_AGE, missed_poll_window)


def needs_continuous_report_stream(
    state: ReportPolicyState,
    *,
    continuous_watch_active: bool,
    pause_watch_active: bool,
) -> bool:
    """Return True when state changes need report-stream freshness."""
    if is_active_report_state(state):
        return True
    if (
        pause_watch_active
        and is_pause_report_state(state)
        and has_unfinished_mow_job(state)
    ):
        return True
    return continuous_watch_active and is_recharge_pause_state(state)


def is_active_report_state(state: ReportPolicyState) -> bool:
    """Return True for mower modes that need near-realtime telemetry."""
    return state.sys_status in CLOUD_REPORT_STREAM_STATES


def is_pause_report_state(state: ReportPolicyState) -> bool:
    """Return True when the mower is paused away from the dock."""
    return state.sys_status == int(WorkMode.MODE_PAUSE) and state.charge_state == 0


def needs_dock_access_watch(state: ReportPolicyState) -> bool:
    """Return True when automation should prepare access to the dock."""
    if state.charge_state is not None and state.charge_state != 0:
        return False

    if state.sys_status in {
        int(WorkMode.MODE_RETURNING),
        int(WorkMode.MODE_CHARGING_PAUSE),
    }:
        return True

    if state.sys_status != int(WorkMode.MODE_WORKING):
        return False

    return is_near_mow_completion(state)


def is_near_mow_completion(state: ReportPolicyState) -> bool:
    """Return True when reported job progress is close to automatic return."""
    if state.work_progress is not None:
        left_time = state.work_progress >> 16
        if 0 < left_time <= DOCK_ACCESS_LEFT_TIME_THRESHOLD:
            return True

    if state.work_area is None:
        return False

    completion_percent = state.work_area >> 16
    return DOCK_ACCESS_PROGRESS_THRESHOLD <= completion_percent < 100


def is_docked_report_state(state: ReportPolicyState) -> bool:
    """Return True when the report state is probably docked or charging."""
    if state.charge_state is not None and state.charge_state != 0:
        return True
    if state.sys_status != int(WorkMode.MODE_READY):
        return False
    return state.battery_val == 100


def report_transition_rejection_reason(
    previous: ReportPolicyState,
    current: ReportPolicyState,
    *,
    elapsed: timedelta,
) -> str | None:
    """Return why a report transition is implausible, or None when accepted."""
    if is_stale_docked_pause_transition(previous, current):
        return (
            "stale pause report after docked status "
            f"{previous.sys_status}->{current.sys_status}"
        )

    if is_stale_active_ready_transition(previous, current):
        return (
            "stale ready report after active status "
            f"{previous.sys_status}->{current.sys_status}"
        )

    if elapsed > REPORT_SANITY_RECENT_WINDOW:
        return None
    if previous.battery_val is None or current.battery_val is None:
        return None

    battery_delta = current.battery_val - previous.battery_val
    if battery_delta < REPORT_SANITY_BATTERY_JUMP:
        return None

    if current.battery_val < REPORT_SANITY_FULL_BATTERY:
        return None

    return (
        "implausible recent battery jump "
        f"{previous.battery_val}->{current.battery_val} in {elapsed}"
    )


def is_stale_docked_pause_transition(
    previous: ReportPolicyState,
    current: ReportPolicyState,
) -> bool:
    """Return True when a docked report is followed by an old pause snapshot."""
    if current.sys_status != int(WorkMode.MODE_PAUSE):
        return False
    if previous.sys_status in PAUSE_REPORT_SOURCE_STATES:
        return False
    if not is_docked_report_state(previous):
        return False
    return has_unfinished_mow_job(current)


def is_stale_active_ready_transition(
    previous: ReportPolicyState,
    current: ReportPolicyState,
) -> bool:
    """Return True when an active job is replaced by an old docked snapshot."""
    if previous.sys_status != int(WorkMode.MODE_WORKING):
        return False
    if current.sys_status != int(WorkMode.MODE_READY):
        return False
    if not has_unfinished_mow_job(previous):
        return False
    if not is_docked_report_state(current):
        return False
    if has_unfinished_mow_job(current):
        return True
    return job_progress_regressed(previous, current)


def job_progress_regressed(
    previous: ReportPolicyState,
    current: ReportPolicyState,
) -> bool:
    """Return True when job fields move backwards across a status transition."""
    previous_completion = _high_word_or_none(previous.work_area)
    current_completion = _high_word_or_none(current.work_area)
    if (
        previous_completion is not None
        and current_completion is not None
        and 0 < previous_completion < 100
        and current_completion < previous_completion
    ):
        return True

    previous_left_time = _high_word_or_none(previous.work_progress)
    current_left_time = _high_word_or_none(current.work_progress)
    return (
        previous_left_time is not None
        and current_left_time is not None
        and previous_left_time > 0
        and current_left_time > previous_left_time
    )


def is_field_error_state(state: ReportPolicyState) -> bool:
    """Return True when an error likely stopped the mower away from charging."""
    return state.sys_status in CLOUD_REPORT_ERROR_STATES and state.charge_state == 0


def error_report_interval(state: ReportPolicyState) -> timedelta:
    """Return snapshot cadence for mower error states."""
    if is_field_error_state(state):
        return CLOUD_REPORT_FIELD_ERROR_INTERVAL
    return CLOUD_REPORT_IDLE_INTERVAL


def has_unfinished_mow_job(state: ReportPolicyState) -> bool:
    """Return True when report fields indicate a resumable unfinished job."""
    if state.bp_info is not None and state.bp_info != 0:
        return True

    if state.work_area is not None:
        completion_percent = state.work_area >> 16
        if 0 < completion_percent < 100:
            return True

    if state.work_progress is None:
        return False
    left_time = state.work_progress >> 16
    return left_time > 0


def is_recharge_pause_state(state: ReportPolicyState) -> bool:
    """Return True when a mower is docked or charging with an unfinished job."""
    if state.sys_status == int(WorkMode.MODE_CHARGING_PAUSE):
        return True
    return (
        state.charge_state is not None
        and state.charge_state != 0
        and has_unfinished_mow_job(state)
    )


def _int_or_none(value: Any) -> int | None:
    """Return value coerced to int, or None when it is unavailable."""
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def _high_word_or_none(value: int | None) -> int | None:
    """Return the high word used by Mammotion packed report fields."""
    if value is None:
        return None
    return value >> 16
