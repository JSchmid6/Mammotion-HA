"""Compatibility patches for pymammotion behavior used by this integration."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import logging
import time
from copy import deepcopy
from datetime import timedelta
from typing import Any

import betterproto2
from pymammotion.account.registry import AccountSession
from pymammotion.aliyun.cloud_gateway import CloudIOTGateway
from pymammotion.aliyun.exceptions import CloudSetupError
from pymammotion.auth.token_manager import TokenManager
from pymammotion.client import MammotionClient
from pymammotion.data.model.report_info import ReportData
from pymammotion.device.handle import DeviceHandle
from pymammotion.device.state_reducer import MowerStateReducer
from pymammotion.http.http import MammotionHTTP
from pymammotion.transport.base import (
    AuthError,
    LoginFailedError,
    Transport,
    TransportRateLimitedError,
    TransportType,
)
from pymammotion.transport.mqtt import MQTTTransport
from pymammotion.utility.constant.device_constant import WorkMode

from .report_policy import (
    has_unfinished_mow_job as _policy_has_unfinished_mow_job,
)
from .report_policy import (
    is_terminal_docked_report_state,
    report_policy_state_from_device,
    report_transition_rejection_reason,
)

_SEND_MARKED_PATCH_ATTR = "_mammotion_ha_cloud_safe_send_marked"
_START_REPORT_STREAM_PATCH_ATTR = "_mammotion_ha_job_watch_report_stream"
_FORCED_REPORT_STREAM_PATCH_ATTR = "_mammotion_ha_forced_report_stream"
_MQTT_PROTO_DISPATCH_PATCH_ATTR = "_mammotion_ha_mqtt_proto_dispatch_patch"
_REPORT_PARTIAL_MERGE_PATCH_ATTR = "_mammotion_ha_report_partial_merge_patch"
_REPORT_SANITY_PATCH_ATTR = "_mammotion_ha_report_sanity_patch"
_MAMMOTION_PROPERTIES_PATCH_ATTR = "_mammotion_ha_mammotion_properties_patch"
_RAW_MESSAGE_SOURCE_PATCH_ATTR = "_mammotion_ha_raw_message_source_patch"
_LEGACY_LOGIN_PATCH_ATTR = "_mammotion_ha_legacy_login_fallback_patch"
_PATCH_WARNING_LOG_INTERVAL = 900.0
_LOGGER = logging.getLogger(__name__)
_last_patch_warning_log_at: dict[str, float] = {}
_REPORT_SOURCE_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = (
    contextvars.ContextVar("mammotion_ha_report_source", default=None)
)

_original_start_report_stream = DeviceHandle.start_report_stream
_original_mqtt_dispatch = MQTTTransport._dispatch  # noqa: SLF001
_original_report_data_update = ReportData.update
_original_mower_state_apply = MowerStateReducer.apply
_original_apply_mammotion_properties = getattr(
    MowerStateReducer, "apply_mammotion_properties", None
)
_original_on_raw_message = DeviceHandle.on_raw_message
_original_login_and_initiate_cloud = MammotionClient.login_and_initiate_cloud


def _patch_warning_allowed(key: str) -> bool:
    """Return True when a patch warning should be emitted."""
    now = time.monotonic()
    if now - _last_patch_warning_log_at.get(key, 0.0) < _PATCH_WARNING_LOG_INTERVAL:
        return False
    _last_patch_warning_log_at[key] = now
    return True


def _log_patch_warning(key: str, message: str, *args: object) -> None:
    """Log recurring runtime patch warnings without flooding the log."""
    if not _patch_warning_allowed(key):
        return
    _LOGGER.warning(message, *args)


def _send_marked_already_cloud_safe() -> bool:
    """Return True when upstream already scopes BLE sync to BLE transports."""
    try:
        source = inspect.getsource(DeviceHandle._send_marked)  # noqa: SLF001
    except OSError, TypeError:
        return False
    return "transport.transport_type is TransportType.BLE" in source


async def _send_marked_cloud_safe(
    self: DeviceHandle, transport: Transport, payload: bytes
) -> None:
    """Send payload while avoiding BLE sync packets on cloud transports."""
    if transport.transport_type != TransportType.BLE and transport.is_rate_limited:
        raise TransportRateLimitedError(
            f"Transport {transport.transport_type.value} is rate-limited - send blocked"
        )

    last = transport.last_send_monotonic
    if (
        transport.transport_type is TransportType.BLE
        and last != 0.0
        and time.monotonic() - last > 50
    ):
        sync = self.commands.send_todev_ble_sync(sync_type=3)
        await transport.send(
            sync,
            iot_id=self.iot_id,
            firmware_version=_current_firmware_version(self),
        )

    await transport.send(
        payload,
        iot_id=self.iot_id,
        firmware_version=_current_firmware_version(self),
    )
    sent_bus = getattr(self, "_sent_bus", None)
    if sent_bus is not None and not getattr(self, "_stopping", False):
        await sent_bus.emit(payload)


def _current_firmware_version(handle: DeviceHandle) -> str:
    """Return the best known firmware version for pymammotion rate-limit logic."""
    raw = getattr(getattr(handle, "snapshot", None), "raw", None)
    update_check = getattr(raw, "update_check", None)
    version = getattr(update_check, "current_version", None) or "1.0.0.0"
    if version == "1.0.0.0":
        mower_state = getattr(raw, "mower_state", None)
        version = getattr(mower_state, "swversion", None) or version
    return str(version)


def _start_report_stream_allows_job_watch() -> bool:
    """Return True when upstream streams through recharge-pause job watches."""
    try:
        source = inspect.getsource(DeviceHandle.start_report_stream)
    except OSError, TypeError:
        return False
    return "MODE_CHARGING_PAUSE" in source and "bp_info" in source


def _has_unfinished_mow_job(self: DeviceHandle) -> bool:
    """Return True when current report data points to a resumable job."""
    try:
        state = report_policy_state_from_device(self.state_machine.current.raw)
    except AttributeError, TypeError, ValueError:
        return False
    else:
        return _policy_has_unfinished_mow_job(state)


def _is_recharge_pause_report_state(self: DeviceHandle) -> bool:
    """Return True when the mower is charging but a job still needs watching."""
    try:
        state = report_policy_state_from_device(self.state_machine.current.raw)
        if state.sys_status == int(WorkMode.MODE_CHARGING_PAUSE):
            return True
        if is_terminal_docked_report_state(state):
            return False
        return state.charge_state not in (None, 0) and _has_unfinished_mow_job(self)
    except AttributeError, TypeError, ValueError:
        return False


async def _start_report_stream_job_watch(
    self: DeviceHandle, duration_ms: int = 300_000
) -> None:
    """Start a report stream for active jobs, including charging pauses."""
    if not _is_recharge_pause_report_state(self):
        await _original_start_report_stream(self, duration_ms)
        return

    already_streaming = self._report_stream_timer is not None  # noqa: SLF001

    if self._report_stream_timer is not None:  # noqa: SLF001
        self._report_stream_timer.cancel()  # noqa: SLF001
        self._report_stream_timer = None  # noqa: SLF001

    if not self._ble_stream_active:  # noqa: SLF001
        if already_streaming:
            await self._send_report_stream_keep()  # noqa: SLF001
        else:
            await self._send_report_stream_start(duration_ms)  # noqa: SLF001

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    self._report_stream_timer = loop.call_later(  # noqa: SLF001
        duration_ms / 1000,
        self._fire_report_stream_stop,  # noqa: SLF001
    )


async def _start_forced_report_stream(
    self: DeviceHandle, duration_ms: int = 300_000
) -> None:
    """Start a report stream without trusting the currently cached device mode."""
    already_streaming = self._report_stream_timer is not None  # noqa: SLF001

    if self._report_stream_timer is not None:  # noqa: SLF001
        self._report_stream_timer.cancel()  # noqa: SLF001
        self._report_stream_timer = None  # noqa: SLF001

    if not self._ble_stream_active:  # noqa: SLF001
        if already_streaming:
            await self._send_report_stream_keep()  # noqa: SLF001
        else:
            await self._send_report_stream_start(duration_ms)  # noqa: SLF001

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    self._report_stream_timer = loop.call_later(  # noqa: SLF001
        duration_ms / 1000,
        self._fire_report_stream_stop,  # noqa: SLF001
    )


def apply_pymammotion_patches() -> None:
    """Apply pymammotion compatibility patches once."""
    _patch_client_login()
    _patch_device_handle_send_and_streams()
    _patch_mqtt_transport()
    _patch_report_reducer()
    _patch_raw_message_source()


def _patch_client_login() -> None:
    """Patch Mammotion cloud login for accounts accepted by the legacy endpoint."""
    if not getattr(MammotionClient, _LEGACY_LOGIN_PATCH_ATTR, False):
        setattr(
            MammotionClient,
            "login_and_initiate_cloud",
            _login_and_initiate_cloud_with_legacy_fallback,
        )
        setattr(MammotionClient, _LEGACY_LOGIN_PATCH_ATTR, True)


async def _login_and_initiate_cloud_with_legacy_fallback(
    self: MammotionClient,
    account: str,
    password: str,
    session: Any | None = None,
) -> None:
    """Log in and register devices, falling back for legacy shared accounts."""
    await self._sign_out_existing_session(account)  # noqa: SLF001
    mammotion_http = MammotionHTTP(session=session, ha_version=self._ha_version)  # noqa: SLF001
    login_resp, legacy_login_used = await _login_mammotion_http_with_legacy_fallback(
        mammotion_http,
        account,
        password,
    )
    if getattr(login_resp, "code", None) != 0:
        raise LoginFailedError(account, getattr(login_resp, "msg", "Login failed"))

    device_list_owned_resp = await mammotion_http.get_user_device_list()
    device_list_resp = await mammotion_http.get_user_shared_device_page()
    if device_list_resp.data and device_list_resp.data.records:
        pending_by_batch: dict[str, list[int]] = {}
        for record in device_list_resp.data.records:
            if record.is_receiver == 1 and record.status == -1:
                pending_by_batch.setdefault(record.batch_id, []).append(
                    int(record.record_id)
                )
        for batch_id, record_ids in pending_by_batch.items():
            await mammotion_http.confirm_share(batch_id, record_ids)

    device_page_resp = await mammotion_http.get_user_device_page()
    mammotion_records = (
        device_page_resp.data.records if device_page_resp.data else []
    ) or []

    owned_iot_id_map: dict[str, str] = {
        d.device_name: d.iot_id
        for d in (device_list_owned_resp.data or [])
        if d.device_name and d.iot_id
    }

    acct_session = AccountSession(
        account_id=account,
        email=account,
        password=password,
        mammotion_http=mammotion_http,
    )
    acct_session.user_account = self._extract_user_account(mammotion_http)  # noqa: SLF001

    await _bootstrap_legacy_aliyun_if_needed(
        self,
        account,
        mammotion_http,
        acct_session,
        owned_iot_id_map,
        legacy_login_used=legacy_login_used,
        mammotion_records=mammotion_records,
    )

    if mammotion_records:
        await self._bootstrap_mammotion_mqtt(  # noqa: SLF001
            account,
            mammotion_http,
            acct_session,
            owned_iot_id_map,
        )

    await self._account_registry.register(acct_session)  # noqa: SLF001


async def _login_mammotion_http_with_legacy_fallback(
    mammotion_http: MammotionHTTP,
    account: str,
    password: str,
) -> tuple[Any, bool]:
    """Return a successful login response, trying legacy auth after v2 failure."""
    login_resp = await mammotion_http.login_v2(account, password)
    if getattr(login_resp, "code", None) == 0:
        return login_resp, False

    legacy_resp = await mammotion_http.login(account, password)
    if getattr(legacy_resp, "code", None) == 0:
        _log_patch_warning(
            "legacy-login-fallback",
            (
                "pymammotion login_v2 failed for account %s with %s; "
                "legacy Mammotion login succeeded"
            ),
            _redacted_account(account),
            getattr(login_resp, "msg", None),
        )
        return legacy_resp, True

    return login_resp, False


async def _bootstrap_legacy_aliyun_if_needed(
    client: MammotionClient,
    account: str,
    mammotion_http: MammotionHTTP,
    acct_session: AccountSession,
    owned_iot_id_map: dict[str, str],
    *,
    legacy_login_used: bool,
    mammotion_records: list[Any],
) -> None:
    """Register legacy Aliyun devices for accounts that v2 cannot enumerate."""
    if not legacy_login_used and mammotion_records:
        return

    cloud_client = CloudIOTGateway(mammotion_http)
    try:
        await client._connect_iot(cloud_client)  # noqa: SLF001
    except (
        AuthError,
        CloudSetupError,
        OSError,
        RuntimeError,
        TimeoutError,
        TypeError,
        ValueError,
    ) as exc:
        if legacy_login_used:
            _log_patch_warning(
                "legacy-aliyun-bootstrap",
                (
                    "pymammotion legacy login succeeded for account %s, but "
                    "Aliyun device bootstrap failed with %s: %s"
                ),
                _redacted_account(account),
                type(exc).__name__,
                exc,
            )
        return

    shared_notice = await cloud_client.get_shared_notice_list()
    if shared_notice.data and shared_notice.data.data:
        pending = [d.record_id for d in shared_notice.data.data if d.status == -1]
        if pending:
            await cloud_client.confirm_share(pending)

    if (
        cloud_client.aep_response is None
        or cloud_client.region_response is None
        or cloud_client.session_by_authcode_response is None
        or cloud_client.session_by_authcode_response.data is None
    ):
        return

    acct_session.cloud_client = cloud_client
    acct_session.token_manager = TokenManager(account, mammotion_http, cloud_client)
    acct_session.token_manager.on_credentials_updated = client.on_credentials_updated
    al_transport = client._setup_aliyun_transport(cloud_client, acct_session)  # noqa: SLF001
    acct_session.aliyun_transport = al_transport
    user_account = acct_session.user_account

    device_response = cloud_client.devices_by_account_response
    devices = device_response.data.data if device_response and device_response.data else []
    for device in devices:
        if not device.device_name:
            continue
        iot_id = owned_iot_id_map.get(device.device_name) or device.iot_id
        await client._register_aliyun_device(  # noqa: SLF001
            device.device_name,
            iot_id,
            al_transport,
            user_account,
            device.product_key,
            token_manager=acct_session.token_manager,
        )
        acct_session.device_ids.add(device.device_name)
    await al_transport.connect()


def _redacted_account(account: str) -> str:
    """Return a log-safe account identifier."""
    if "@" in account:
        local, domain = account.split("@", 1)
        prefix = local[:2] if len(local) > 2 else local[:1]
        return f"{prefix}***@{domain}"
    if len(account) <= 4:
        return "*" * len(account)
    return f"{account[:2]}***{account[-2:]}"


def _patch_device_handle_send_and_streams() -> None:
    """Patch DeviceHandle send and report-stream behavior."""
    if not getattr(DeviceHandle, _SEND_MARKED_PATCH_ATTR, False):
        if not _send_marked_already_cloud_safe():
            setattr(DeviceHandle, "_send_marked", _send_marked_cloud_safe)
        setattr(DeviceHandle, _SEND_MARKED_PATCH_ATTR, True)

    if not _start_report_stream_allows_job_watch() and not getattr(
        DeviceHandle, _START_REPORT_STREAM_PATCH_ATTR, False
    ):
        setattr(DeviceHandle, "start_report_stream", _start_report_stream_job_watch)
        setattr(DeviceHandle, _START_REPORT_STREAM_PATCH_ATTR, True)

    if not getattr(DeviceHandle, _FORCED_REPORT_STREAM_PATCH_ATTR, False):
        setattr(DeviceHandle, "start_forced_report_stream", _start_forced_report_stream)
        setattr(DeviceHandle, _FORCED_REPORT_STREAM_PATCH_ATTR, True)


def _patch_mqtt_transport() -> None:
    """Patch MQTT proto dispatch routing."""
    if not _mqtt_dispatch_proto_aware() and not getattr(
        MQTTTransport, _MQTT_PROTO_DISPATCH_PATCH_ATTR, False
    ):
        setattr(MQTTTransport, "_dispatch", _mqtt_dispatch_with_proto_routing)
        setattr(MQTTTransport, _MQTT_PROTO_DISPATCH_PATCH_ATTR, True)


def _patch_report_reducer() -> None:
    """Patch report merging and sanity checks."""
    if not getattr(ReportData, _REPORT_PARTIAL_MERGE_PATCH_ATTR, False):
        setattr(ReportData, "update", _report_data_update_preserve_partial_state)
        setattr(ReportData, _REPORT_PARTIAL_MERGE_PATCH_ATTR, True)

    if not getattr(MowerStateReducer, _REPORT_SANITY_PATCH_ATTR, False):
        setattr(MowerStateReducer, "apply", _mower_state_apply_with_report_sanity)
        setattr(MowerStateReducer, _REPORT_SANITY_PATCH_ATTR, True)

    if _original_apply_mammotion_properties is not None and not getattr(
        MowerStateReducer, _MAMMOTION_PROPERTIES_PATCH_ATTR, False
    ):
        setattr(
            MowerStateReducer,
            "apply_mammotion_properties",
            _apply_mammotion_properties_preserve_absent_scalars,
        )
        setattr(MowerStateReducer, _MAMMOTION_PROPERTIES_PATCH_ATTR, True)


def _patch_raw_message_source() -> None:
    """Patch DeviceHandle so report diagnostics know the inbound transport."""
    if not getattr(DeviceHandle, _RAW_MESSAGE_SOURCE_PATCH_ATTR, False):
        setattr(DeviceHandle, "on_raw_message", _on_raw_message_with_source)
        setattr(DeviceHandle, _RAW_MESSAGE_SOURCE_PATCH_ATTR, True)


async def _on_raw_message_with_source(
    self: DeviceHandle,
    payload: bytes,
    transport_type: TransportType = TransportType.CLOUD_ALIYUN,
) -> None:
    """Track the inbound transport while a raw device message is reduced."""
    if _REPORT_SOURCE_CONTEXT.get() is not None:
        await _original_on_raw_message(self, payload, transport_type)
        return

    token = _REPORT_SOURCE_CONTEXT.set(
        {"transport": str(transport_type.value), "topic": "unknown"}
    )
    try:
        await _original_on_raw_message(self, payload, transport_type)
    finally:
        _REPORT_SOURCE_CONTEXT.reset(token)


def _report_data_update_preserve_partial_state(self: ReportData, data: Any) -> None:
    """Update report data without letting partial dev/connect reports reset state."""
    previous_dev = deepcopy(self.dev) if data.dev is not None else None
    previous_connect = deepcopy(self.connect) if data.connect is not None else None
    previous_work = deepcopy(self.work) if data.work is not None else None

    _original_report_data_update(self, data)

    if previous_dev is not None:
        self.dev = _merge_model_update(previous_dev, data.dev)
        _normalize_partial_dev_update(self.dev, data.dev)
    if previous_connect is not None:
        self.connect = _merge_model_update(previous_connect, data.connect)
    if previous_work is not None:
        self.work = _merge_model_update(previous_work, data.work)


def _normalize_partial_dev_update(dev: Any, proto_update: Any) -> None:
    """Keep preserved dev fields compatible with an explicit status update."""
    incoming = proto_update.to_dict(casing=betterproto2.Casing.SNAKE)
    if "sys_status" not in incoming or "charge_state" in incoming:
        return

    if incoming["sys_status"] not in {
        int(WorkMode.MODE_WORKING),
        int(WorkMode.MODE_RETURNING),
    }:
        return

    if getattr(dev, "charge_state", 0) == 0:
        return

    _log_patch_warning(
        "partial-report-active-charge-clear",
        (
            "pymammotion report reducer cleared stale charge_state after "
            "active sys_status without charge_state: sys_status=%s; "
            "previous_charge_state=%s; source=%s; incoming_dev=%s"
        ),
        incoming["sys_status"],
        getattr(dev, "charge_state", None),
        _current_report_source(),
        incoming,
    )
    dev.charge_state = 0


def _merge_model_update(current: Any, proto_update: Any) -> Any:
    """Merge non-default proto fields into an existing dataclass model."""
    incoming = proto_update.to_dict(casing=betterproto2.Casing.SNAKE)
    if not incoming:
        return current

    merged = current.to_dict()
    _deep_update(merged, incoming)
    return type(current).from_dict(merged)


def _deep_update(target: dict[str, Any], updates: dict[str, Any]) -> None:
    """Recursively merge update values into a dict model."""
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value


def _mower_state_apply_with_report_sanity(
    self: MowerStateReducer, current: Any, message: Any
) -> Any:
    """Apply mower state while rejecting implausible stale report snapshots."""
    updated = _original_mower_state_apply(self, current, message)
    if not _message_updates_report_data(message):
        return updated

    now = time.monotonic()
    last_accepted_at = getattr(self, "_mammotion_ha_last_report_accept_at", 0.0)
    if last_accepted_at:
        previous_state = report_policy_state_from_device(current)
        updated_state = report_policy_state_from_device(updated)
        reason = report_transition_rejection_reason(
            previous_state,
            updated_state,
            elapsed=timedelta(seconds=now - last_accepted_at),
        )
        if reason is not None:
            _log_patch_warning(
                "report-sanity",
                (
                    "pymammotion report reducer rejected stale-looking report: "
                    "%s; source=%s; incoming=%s; previous=%s; reduced=%s"
                ),
                reason,
                _current_report_source(),
                _incoming_report_fields(message),
                previous_state,
                updated_state,
            )
            return current
        _log_report_diagnostic_if_needed(previous_state, updated_state, message)

    setattr(self, "_mammotion_ha_last_report_accept_at", now)
    return updated


def _apply_mammotion_properties_preserve_absent_scalars(
    self: MowerStateReducer,
    current: Any,
    properties: Any,
) -> Any:
    """Apply Mammotion property pushes without turning absent scalars into state."""
    if _original_apply_mammotion_properties is None:
        return current

    updated = _original_apply_mammotion_properties(self, current, properties)
    property_keys = _mammotion_property_keys()
    restored: list[str] = []

    if not _property_key_present(
        property_keys, "batteryPercentage", "battery_percentage"
    ):
        updated.report_data.dev.battery_val = current.report_data.dev.battery_val
        restored.append("batteryPercentage")
    if not _property_key_present(property_keys, "deviceState", "device_state"):
        updated.report_data.dev.sys_status = current.report_data.dev.sys_status
        restored.append("deviceState")
    if not _property_key_present(property_keys, "knifeHeight", "knife_height"):
        updated.report_data.work.knife_height = current.report_data.work.knife_height
        restored.append("knifeHeight")

    if restored:
        _log_patch_warning(
            "mammotion-property-partial-scalars",
            (
                "pymammotion property reducer preserved absent scalar defaults: "
                "restored=%s; source=%s"
            ),
            restored,
            _current_report_source(),
        )
    return updated


def _mammotion_property_keys() -> set[str]:
    """Return raw Mammotion property keys from the active dispatch context."""
    source = _current_report_source()
    property_keys = source.get("property_keys")
    if not isinstance(property_keys, (list, tuple, set, frozenset)):
        return set()
    return {str(key) for key in property_keys}


def _property_key_present(property_keys: set[str], *aliases: str) -> bool:
    """Return True when a Mammotion property key was present in the raw payload."""
    return any(alias in property_keys for alias in aliases)


def _log_report_diagnostic_if_needed(
    previous: Any,
    updated: Any,
    message: Any,
) -> None:
    """Log accepted report transitions that need postmortem evidence."""
    reason = _accepted_report_diagnostic_reason(previous, updated)
    if reason is None:
        return

    _log_patch_warning(
        f"report-diagnostic:{reason}:{previous.sys_status}->{updated.sys_status}",
        (
            "pymammotion report reducer accepted diagnostic report: "
            "%s; envelope=%s; source=%s; incoming=%s; previous=%s; reduced=%s"
        ),
        reason,
        _report_message_envelope(message),
        _current_report_source(),
        _incoming_report_fields(message),
        previous,
        updated,
    )


def _accepted_report_diagnostic_reason(previous: Any, updated: Any) -> str | None:
    """Return why an accepted report should be visible in logs."""
    if _is_active_docked_hybrid(updated):
        return "active status with dock/charge evidence"
    if previous.sys_status != updated.sys_status:
        return "sys_status changed"
    return None


def _is_active_docked_hybrid(state: Any) -> bool:
    """Return True when a report is both active and docked/charging."""
    return state.sys_status in {
        int(WorkMode.MODE_WORKING),
        int(WorkMode.MODE_RETURNING),
        int(WorkMode.MODE_CHARGING_PAUSE),
    } and state.charge_state not in (None, 0)


def _message_updates_report_data(message: Any) -> bool:
    """Return True when a Luba message carries mower report data."""
    try:
        if betterproto2.which_one_of(message, "LubaSubMsg")[0] != "sys":
            return False
        return betterproto2.which_one_of(message.sys, "SubSysMsg")[0] == (
            "toapp_report_data"
        )
    except AttributeError, TypeError, ValueError:
        return False


def _incoming_report_fields(message: Any) -> dict[str, Any]:
    """Return compact raw report fields carried by a toapp_report_data message."""
    try:
        report = message.sys.toapp_report_data
    except AttributeError:
        return {}

    fields: dict[str, Any] = {}
    if getattr(report, "dev", None) is not None:
        fields["dev"] = report.dev.to_dict(casing=betterproto2.Casing.SNAKE)
    if getattr(report, "work", None) is not None:
        fields["work"] = report.work.to_dict(casing=betterproto2.Casing.SNAKE)
    if getattr(report, "connect", None) is not None:
        fields["connect"] = report.connect.to_dict(casing=betterproto2.Casing.SNAKE)
    return fields


def _report_message_envelope(message: Any) -> dict[str, Any]:
    """Return compact LubaMsg envelope fields for report diagnostics."""
    envelope: dict[str, Any] = {}
    for key in (
        "msgtype",
        "sender",
        "rcver",
        "msgattr",
        "seqs",
        "version",
        "subtype",
        "timestamp",
    ):
        if not hasattr(message, key):
            continue
        envelope[key] = _compact_log_value(getattr(message, key))

    try:
        sub_name, sub_val = betterproto2.which_one_of(message, "LubaSubMsg")
        envelope["sub_msg"] = sub_name
        if sub_val is not None:
            leaf_name, _ = betterproto2.which_one_of(sub_val, "SubSysMsg")
            envelope["sys_msg"] = leaf_name
    except AttributeError, TypeError, ValueError:
        pass
    return envelope


def _compact_log_value(value: Any) -> Any:
    """Return a JSON-like scalar useful in diagnostic logs."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    name = getattr(value, "name", None)
    if name is not None:
        return str(name)

    try:
        return int(value)
    except TypeError, ValueError:
        return str(value)


def _current_report_source() -> dict[str, Any]:
    """Return the best known transport source for the report currently reducing."""
    return _REPORT_SOURCE_CONTEXT.get() or {"transport": "unknown", "topic": "unknown"}


def _mqtt_dispatch_proto_aware() -> bool:
    """Return True when upstream parses /sys/proto/<pk>/<dn> topics correctly."""
    try:
        source = inspect.getsource(MQTTTransport._dispatch)  # noqa: SLF001
    except OSError, TypeError:
        return False
    return "is_raw_proto" in source or (
        ("is_proto_topic" in source or 'parts[2] == "proto"' in source)
        and "down_raw" in source
        and "on_device_message(iot_id, raw" in source
    )


async def _mqtt_dispatch_with_proto_routing(
    self: MQTTTransport, topic: str, raw: bytes
) -> None:
    """Route direct MQTT raw protobuf topics with the correct pk/dn offset."""
    token = _REPORT_SOURCE_CONTEXT.set(_mqtt_source_context(self, topic, raw))
    try:
        await _mqtt_dispatch_with_proto_routing_inner(self, topic, raw)
    finally:
        _REPORT_SOURCE_CONTEXT.reset(token)


def _mqtt_source_context(
    transport: MQTTTransport,
    topic: str,
    raw: bytes,
) -> dict[str, Any]:
    """Return MQTT source metadata for report diagnostics."""
    source: dict[str, Any] = {
        "transport": str(transport.transport_type.value),
        "topic": topic,
    }
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError, UnicodeDecodeError, ValueError:
        source["payload"] = "raw-protobuf"
        return source

    params = parsed.get("params")
    if not isinstance(params, dict):
        return source

    for key in ("iotId", "identifier", "time"):
        if key in params:
            source[key] = params[key]
    source["property_keys"] = tuple(sorted(str(key) for key in params))
    source["payload"] = "json-envelope"
    return source


async def _mqtt_dispatch_with_proto_routing_inner(
    self: MQTTTransport, topic: str, raw: bytes
) -> None:
    """Route direct MQTT raw protobuf topics with source context already set."""
    is_proto_topic = topic.startswith("/sys/proto/")
    is_down_raw_topic = topic.endswith("/thing/model/down_raw")
    if not (is_proto_topic or is_down_raw_topic) or self.on_device_message is None:
        await _original_mqtt_dispatch(self, topic, raw)
        return

    parts = topic.split("/")
    if len(parts) < (5 if is_proto_topic else 4):
        await _original_mqtt_dispatch(self, topic, raw)
        return

    product_key = parts[3] if is_proto_topic else parts[2]
    device_name = parts[4] if is_proto_topic else parts[3]
    iot_id = self._device_to_iot.get((product_key, device_name))
    if not iot_id:
        _LOGGER.debug(
            "MQTTTransport: could not route raw protobuf message on topic %s", topic
        )
        return

    decoded = self._unwrap_envelope(topic, raw)
    if decoded is None:
        decoded = raw

    await self.on_device_message(iot_id, decoded)
