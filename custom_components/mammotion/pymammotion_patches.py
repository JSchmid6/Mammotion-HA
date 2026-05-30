"""Compatibility patches for pymammotion behavior used by this integration."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from copy import deepcopy
from datetime import timedelta
from typing import Any

import betterproto2
from pymammotion.auth.token_manager import MQTTCredentials, TokenManager
from pymammotion.data.model.report_info import ReportData
from pymammotion.device.handle import DeviceHandle
from pymammotion.device.state_reducer import MowerStateReducer
from pymammotion.transport.base import (
    AuthError,
    ReLoginRequiredError,
    Transport,
    TransportRateLimitedError,
    TransportType,
)
from pymammotion.transport.mqtt import MQTTTransport
from pymammotion.utility.constant.device_constant import WorkMode

from .report_policy import (
    report_policy_state_from_device,
    report_transition_rejection_reason,
)

_SEND_MARKED_PATCH_ATTR = "_mammotion_ha_cloud_safe_send_marked"
_START_REPORT_STREAM_PATCH_ATTR = "_mammotion_ha_job_watch_report_stream"
_FORCED_REPORT_STREAM_PATCH_ATTR = "_mammotion_ha_forced_report_stream"
_MQTT_REALTIME_TOPICS_PATCH_ATTR = "_mammotion_ha_mqtt_realtime_topics_patch"
_MQTT_PROTO_DISPATCH_PATCH_ATTR = "_mammotion_ha_mqtt_proto_dispatch_patch"
_TRANSPORT_AUTH_PATCH_ATTR = "_mammotion_ha_transport_auth_state_patch"
_RECORD_SEND_PATCH_ATTR = "_mammotion_ha_record_send_patch"
_MQTT_SEND_PATCH_ATTR = "_mammotion_ha_mqtt_send_patch"
_INVOKE_REFRESH_PATCH_ATTR = "_mammotion_ha_invoke_refresh_patch"
_MQTT_CREDS_PATCH_ATTR = "_mammotion_ha_mqtt_creds_patch"
_REPORT_PARTIAL_MERGE_PATCH_ATTR = "_mammotion_ha_report_partial_merge_patch"
_REPORT_SANITY_PATCH_ATTR = "_mammotion_ha_report_sanity_patch"
_INVOKE_REFRESH_COOLDOWN = 30.0
_PATCH_WARNING_LOG_INTERVAL = 900.0
_LOGGER = logging.getLogger(__name__)
_last_patch_warning_log_at: dict[str, float] = {}

_original_register_device = MQTTTransport.register_device
_original_start_report_stream = DeviceHandle.start_report_stream
_original_mqtt_dispatch = MQTTTransport._dispatch  # noqa: SLF001
_original_mqtt_send = MQTTTransport.send
_original_force_refresh_invoke_token = TokenManager.force_refresh_invoke_token
_original_refresh_mqtt_creds = TokenManager.refresh_mqtt_creds
_original_report_data_update = ReportData.update
_original_mower_state_apply = MowerStateReducer.apply


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
        await transport.send(sync, iot_id=self.iot_id)

    await transport.send(payload, iot_id=self.iot_id)


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
        work = self.state_machine.current.raw.report_data.work
        if int(work.bp_info) != 0:
            return True

        completion_percent = int(work.area) >> 16
        if 0 < completion_percent < 100:
            return True

        left_time = int(work.progress) >> 16
        return left_time > 0
    except AttributeError, TypeError, ValueError:
        return False


def _is_recharge_pause_report_state(self: DeviceHandle) -> bool:
    """Return True when the mower is charging but a job still needs watching."""
    try:
        dev = self.state_machine.current.raw.report_data.dev
        if int(dev.sys_status) == int(WorkMode.MODE_CHARGING_PAUSE):
            return True
        return int(dev.charge_state) != 0 and _has_unfinished_mow_job(self)
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

    if not getattr(MQTTTransport, _MQTT_REALTIME_TOPICS_PATCH_ATTR, False):
        setattr(MQTTTransport, "register_device", _register_device_with_realtime_topics)
        setattr(MQTTTransport, _MQTT_REALTIME_TOPICS_PATCH_ATTR, True)

    if not _mqtt_dispatch_proto_aware() and not getattr(
        MQTTTransport, _MQTT_PROTO_DISPATCH_PATCH_ATTR, False
    ):
        setattr(MQTTTransport, "_dispatch", _mqtt_dispatch_with_proto_routing)
        setattr(MQTTTransport, _MQTT_PROTO_DISPATCH_PATCH_ATTR, True)

    if not _transport_auth_state_already_present() and not getattr(
        Transport, _TRANSPORT_AUTH_PATCH_ATTR, False
    ):
        Transport.is_usable = property(_transport_is_usable_auth_safe)  # type: ignore[attr-defined,method-assign]
        setattr(Transport, "mark_auth_failed", _mark_auth_failed)
        setattr(Transport, "clear_auth_failed", _clear_auth_failed)
        setattr(Transport, _TRANSPORT_AUTH_PATCH_ATTR, True)

    if not _record_send_warns_once() and not getattr(
        Transport, _RECORD_SEND_PATCH_ATTR, False
    ):
        setattr(Transport, "record_send", _record_send_warn_once)
        setattr(Transport, _RECORD_SEND_PATCH_ATTR, True)

    if not _mqtt_send_rate_auth_safe() and not getattr(
        MQTTTransport, _MQTT_SEND_PATCH_ATTR, False
    ):
        setattr(MQTTTransport, "send", _mqtt_send_with_rate_and_auth_guard)
        setattr(MQTTTransport, _MQTT_SEND_PATCH_ATTR, True)

    if not _invoke_refresh_has_cooldown() and not getattr(
        TokenManager, _INVOKE_REFRESH_PATCH_ATTR, False
    ):
        setattr(
            TokenManager,
            "force_refresh_invoke_token",
            _force_refresh_invoke_token_with_cooldown,
        )
        setattr(TokenManager, _INVOKE_REFRESH_PATCH_ATTR, True)

    if not _refresh_mqtt_creds_rejects_none() and not getattr(
        TokenManager, _MQTT_CREDS_PATCH_ATTR, False
    ):
        setattr(TokenManager, "refresh_mqtt_creds", _refresh_mqtt_creds_require_result)
        setattr(TokenManager, _MQTT_CREDS_PATCH_ATTR, True)

    if not getattr(ReportData, _REPORT_PARTIAL_MERGE_PATCH_ATTR, False):
        setattr(ReportData, "update", _report_data_update_preserve_partial_state)
        setattr(ReportData, _REPORT_PARTIAL_MERGE_PATCH_ATTR, True)

    if not getattr(MowerStateReducer, _REPORT_SANITY_PATCH_ATTR, False):
        setattr(MowerStateReducer, "apply", _mower_state_apply_with_report_sanity)
        setattr(MowerStateReducer, _REPORT_SANITY_PATCH_ATTR, True)


def _report_data_update_preserve_partial_state(self: ReportData, data: Any) -> None:
    """Update report data without letting partial dev/connect reports reset state."""
    previous_dev = deepcopy(self.dev) if data.dev is not None else None
    previous_connect = deepcopy(self.connect) if data.connect is not None else None

    _original_report_data_update(self, data)

    if previous_dev is not None:
        self.dev = _merge_model_update(previous_dev, data.dev)
    if previous_connect is not None:
        self.connect = _merge_model_update(previous_connect, data.connect)


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
        reason = report_transition_rejection_reason(
            report_policy_state_from_device(current),
            report_policy_state_from_device(updated),
            elapsed=timedelta(seconds=now - last_accepted_at),
        )
        if reason is not None:
            _log_patch_warning(
                "report-sanity",
                "pymammotion report reducer rejected stale-looking report: %s",
                reason,
            )
            return current

    setattr(self, "_mammotion_ha_last_report_accept_at", now)
    return updated


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


def _register_device_with_realtime_topics(
    self: MQTTTransport, product_key: str, device_name: str, iot_id: str
) -> None:
    """Register Mammotion MQTT devices with additional realtime push topics."""
    _original_register_device(self, product_key, device_name, iot_id)
    base_topic = f"/sys/{product_key}/{device_name}"
    for topic in (
        f"{base_topic}/thing/event/+/post",
        f"/sys/proto/{product_key}/{device_name}/thing/event/+/post",
        f"{base_topic}/app/down/thing/status",
        f"{base_topic}/app/down/thing/properties",
        f"{base_topic}/app/down/thing/events",
        f"{base_topic}/app/down/thing/model/down_raw",
        f"{base_topic}/app/down/_thing/event/notify",
        f"{base_topic}/app/down/thing/event/property/post_reply",
        f"{base_topic}/thing/event/property/post",
    ):
        self.add_topic(topic)


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


def _transport_auth_state_already_present() -> bool:
    """Return True when upstream already tracks auth-failed transport state."""
    if not all(
        hasattr(Transport, name) for name in ("mark_auth_failed", "clear_auth_failed")
    ):
        return False
    try:
        source = inspect.getsource(Transport.is_usable.fget)  # type: ignore[arg-type]
    except OSError, TypeError:
        return False
    return "_auth_failed" in source


def _transport_is_usable_auth_safe(self: Transport) -> bool:
    """Return whether the transport is usable after local auth-failure gating."""
    return not getattr(self, "_auth_failed", False)


def _mark_auth_failed(self: Transport) -> None:
    """Mark the transport unusable until a successful re-login clears it."""
    setattr(self, "_auth_failed", True)


def _clear_auth_failed(self: Transport) -> None:
    """Clear the local auth-failed marker after successful credential recovery."""
    setattr(self, "_auth_failed", False)


def _record_send_warns_once() -> bool:
    """Return True when upstream already logs the send-limit warning once."""
    try:
        source = inspect.getsource(Transport.record_send)
    except OSError, TypeError:
        return False
    return "if not self.is_rate_limited" in source


def _record_send_warn_once(self: Transport) -> None:
    """Record an outbound send without spamming warnings past the quota."""
    now = time.monotonic()
    self._last_send_monotonic = now  # noqa: SLF001
    self._send_timestamps.append(now)  # noqa: SLF001
    cutoff = now - self._SEND_WINDOW  # noqa: SLF001
    while self._send_timestamps and self._send_timestamps[0] < cutoff:  # noqa: SLF001
        self._send_timestamps.popleft()  # noqa: SLF001
    if len(self._send_timestamps) >= self._SEND_LIMIT:  # noqa: SLF001
        if not self.is_rate_limited:
            _LOGGER.warning(
                "%s: %d sends in %.0f h - self-imposing rate limit",
                type(self).__name__,
                len(self._send_timestamps),  # noqa: SLF001
                self._SEND_WINDOW / 3600,  # noqa: SLF001
            )
        self.set_rate_limited()


def _mqtt_send_rate_auth_safe() -> bool:
    """Return True when upstream MQTT send has both rate and auth-failure guards."""
    try:
        source = inspect.getsource(MQTTTransport.send)
    except OSError, TypeError:
        return False
    return "if self.is_rate_limited" in source and "mark_auth_failed" in source


async def _mqtt_send_with_rate_and_auth_guard(
    self: MQTTTransport, payload: bytes, iot_id: str = ""
) -> None:
    """Send while honoring local rate-limit and fatal-auth guards."""
    if self.is_rate_limited:
        remaining = self._rate_limited_until - time.monotonic()  # noqa: SLF001
        raise TransportRateLimitedError(
            f"MQTTTransport rate-limited for {remaining:.0f}s more"
        )
    try:
        await _original_mqtt_send(self, payload, iot_id)
    except ReLoginRequiredError as exc:
        _log_patch_warning(
            "mqtt-auth-failed",
            "MQTT transport authentication failed; marking transport unusable "
            "until credentials are refreshed: %s",
            exc,
        )
        _mark_transport_auth_failed(self)
        await _fire_fatal_auth(self, exc)
        raise


async def _fire_fatal_auth(transport: MQTTTransport, exc: Exception) -> None:
    """Run the upstream fatal-auth callback and clear the guard on recovery."""
    callback = transport.on_fatal_auth_error
    if callback is None:
        return
    try:
        await callback(exc)
    except Exception:  # noqa: BLE001
        if _patch_warning_allowed("fatal-auth-callback"):
            _LOGGER.exception("Fatal MQTT auth recovery callback failed")
        return
    _clear_transport_auth_failed(transport)


def _invoke_refresh_has_cooldown() -> bool:
    """Return True when upstream already rate-limits invoke-token refresh failures."""
    try:
        source = inspect.getsource(TokenManager.force_refresh_invoke_token)
    except OSError, TypeError:
        return False
    return "_invoke_refresh_failed_at" in source


async def _force_refresh_invoke_token_with_cooldown(self: TokenManager) -> None:
    """Avoid hammering auth after repeated invoke-token refresh failures."""
    failed_at = getattr(self, "_invoke_refresh_failed_at", None)
    if failed_at is not None:
        elapsed = time.monotonic() - failed_at
        if elapsed < _INVOKE_REFRESH_COOLDOWN:
            raise ReLoginRequiredError(
                _token_manager_account_id(self),
                "invoke token refresh in cooldown "
                f"({_INVOKE_REFRESH_COOLDOWN - elapsed:.0f}s remaining)",
            )
    try:
        await _original_force_refresh_invoke_token(self)
    except (AuthError, ReLoginRequiredError) as exc:
        setattr(self, "_invoke_refresh_failed_at", time.monotonic())
        _log_patch_warning(
            "invoke-token-refresh",
            "Mammotion invoke-token refresh failed; backing off for %.0fs: %s",
            _INVOKE_REFRESH_COOLDOWN,
            exc,
        )
        raise
    setattr(self, "_invoke_refresh_failed_at", None)


def _refresh_mqtt_creds_rejects_none() -> bool:
    """Return True when upstream raises if MQTT credential refresh returns None."""
    try:
        source = inspect.getsource(TokenManager.refresh_mqtt_creds)
    except OSError, TypeError:
        return False
    return "MQTT credentials not set after refresh" in source


async def _refresh_mqtt_creds_require_result(self: TokenManager) -> MQTTCredentials:
    """Raise a re-login error instead of returning None MQTT credentials."""
    creds = await _original_refresh_mqtt_creds(self)
    if creds is None:
        _log_patch_warning(
            "mqtt-creds-none",
            "Mammotion MQTT credential refresh returned no credentials",
        )
        raise ReLoginRequiredError(
            _token_manager_account_id(self),
            "MQTT credentials not set after refresh",
        )
    return creds


def _token_manager_account_id(token_manager: TokenManager) -> str:
    """Return the account id across pymammotion versions."""
    return str(
        getattr(
            token_manager,
            "account_id",
            getattr(token_manager, "_account_id", ""),
        )
    )


def _mark_transport_auth_failed(transport: Transport) -> None:
    """Call mark_auth_failed across pymammotion versions."""
    mark_auth_failed = getattr(transport, "mark_auth_failed", None)
    if mark_auth_failed is None:
        _mark_auth_failed(transport)
        return
    mark_auth_failed()


def _clear_transport_auth_failed(transport: Transport) -> None:
    """Call clear_auth_failed across pymammotion versions."""
    clear_auth_failed = getattr(transport, "clear_auth_failed", None)
    if clear_auth_failed is None:
        _clear_auth_failed(transport)
        return
    clear_auth_failed()
