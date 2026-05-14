"""Compatibility patches for pymammotion behavior used by this integration."""

from __future__ import annotations

import inspect
import logging
import time

from pymammotion.auth.token_manager import MQTTCredentials, TokenManager
from pymammotion.device.handle import DeviceHandle
from pymammotion.transport.base import (
    AuthError,
    ReLoginRequiredError,
    Transport,
    TransportRateLimitedError,
    TransportType,
)
from pymammotion.transport.mqtt import MQTTTransport

_SEND_MARKED_PATCH_ATTR = "_mammotion_ha_cloud_safe_send_marked"
_PROPERTIES_TOPIC_PATCH_ATTR = "_mammotion_ha_properties_topic_patch"
_TRANSPORT_AUTH_PATCH_ATTR = "_mammotion_ha_transport_auth_state_patch"
_RECORD_SEND_PATCH_ATTR = "_mammotion_ha_record_send_patch"
_MQTT_SEND_PATCH_ATTR = "_mammotion_ha_mqtt_send_patch"
_INVOKE_REFRESH_PATCH_ATTR = "_mammotion_ha_invoke_refresh_patch"
_MQTT_CREDS_PATCH_ATTR = "_mammotion_ha_mqtt_creds_patch"
_INVOKE_REFRESH_COOLDOWN = 30.0
_PATCH_WARNING_LOG_INTERVAL = 900.0
_LOGGER = logging.getLogger(__name__)
_last_patch_warning_log_at: dict[str, float] = {}

_original_register_device = MQTTTransport.register_device
_original_mqtt_send = MQTTTransport.send
_original_force_refresh_invoke_token = TokenManager.force_refresh_invoke_token
_original_refresh_mqtt_creds = TokenManager.refresh_mqtt_creds


def _patch_warning_allowed(key: str) -> bool:
    """Return True when a patch warning should be emitted."""
    now = time.monotonic()
    if (
        now - _last_patch_warning_log_at.get(key, 0.0)
        < _PATCH_WARNING_LOG_INTERVAL
    ):
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
    except (OSError, TypeError):
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


def apply_pymammotion_patches() -> None:
    """Apply pymammotion compatibility patches once."""
    if not getattr(DeviceHandle, _SEND_MARKED_PATCH_ATTR, False):
        if not _send_marked_already_cloud_safe():
            setattr(DeviceHandle, "_send_marked", _send_marked_cloud_safe)
        setattr(DeviceHandle, _SEND_MARKED_PATCH_ATTR, True)

    if not getattr(MQTTTransport, _PROPERTIES_TOPIC_PATCH_ATTR, False):
        setattr(MQTTTransport, "register_device", _register_device_with_properties)
        setattr(MQTTTransport, _PROPERTIES_TOPIC_PATCH_ATTR, True)

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


def _register_device_with_properties(
    self: MQTTTransport, product_key: str, device_name: str, iot_id: str
) -> None:
    """Register Mammotion MQTT devices with property-state push topics enabled."""
    _original_register_device(self, product_key, device_name, iot_id)
    self.add_topic(f"/sys/{product_key}/{device_name}/app/down/thing/properties")


def _transport_auth_state_already_present() -> bool:
    """Return True when upstream already tracks auth-failed transport state."""
    if not all(
        hasattr(Transport, name) for name in ("mark_auth_failed", "clear_auth_failed")
    ):
        return False
    try:
        source = inspect.getsource(Transport.is_usable.fget)  # type: ignore[arg-type]
    except (OSError, TypeError):
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
    except (OSError, TypeError):
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
    except (OSError, TypeError):
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
    except (OSError, TypeError):
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
    except (OSError, TypeError):
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
