"""Compatibility patches for pymammotion behavior used by this integration."""

from __future__ import annotations

import inspect
import time

from pymammotion.device.handle import DeviceHandle
from pymammotion.transport.base import (
    Transport,
    TransportRateLimitedError,
    TransportType,
)
from pymammotion.transport.mqtt import MQTTTransport

_SEND_MARKED_PATCH_ATTR = "_mammotion_ha_cloud_safe_send_marked"
_PROPERTIES_TOPIC_PATCH_ATTR = "_mammotion_ha_properties_topic_patch"
_original_register_device = MQTTTransport.register_device


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


def _register_device_with_properties(
    self: MQTTTransport, product_key: str, device_name: str, iot_id: str
) -> None:
    """Register Mammotion MQTT devices with property-state push topics enabled."""
    _original_register_device(self, product_key, device_name, iot_id)
    self.add_topic(f"/sys/{product_key}/{device_name}/app/down/thing/properties")
