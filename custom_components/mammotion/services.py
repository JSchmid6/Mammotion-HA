"""Mammotion services."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.const import ATTR_ENTITY_ID
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN, LOGGER
from .geojson_utils import apply_geojson_offset
from .models import MammotionMowerData

SERVICE_GET_GEOJSON = "get_geojson"
SERVICE_GET_MOW_PATH_GEOJSON = "get_mow_path_geojson"
SERVICE_GET_MOW_PROGRESS_GEOJSON = "get_mow_progress_geojson"
SERVICE_REQUEST_REPORT = "request_report"
SERVICE_START_REPORT_STREAM = "start_report_stream"

ATTR_DURATION_SECONDS = "duration_seconds"
DEFAULT_REPORT_STREAM_DURATION_SECONDS = 300
MAX_REPORT_STREAM_DURATION_SECONDS = 1800
MIN_REPORT_STREAM_DURATION_SECONDS = 10

GEOJSON_SCHEMA = vol.Schema(
    {vol.Required(ATTR_ENTITY_ID): cv.entity_id}, extra=vol.ALLOW_EXTRA
)
REPORT_SCHEMA = vol.Schema(
    {vol.Required(ATTR_ENTITY_ID): cv.entity_id}, extra=vol.ALLOW_EXTRA
)
REPORT_STREAM_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_ENTITY_ID): cv.entity_id,
        vol.Optional(
            ATTR_DURATION_SECONDS,
            default=DEFAULT_REPORT_STREAM_DURATION_SECONDS,
        ): vol.All(
            vol.Coerce(int),
            vol.Range(
                min=MIN_REPORT_STREAM_DURATION_SECONDS,
                max=MAX_REPORT_STREAM_DURATION_SECONDS,
            ),
        ),
    },
    extra=vol.ALLOW_EXTRA,
)


def _get_mower_by_entity_id(
    hass: HomeAssistant, entity_id: str
) -> MammotionMowerData | None:
    """Find the MammotionMowerData for the given entity_id across all config entries."""
    from . import MammotionConfigEntry  # noqa: PLC0415

    entity_reg = er.async_get(hass)
    entity_entry = entity_reg.async_get(entity_id)
    if entity_entry is None:
        LOGGER.error("Could not find entity %s", entity_id)
        return None

    entries: list[MammotionConfigEntry] = hass.config_entries.async_entries(DOMAIN)
    for entry in entries:
        if not entry.runtime_data:
            continue
        mower = next(
            (
                m
                for m in entry.runtime_data.mowers
                if entity_entry.unique_id.startswith(
                    m.reporting_coordinator.unique_name
                )
            ),
            None,
        )
        if mower is not None:
            return mower
    return None


@callback
def async_setup_services(hass: HomeAssistant) -> None:
    """Register Mammotion services."""

    async def handle_request_report(call: ServiceCall) -> None:
        mower = _get_mower_by_entity_id(hass, call.data[ATTR_ENTITY_ID])
        if mower is None:
            LOGGER.error("Could not find entity %s", call.data[ATTR_ENTITY_ID])
            return
        await mower.reporting_coordinator.async_request_report_snapshot()

    async def handle_start_report_stream(call: ServiceCall) -> None:
        mower = _get_mower_by_entity_id(hass, call.data[ATTR_ENTITY_ID])
        if mower is None:
            LOGGER.error("Could not find entity %s", call.data[ATTR_ENTITY_ID])
            return
        duration_ms = call.data[ATTR_DURATION_SECONDS] * 1000
        await mower.reporting_coordinator.async_start_report_stream(duration_ms)

    async def handle_get_geojson(call: ServiceCall) -> dict[str, Any]:
        mower = _get_mower_by_entity_id(hass, call.data[ATTR_ENTITY_ID])
        if mower is None:
            LOGGER.error("Could not find entity %s", call.data[ATTR_ENTITY_ID])
            return {}
        coordinator = mower.reporting_coordinator
        await coordinator.async_start_report_stream(duration_ms=300_000)
        return apply_geojson_offset(
            coordinator.data.map.generated_geojson,
            coordinator.map_offset_lat,
            coordinator.map_offset_lon,
        )

    async def handle_get_mow_path_geojson(call: ServiceCall) -> dict[str, Any]:
        mower = _get_mower_by_entity_id(hass, call.data[ATTR_ENTITY_ID])
        if mower is None:
            LOGGER.error("Could not find entity %s", call.data[ATTR_ENTITY_ID])
            return {}
        coordinator = mower.reporting_coordinator
        return apply_geojson_offset(
            coordinator.data.map.generated_mow_path_geojson,
            coordinator.map_offset_lat,
            coordinator.map_offset_lon,
        )

    async def handle_get_mow_progress_geojson(call: ServiceCall) -> dict[str, Any]:
        mower = _get_mower_by_entity_id(hass, call.data[ATTR_ENTITY_ID])
        if mower is None:
            LOGGER.error("Could not find entity %s", call.data[ATTR_ENTITY_ID])
            return {}
        coordinator = mower.reporting_coordinator
        return apply_geojson_offset(
            coordinator.data.map.generated_mow_progress_geojson,
            coordinator.map_offset_lat,
            coordinator.map_offset_lon,
        )

    hass.services.async_register(
        DOMAIN,
        SERVICE_REQUEST_REPORT,
        handle_request_report,
        schema=REPORT_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_START_REPORT_STREAM,
        handle_start_report_stream,
        schema=REPORT_STREAM_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_GEOJSON,
        handle_get_geojson,
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_MOW_PATH_GEOJSON,
        handle_get_mow_path_geojson,
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_MOW_PROGRESS_GEOJSON,
        handle_get_mow_progress_geojson,
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
