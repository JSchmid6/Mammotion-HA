"""Mammotion services."""

from __future__ import annotations

from functools import partial
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

ENTITY_IDS_SCHEMA = vol.All(cv.entity_ids, vol.Length(min=1))
SINGLE_ENTITY_ID_SCHEMA = vol.All(cv.entity_ids, vol.Length(min=1, max=1))

GEOJSON_SCHEMA = vol.Schema(
    {vol.Required(ATTR_ENTITY_ID): SINGLE_ENTITY_ID_SCHEMA}, extra=vol.ALLOW_EXTRA
)
REPORT_SCHEMA = vol.Schema(
    {vol.Required(ATTR_ENTITY_ID): ENTITY_IDS_SCHEMA}, extra=vol.ALLOW_EXTRA
)
REPORT_STREAM_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_ENTITY_ID): ENTITY_IDS_SCHEMA,
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


def _entity_ids_from_call(call: ServiceCall) -> list[str]:
    """Return entity ids from a service call after HA target validation."""
    entity_ids = call.data[ATTR_ENTITY_ID]
    if isinstance(entity_ids, str):
        return [entity_ids]
    return list(entity_ids)


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


def _get_mowers_from_call(
    hass: HomeAssistant, call: ServiceCall
) -> list[MammotionMowerData]:
    """Return all mower data objects referenced by a service call."""
    mowers: list[MammotionMowerData] = []
    for entity_id in _entity_ids_from_call(call):
        mower = _get_mower_by_entity_id(hass, entity_id)
        if mower is None:
            LOGGER.error("Could not find entity %s", entity_id)
            continue
        mowers.append(mower)
    return mowers


def _get_single_mower_from_call(
    hass: HomeAssistant, call: ServiceCall
) -> MammotionMowerData | None:
    """Return the single mower referenced by a response-oriented service call."""
    entity_ids = _entity_ids_from_call(call)
    if len(entity_ids) != 1:
        LOGGER.error("Expected exactly one entity_id, got %s", entity_ids)
        return None
    return _get_mower_by_entity_id(hass, entity_ids[0])


async def _handle_request_report(hass: HomeAssistant, call: ServiceCall) -> None:
    """Request one report snapshot for every referenced mower."""
    for mower in _get_mowers_from_call(hass, call):
        await mower.reporting_coordinator.async_request_report_snapshot()


async def _handle_start_report_stream(hass: HomeAssistant, call: ServiceCall) -> None:
    """Start a temporary report stream for every referenced mower."""
    duration_ms = call.data[ATTR_DURATION_SECONDS] * 1000
    for mower in _get_mowers_from_call(hass, call):
        await mower.reporting_coordinator.async_start_report_stream(duration_ms)


async def _handle_get_geojson(hass: HomeAssistant, call: ServiceCall) -> dict[str, Any]:
    """Return the generated map GeoJSON for one mower."""
    mower = _get_single_mower_from_call(hass, call)
    if mower is None:
        return {}
    coordinator = mower.reporting_coordinator
    await coordinator.async_start_report_stream(duration_ms=300_000)
    return apply_geojson_offset(
        coordinator.data.map.generated_geojson,
        coordinator.map_offset_lat,
        coordinator.map_offset_lon,
    )


async def _handle_get_mow_path_geojson(
    hass: HomeAssistant, call: ServiceCall
) -> dict[str, Any]:
    """Return the generated mow path GeoJSON for one mower."""
    mower = _get_single_mower_from_call(hass, call)
    if mower is None:
        return {}
    coordinator = mower.reporting_coordinator
    return apply_geojson_offset(
        coordinator.data.map.generated_mow_path_geojson,
        coordinator.map_offset_lat,
        coordinator.map_offset_lon,
    )


async def _handle_get_mow_progress_geojson(
    hass: HomeAssistant, call: ServiceCall
) -> dict[str, Any]:
    """Return the generated mow progress GeoJSON for one mower."""
    mower = _get_single_mower_from_call(hass, call)
    if mower is None:
        return {}
    coordinator = mower.reporting_coordinator
    return apply_geojson_offset(
        coordinator.data.map.generated_mow_progress_geojson,
        coordinator.map_offset_lat,
        coordinator.map_offset_lon,
    )


@callback
def async_setup_services(hass: HomeAssistant) -> None:
    """Register Mammotion services."""
    hass.services.async_register(
        DOMAIN,
        SERVICE_REQUEST_REPORT,
        partial(_handle_request_report, hass),
        schema=REPORT_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_START_REPORT_STREAM,
        partial(_handle_start_report_stream, hass),
        schema=REPORT_STREAM_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_GEOJSON,
        partial(_handle_get_geojson, hass),
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_MOW_PATH_GEOJSON,
        partial(_handle_get_mow_path_geojson, hass),
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_MOW_PROGRESS_GEOJSON,
        partial(_handle_get_mow_progress_geojson, hass),
        schema=GEOJSON_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
