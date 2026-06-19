"""Regression tests for Mammotion pymammotion compatibility patches."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
import types
from pathlib import Path
from typing import Any

import pytest
from pymammotion.auth.token_manager import TokenManager
from pymammotion.client import MammotionClient
from pymammotion.data.model.device import MowerDevice
from pymammotion.data.model.report_info import ReportData
from pymammotion.data.mqtt.properties import MammotionPropertiesMessage
from pymammotion.device.handle import DeviceHandle
from pymammotion.device.state_reducer import MowerStateReducer
from pymammotion.proto import ReportInfoData, RptDevLocation, RptDevStatus, RptWork
from pymammotion.transport.aliyun_mqtt import AliyunMQTTTransport
from pymammotion.transport.base import Transport
from pymammotion.transport.mqtt import MQTTTransport
from pymammotion.utility.constant.device_constant import WorkMode


def _load_pymammotion_patches() -> Any:
    """Load pymammotion_patches without importing the HA integration package."""
    root = Path(__file__).parents[1]
    custom_components_path = root / "custom_components"
    mammotion_path = custom_components_path / "mammotion"

    custom_components = sys.modules.setdefault(
        "custom_components", types.ModuleType("custom_components")
    )
    custom_components.__path__ = [str(custom_components_path)]  # type: ignore[attr-defined]

    mammotion = sys.modules.setdefault(
        "custom_components.mammotion",
        types.ModuleType("custom_components.mammotion"),
    )
    mammotion.__path__ = [str(mammotion_path)]  # type: ignore[attr-defined]

    module_path = mammotion_path / "pymammotion_patches.py"
    spec = importlib.util.spec_from_file_location(
        "custom_components.mammotion.pymammotion_patches", module_path
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


patches = _load_pymammotion_patches()
patches.apply_pymammotion_patches()


def test_upstream_mqtt_auth_sync_and_rate_guards_are_kept() -> None:
    """PyMammotion native transport/auth/sync fixes are not monkeypatched."""
    assert DeviceHandle._send_marked.__module__ == "pymammotion.device.handle"  # noqa: SLF001
    assert MQTTTransport.send.__module__ == "pymammotion.transport.mqtt"
    assert Transport.record_send.__module__ == "pymammotion.transport.base"
    assert (
        TokenManager.force_refresh_invoke_token.__module__
        == "pymammotion.auth.token_manager"
    )
    assert TokenManager.refresh_mqtt_creds.__module__ == (
        "pymammotion.auth.token_manager"
    )


class _Response:
    """Small response stub for login fallback tests."""

    def __init__(self, code: int, msg: str) -> None:
        """Store response code and message."""
        self.code = code
        self.msg = msg


class _FakeHttp:
    """Fake MammotionHTTP login surface."""

    def __init__(self, v2: _Response, legacy: _Response) -> None:
        """Store responses returned by each auth path."""
        self.v2 = v2
        self.legacy = legacy
        self.calls: list[str] = []

    async def login_v2(self, account: str, password: str) -> _Response:
        """Return the configured v2 login response."""
        self.calls.append("login_v2")
        return self.v2

    async def login(self, account: str, password: str) -> _Response:
        """Return the configured legacy login response."""
        self.calls.append("login")
        return self.legacy


def test_login_and_initiate_cloud_is_patched_for_legacy_shared_accounts() -> None:
    """The integration patches PyMammotion's v2-only login entry point."""
    assert (
        MammotionClient.login_and_initiate_cloud.__module__
        == "custom_components.mammotion.pymammotion_patches"
    )


def test_login_helper_uses_legacy_when_v2_rejects_shared_account() -> None:
    """Shared test accounts may need the legacy Mammotion auth endpoint."""
    fake = _FakeHttp(
        v2=_Response(200, "Account or password mismatch"),
        legacy=_Response(0, "Request success"),
    )

    response, legacy_used = asyncio.run(
        patches._login_mammotion_http_with_legacy_fallback(  # noqa: SLF001
            fake,
            "test@example.com",
            "secret",
        )
    )

    assert response is fake.legacy
    assert legacy_used is True
    assert fake.calls == ["login_v2", "login"]


def test_login_helper_keeps_successful_v2_response() -> None:
    """The legacy endpoint is not called when v2 auth succeeds."""
    fake = _FakeHttp(
        v2=_Response(0, "Request success"),
        legacy=_Response(0, "Request success"),
    )

    response, legacy_used = asyncio.run(
        patches._login_mammotion_http_with_legacy_fallback(  # noqa: SLF001
            fake,
            "test@example.com",
            "secret",
        )
    )

    assert response is fake.v2
    assert legacy_used is False
    assert fake.calls == ["login_v2"]


def test_empty_v2_device_page_still_bootstraps_aliyun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accepted shared devices may only be visible through Aliyun discovery."""
    events: list[Any] = []
    transport = types.SimpleNamespace(connected=False)

    async def connect_transport() -> None:
        """Record transport connection."""
        transport.connected = True

    transport.connect = connect_transport

    class FakeCloudIOTGateway:
        """Cloud gateway stub with one accepted shared device."""

        def __init__(self, mammotion_http: object) -> None:
            """Store required Aliyun setup responses."""
            self.mammotion_http = mammotion_http
            self.aep_response = object()
            self.region_response = object()
            self.session_by_authcode_response = types.SimpleNamespace(data=object())
            self.devices_by_account_response = types.SimpleNamespace(
                data=types.SimpleNamespace(
                    data=[
                        types.SimpleNamespace(
                            device_name="Yuka-YVFR8E9D",
                            iot_id="aliyun-iot",
                            product_key="pk",
                        )
                    ]
                )
            )

        async def get_shared_notice_list(self) -> object:
            """Return no pending shares."""
            return types.SimpleNamespace(data=types.SimpleNamespace(data=[]))

    class FakeTokenManager:
        """Token manager stub used by the account session."""

        def __init__(
            self,
            account: str,
            mammotion_http: object,
            cloud_client: object,
        ) -> None:
            """Store constructor args for introspection."""
            self.account = account
            self.mammotion_http = mammotion_http
            self.cloud_client = cloud_client
            self.on_credentials_updated = None

    class FakeClient:
        """Mammotion client stub exposing the Aliyun bootstrap surface."""

        on_credentials_updated = object()

        async def _connect_iot(self, cloud_client: object) -> None:
            """Record Aliyun cloud bootstrap."""
            events.append(("connect_iot", cloud_client))

        def _setup_aliyun_transport(
            self,
            cloud_client: object,
            account_session: object,
        ) -> object:
            """Return the fake transport."""
            events.append(("setup_transport", cloud_client, account_session))
            return transport

        async def _register_aliyun_device(
            self,
            device_name: str,
            iot_id: str,
            al_transport: object,
            user_account: str,
            product_key: str,
            *,
            token_manager: object,
        ) -> None:
            """Record registered devices."""
            events.append(
                (
                    "register",
                    device_name,
                    iot_id,
                    al_transport,
                    user_account,
                    product_key,
                    token_manager,
                )
            )

    monkeypatch.setattr(patches, "CloudIOTGateway", FakeCloudIOTGateway)
    monkeypatch.setattr(patches, "TokenManager", FakeTokenManager)

    account_session = types.SimpleNamespace(
        user_account="user-account",
        device_ids=set(),
        cloud_client=None,
        token_manager=None,
        aliyun_transport=None,
    )

    asyncio.run(
        patches._bootstrap_legacy_aliyun_if_needed(  # noqa: SLF001
            FakeClient(),
            "test@example.com",
            object(),
            account_session,
            {},
            legacy_login_used=False,
            mammotion_records=[],
        )
    )

    assert any(event[0] == "connect_iot" for event in events)
    assert any(
        event[:3] == ("register", "Yuka-YVFR8E9D", "aliyun-iot") for event in events
    )
    assert account_session.device_ids == {"Yuka-YVFR8E9D"}
    assert account_session.aliyun_transport is transport
    assert transport.connected is True


def _aliyun_properties_envelope(generate_time_ms: int) -> bytes:
    """Build a realistic Aliyun thing/properties envelope."""
    return json.dumps(
        {
            "method": "thing.properties",
            "id": "test-props-id",
            "version": "1.0",
            "params": {
                "deviceType": "LawnMower",
                "checkFailedData": {},
                "groupIdList": [],
                "_tenantId": "",
                "groupId": "",
                "categoryKey": "LawnMower",
                "batchId": "",
                "gmtCreate": generate_time_ms,
                "productKey": "testpk",
                "generateTime": generate_time_ms,
                "deviceName": "testdn",
                "_traceId": "",
                "iotId": "test_iot_id",
                "JMSXDeliveryCount": 1,
                "checkLevel": 0,
                "qos": 1,
                "requestId": "1",
                "_categoryKey": "TmallGenie.LawnMower",
                "namespace": "",
                "tenantId": "",
                "thingType": "DEVICE",
                "items": {
                    "batteryPercentage": {
                        "time": generate_time_ms,
                        "value": 80,
                    }
                },
                "tenantInstanceId": "",
            },
        }
    ).encode()


def _aliyun_transport_with_property_callback(events: list[Any]) -> Any:
    """Build a minimal Aliyun transport object for dispatch tests."""
    transport = AliyunMQTTTransport.__new__(AliyunMQTTTransport)
    transport.on_device_event = None

    async def on_properties(iot_id: str, message: object) -> None:
        """Record dispatched property events."""
        events.append((iot_id, message))

    transport.on_device_properties = on_properties
    return transport


def test_stale_aliyun_properties_are_dropped_by_generate_time() -> None:
    """Old Aliyun property snapshots must not overwrite current mower state."""
    events: list[Any] = []
    transport = _aliyun_transport_with_property_callback(events)
    stale_time_ms = int(time.time() * 1000) - 120_000

    asyncio.run(
        transport._dispatch_aliyun_event(  # noqa: SLF001
            "/sys/testpk/testdn/app/down/thing/properties",
            _aliyun_properties_envelope(stale_time_ms),
        )
    )

    assert events == []


def test_fresh_aliyun_properties_are_forwarded() -> None:
    """Fresh Aliyun property snapshots remain valid state updates."""
    events: list[Any] = []
    transport = _aliyun_transport_with_property_callback(events)
    fresh_time_ms = int(time.time() * 1000) - 5_000

    asyncio.run(
        transport._dispatch_aliyun_event(  # noqa: SLF001
            "/sys/testpk/testdn/app/down/thing/properties",
            _aliyun_properties_envelope(fresh_time_ms),
        )
    )

    assert len(events) == 1
    assert events[0][0] == "test_iot_id"


def _mower() -> MowerDevice:
    """Return a mower with non-default report values."""
    mower = MowerDevice(name="Luba-Test")
    mower.report_data.dev.sys_status = int(WorkMode.MODE_WORKING)
    mower.report_data.dev.battery_val = 77
    mower.report_data.work.knife_height = 45
    return mower


def _properties(params: dict[str, Any]) -> MammotionPropertiesMessage:
    """Build a Mammotion direct-MQTT property message."""
    return MammotionPropertiesMessage.from_dict(
        {
            "id": "1",
            "version": "1.0",
            "sys": {},
            "params": params,
        }
    )


def _device_handle_with_report(
    *,
    sys_status: int,
    charge_state: int,
    battery_val: int = 90,
    bp_info: int,
    work_area: int,
    work_progress: int,
) -> Any:
    """Build a minimal DeviceHandle-like object for report-watch helpers."""
    report_data = types.SimpleNamespace(
        dev=types.SimpleNamespace(
            sys_status=sys_status,
            charge_state=charge_state,
            battery_val=battery_val,
            last_status=None,
        ),
        work=types.SimpleNamespace(
            bp_info=bp_info,
            area=work_area,
            progress=work_progress,
        ),
    )
    return types.SimpleNamespace(
        state_machine=types.SimpleNamespace(
            current=types.SimpleNamespace(
                raw=types.SimpleNamespace(report_data=report_data)
            )
        )
    )


def test_job_watch_patch_ignores_terminal_docked_breakpoint_info() -> None:
    """The stream patch must not keep polling a finished docked mower forever."""
    handle = _device_handle_with_report(
        sys_status=int(WorkMode.MODE_READY),
        charge_state=1,
        bp_info=7,
        work_area=186,
        work_progress=0,
    )

    assert not patches._has_unfinished_mow_job(handle)  # noqa: SLF001
    assert not patches._is_recharge_pause_report_state(handle)  # noqa: SLF001


def test_job_watch_patch_ignores_terminal_docked_tiny_remaining_time() -> None:
    """A stale one-minute remainder must not keep cloud report streaming alive."""
    handle = _device_handle_with_report(
        sys_status=int(WorkMode.MODE_READY),
        charge_state=1,
        battery_val=100,
        bp_info=1,
        work_area=142,
        work_progress=(1 << 16) | 93,
    )

    assert not patches._has_unfinished_mow_job(handle)  # noqa: SLF001
    assert not patches._is_recharge_pause_report_state(handle)  # noqa: SLF001


def test_job_watch_patch_keeps_real_unfinished_work() -> None:
    """Open work fields still keep recharge-pause report watching alive."""
    handle = _device_handle_with_report(
        sys_status=int(WorkMode.MODE_READY),
        charge_state=1,
        bp_info=7,
        work_area=(40 << 16) | 186,
        work_progress=(60 << 16) | 100,
    )

    assert patches._has_unfinished_mow_job(handle)  # noqa: SLF001
    assert patches._is_recharge_pause_report_state(handle)  # noqa: SLF001


def test_mammotion_property_push_preserves_absent_status_scalars() -> None:
    """Partial property posts must not turn absent scalar fields into zero state."""
    reducer = MowerStateReducer()
    current = _mower()
    token = patches._REPORT_SOURCE_CONTEXT.set(  # noqa: SLF001
        {
            "transport": "cloud_mammotion",
            "topic": "/sys/pk/dn/thing/event/property/post",
            "property_keys": ("deviceVersion",),
        }
    )
    try:
        updated = reducer.apply_mammotion_properties(
            current,
            _properties({"deviceVersion": "1.2.3"}),
        )
    finally:
        patches._REPORT_SOURCE_CONTEXT.reset(token)  # noqa: SLF001

    assert updated.report_data.dev.sys_status == current.report_data.dev.sys_status
    assert updated.report_data.dev.battery_val == current.report_data.dev.battery_val
    assert (
        updated.report_data.work.knife_height == current.report_data.work.knife_height
    )
    assert updated.device_firmwares.device_version == "1.2.3"


def test_mammotion_property_push_accepts_explicit_zero_status() -> None:
    """An explicitly present zero status is still a real Mammotion state update."""
    reducer = MowerStateReducer()
    current = _mower()
    token = patches._REPORT_SOURCE_CONTEXT.set(  # noqa: SLF001
        {
            "transport": "cloud_mammotion",
            "topic": "/sys/pk/dn/thing/event/property/post",
            "property_keys": ("deviceState",),
        }
    )
    try:
        updated = reducer.apply_mammotion_properties(
            current,
            _properties({"deviceState": 0}),
        )
    finally:
        patches._REPORT_SOURCE_CONTEXT.reset(token)  # noqa: SLF001

    assert updated.report_data.dev.sys_status == 0
    assert updated.report_data.dev.battery_val == current.report_data.dev.battery_val
    assert (
        updated.report_data.work.knife_height == current.report_data.work.knife_height
    )


def test_partial_active_report_clears_stale_charge_state() -> None:
    """An active status without charge_state must not inherit dock charging."""
    report = ReportData()
    report.dev.sys_status = int(WorkMode.MODE_READY)
    report.dev.charge_state = 1
    report.dev.battery_val = 100

    report.update(
        ReportInfoData(dev=RptDevStatus(sys_status=int(WorkMode.MODE_WORKING)))
    )

    assert report.dev.sys_status == int(WorkMode.MODE_WORKING)
    assert report.dev.charge_state == 0
    assert report.dev.battery_val == 100


def test_partial_returning_report_clears_stale_charge_state() -> None:
    """Returning without charge_state must not keep a stale docked flag."""
    report = ReportData()
    report.dev.sys_status = int(WorkMode.MODE_READY)
    report.dev.charge_state = 1
    report.dev.battery_val = 95

    report.update(
        ReportInfoData(dev=RptDevStatus(sys_status=int(WorkMode.MODE_RETURNING)))
    )

    assert report.dev.sys_status == int(WorkMode.MODE_RETURNING)
    assert report.dev.charge_state == 0
    assert report.dev.battery_val == 95


def test_partial_ready_report_with_motion_clears_stale_charge_state() -> None:
    """Ready plus movement evidence without charge_state is no longer docked."""
    report = ReportData()
    report.update(
        ReportInfoData(
            locations=[
                RptDevLocation(
                    real_pos_x=14490,
                    real_pos_y=11146,
                    real_toward=892800,
                )
            ]
        )
    )
    report.dev.sys_status = int(WorkMode.MODE_READY)
    report.dev.charge_state = 1
    report.dev.battery_val = 95

    report.update(
        ReportInfoData(
            dev=RptDevStatus(
                sys_status=int(WorkMode.MODE_READY),
                battery_val=95,
                last_status=int(WorkMode.MODE_RETURNING),
            ),
            locations=[
                RptDevLocation(
                    real_pos_x=14002,
                    real_pos_y=12486,
                    real_toward=880123,
                )
            ],
            work=RptWork(man_run_speed=-2),
        )
    )

    assert report.dev.sys_status == int(WorkMode.MODE_READY)
    assert report.dev.charge_state == 0
    assert report.dev.battery_val == 95


def test_partial_ready_report_without_motion_keeps_charge_state() -> None:
    """Ready without charge_state also needs movement evidence before clearing."""
    report = ReportData()
    report.update(
        ReportInfoData(
            locations=[
                RptDevLocation(
                    real_pos_x=14490,
                    real_pos_y=11146,
                    real_toward=892800,
                )
            ]
        )
    )
    report.dev.sys_status = int(WorkMode.MODE_READY)
    report.dev.charge_state = 1
    report.dev.battery_val = 95

    report.update(
        ReportInfoData(
            dev=RptDevStatus(
                sys_status=int(WorkMode.MODE_READY),
                battery_val=95,
                last_status=int(WorkMode.MODE_RETURNING),
            ),
            locations=[
                RptDevLocation(
                    real_pos_x=14490,
                    real_pos_y=11146,
                    real_toward=892800,
                )
            ],
        )
    )

    assert report.dev.sys_status == int(WorkMode.MODE_READY)
    assert report.dev.charge_state == 1
    assert report.dev.battery_val == 95


def test_partial_work_report_preserves_absent_job_fields() -> None:
    """Partial work reports must not reset active job progress to defaults."""
    report = ReportData()
    report.work.area = (15 << 16) | 186
    report.work.progress = (85 << 16) | 100
    report.work.bp_info = 7
    report.work.knife_height = 45

    report.update(ReportInfoData(work=RptWork(knife_height=50)))

    assert report.work.area == (15 << 16) | 186
    assert report.work.progress == (85 << 16) | 100
    assert report.work.bp_info == 7
    assert report.work.knife_height == 50
