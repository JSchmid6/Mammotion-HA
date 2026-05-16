# pymammotion patches

This folder contains small upstream patches that are also mirrored by the
Home Assistant integration runtime patches in
`custom_components/mammotion/pymammotion_patches.py`.

## cloud sends do not prepend BLE sync

Target: `pymammotion` 0.7.109 and 0.7.110.

The current `DeviceHandle._send_marked` prepends a BLE sync packet after 50
seconds of transport inactivity without checking the active transport type.
When the selected transport is MQTT/cloud, that turns a normal command or
snapshot request into two cloud sends. With Mammotion's strict daily send
budget this can burn quota faster than intended.

The patch scopes the BLE sync packet to `TransportType.BLE` only. The HA
integration applies the same fix at startup so a local HA install can use the
fix before an upstream `pymammotion` release is available.

## Mammotion MQTT subscribes to properties

Target: `pymammotion` 0.7.109.

The newer Mammotion MQTT transport subscribes to `thing/status` and event
topics, but the `thing/properties` topic is commented out. That properties
topic can carry `deviceState`, which the state reducer maps to
`report_data.dev.sys_status`. Missing it can make HA look stale until the
official Mammotion app opens and causes additional report traffic.

The HA runtime patch adds the missing properties topic whenever Mammotion MQTT
registers a device.

## MQTT auth and rate-limit recovery

Target: `pymammotion` 0.7.109 and 0.7.110.

Upstream `pymammotion` has unreleased fixes for several failure storms around
Mammotion MQTT:

- direct `MQTTTransport.send()` calls should stop immediately when the transport
  has self-imposed its 24-hour send limit;
- the send-limit warning should only be logged when the limit is first crossed;
- repeated `force_refresh_invoke_token()` failures should cool down instead of
  retrying on every queued command;
- MQTT credential refresh returning `None` should surface as a re-login error
  instead of an `AttributeError`;
- a fatal MQTT auth failure should mark the transport unusable until the
  integration's re-login callback succeeds.

The HA runtime patch mirrors those guards defensively and skips them when a
future upstream version already contains the same behavior.

For a clean dependency-level install, apply the patch to a public fork or local
wheel and point `custom_components/mammotion/manifest.json` at that build.
Home Assistant supports pip-compatible requirement strings, including public
git requirements in the form:

```json
"requirements": [
  "pymammotion@git+https://github.com/<user>/PyMammotion.git@<git-ref>"
]
```

## Report sanity guard

Target: `pymammotion` 0.7.109 and 0.7.110.

Mammotion can publish stale-looking full-charge report snapshots while the mower
is still away from the dock. The observed failure was a mower report sequence
that jumped from 64% to 100% and back to 64% within roughly a minute, together
with a matching RSSI/location jump. The raw values are upstream of HA, but
publishing a single impossible snapshot can make automations treat the mower as
docked or charging.

The HA runtime patch wraps `MowerStateReducer.apply()` and rejects fresh
`toapp_report_data` snapshots with an implausible jump to near-full battery.
The HA coordinator keeps the same check as a second safety net, so future
upstream `pymammotion` changes cannot accidentally re-expose the bad state to
Home Assistant automations.
