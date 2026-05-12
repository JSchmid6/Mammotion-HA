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

For a clean dependency-level install, apply the patch to a public fork or local
wheel and point `custom_components/mammotion/manifest.json` at that build.
Home Assistant supports pip-compatible requirement strings, including public
git requirements in the form:

```json
"requirements": [
  "pymammotion@git+https://github.com/<user>/PyMammotion.git@<git-ref>"
]
```
