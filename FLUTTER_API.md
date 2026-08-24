# Warehouse Digital Twin - Flutter Integration Guide

**API version:** 1.1.0  
**Contract date:** 2026-08-11  
**Backend:** FastAPI  
**Realtime transport:** Native WebSocket  
**Internal transport:** MQTT through EMQX  
**Historical storage:** InfluxDB 3

This document is the canonical integration guide for the Flutter application. It describes the contract that is implemented by the current backend, including offline behavior, nullable fields, realtime updates, device control, and known limitations.

## 1. System boundary

```text
Unity simulation
   | MQTT telemetry and alerts
   v
EMQX :1883 <-> FastAPI :8000 <-> Flutter
                     | REST + WebSocket
                     v
                 InfluxDB :8181
```

Flutter communicates only with FastAPI. It must not connect directly to EMQX or InfluxDB.

## 2. Service URLs

### Local development

```text
REST base URL: http://127.0.0.1:8000
WebSocket URL: ws://127.0.0.1:8000/ws/sensors
Swagger UI:   http://127.0.0.1:8000/docs
OpenAPI:      http://127.0.0.1:8000/openapi.json
```

`127.0.0.1` works only when Flutter runs on the same computer. For a physical phone on the same Wi-Fi network, use the computer LAN address, for example `http://192.168.0.160:8000`.

### Cloudflare Quick Tunnel

```text
REST base URL: https://api.syrian-dev.com
WebSocket URL: wss://api.syrian-dev.com/ws/sensors
```

The Quick Tunnel hostname changes whenever `cloudflared` restarts. Keep it in Flutter configuration, never hard-code it throughout the UI.

## 3. Official device inventory

| Device type | Implemented IDs | Control status |
|---|---|---|
| Machine / robotic arm | `arm_1` | Telemetry and STOP/RESUME implemented |
| AGV robot | `agv_01`, `agv_02` | Telemetry and commands implemented |
| Forklift | `forklift_01` | Telemetry and STOP/RESUME implemented |
| Conveyor | `conveyor_01` | Configured placeholder; Unity telemetry/control not implemented |

The backend rejects telemetry from unknown IDs. IDs are normalized to lowercase.

## 4. Recommended Flutter startup sequence

1. Call `GET /api/health`.
2. Call `GET /api/dashboard` and replace the complete local state.
3. Connect to `GET /ws/sensors` using WebSocket.
4. Process the first WebSocket `snapshot` by replacing the complete local state again.
5. Merge later WebSocket events into the matching entity.
6. Send control commands through REST, not through WebSocket.
7. Reconnect WebSocket with backoff after a network interruption.

REST provides commands and recovery. WebSocket provides live state changes. The initial snapshot prevents Flutter from waiting for every device to emit a new event.

## 5. Core state rules

### Online state

A machine, robot, or forklift is `online: true` when valid telemetry was received during the last 15 seconds. Flutter must use the backend `online` value and must not calculate it from the Unity timestamp.

Before Unity sends telemetry, configured records are returned with:

```json
{
  "source": "configured",
  "last_seen": null,
  "online": false
}
```

After live telemetry arrives:

```json
{
  "source": "live",
  "last_seen": "2026-08-11T19:20:31.821967Z",
  "online": true
}
```

### Desired and reported control state

Device control state contains three separate values:

| Field | Meaning |
|---|---|
| `desired_enabled` | Last state requested by FastAPI/Flutter |
| `reported_enabled` | State confirmed by later Unity telemetry; nullable before first telemetry |
| `pending` | `true` while desired and reported states differ |

Do not show a successful command as confirmed merely because the POST returned HTTP 200. Show a pending indicator until telemetry updates `reported_enabled`.

### Nullable values

The initial configured snapshot legitimately contains `null` telemetry values. Flutter models must therefore allow:

```text
Machine: temperature, current, active, last_seen
Robot: battery, timestamp, last_seen
Forklift: battery, last_seen
Device: reported_enabled, last_command_at
```

## 6. Primary snapshot

### `GET /api/dashboard`

Use this endpoint to bootstrap or recover the complete application state.

```json
{
  "machines": {
    "arm_1": {
      "machine_id": "arm_1",
      "event": "TELEMETRY_UPDATE",
      "temperature": 31.4,
      "current": 5.6,
      "state": "normal",
      "active": true,
      "source": "live",
      "last_seen": "2026-08-11T19:20:31.821967Z",
      "online": true
    }
  },
  "robot_weights": {},
  "alerts": [],
  "robots": {},
  "forklifts": {},
  "devices": {}
}
```

Object maps are keyed by device ID. Do not assume list ordering.

## 7. Realtime WebSocket

Connect to:

```text
Local: ws://127.0.0.1:8000/ws/sensors
Cloud: wss://<host>/ws/sensors
```

Every message has one envelope:

```json
{
  "stream_type": "robot_telemetry",
  "data": {}
}
```

Supported `stream_type` values:

| Stream type | `data` payload | Flutter action |
|---|---|---|
| `snapshot` | Full dashboard snapshot | Replace all local state |
| `machine_telemetry` | One machine | Upsert by `machine_id` |
| `robot_telemetry` | One robot | Upsert by `robot_id` |
| `robot_weight` | One weight reading | Upsert by `robot_id` |
| `forklift_telemetry` | One forklift | Upsert by `forklift_id` |
| `alert` | One alert | Prepend and de-duplicate if needed |
| `device_command` | One device control record | Upsert by `device_id` |

The backend does not currently send an application-level heartbeat. A client should treat socket close/error as disconnected, reconnect, and accept the next `snapshot` as authoritative.

Recommended retry delays: `1s, 2s, 5s, 10s, 15s`, capped at 15 seconds. Reset the delay after a successful connection.

## 8. Flutter implementation skeleton

Recommended packages:

```yaml
dependencies:
  http: ^1.0.0
  web_socket_channel: ^3.0.0
```

Keep URLs in one environment object:

```dart
class ApiEnvironment {
  const ApiEnvironment({required this.httpBaseUrl});

  final String httpBaseUrl;

  Uri rest(String path) => Uri.parse('$httpBaseUrl$path');

  Uri get webSocketUri {
    final uri = Uri.parse(httpBaseUrl);
    return uri.replace(
      scheme: uri.scheme == 'https' ? 'wss' : 'ws',
      path: '/ws/sensors',
    );
  }
}
```

Fetch the initial snapshot:

```dart
final response = await http.get(environment.rest('/api/dashboard'));
if (response.statusCode != 200) {
  throw ApiException(response.statusCode, response.body);
}
final snapshot = jsonDecode(response.body) as Map<String, dynamic>;
```

Connect to realtime updates:

```dart
final channel = WebSocketChannel.connect(environment.webSocketUri);

channel.stream.listen((rawMessage) {
  final envelope = jsonDecode(rawMessage as String) as Map<String, dynamic>;
  final streamType = envelope['stream_type'] as String;
  final data = envelope['data'];
  warehouseStore.apply(streamType, data);
});
```

Send an explicit device state:

```dart
Future<Map<String, dynamic>> setDeviceEnabled(
  String deviceId,
  bool enabled,
) async {
  final response = await http.post(
    environment.rest('/api/devices/$deviceId/toggle'),
    headers: {'Content-Type': 'application/json'},
    body: jsonEncode({'enabled': enabled}),
  );

  final body = jsonDecode(response.body) as Map<String, dynamic>;
  if (response.statusCode != 200) {
    throw ApiException(response.statusCode, body['detail']);
  }
  return body;
}
```

Always send `enabled: true` or `enabled: false`. Avoid the bodyless toggle in Flutter because retries could invert the state twice.

## 9. Error handling contract

FastAPI errors use:

```json
{
  "detail": "Human-readable error message"
}
```

| HTTP status | Meaning | Flutter behavior |
|---|---|---|
| `200` | Request accepted/succeeded | Parse response |
| `400` | Unsupported action | Do not retry; show validation message |
| `401` | Invalid login credentials | Show authentication error |
| `404` | Unknown device/entity ID | Refresh configuration or report app defect |
| `409` | Device is offline | Keep control unchanged; show offline message |
| `422` | Invalid JSON/body type | Fix client request; do not retry |
| `503` | MQTT broker unavailable | Show gateway unavailable; allow manual retry |

Use timeouts around 5-10 seconds for REST calls. Retry idempotent GET requests. Do not automatically retry bodyless toggle requests.

## 10. Current security status

The current `/api/auth/login` endpoint is a prototype. It accepts fixed demo credentials and returns a mock token, but the token is not validated by any REST or WebSocket endpoint.

Current consequences:

- REST reads are public while the Cloudflare URL is active.
- Control POST endpoints are public while the Cloudflare URL is active.
- WebSocket data and inbound WebSocket commands are public.
- CORS currently allows every origin.

Use the Quick Tunnel only for a controlled demonstration and do not publish its URL broadly. Production release requires real JWT validation, WebSocket authentication, restricted CORS, secrets in environment variables, and rate limiting.

## 11. Persistence behavior

The dashboard and WebSocket read from an in-memory live cache. InfluxDB stores telemetry and alerts for future historical queries; it is not currently read by any public history endpoint.

Consequences:

- Restarting FastAPI clears the current snapshot and alert list.
- Unity telemetry repopulates live device state after restart.
- History charts are not implemented yet.
- Flutter must not connect directly to InfluxDB.

## 12. Acceptance checklist

- `GET /api/health` returns HTTP 200.
- `mqtt.connected` is `true`.
- Unity is in Play mode.
- `GET /api/dashboard` reports expected live devices.
- WebSocket receives `snapshot` immediately after connection.
- WebSocket receives live device updates afterward.
- `POST /api/devices/arm_1/toggle` returns 200 while `arm_1` is online.
- `pending` becomes `false` after Unity confirms the requested state.
- Flutter reconnects after disabling and re-enabling network access.
- Cloudflare builds use `https://` and `wss://`, never `http://` or `ws://`.

See `docs/API_REFERENCE.md` for every endpoint and schema. See `docs/REALTIME_MQTT_CONTRACT.md` for the internal event contract.
