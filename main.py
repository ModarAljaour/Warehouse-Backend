import os
import asyncio
import json
import threading
import time
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from queue import Full, Queue
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt
from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import Point
from pydantic import BaseModel, Field


app = FastAPI(
    title="Digital Twin Warehouse IIoT Gateway",
    description="REST, WebSocket, and MQTT gateway between Unity, Flutter, EMQX, and InfluxDB.",
    version="1.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "emqx")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))

INFLUX_ENABLED = os.getenv("INFLUX_ENABLED", "false").lower() == "true"
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8181")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensor_data")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "").strip()
API_USERNAME = os.getenv("API_USERNAME", "admin").strip()
API_PASSWORD = os.getenv("API_PASSWORD", "").strip()
API_ACCESS_TOKEN = os.getenv("API_ACCESS_TOKEN", "").strip()
#MQTT_BROKER_HOST = "127.0.0.1"
#MQTT_BROKER_PORT = 1883

TOPIC_MACHINE_TELEMETRY = "warehouse/machine/telemetry"
TOPIC_MACHINE_ALERTS = "warehouse/machine/alerts"
TOPIC_ROBOT_TELEMETRY = "warehouse/robot/telemetry"
TOPIC_ROBOT_WEIGHT = "warehouse/robot/weight"
TOPIC_ROBOT_ALERTS = "warehouse/robot/alerts"
TOPIC_FORKLIFT_TELEMETRY = "warehouse/forklift/telemetry"
TOPIC_FORKLIFT_ALERTS = "warehouse/forklift/alerts"
TOPIC_ENVIRONMENT_TELEMETRY = "warehouse/environment/telemetry"
TOPIC_CONTROL = "warehouse/control"

CONFIGURED_MACHINE_IDS = ["arm_1"]
CONFIGURED_ROBOT_IDS = ["agv_01", "agv_02"]
CONFIGURED_FORKLIFT_IDS = ["forklift_01"]
CONFIGURED_CONVEYOR_IDS = ["conveyor_01"]
CONFIGURED_ENVIRONMENT_SENSOR_IDS = [f"sensor_{index:02d}" for index in range(1, 7)]
ONLINE_TIMEOUT_SECONDS = int(os.getenv("ONLINE_TIMEOUT_SECONDS", "60"))

#INFLUX_URL = "http://localhost:8181"
#INFLUX_BUCKET = "sensor_data"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def device_record(device_id: str, device_type: str) -> dict:
    return {
        "device_id": device_id,
        "device_type": device_type,
        "desired_enabled": True,
        "reported_enabled": None,
        "pending": False,
        "last_command": "RESUME",
        "last_command_at": None,
    }


latest_cache: Dict[str, Any] = {
    "machines": {
        machine_id: {
            "machine_id": machine_id,
            "temperature": None,
            "current": None,
            "state": "unknown",
            "active": None,
            "source": "configured",
            "last_seen": None,
        }
        for machine_id in CONFIGURED_MACHINE_IDS
    },
    "robot_weights": {},
    "alerts": [],
    "robots": {
        robot_id: {
            "robot_id": robot_id,
            "battery": None,
            "status": "unknown",
            "low_battery": False,
            "safety_stop": False,
            "timestamp": None,
            "source": "configured",
            "last_seen": None,
        }
        for robot_id in CONFIGURED_ROBOT_IDS
    },
    "forklifts": {
        forklift_id: {
            "forklift_id": forklift_id,
            "battery": None,
            "speed": 0.0,
            "status": "unknown",
            "source": "configured",
            "last_seen": None,
        }
        for forklift_id in CONFIGURED_FORKLIFT_IDS
    },
    "environment_sensors": {
        sensor_id: {
            "sensor_id": sensor_id,
            "sensor_type": "environment",
            "temperature": None,
            "humidity": None,
            "event_name": None,
            "timestamp": None,
            "source": "configured",
            "last_seen": None,
        }
        for sensor_id in CONFIGURED_ENVIRONMENT_SENSOR_IDS
    },
    "devices": {
        **{machine_id: device_record(machine_id, "machine") for machine_id in CONFIGURED_MACHINE_IDS},
        **{robot_id: device_record(robot_id, "robot") for robot_id in CONFIGURED_ROBOT_IDS},
        **{forklift_id: device_record(forklift_id, "forklift") for forklift_id in CONFIGURED_FORKLIFT_IDS},
        **{conveyor_id: device_record(conveyor_id, "conveyor") for conveyor_id in CONFIGURED_CONVEYOR_IDS},
    },
}

cache_lock = threading.RLock()
connected_websockets: List[WebSocket] = []
main_loop = None
mqtt_connected = False
last_mqtt_message_at: Optional[str] = None
last_influx_write_at: Optional[str] = None
last_influx_error: Optional[str] = None
influx_queue: Queue = Queue(maxsize=1000)


class LoginModel(BaseModel):
    username: str = Field(..., examples=["admin"])
    password: str = Field(..., examples=["your_password"])


class DeviceToggleRequest(BaseModel):
    enabled: Optional[bool] = Field(
        default=None,
        description="true sends RESUME, false sends STOP. If omitted, the desired state is toggled.",
    )


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def record_is_online(record: dict) -> bool:
    last_seen = parse_iso(record.get("last_seen"))
    if last_seen is None:
        return False
    return (datetime.now(timezone.utc) - last_seen).total_seconds() <= ONLINE_TIMEOUT_SECONDS


def build_snapshot() -> dict:
    with cache_lock:
        snapshot = deepcopy(latest_cache)

    for group_name in ("machines", "robots", "forklifts", "environment_sensors"):
        for record in snapshot[group_name].values():
            record["online"] = record_is_online(record)

    for device_id, device in snapshot["devices"].items():
        source = (
            snapshot["machines"].get(device_id)
            or snapshot["robots"].get(device_id)
            or snapshot["forklifts"].get(device_id)
        )
        device["online"] = record_is_online(source) if source else False

    return snapshot


async def send_to_sockets(data_dict: dict):
    for websocket in list(connected_websockets):
        try:
            await websocket.send_json(data_dict)
        except Exception:
            if websocket in connected_websockets:
                connected_websockets.remove(websocket)


def broadcast_from_mqtt(stream_type: str, payload: dict):
    if main_loop is not None:
        asyncio.run_coroutine_threadsafe(
            send_to_sockets({"stream_type": stream_type, "data": payload}),
            main_loop,
        )


def write_point(point: Point):
    global last_influx_error
    if not INFLUX_ENABLED:
        return
    try:
        influx_queue.put_nowait(point)
    except Full:
        last_influx_error = "InfluxDB write queue is full"
        print(last_influx_error)


def start_influx_writer_thread():
    global last_influx_write_at, last_influx_error
    while True:
        point = influx_queue.get()
        try:
            line_protocol = point.to_line_protocol()
            request = urllib.request.Request(
                f"{INFLUX_URL}/api/v3/write_lp?db={INFLUX_BUCKET}",
                data=line_protocol.encode("utf-8"),
                method="POST",
                headers=influx_headers(),
            )
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status not in (200, 204):
                    raise RuntimeError(f"InfluxDB returned HTTP {response.status}")
            last_influx_write_at = utc_now_iso()
            last_influx_error = None
        except Exception as exc:
            last_influx_error = str(exc)
            print(f"InfluxDB write error: {exc}")
        finally:
            influx_queue.task_done()


def influx_headers() -> dict:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if INFLUX_TOKEN:
        headers["Authorization"] = f"Bearer {INFLUX_TOKEN}"
    return headers


def query_influx(sql: str, params: Optional[dict] = None):
    if not INFLUX_ENABLED:
        raise HTTPException(status_code=503, detail="InfluxDB history is disabled")

    body = json.dumps({
        "db": INFLUX_BUCKET,
        "q": sql,
        "format": "json",
        "params": params or {},
    }).encode("utf-8")
    request = urllib.request.Request(
        f"{INFLUX_URL}/api/v3/query_sql",
        data=body,
        method="POST",
        headers=influx_headers(),
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else []
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(
            status_code=502,
            detail=f"InfluxDB query failed ({exc.code}): {detail}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"InfluxDB is unavailable: {exc}",
        ) from exc


def history_start(hours: int) -> str:
    return (
        datetime.now(timezone.utc) - timedelta(hours=hours)
    ).isoformat().replace("+00:00", "Z")


def query_device_history(
    table: str,
    id_field: str,
    device_id: str,
    hours: int,
    limit: int,
):
    sql = (
        f'SELECT * FROM "{table}" '
        f'WHERE time >= $start_time AND "{id_field}" = $device_id '
        f'ORDER BY time DESC LIMIT {limit}'
    )
    return query_influx(sql, {
        "start_time": history_start(hours),
        "device_id": device_id,
    })


def update_device_report(device_id: str, reported_enabled: bool):
    device = latest_cache["devices"].get(device_id)
    if device is None:
        return
    device["reported_enabled"] = reported_enabled
    device["pending"] = device["desired_enabled"] != reported_enabled


def send_mqtt_command(command_str: str, device_id: Optional[str] = None, robot_id: Optional[str] = None):
    payload = {"command": command_str.upper(), "sender": "central_backend_gateway"}
    if device_id:
        payload["device_id"] = device_id
    if robot_id:
        payload["robot_id"] = robot_id

    try:
        pub_client = mqtt.Client()
        pub_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        pub_client.loop_start()
        result = pub_client.publish(TOPIC_CONTROL, json.dumps(payload))
        result.wait_for_publish(timeout=3)
        pub_client.loop_stop()
        pub_client.disconnect()
        print(f"Forwarded control command to Unity: {payload}")
        return payload
    except Exception as exc:
        print(f"Failed to send MQTT command: {exc}")
        raise HTTPException(status_code=503, detail="MQTT broker is not available")


def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = rc == 0
    print(f"FastAPI MQTT connection result: {rc}")
    for topic in (
        TOPIC_MACHINE_TELEMETRY,
        TOPIC_MACHINE_ALERTS,
        TOPIC_ROBOT_TELEMETRY,
        TOPIC_ROBOT_WEIGHT,
        TOPIC_ROBOT_ALERTS,
        TOPIC_FORKLIFT_TELEMETRY,
        TOPIC_FORKLIFT_ALERTS,
        TOPIC_ENVIRONMENT_TELEMETRY,
    ):
        client.subscribe(topic)


def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print(f"FastAPI disconnected from MQTT broker: {rc}")


def on_message(client, userdata, msg):
    global last_mqtt_message_at
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        last_mqtt_message_at = utc_now_iso()
        handlers = {
            TOPIC_MACHINE_TELEMETRY: handle_machine_telemetry,
            TOPIC_MACHINE_ALERTS: handle_machine_alert,
            TOPIC_ROBOT_TELEMETRY: handle_robot_telemetry,
            TOPIC_ROBOT_WEIGHT: handle_robot_weight,
            TOPIC_ROBOT_ALERTS: handle_robot_alert,
            TOPIC_FORKLIFT_TELEMETRY: handle_forklift_telemetry,
            TOPIC_FORKLIFT_ALERTS: handle_forklift_alert,
            TOPIC_ENVIRONMENT_TELEMETRY: handle_environment_telemetry,
        }
        handlers[msg.topic](payload)
    except Exception as exc:
        print(f"MQTT processing error on {msg.topic}: {exc}")


def handle_machine_telemetry(payload: dict):
    machine_id = str(payload.get("machine_id", "")).strip().lower()
    if machine_id not in CONFIGURED_MACHINE_IDS:
        print(f"Ignored unknown machine_id: {machine_id!r}")
        return

    normalized = dict(payload)
    normalized["machine_id"] = machine_id
    normalized["source"] = "live"
    normalized["last_seen"] = utc_now_iso()
    normalized["active"] = bool(
        payload.get("active", str(payload.get("state", "")).lower() != "stopped")
    )
    with cache_lock:
        latest_cache["machines"][machine_id] = normalized
        update_device_report(machine_id, normalized["active"])

    broadcast_from_mqtt("machine_telemetry", normalized)
    point = Point("machine_telemetry").tag("machine_id", machine_id)
    for key in (
        "temperature",
        "current",
        "state",
        "active",
        "event",
        "unity_object",
        "unity_path",
        "position_x",
        "position_y",
        "position_z",
    ):
        add_field(point, key, normalized.get(key))
    write_point(point)


def handle_machine_alert(payload: dict):
    machine_id = str(payload.get("machine_id", "")).strip().lower()
    if machine_id not in CONFIGURED_MACHINE_IDS:
        print(f"Ignored alert from unknown machine_id: {machine_id!r}")
        return
    payload = {**payload, "machine_id": machine_id}
    add_alert("machine_alert", payload)
    point = Point("machine_alerts").tag("machine_id", machine_id)
    for key, value in payload.items():
        if key != "machine_id":
            add_field(point, key, value)
    write_point(point)


def handle_robot_telemetry(payload: dict):
    robot_id = str(payload.get("robot_id", "")).strip().lower()
    if robot_id not in CONFIGURED_ROBOT_IDS:
        print(f"Ignored unknown robot_id: {robot_id!r}")
        return

    status = str(payload.get("status", "unknown")).lower()
    normalized = {
        **payload,
        "robot_id": robot_id,
        "status": status,
        "source": "live",
        "last_seen": utc_now_iso(),
    }
    reported_enabled = not status.startswith("stopped")
    with cache_lock:
        latest_cache["robots"][robot_id] = normalized
        update_device_report(robot_id, reported_enabled)

    broadcast_from_mqtt("robot_telemetry", normalized)
    point = Point("robot_telemetry").tag("robot_id", robot_id)
    for key in ("battery", "status", "low_battery", "safety_stop"):
        add_field(point, key, normalized.get(key))
    write_point(point)


def handle_robot_weight(payload: dict):
    robot_id = str(payload.get("robot_id", "")).strip().lower()
    if robot_id not in CONFIGURED_ROBOT_IDS:
        print(f"Ignored weight from unknown robot_id: {robot_id!r}")
        return
    normalized = {**payload, "robot_id": robot_id, "last_seen": utc_now_iso()}
    with cache_lock:
        latest_cache["robot_weights"][robot_id] = normalized
    broadcast_from_mqtt("robot_weight", normalized)
    point = Point("robot_weight").tag("robot_id", robot_id)
    for key, value in normalized.items():
        if key not in ("robot_id", "last_seen"):
            add_field(point, key, value)
    write_point(point)


def handle_robot_alert(payload: dict):
    robot_id = str(payload.get("robot_id", "")).strip().lower()
    if robot_id not in CONFIGURED_ROBOT_IDS:
        print(f"Ignored alert from unknown robot_id: {robot_id!r}")
        return
    payload = {**payload, "robot_id": robot_id}
    add_alert("robot_alert", payload)
    point = Point("robot_alerts").tag("robot_id", robot_id)
    for key, value in payload.items():
        if key != "robot_id":
            add_field(point, key, value)
    write_point(point)


def handle_forklift_telemetry(payload: dict):
    forklift_id = str(payload.get("forklift_id", "")).strip().lower()
    if forklift_id not in CONFIGURED_FORKLIFT_IDS:
        print(f"Ignored unknown forklift_id: {forklift_id!r}")
        return

    status = str(payload.get("status", "unknown")).lower()
    normalized = {
        **payload,
        "forklift_id": forklift_id,
        "status": status,
        "source": "live",
        "last_seen": utc_now_iso(),
    }
    reported_enabled = status != "stopped"
    with cache_lock:
        latest_cache["forklifts"][forklift_id] = normalized
        update_device_report(forklift_id, reported_enabled)

    broadcast_from_mqtt("forklift_telemetry", normalized)
    point = Point("forklift_telemetry").tag("forklift_id", forklift_id)
    for key in ("battery", "speed", "status", "event_name"):
        add_field(point, key, normalized.get(key))
    write_point(point)


def handle_forklift_alert(payload: dict):
    forklift_id = str(payload.get("forklift_id", "")).strip().lower()
    if forklift_id not in CONFIGURED_FORKLIFT_IDS:
        print(f"Ignored alert from unknown forklift_id: {forklift_id!r}")
        return
    payload = {**payload, "forklift_id": forklift_id}
    add_alert("forklift_alert", payload)
    point = Point("forklift_alerts").tag("forklift_id", forklift_id)
    for key, value in payload.items():
        if key != "forklift_id":
            add_field(point, key, value)
    write_point(point)


def handle_environment_telemetry(payload: dict):
    sensor_id = str(payload.get("sensor_id", "")).strip().lower()
    if sensor_id not in CONFIGURED_ENVIRONMENT_SENSOR_IDS:
        print(f"Ignored unknown sensor_id: {sensor_id!r}")
        return

    normalized = {
        **payload,
        "sensor_id": sensor_id,
        "sensor_type": "environment",
        "source": "live",
        "last_seen": utc_now_iso(),
    }
    with cache_lock:
        latest_cache["environment_sensors"][sensor_id] = normalized

    broadcast_from_mqtt("environment_sensor_telemetry", normalized)
    point = Point("environment_telemetry").tag("sensor_id", sensor_id)
    for key in ("temperature", "humidity", "event_name"):
        add_field(point, key, normalized.get(key))
    write_point(point)


def add_alert(alert_type: str, payload: dict):
    alert = dict(payload)
    alert["alert_type"] = alert_type
    alert.setdefault("timestamp", utc_now_iso())
    with cache_lock:
        latest_cache["alerts"].insert(0, alert)
        latest_cache["alerts"] = latest_cache["alerts"][:100]
    broadcast_from_mqtt("alert", alert)


def add_field(point: Point, key: str, value: Any):
    if value is None:
        return
    if isinstance(value, bool):
        point.field(key, value)
    elif isinstance(value, (int, float)):
        point.field(key, float(value))
    else:
        point.field(key, str(value))


def start_mqtt_thread():
    while True:
        try:
            mqtt_client = mqtt.Client()
            mqtt_client.on_connect = on_connect
            mqtt_client.on_disconnect = on_disconnect
            mqtt_client.on_message = on_message
            mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
            mqtt_client.loop_forever()
        except Exception as exc:
            print(f"MQTT listener error: {exc}. Retrying in 3 seconds...")
            time.sleep(3)

@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()

    threading.Thread(
        target=start_mqtt_thread,
        daemon=True
    ).start()

    if INFLUX_ENABLED:
        threading.Thread(
            target=start_influx_writer_thread,
            daemon=True
        ).start()
#@app.on_event("startup")
#async def startup_event():
#    global main_loop
#    main_loop = asyncio.get_running_loop()
#    threading.Thread(target=start_mqtt_thread, daemon=True).start()
#    threading.Thread(target=start_influx_writer_thread, daemon=True).start()


@app.get("/", tags=["System"], summary="API landing endpoint")
def root():
    return {
        "name": "Digital Twin Warehouse IIoT Gateway",
        "version": app.version,
        "status": "running",
        "docs": "/docs",
        "websocket": "/ws/sensors",
    }


@app.get("/api/health", tags=["System"], summary="Health check")
def health_check():
    influx_connected = False
    if INFLUX_ENABLED:
        try:
            request = urllib.request.Request(
                f"{INFLUX_URL}/health",
                headers=influx_headers(),
            )
            with urllib.request.urlopen(request, timeout=2) as response:
                influx_connected = response.status == 200
        except Exception:
            influx_connected = False
    return {
        "status": "ok" if mqtt_connected else "degraded",
        "mqtt": {
            "connected": mqtt_connected,
            "broker": f"{MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}",
            "last_message_at": last_mqtt_message_at,
        },
        "influxdb": {
            "enabled": INFLUX_ENABLED,
            "connected": influx_connected,
            "url": INFLUX_URL,
            "last_write_at": last_influx_write_at,
            "last_error": last_influx_error,
            "queued_writes": influx_queue.qsize(),
        },
        "websocket_clients": len(connected_websockets),
    }


@app.websocket("/ws/sensors")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.append(websocket)
    await websocket.send_json({"stream_type": "snapshot", "data": build_snapshot()})
    try:
        while True:
            incoming_text = await websocket.receive_text()
            try:
                incoming = json.loads(incoming_text)
                command = str(incoming.get("command", "")).upper()
                device_id = incoming.get("device_id")
            except json.JSONDecodeError:
                command = incoming_text.strip().upper()
                device_id = None
            if command in {"STOP", "RESUME", "RESET", "CLEAR", "CHARGE"}:
                send_mqtt_command(command, device_id=device_id)
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)


@app.post("/api/auth/login", tags=["Auth"], summary="Login")
def login(user: LoginModel):
    if API_PASSWORD and API_ACCESS_TOKEN and user.username == API_USERNAME and user.password == API_PASSWORD:
        return {"access_token": API_ACCESS_TOKEN, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/sensors", tags=["Dashboard"], summary="Current sensor readings")
def get_sensors():
    snapshot = build_snapshot()
    return {
        "machines": snapshot["machines"],
        "robot_weights": snapshot["robot_weights"],
        "robots": snapshot["robots"],
        "forklifts": snapshot["forklifts"],
        "environment_sensors": snapshot["environment_sensors"],
    }


@app.get("/api/stats", tags=["Dashboard"], summary="Warehouse KPIs")
def get_stats():
    snapshot = build_snapshot()
    machines = list(snapshot["machines"].values())
    robots = list(snapshot["robots"].values())
    forklifts = list(snapshot["forklifts"].values())
    active_machines = sum(1 for item in machines if item["online"] and item.get("active") is True)
    active_robots = sum(
        1 for item in robots
        if item["online"] and not str(item.get("status", "")).startswith("stopped")
    )
    active_forklifts = sum(
        1 for item in forklifts if item["online"] and item.get("status") != "stopped"
    )
    attention_required = any(
        item["online"] and str(item.get("state", "")) in {"danger", "stopped"}
        for item in machines
    ) or any(
        item["online"] and str(item.get("status", "")).startswith("stopped")
        for item in robots + forklifts
    )
    return {
        "machines": {"total": len(machines), "online": sum(item["online"] for item in machines), "active": active_machines},
        "robots": {"total": len(robots), "online": sum(item["online"] for item in robots), "active": active_robots},
        "forklifts": {"total": len(forklifts), "online": sum(item["online"] for item in forklifts), "active": active_forklifts},
        "alerts_count": len(snapshot["alerts"]),
        "devices_count": len(snapshot["devices"]),
        "system_health": "Attention Required" if attention_required else "Excellent",
    }


@app.get("/api/machines", tags=["Machines"], summary="Current machine states")
def get_machines():
    return build_snapshot()["machines"]


@app.get("/api/machines/{machine_id}", tags=["Machines"], summary="Current state for one machine")
def get_machine(machine_id: str):
    machine = build_snapshot()["machines"].get(machine_id.lower())
    if machine is None:
        raise HTTPException(status_code=404, detail="Machine not found")
    return machine


@app.get("/api/robots", tags=["Robots"], summary="Current robot states")
def get_robots():
    return build_snapshot()["robots"]


@app.get("/api/robots/{robot_id}", tags=["Robots"], summary="Current state for one robot")
def get_robot(robot_id: str):
    robot = build_snapshot()["robots"].get(robot_id.lower())
    if robot is None:
        raise HTTPException(status_code=404, detail="Robot not found")
    return robot


@app.get("/api/forklifts", tags=["Forklifts"], summary="Current forklift states")
def get_forklifts():
    return build_snapshot()["forklifts"]


@app.get("/api/forklifts/{forklift_id}", tags=["Forklifts"], summary="Current state for one forklift")
def get_forklift(forklift_id: str):
    forklift = build_snapshot()["forklifts"].get(forklift_id.lower())
    if forklift is None:
        raise HTTPException(status_code=404, detail="Forklift not found")
    return forklift


@app.get("/api/environment-sensors", tags=["Sensors"], summary="Current environment sensor readings")
def get_environment_sensors():
    return build_snapshot()["environment_sensors"]


@app.get("/api/environment-sensors/{sensor_id}", tags=["Sensors"], summary="Current reading for one environment sensor")
def get_environment_sensor(sensor_id: str):
    sensor = build_snapshot()["environment_sensors"].get(sensor_id.lower())
    if sensor is None:
        raise HTTPException(status_code=404, detail="Environment sensor not found")
    return sensor


@app.get("/api/devices", tags=["Control"], summary="Configured controllable devices")
def get_devices():
    return build_snapshot()["devices"]


@app.get("/api/alerts", tags=["Dashboard"], summary="Recent alerts")
def get_alerts():
    return build_snapshot()["alerts"]


@app.get("/api/dashboard", tags=["Dashboard"], summary="Full warehouse snapshot for Flutter")
def get_dashboard():
    return build_snapshot()


@app.get("/api/robot-weight", tags=["Robots"], summary="Current robot weight sensor readings")
def get_robot_weight():
    return build_snapshot()["robot_weights"]


@app.get("/api/history/machines/{machine_id}", tags=["History"], summary="Machine telemetry history")
def get_machine_history(
    machine_id: str,
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(500, ge=1, le=5000),
):
    machine_id = machine_id.lower()
    if machine_id not in CONFIGURED_MACHINE_IDS:
        raise HTTPException(status_code=404, detail="Machine not found")
    data = query_device_history(
        "machine_telemetry", "machine_id", machine_id, hours, limit
    )
    return {"machine_id": machine_id, "hours": hours, "data": data}


@app.get("/api/history/robots/{robot_id}", tags=["History"], summary="Robot telemetry history")
def get_robot_history(
    robot_id: str,
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(500, ge=1, le=5000),
):
    robot_id = robot_id.lower()
    if robot_id not in CONFIGURED_ROBOT_IDS:
        raise HTTPException(status_code=404, detail="Robot not found")
    data = query_device_history(
        "robot_telemetry", "robot_id", robot_id, hours, limit
    )
    return {"robot_id": robot_id, "hours": hours, "data": data}


@app.get("/api/history/forklifts/{forklift_id}", tags=["History"], summary="Forklift telemetry history")
def get_forklift_history(
    forklift_id: str,
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(500, ge=1, le=5000),
):
    forklift_id = forklift_id.lower()
    if forklift_id not in CONFIGURED_FORKLIFT_IDS:
        raise HTTPException(status_code=404, detail="Forklift not found")
    data = query_device_history(
        "forklift_telemetry", "forklift_id", forklift_id, hours, limit
    )
    return {"forklift_id": forklift_id, "hours": hours, "data": data}


@app.get("/api/history/alerts", tags=["History"], summary="Persisted alert history")
def get_alert_history(
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(200, ge=1, le=2000),
):
    start_time = history_start(hours)
    histories = {}
    for alert_type, table in (
        ("machines", "machine_alerts"),
        ("robots", "robot_alerts"),
        ("forklifts", "forklift_alerts"),
    ):
        histories[alert_type] = query_influx(
            f'SELECT * FROM "{table}" '
            f'WHERE time >= $start_time ORDER BY time DESC LIMIT {limit}',
            {"start_time": start_time},
        )
    return {"hours": hours, "alerts": histories}


@app.get("/api/history/dashboard", tags=["History"], summary="Historical dashboard snapshot")
def get_history_dashboard(
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(200, ge=1, le=1000),
):
    return {
        "hours": hours,
        "machines": {
            device_id: query_device_history(
                "machine_telemetry", "machine_id", device_id, hours, limit
            )
            for device_id in CONFIGURED_MACHINE_IDS
        },
        "robots": {
            device_id: query_device_history(
                "robot_telemetry", "robot_id", device_id, hours, limit
            )
            for device_id in CONFIGURED_ROBOT_IDS
        },
        "forklifts": {
            device_id: query_device_history(
                "forklift_telemetry", "forklift_id", device_id, hours, limit
            )
            for device_id in CONFIGURED_FORKLIFT_IDS
        },
    }


@app.post("/api/devices/{device_id}/toggle", tags=["Control"], summary="Toggle a device through MQTT")
def toggle_device(device_id: str, body: Optional[DeviceToggleRequest] = Body(default=None)):
    device_id = device_id.lower()
    snapshot_device = build_snapshot()["devices"].get(device_id)
    if snapshot_device is None:
        raise HTTPException(status_code=404, detail="Unknown device_id")
    if not snapshot_device.get("online", False):
        raise HTTPException(
            status_code=409,
            detail=f"{device_id} is offline. Start Unity and wait for live telemetry before sending control commands.",
        )

    with cache_lock:
        current = latest_cache["devices"].get(device_id)
        if current is None:
            raise HTTPException(status_code=404, detail="Unknown device_id")
        next_enabled = (
            not current["desired_enabled"]
            if body is None or body.enabled is None
            else body.enabled
        )

    command = "RESUME" if next_enabled else "STOP"
    mqtt_payload = send_mqtt_command(command, device_id=device_id)
    with cache_lock:
        current.update({
            "desired_enabled": next_enabled,
            "pending": current["reported_enabled"] != next_enabled,
            "last_command": command,
            "last_command_at": utc_now_iso(),
            "last_mqtt_payload": mqtt_payload,
        })
        response_device = deepcopy(current)
    broadcast_from_mqtt("device_command", response_device)
    return {
        "status": "COMMAND_SENT",
        "device": response_device,
        "message": f"Waiting for {device_id} to confirm {command}",
    }


@app.post("/api/emergency/{action}", tags=["Control"], summary="Send emergency command to Unity")
def remote_emergency_control(action: str):
    command = action.upper()
    if command not in {"STOP", "RESUME", "RESET", "CLEAR", "CHARGE"}:
        raise HTTPException(status_code=400, detail="Use STOP, RESUME, RESET, CLEAR, or CHARGE")
    mqtt_payload = send_mqtt_command(command)
    return {"status": "COMMAND_SENT", "mqtt_payload": mqtt_payload, "message": f"Global {command} sent"}


@app.post("/api/robots/{robot_id}/command/{action}", tags=["Control"], summary="Send command to one robot")
def robot_control(robot_id: str, action: str):
    robot_id = robot_id.lower()
    if robot_id not in CONFIGURED_ROBOT_IDS:
        raise HTTPException(status_code=404, detail="Robot not found")
    command = action.upper()
    if command not in {"STOP", "RESUME", "RESET", "CLEAR", "CHARGE"}:
        raise HTTPException(status_code=400, detail="Use STOP, RESUME, RESET, CLEAR, or CHARGE")
    mqtt_payload = send_mqtt_command(command, robot_id=robot_id)
    return {"status": "COMMAND_SENT", "robot_id": robot_id, "mqtt_payload": mqtt_payload}
