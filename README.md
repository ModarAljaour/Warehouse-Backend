# Warehouse Digital Twin Backend

FastAPI gateway connecting Unity, Flutter, EMQX MQTT, WebSocket clients, and optional InfluxDB history.

## Features

- Machine, AGV, forklift, weight, alert, and environmental sensor telemetry
- MQTT command delivery to Unity
- REST API with Swagger/OpenAPI documentation
- Real-time WebSocket snapshots and telemetry events
- Optional InfluxDB history endpoints
- Firebase fire notifications to the `warehouse_alerts` topic
- Docker Compose deployment

## Local setup

```bash
cp .env.example .env
docker compose up -d --build
```

Open:

- API: `http://127.0.0.1:8000`
- Swagger: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/api/health`
- WebSocket: `ws://127.0.0.1:8000/ws/sensors`

The demo login endpoint uses the fixed project credentials expected by the Flutter application.

## Firebase notifications

Download a Firebase Admin service-account JSON file and keep it outside the repository. Set `FIREBASE_CREDENTIALS` to its path and optionally change `FCM_TOPIC`. Fire alerts received from Unity are sent as high-priority Android notifications.

For Docker Compose, save the key as `secrets/firebase-service-account.json`. It is mounted read-only at `/run/secrets/firebase-service-account.json` and the entire `secrets` directory is ignored by Git.

## Documentation

- Flutter integration: `FLUTTER_API.md`
- OpenAPI schema: `/openapi.json` on the running API
- Postman collections: `postman/`
