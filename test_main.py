import unittest
from copy import deepcopy

import main


class _MessagePart:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeMessaging:
    Message = _MessagePart
    Notification = _MessagePart
    AndroidConfig = _MessagePart
    AndroidNotification = _MessagePart

    def __init__(self):
        self.sent_message = None

    def send(self, message, app=None):
        self.sent_message = message
        return "test-message-id"


class BackendLogicTests(unittest.TestCase):
    def setUp(self):
        self.original_device = deepcopy(main.latest_cache["devices"]["arm_1"])
        self.original_robot_device = deepcopy(main.latest_cache["devices"]["agv_01"])
        self.original_send_mqtt = main.send_mqtt_command
        self.original_messaging = main.messaging
        self.original_firebase_app = main.firebase_app
        self.original_query_device_history = main.query_device_history
        self.original_fire_emergency_state = main.fire_emergency_state
        self.original_devices = deepcopy(main.latest_cache["devices"])

    def tearDown(self):
        main.latest_cache["devices"]["arm_1"] = self.original_device
        main.latest_cache["devices"]["agv_01"] = self.original_robot_device
        main.send_mqtt_command = self.original_send_mqtt
        main.messaging = self.original_messaging
        main.firebase_app = self.original_firebase_app
        main.query_device_history = self.original_query_device_history
        main.fire_emergency_state = self.original_fire_emergency_state
        main.latest_cache["devices"] = self.original_devices

    def test_queued_command_is_dispatched_once(self):
        calls = []
        main.send_mqtt_command = lambda command, device_id=None, robot_id=None: (
            calls.append((command, device_id)) or {"command": command}
        )
        device = main.latest_cache["devices"]["arm_1"]
        device.update({
            "desired_enabled": True,
            "reported_enabled": False,
            "pending": True,
            "command_queued": True,
            "last_command_at": None,
        })

        main.dispatch_pending_device_command("arm_1")
        main.dispatch_pending_device_command("arm_1")

        self.assertEqual(calls, [("RESUME", "arm_1")])
        self.assertFalse(device["command_queued"])

    def test_fire_notification_uses_high_priority_topic_message(self):
        fake_messaging = _FakeMessaging()
        main.messaging = fake_messaging
        main.firebase_app = object()

        main.send_fire_notification({
            "machine_id": "arm_1",
            "fire_detected": True,
            "final_temperature": 120,
        })

        message = fake_messaging.sent_message
        self.assertIsNotNone(message)
        self.assertEqual(message.topic, "warehouse_alerts")
        self.assertEqual(message.data["event"], "FIRE_EMERGENCY")
        self.assertEqual(message.android.priority, "high")
        self.assertEqual(message.android.notification.channel_id, "warehouse_fire_alerts")

    def test_fire_emergency_stops_all_devices_only_once(self):
        calls = []
        main.fire_emergency_state = "idle"
        main.send_mqtt_command = lambda command, device_id=None, robot_id=None: (
            calls.append(command) or {"command": command}
        )
        main.messaging = None
        for device in main.latest_cache["devices"].values():
            device.update({
                "desired_enabled": True,
                "reported_enabled": True,
                "pending": False,
            })

        first = main.trigger_fire_emergency({"machine_id": "arm_1", "fire_detected": True})
        second = main.trigger_fire_emergency({"machine_id": "arm_1", "fire_detected": True})

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(calls, ["STOP"])
        self.assertEqual(main.fire_emergency_state, "active")
        for device in main.latest_cache["devices"].values():
            self.assertFalse(device["desired_enabled"])
            self.assertTrue(device["pending"])
            self.assertEqual(device["last_command"], "STOP")

    def test_influx_history_builds_flux_query(self):
        flux = main.build_history_flux(
            "environment_telemetry",
            12,
            100,
            "sensor_id",
            "sensor_01",
            fields=("temperature", "humidity"),
            aggregate_seconds=432,
        )

        self.assertIn('from(bucket: "warehouse")', flux)
        self.assertIn("range(start: -12h)", flux)
        self.assertIn('r["sensor_id"] == "sensor_01"', flux)
        self.assertIn(
            'contains(value: r._field, set: ["temperature", "humidity"])',
            flux,
        )
        self.assertIn("aggregateWindow(every: 432s", flux)
        self.assertIn("limit(n: 100)", flux)

    def test_environment_sensor_history_uses_environment_table(self):
        calls = []
        main.query_device_history = (
            lambda *args, **kwargs: calls.append((args, kwargs)) or []
        )

        response = main.get_environment_sensor_history("SENSOR_01", 12, 100)

        self.assertEqual(response["sensor_id"], "sensor_01")
        self.assertEqual(
            calls,
            [
                (
                    ("environment_telemetry", "sensor_id", "sensor_01", 12, 100),
                    {
                        "fields": ("temperature", "humidity"),
                        "aggregate_seconds": 432,
                    },
                )
            ],
        )

    def test_robot_command_updates_shared_device_state(self):
        main.send_mqtt_command = lambda command, device_id=None, robot_id=None: {
            "command": command,
            "robot_id": robot_id,
        }
        device = main.latest_cache["devices"]["agv_01"]
        device.update({
            "desired_enabled": False,
            "reported_enabled": False,
            "pending": False,
        })

        response = main.robot_control("AGV_01", "resume")

        self.assertEqual(response["mqtt_payload"]["robot_id"], "agv_01")
        self.assertTrue(response["device"]["desired_enabled"])
        self.assertTrue(response["device"]["pending"])
        self.assertEqual(response["device"]["last_command"], "RESUME")


if __name__ == "__main__":
    unittest.main()
