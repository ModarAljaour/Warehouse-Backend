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
        self.original_send_mqtt = main.send_mqtt_command
        self.original_messaging = main.messaging
        self.original_firebase_app = main.firebase_app
        self.original_query_device_history = main.query_device_history

    def tearDown(self):
        main.latest_cache["devices"]["arm_1"] = self.original_device
        main.send_mqtt_command = self.original_send_mqtt
        main.messaging = self.original_messaging
        main.firebase_app = self.original_firebase_app
        main.query_device_history = self.original_query_device_history

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

    def test_influx_write_uses_line_protocol_content_type(self):
        headers = main.influx_headers("text/plain")

        self.assertEqual(headers["Content-Type"], "text/plain")

    def test_environment_sensor_history_uses_environment_table(self):
        calls = []
        main.query_device_history = lambda *args: calls.append(args) or []

        response = main.get_environment_sensor_history("SENSOR_01", 12, 100)

        self.assertEqual(response["sensor_id"], "sensor_01")
        self.assertEqual(
            calls,
            [("environment_telemetry", "sensor_id", "sensor_01", 12, 100)],
        )


if __name__ == "__main__":
    unittest.main()
