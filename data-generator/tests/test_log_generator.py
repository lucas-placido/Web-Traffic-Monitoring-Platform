import unittest
from src.log_generator import WebLogGenerator


class TestWebLogGenerator(unittest.TestCase):
    def setUp(self):
        self.generator = WebLogGenerator(
            kafka_broker="localhost:9091", topic="test-logs"
        )

    def test_generate_ip(self):
        ip = self.generator.generate_ip()
        self.assertIsInstance(ip, str)
        parts = ip.split(".")
        self.assertEqual(len(parts), 4)
        for part in parts:
            self.assertTrue(0 <= int(part) <= 255)

    def test_generate_log(self):
        log = self.generator.generate_log()
        self.assertIsInstance(log, dict)
        self.assertIn("timestamp", log)
        self.assertIn("ip", log)
        self.assertIn("endpoint", log)
        self.assertIn("user_agent", log)
        self.assertIn("status_code", log)
        self.assertIn("response_time", log)
        self.assertIn("bytes_sent", log)

        self.assertIn(log["endpoint"], self.generator.endpoints)
        self.assertIn(log["user_agent"], self.generator.user_agents)
        self.assertIn(log["status_code"], self.generator.status_codes)
        self.assertTrue(0.1 <= log["response_time"] <= 5.0)
        self.assertTrue(100 <= log["bytes_sent"] <= 10000)


if __name__ == "__main__":
    unittest.main()
