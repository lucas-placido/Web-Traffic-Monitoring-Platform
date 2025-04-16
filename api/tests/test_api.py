import unittest
from fastapi.testclient import TestClient
from src.main import app


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_root_endpoint(self):
        """Test the root endpoint."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["name"], "Web Traffic Analytics API")
        self.assertEqual(data["version"], "1.0.0")
        self.assertIn("endpoints", data)

    def test_endpoint_metrics(self):
        """Test the endpoint metrics endpoint."""
        response = self.client.get("/metrics/endpoints?hours=1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)

    def test_ip_metrics(self):
        """Test the IP metrics endpoint."""
        response = self.client.get("/metrics/ip/192.168.1.1?hours=1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("ip", data)
        self.assertIn("total_requests", data)
        self.assertIn("avg_response_time", data)
        self.assertIn("success_rate", data)

    def test_top_ips(self):
        """Test the top IPs endpoint."""
        response = self.client.get("/metrics/top-ips?limit=5&hours=1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertLessEqual(len(data), 5)

    def test_response_times(self):
        """Test the response times endpoint."""
        response = self.client.get("/metrics/response-times?hours=1")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("min_response_time", data)
        self.assertIn("max_response_time", data)
        self.assertIn("avg_response_time", data)
        self.assertIn("stddev_response_time", data)


if __name__ == "__main__":
    unittest.main()
