import unittest
from pyspark.sql import SparkSession
from src.anomaly_detector import AnomalyDetector


class TestAnomalyDetector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("TestAnomalyDetector")
            .master("local[2]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.detector = AnomalyDetector()

        # Create sample data
        self.sample_data = [
            {
                "timestamp": "2023-01-01T00:00:00",
                "ip": "192.168.1.1",
                "response_time": 1.0,
            },
            {
                "timestamp": "2023-01-01T00:01:00",
                "ip": "192.168.1.1",
                "response_time": 1.1,
            },
            {
                "timestamp": "2023-01-01T00:02:00",
                "ip": "192.168.1.2",
                "response_time": 10.0,  # Anomaly
            },
        ]

        self.df = self.spark.createDataFrame(self.sample_data)

    def test_prepare_features(self):
        """Test feature preparation."""
        features_df = self.detector.prepare_features(self.df)
        self.assertIsNotNone(features_df)
        self.assertIn("request_count", features_df.columns)
        self.assertIn("avg_response_time", features_df.columns)

    def test_train_model(self):
        """Test model training."""
        model = self.detector.train_model(self.df)
        self.assertIsNotNone(model)
        self.assertIsNotNone(self.detector.model)

    def test_detect_anomalies(self):
        """Test anomaly detection."""
        # Train model first
        self.detector.train_model(self.df)

        # Detect anomalies
        anomalies = self.detector.detect_anomalies(self.df)
        self.assertIsNotNone(anomalies)

        # Check if anomaly was detected
        anomaly_ips = [row.ip for row in anomalies.collect()]
        self.assertIn("192.168.1.2", anomaly_ips)  # IP with high response time


if __name__ == "__main__":
    unittest.main()
