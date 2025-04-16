import unittest
from pyspark.sql import SparkSession
from src.log_processor import LogProcessor


class TestLogProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("TestWebLogProcessor")
            .master("local[2]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.processor = LogProcessor()

    def test_schema(self):
        """Test if the schema is correctly defined."""
        schema = self.processor.schema
        self.assertEqual(len(schema.fields), 7)

        # Check field names and types
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "timestamp",
            "ip",
            "endpoint",
            "user_agent",
            "status_code",
            "response_time",
            "bytes_sent",
        ]
        self.assertEqual(field_names, expected_fields)

    def test_process_stream(self):
        """Test the stream processing logic with sample data."""
        # Create sample data
        sample_data = [
            {
                "timestamp": "2023-01-01T00:00:00",
                "ip": "192.168.1.1",
                "endpoint": "/api/v1/users",
                "user_agent": "Mozilla/5.0",
                "status_code": 200,
                "response_time": 1.5,
                "bytes_sent": 1000,
            }
        ]

        # Create DataFrame
        df = self.spark.createDataFrame(sample_data, self.processor.schema)

        # Test processing
        processed_df = self.processor.process_stream()
        self.assertIsNotNone(processed_df)

        # Check if required columns exist
        expected_columns = [
            "window",
            "endpoint",
            "request_count",
            "avg_response_time",
            "total_requests",
        ]
        self.assertTrue(all(col in processed_df.columns for col in expected_columns))


if __name__ == "__main__":
    unittest.main()
