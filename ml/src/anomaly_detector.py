from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import json
from datetime import datetime, timedelta


class AnomalyDetector:
    def __init__(self, kafka_broker: str = "localhost:9092", topic: str = "web-logs"):
        self.spark = (
            SparkSession.builder.appName("AnomalyDetector")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .getOrCreate()
        )

        self.kafka_broker = kafka_broker
        self.topic = topic

        # Initialize ML model
        self.model = None
        self.assembler = VectorAssembler(
            inputCols=["request_count", "avg_response_time"], outputCol="features"
        )
        self.kmeans = KMeans(k=3, seed=42)
        self.pipeline = Pipeline(stages=[self.assembler, self.kmeans])

    def prepare_features(self, df):
        """Prepare features for anomaly detection."""
        # Aggregate data by IP and time window
        aggregated = (
            df.withWatermark("timestamp", "1 minute")
            .groupBy(window("timestamp", "5 minutes"), "ip")
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
            )
        )

        return aggregated

    def train_model(self, df):
        """Train the K-means model."""
        features_df = self.prepare_features(df)
        self.model = self.pipeline.fit(features_df)
        return self.model

    def detect_anomalies(self, df):
        """Detect anomalies in the data."""
        if self.model is None:
            raise ValueError("Model not trained. Call train_model first.")

        features_df = self.prepare_features(df)
        predictions = self.model.transform(features_df)

        # Calculate distance to cluster centers
        centers = self.model.stages[-1].clusterCenters()
        predictions_with_distance = predictions.withColumn(
            "distance", col("prediction").cast("int").apply(lambda x: centers[x])
        )

        # Identify anomalies (points far from their cluster center)
        threshold = 2.0  # Adjust based on your data
        anomalies = predictions_with_distance.filter(col("distance") > threshold)

        return anomalies

    def save_anomalies(self, anomalies, output_path: str):
        """Save detected anomalies to JSON file."""
        anomalies_df = anomalies.select(
            "window", "ip", "request_count", "avg_response_time", "distance"
        )

        # Convert to JSON and save
        anomalies_json = anomalies_df.toJSON().collect()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        with open(f"{output_path}/anomalies_{timestamp}.json", "w") as f:
            json.dump(anomalies_json, f)

    def run(self, training_hours: int = 24, output_path: str = "data/anomalies"):
        """Run the anomaly detection pipeline."""
        # Read historical data for training
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=training_hours)

        historical_df = self.spark.read.parquet("data/processed/logs").filter(
            (col("timestamp") >= start_time) & (col("timestamp") <= end_time)
        )

        # Train model
        self.train_model(historical_df)

        # Read streaming data for detection
        streaming_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_broker)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

        # Detect anomalies
        anomalies = self.detect_anomalies(streaming_df)

        # Save anomalies
        self.save_anomalies(anomalies, output_path)

        return anomalies


if __name__ == "__main__":
    detector = AnomalyDetector()
    detector.run()
