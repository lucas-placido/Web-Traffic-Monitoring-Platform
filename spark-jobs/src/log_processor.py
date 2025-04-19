from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


class LogProcessor:
    def __init__(self, kafka_broker: str = "kafka:19092", topic: str = "web-logs"):
        self.spark = (
            SparkSession.builder.appName("WebLogProcessor")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .getOrCreate()
        )

        self.kafka_broker = kafka_broker
        self.topic = topic

        # Define schema for web logs
        self.schema = StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("ip", StringType(), True),
                StructField("endpoint", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("status_code", IntegerType(), True),
                StructField("response_time", DoubleType(), True),
                StructField("bytes_sent", IntegerType(), True),
            ]
        )

    def read_from_kafka(self):
        """Read streaming data from Kafka."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_broker)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

    def process_stream(self):
        """Process the streaming data and save raw data."""
        # Read from Kafka
        df = self.read_from_kafka()

        # Parse JSON and apply schema
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        return parsed_df

    def write_to_hdfs(self, df, output_path: str):
        """Write the raw data to HDFS in Parquet format."""
        return (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"hdfs://namenode:9000{output_path}")
            .option(
                "checkpointLocation", f"hdfs://namenode:9000{output_path}/checkpoint"
            )
            .start()
        )

    def run(self, output_path: str = "/data/raw-logs"):
        """Run the streaming job."""
        raw_df = self.process_stream()
        query = self.write_to_hdfs(raw_df, output_path)
        query.awaitTermination()


if __name__ == "__main__":
    processor = LogProcessor()
    processor.run()
