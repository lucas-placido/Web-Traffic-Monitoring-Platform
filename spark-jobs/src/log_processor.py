from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, window, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


class LogProcessor:
    def __init__(self, kafka_broker: str = "localhost:9091", topic: str = "web-logs"):
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
                StructField(
                    "timestamp", StringType(), True
                ),  # Alterado para StringType
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
            .option("startingOffsets", "earliest")
            .load()
        )

    def process_stream(self):
        """Process the streaming data and calculate metrics."""
        # Read from Kafka
        df = self.read_from_kafka()

        # Parse JSON and apply schema
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        # Converter a string de timestamp para o tipo timestamp
        parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))

        # Agora podemos usar a função window com o campo timestamp convertido
        metrics = (
            parsed_df.withWatermark("timestamp", "1 minute")
            .groupBy(window("timestamp", "5 minutes"), "endpoint")
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                count("status_code").alias("total_requests"),
            )
        )

        return metrics

    def write_to_console(self, df):
        """Write the processed data to console (for testing)."""
        return (
            df.writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start()
        )

    def write_to_parquet(self, df, output_path: str):
        """Write the processed data to Parquet files."""
        return (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{output_path}/checkpoint")
            .start()
        )

    def run(self, output_path: str = None):
        """Run the streaming job."""
        metrics_df = self.process_stream()

        if output_path:
            query = self.write_to_parquet(metrics_df, output_path)
        else:
            query = self.write_to_console(metrics_df)

        query.awaitTermination()


if __name__ == "__main__":
    processor = LogProcessor()
    processor.run("data/spark-jobs/output")
