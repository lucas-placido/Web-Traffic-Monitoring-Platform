import random
import time
import json
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebLogGenerator:
    def __init__(self, kafka_broker: str = "localhost:9092", topic: str = "web-logs"):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic = topic

        # Common endpoints and user agents for simulation
        self.endpoints = [
            "/",
            "/about",
            "/products",
            "/contact",
            "/login",
            "/api/v1/users",
            "/api/v1/products",
            "/api/v1/orders",
        ]
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
            "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
        ]
        self.status_codes = [200, 201, 400, 401, 403, 404, 500]

    def generate_ip(self) -> str:
        """Generate a random IP address."""
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

    def generate_log(self) -> Dict[str, Any]:
        """Generate a single web log entry."""
        # Usar formato ISO 8601 completo para garantir compatibilidade com Spark
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        response_time = random.uniform(0.1, 5.0)

        # Limitar a uma lista menor de endpoints para facilitar visualização de grupos
        endpoint = random.choice(
            self.endpoints[:3]
        )  # Usar apenas os primeiros 3 endpoints

        return {
            "timestamp": timestamp,
            "ip": self.generate_ip(),
            "endpoint": endpoint,
            "user_agent": random.choice(self.user_agents),
            "status_code": random.choice(self.status_codes),
            "response_time": round(response_time, 3),
            "bytes_sent": random.randint(100, 10000),
        }

    def send_log(self, log: Dict[str, Any]) -> None:
        """Send a log entry to Kafka."""
        try:
            self.producer.send(self.topic, value=log)
            self.producer.flush()
            logger.info(f"Sent log: {log}")
        except Exception as e:
            logger.error(f"Error sending log: {e}")

    def run(self, interval: float = 1.0, count: int = None) -> None:
        """
        Run the log generator.

        Args:
            interval: Time between log generations in seconds
            count: Number of logs to generate (None for infinite)
        """
        generated = 0
        try:
            while count is None or generated < count:
                log = self.generate_log()
                self.send_log(log)
                generated += 1
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Stopping log generator...")
        finally:
            self.producer.close()


if __name__ == "__main__":
    print("Iniciando o gerador de logs...")
    # Gerar logs mais rapidamente para testes
    generator = WebLogGenerator()
    generator.run(interval=0.2)  # Generate logs every 0.2 seconds
