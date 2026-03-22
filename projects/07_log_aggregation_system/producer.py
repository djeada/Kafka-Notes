import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Optional, Sequence

from kafka import KafkaProducer

# Configuration
DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "app-logs"
DEFAULT_MESSAGES = 100
DEFAULT_INTERVAL = 0.1
DEFAULT_APPS = ["web-server", "auth-service", "payment-service", "notification-service"]

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Weighted probabilities: most logs are INFO/DEBUG, fewer are ERROR/CRITICAL
LOG_LEVEL_WEIGHTS = [20, 40, 20, 15, 5]

LOG_TEMPLATES: dict[str, list[str]] = {
    "web-server": [
        "GET /api/users returned 200",
        "POST /api/orders returned 201",
        "Connection timeout to upstream service",
        "SSL certificate renewal scheduled",
        "Request rate exceeded threshold",
        "Static asset cache miss for /img/logo.png",
    ],
    "auth-service": [
        "User login successful",
        "Failed login attempt for user admin",
        "Token refreshed for session abc-123",
        "Password reset requested",
        "OAuth callback received from provider",
        "Session expired for user guest",
    ],
    "payment-service": [
        "Payment processed: $49.99",
        "Refund initiated for order #1042",
        "Payment gateway timeout",
        "Currency conversion applied: USD to EUR",
        "Duplicate transaction detected",
        "Invoice generated for subscription renewal",
    ],
    "notification-service": [
        "Email sent to user@example.com",
        "SMS delivery failed: invalid number",
        "Push notification queued",
        "Webhook delivery confirmed",
        "Rate limit reached for email provider",
        "Template rendering error for welcome email",
    ],
}

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_log_entry(apps: list[str]) -> dict:
    """Generate a single simulated log entry."""
    app = random.choice(apps)
    level = random.choices(LOG_LEVELS, weights=LOG_LEVEL_WEIGHTS, k=1)[0]
    templates = LOG_TEMPLATES.get(app, ["Generic log message"])
    message = random.choice(templates)
    return {
        "app": app,
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_logs(
    broker: str,
    topic: str,
    num_messages: int,
    interval: float,
    apps: list[str],
) -> None:
    """Produce simulated log entries to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for i in range(1, num_messages + 1):
            entry = generate_log_entry(apps)
            producer.send(topic, value=entry)
            logger.info(
                "Sent log %d/%d: [%s] %s - %s",
                i,
                num_messages,
                entry["level"],
                entry["app"],
                entry["message"],
            )
            time.sleep(interval)
        producer.flush()
        logger.info(
            "Successfully sent %d log entries to topic '%s'", num_messages, topic
        )
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simulate log output from multiple applications and produce to Kafka."
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER, help="Kafka broker address"
    )
    parser.add_argument(
        "--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to"
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=DEFAULT_MESSAGES,
        help="Number of log entries to send",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="Seconds between messages",
    )
    parser.add_argument(
        "--apps",
        nargs="+",
        default=DEFAULT_APPS,
        help="Application names to simulate",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    send_logs(args.broker, args.topic, args.messages, args.interval, args.apps)


if __name__ == "__main__":
    main()
