"""Producer that sends randomized notification messages to a Kafka topic."""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Sequence

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "notifications"
DEFAULT_MESSAGES = 30
DEFAULT_INTERVAL = 0.3

NOTIFICATION_TYPES = ["email", "sms", "push"]

SAMPLE_RECIPIENTS = [
    "alice@example.com",
    "bob@example.com",
    "+1-555-0101",
    "+1-555-0202",
    "user_alice",
    "user_bob",
    "charlie@example.com",
    "+1-555-0303",
    "user_charlie",
]

SAMPLE_MESSAGES = [
    "Your order has been shipped.",
    "Password reset requested.",
    "New login detected on your account.",
    "Your subscription is about to expire.",
    "Payment received — thank you!",
    "Weekly summary is ready.",
    "You have a new follower.",
    "Scheduled maintenance tonight at 11 PM.",
    "Your report is ready to download.",
    "Flash sale: 20% off everything today!",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce random notification messages to a Kafka topic."
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument(
        "--messages", type=int, default=DEFAULT_MESSAGES, help="Number of messages to send"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="Seconds between messages",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def generate_notification() -> dict:
    """Create a single random notification payload."""
    notif_type = random.choice(NOTIFICATION_TYPES)
    return {
        "notification_id": str(uuid.uuid4()),
        "type": notif_type,
        "recipient": random.choice(SAMPLE_RECIPIENTS),
        "message": random.choice(SAMPLE_MESSAGES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_notifications(broker: str, topic: str, count: int, interval: float) -> None:
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
    )

    try:
        for i in range(1, count + 1):
            notification = generate_notification()
            producer.send(topic, value=notification)
            logger.info(
                "Sent %d/%d  [%s] to=%s — %s",
                i,
                count,
                notification["type"],
                notification["recipient"],
                notification["message"],
            )
            time.sleep(interval)
        producer.flush()
        logger.info("All %d notifications sent successfully.", count)
    finally:
        producer.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    send_notifications(args.broker, args.topic, args.messages, args.interval)


if __name__ == "__main__":
    main()
