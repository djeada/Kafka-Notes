"""Producer that simulates IoT sensors sending readings to a Kafka topic."""

import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Optional, Sequence

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "sensor-data"
DEFAULT_SENSORS = 5
DEFAULT_READINGS = 100
DEFAULT_INTERVAL = 0.2

SENSOR_CONFIGS = [
    {"type": "temperature", "unit": "celsius", "range": (15.0, 40.0)},
    {"type": "humidity", "unit": "percent", "range": (20.0, 95.0)},
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Simulate IoT sensors publishing readings to Kafka."
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument(
        "--sensors", type=int, default=DEFAULT_SENSORS, help="Number of sensors to simulate"
    )
    parser.add_argument(
        "--readings", type=int, default=DEFAULT_READINGS, help="Total readings to send"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="Seconds between readings",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def build_sensors(count: int) -> list:
    """Create a list of sensor definitions."""
    sensors = []
    for i in range(count):
        config = SENSOR_CONFIGS[i % len(SENSOR_CONFIGS)]
        sensors.append(
            {
                "sensor_id": f"sensor-{i + 1:02d}",
                "type": config["type"],
                "unit": config["unit"],
                "range": config["range"],
            }
        )
    return sensors


def generate_reading(sensor: dict) -> dict:
    """Produce a single reading for the given sensor."""
    lo, hi = sensor["range"]
    return {
        "sensor_id": sensor["sensor_id"],
        "type": sensor["type"],
        "value": round(random.uniform(lo, hi), 2),
        "unit": sensor["unit"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_readings(
    broker: str, topic: str, sensor_count: int, total_readings: int, interval: float
) -> None:
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )

    sensors = build_sensors(sensor_count)
    logger.info("Simulating %d sensors, sending %d total readings.", sensor_count, total_readings)

    try:
        for i in range(1, total_readings + 1):
            sensor = random.choice(sensors)
            reading = generate_reading(sensor)
            # Use sensor_id as key so all readings for a sensor go to the same partition
            producer.send(topic, key=reading["sensor_id"], value=reading)
            logger.info(
                "Sent %d/%d  sensor=%s  %s=%.2f %s",
                i,
                total_readings,
                reading["sensor_id"],
                reading["type"],
                reading["value"],
                reading["unit"],
            )
            time.sleep(interval)
        producer.flush()
        logger.info("All %d readings sent.", total_readings)
    finally:
        producer.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    send_readings(args.broker, args.topic, args.sensors, args.readings, args.interval)


if __name__ == "__main__":
    main()
