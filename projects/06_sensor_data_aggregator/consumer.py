"""Consumer that aggregates IoT sensor readings (min, max, average) per sensor."""

import argparse
import json
import logging
from typing import Optional, Sequence

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "sensor-data"
DEFAULT_GROUP = "sensor-aggregator"
DEFAULT_TIMEOUT = 20


class SensorAggregator:
    """Maintains running min/max/sum/count per sensor_id."""

    def __init__(self) -> None:
        self._stats: dict = {}

    def update(self, sensor_id: str, value: float) -> None:
        if sensor_id not in self._stats:
            self._stats[sensor_id] = {
                "count": 0,
                "min": value,
                "max": value,
                "sum": 0.0,
            }
        entry = self._stats[sensor_id]
        entry["count"] += 1
        entry["sum"] += value
        if value < entry["min"]:
            entry["min"] = value
        if value > entry["max"]:
            entry["max"] = value

    def summary(self) -> dict:
        """Return a dict of sensor_id -> {count, min, max, avg}."""
        result = {}
        for sensor_id, entry in sorted(self._stats.items()):
            result[sensor_id] = {
                "count": entry["count"],
                "min": entry["min"],
                "max": entry["max"],
                "avg": round(entry["sum"] / entry["count"], 2),
            }
        return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume sensor data from Kafka and compute per-sensor aggregations."
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Consumer group ID")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Consumer poll timeout in seconds",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def consume_and_aggregate(broker: str, topic: str, group: str, timeout: int) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        group_id=group,
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    aggregator = SensorAggregator()
    total = 0

    try:
        logger.info("Listening on topic '%s' (group=%s) …", topic, group)

        for message in consumer:
            reading = message.value
            sensor_id = reading.get("sensor_id", "unknown")
            value = reading.get("value", 0.0)

            aggregator.update(sensor_id, value)
            total += 1

            if total % 20 == 0:
                _log_summary(aggregator, total)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        consumer.close()

    logger.info("Final results after %d readings:", total)
    _log_summary(aggregator, total)


def _log_summary(aggregator: SensorAggregator, total: int) -> None:
    logger.info("--- Aggregation snapshot (%d readings) ---", total)
    for sensor_id, stats in aggregator.summary().items():
        logger.info(
            "  %s : count=%d  min=%.2f  max=%.2f  avg=%.2f",
            sensor_id,
            stats["count"],
            stats["min"],
            stats["max"],
            stats["avg"],
        )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    consume_and_aggregate(args.broker, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
