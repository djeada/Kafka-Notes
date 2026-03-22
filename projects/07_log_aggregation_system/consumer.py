import argparse
import json
import logging
import sys
from typing import Optional, Sequence, TextIO

from kafka import KafkaConsumer

# Configuration
DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "app-logs"
DEFAULT_GROUP = "log-aggregator"
DEFAULT_MIN_LEVEL = "INFO"
DEFAULT_TIMEOUT = 30

LOG_LEVEL_HIERARCHY = {
    "DEBUG": 0,
    "INFO": 1,
    "WARNING": 2,
    "ERROR": 3,
    "CRITICAL": 4,
}

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LogAggregator:
    """Consumes log entries from Kafka and filters by minimum log level."""

    def __init__(self, min_level: str, output_file: Optional[str] = None) -> None:
        self.min_level = min_level.upper()
        self.min_level_value = LOG_LEVEL_HIERARCHY.get(self.min_level, 1)
        self.output_file = output_file
        self.total_received = 0
        self.total_matched = 0
        self.level_counts: dict[str, int] = {level: 0 for level in LOG_LEVEL_HIERARCHY}

    def _matches_level(self, level: str) -> bool:
        """Return True if the log level meets or exceeds the minimum threshold."""
        return LOG_LEVEL_HIERARCHY.get(level.upper(), 0) >= self.min_level_value

    def _format_log(self, entry: dict) -> str:
        """Format a log entry for display."""
        return "[{timestamp}] {level:<8} {app}: {message}".format(
            timestamp=entry.get("timestamp", "unknown"),
            level=entry.get("level", "UNKNOWN"),
            app=entry.get("app", "unknown"),
            message=entry.get("message", ""),
        )

    def _write_output(self, text: str, out: Optional[TextIO]) -> None:
        """Write formatted log line to stdout and optionally to a file."""
        logger.info(text)
        if out is not None:
            out.write(text + "\n")
            out.flush()

    def consume(self, broker: str, topic: str, group: str, timeout: int) -> None:
        """Consume and filter log entries from Kafka."""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout * 1000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        out: Optional[TextIO] = None
        try:
            if self.output_file:
                out = open(self.output_file, "a", encoding="utf-8")  # noqa: SIM115
                logger.info("Writing matching logs to '%s'", self.output_file)

            logger.info(
                "Consuming from topic '%s' (min level: %s) ...", topic, self.min_level
            )

            for message in consumer:
                entry: dict = message.value
                level = entry.get("level", "INFO").upper()
                self.total_received += 1
                self.level_counts[level] = self.level_counts.get(level, 0) + 1

                if self._matches_level(level):
                    self.total_matched += 1
                    self._write_output(self._format_log(entry), out)
        finally:
            if out is not None:
                out.close()
            consumer.close()

        self._print_summary()

    def _print_summary(self) -> None:
        """Print aggregation statistics."""
        logger.info("--- Aggregation Summary ---")
        logger.info("Total logs received : %d", self.total_received)
        logger.info("Logs matching >=%s : %d", self.min_level, self.total_matched)
        for level in LOG_LEVEL_HIERARCHY:
            count = self.level_counts.get(level, 0)
            if count:
                logger.info("  %-8s : %d", level, count)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume log entries from Kafka and filter by minimum log level."
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER, help="Kafka broker address"
    )
    parser.add_argument(
        "--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--group", default=DEFAULT_GROUP, help="Consumer group ID"
    )
    parser.add_argument(
        "--min-level",
        default=DEFAULT_MIN_LEVEL,
        choices=list(LOG_LEVEL_HIERARCHY.keys()),
        help="Minimum log level to display",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file path to write matching logs",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Consumer timeout in seconds",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    aggregator = LogAggregator(min_level=args.min_level, output_file=args.output)
    aggregator.consume(args.broker, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
