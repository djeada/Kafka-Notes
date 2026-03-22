"""Fraud detector that consumes transactions and produces alerts.

Acts as both a Kafka consumer (reads from the transactions topic) and
a Kafka producer (writes detected fraud to the fraud-alerts topic).
Applies rule-based detection: high amounts, rapid successive transactions
from the same user, and unusual locations.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:9092"
DEFAULT_INPUT_TOPIC = "transactions"
DEFAULT_OUTPUT_TOPIC = "fraud-alerts"
DEFAULT_GROUP = "fraud-detectors"
DEFAULT_AMOUNT_THRESHOLD = 5000.0
DEFAULT_TIMEOUT = 30

RAPID_TRANSACTION_WINDOW_SEC = 10.0
RAPID_TRANSACTION_COUNT = 3

SUSPICIOUS_LOCATIONS = {
    "Unknown Location", "Offshore Territory", "Unregistered Zone",
}


class FraudDetector:
    """Rule-based fraud detection engine."""

    def __init__(self, amount_threshold: float) -> None:
        self._amount_threshold = amount_threshold
        self._user_history: Dict[str, List[float]] = {}

    def _check_high_amount(self, transaction: dict) -> Optional[str]:
        """Flag transactions above the amount threshold."""
        if transaction["amount"] > self._amount_threshold:
            return (
                f"High amount: {transaction['amount']:.2f} "
                f"exceeds threshold {self._amount_threshold:.2f}"
            )
        return None

    def _check_rapid_transactions(self, transaction: dict) -> Optional[str]:
        """Flag users with too many transactions in a short window."""
        user_id = transaction["user_id"]
        now = time.time()

        if user_id not in self._user_history:
            self._user_history[user_id] = []

        history = self._user_history[user_id]
        history.append(now)

        # Keep only timestamps within the window
        cutoff = now - RAPID_TRANSACTION_WINDOW_SEC
        self._user_history[user_id] = [t for t in history if t >= cutoff]

        if len(self._user_history[user_id]) >= RAPID_TRANSACTION_COUNT:
            return (
                f"Rapid transactions: {len(self._user_history[user_id])} "
                f"transactions within {RAPID_TRANSACTION_WINDOW_SEC}s"
            )
        return None

    def _check_suspicious_location(self, transaction: dict) -> Optional[str]:
        """Flag transactions from suspicious locations."""
        if transaction.get("location") in SUSPICIOUS_LOCATIONS:
            return f"Suspicious location: {transaction['location']}"
        return None

    def evaluate(self, transaction: dict) -> List[dict]:
        """Run all fraud rules and return a list of alerts."""
        checks = [
            self._check_high_amount,
            self._check_rapid_transactions,
            self._check_suspicious_location,
        ]
        alerts = []
        for check in checks:
            reason = check(transaction)
            if reason:
                alerts.append({
                    "alert_id": f"alert-{transaction['transaction_id'][:8]}",
                    "transaction_id": transaction["transaction_id"],
                    "user_id": transaction["user_id"],
                    "amount": transaction["amount"],
                    "rule_triggered": reason,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
        return alerts


def run_detector(
    broker: str,
    input_topic: str,
    output_topic: str,
    group: str,
    amount_threshold: float,
    timeout: int,
) -> None:
    """Consume transactions, detect fraud, and produce alerts."""
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )
    consumer.subscribe([input_topic])

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    detector = FraudDetector(amount_threshold)
    transactions_processed = 0
    alerts_sent = 0

    try:
        logger.info(
            "Fraud detector started: reading '%s', writing alerts to '%s'",
            input_topic, output_topic,
        )
        for message in consumer:
            transaction = message.value
            transactions_processed += 1
            logger.info(
                "Processing transaction %s from %s (amount=%.2f)",
                transaction["transaction_id"][:8],
                transaction["user_id"],
                transaction["amount"],
            )

            alerts = detector.evaluate(transaction)
            for alert in alerts:
                producer.send(output_topic, value=alert)
                alerts_sent += 1
                logger.warning(
                    "FRAUD ALERT: %s — %s",
                    alert["alert_id"], alert["rule_triggered"],
                )

        producer.flush()
        logger.info(
            "Detector finished: processed %d transactions, sent %d alerts",
            transactions_processed, alerts_sent,
        )
    finally:
        consumer.close()
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Consume transactions and detect fraud",
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER,
        help="Kafka broker address (default: %(default)s)",
    )
    parser.add_argument(
        "--input-topic", default=DEFAULT_INPUT_TOPIC,
        help="Topic to consume transactions from (default: %(default)s)",
    )
    parser.add_argument(
        "--output-topic", default=DEFAULT_OUTPUT_TOPIC,
        help="Topic to produce fraud alerts to (default: %(default)s)",
    )
    parser.add_argument(
        "--group", default=DEFAULT_GROUP,
        help="Consumer group ID (default: %(default)s)",
    )
    parser.add_argument(
        "--amount-threshold", type=float, default=DEFAULT_AMOUNT_THRESHOLD,
        help="Amount above which a transaction is flagged (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help="Consumer timeout in seconds (default: %(default)s)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    return build_parser().parse_args(argv)


def main() -> None:
    """Entry point."""
    args = parse_args()
    run_detector(
        broker=args.broker,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        group=args.group,
        amount_threshold=args.amount_threshold,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
