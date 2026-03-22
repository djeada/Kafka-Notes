import argparse
import logging
import re
from collections import Counter
from typing import Optional, Sequence

from kafka import KafkaConsumer

# Configuration
DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "text-stream"
DEFAULT_GROUP = "word-counter"
DEFAULT_TOP_N = 10
DEFAULT_TIMEOUT = 20

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Pattern that keeps only word characters (letters, digits, underscores)
WORD_PATTERN = re.compile(r"[a-z0-9]+")


class WordCounter:
    """Consumes text from Kafka, tokenises into words, and tracks frequencies."""

    def __init__(self, top_n: int = DEFAULT_TOP_N) -> None:
        self.top_n = top_n
        self.word_counts: Counter[str] = Counter()
        self.total_messages = 0

    @staticmethod
    def tokenize(text: str) -> list[str]:
        """Normalise and split text into lowercase words, stripping punctuation."""
        return WORD_PATTERN.findall(text.lower())

    def process_message(self, text: str) -> None:
        """Tokenise a message and update word frequency counts."""
        words = self.tokenize(text)
        self.word_counts.update(words)
        self.total_messages += 1

    def print_top_words(self) -> None:
        """Print the top N most frequent words."""
        logger.info("--- Top %d Words ---", self.top_n)
        for rank, (word, count) in enumerate(
            self.word_counts.most_common(self.top_n), start=1
        ):
            logger.info("  %2d. %-20s %d", rank, word, count)
        logger.info("Total unique words: %d", len(self.word_counts))

    def consume(self, broker: str, topic: str, group: str, timeout: int) -> None:
        """Consume text messages from Kafka and perform word counting."""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=group,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout * 1000,
            value_deserializer=lambda v: v.decode("utf-8"),
        )

        report_interval = 10

        try:
            logger.info("Consuming from topic '%s' ...", topic)
            for message in consumer:
                self.process_message(message.value)
                logger.info("Processed: %s", message.value[:80])

                if self.total_messages % report_interval == 0:
                    self.print_top_words()
        finally:
            consumer.close()

        logger.info("--- Final Results (%d messages) ---", self.total_messages)
        self.print_top_words()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume text from Kafka and perform real-time word frequency counting."
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
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="Number of top words to display",
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
    counter = WordCounter(top_n=args.top_n)
    counter.consume(args.broker, args.topic, args.group, args.timeout)


if __name__ == "__main__":
    main()
