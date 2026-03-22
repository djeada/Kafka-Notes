import argparse
import logging
import random
import time
from typing import Optional, Sequence

from kafka import KafkaProducer

# Configuration
DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "text-stream"
DEFAULT_MESSAGES = 50
DEFAULT_INTERVAL = 0.3

SAMPLE_SENTENCES = [
    "Apache Kafka is a distributed streaming platform used for building real time data pipelines",
    "Stream processing allows you to analyse data as it arrives rather than in batch",
    "Word count is the hello world of stream processing applications",
    "Kafka consumers can form groups to share the work of reading from topic partitions",
    "Producers send records to topics and consumers read records from topics",
    "Real time analytics help businesses react to events as they happen",
    "The quick brown fox jumps over the lazy dog near the river bank",
    "Data pipelines move information from source systems to target systems reliably",
    "Distributed systems require careful handling of failures and network partitions",
    "Event driven architecture decouples producers of events from consumers of events",
    "Kafka topics are divided into partitions for scalability and parallel processing",
    "Streaming data is increasingly important for modern data engineering workflows",
    "Natural language processing techniques can extract insights from text streams",
    "Low latency processing is critical for fraud detection and monitoring systems",
    "Message brokers like Kafka provide durable storage and replay capabilities",
    "Tokenisation is the first step in many text processing and search pipelines",
    "Horizontal scaling allows Kafka clusters to handle millions of messages per second",
    "Consumer offsets track which messages have already been processed by each group",
    "Exactly once semantics ensure that messages are neither lost nor duplicated",
    "Microservices communicate through event streams for loose coupling and resilience",
]

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def send_sentences(
    broker: str, topic: str, num_messages: int, interval: float
) -> None:
    """Produce text sentences to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    try:
        for i in range(1, num_messages + 1):
            sentence = random.choice(SAMPLE_SENTENCES)
            producer.send(topic, value=sentence)
            logger.info("Sent %d/%d: %s", i, num_messages, sentence)
            time.sleep(interval)
        producer.flush()
        logger.info(
            "Successfully sent %d sentences to topic '%s'", num_messages, topic
        )
    finally:
        producer.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream text sentences to a Kafka topic for word count processing."
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
        help="Number of sentences to send",
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


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    send_sentences(args.broker, args.topic, args.messages, args.interval)


if __name__ == "__main__":
    main()
