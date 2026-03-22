import argparse
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import logging
from typing import Dict, Optional, Sequence

# Configuration
DEFAULT_BROKER = 'localhost:9092'  # Kafka broker address
DEFAULT_CONSUMER_GROUP = 'your_consumer_group'  # Consumer group to monitor
DEFAULT_TOPIC = 'your_topic'  # Topic to monitor
DEFAULT_LAG_THRESHOLD = 100  # Lag threshold for alerting

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_consumer_offsets(admin_client: KafkaAdminClient, group_id: str) -> Dict[TopicPartition, int]:
    try:
        group_offsets = admin_client.list_consumer_group_offsets(group_id)
        return group_offsets
    except KafkaError as e:
        logger.error(f"Failed to get consumer offsets for group '{group_id}': {e}")
        return {}

def get_topic_partitions_offsets(consumer: KafkaConsumer, topic: str) -> Dict[TopicPartition, int]:
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        logger.error(f"No partitions found for topic '{topic}'")
        return {}

    end_offsets = consumer.end_offsets([TopicPartition(topic, p) for p in partitions])
    return end_offsets

def monitor_consumer_lag(admin_client: KafkaAdminClient, consumer: KafkaConsumer, group_id: str, topic: str, lag_threshold: int) -> None:
    consumer_offsets = get_consumer_offsets(admin_client, group_id)
    topic_end_offsets = get_topic_partitions_offsets(consumer, topic)

    for tp, end_offset in topic_end_offsets.items():
        consumer_offset = consumer_offsets.get(tp, None)
        if consumer_offset is not None:
            lag = end_offset - consumer_offset.offset
            if lag > lag_threshold:
                logger.warning(f"High consumer lag detected for group '{group_id}', topic '{tp.topic}', partition {tp.partition}: {lag} messages")

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check Kafka consumer lag and warn when it exceeds a threshold.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--consumer-group", default=DEFAULT_CONSUMER_GROUP, help="Consumer group to monitor")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Topic to monitor")
    parser.add_argument("--lag-threshold", type=int, default=DEFAULT_LAG_THRESHOLD, help="Lag threshold for alerting")
    return parser

def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)

def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    admin_client = KafkaAdminClient(bootstrap_servers=parsed_args.broker)
    consumer = KafkaConsumer(bootstrap_servers=parsed_args.broker, group_id=parsed_args.consumer_group)

    monitor_consumer_lag(admin_client, consumer, parsed_args.consumer_group, parsed_args.topic, parsed_args.lag_threshold)

if __name__ == "__main__":
    main()
