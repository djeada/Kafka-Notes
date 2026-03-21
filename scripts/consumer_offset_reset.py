import argparse
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata
import logging
from typing import Optional, Sequence

# Configuration
DEFAULT_BROKER = 'localhost:9092'  # Kafka broker address
DEFAULT_CONSUMER_GROUP = 'your_consumer_group'  # Consumer group to reset offsets
DEFAULT_TOPIC = 'your_topic'  # Topic to reset offsets
DEFAULT_RESET_OFFSET_TO = 'earliest'  # Options: 'earliest', 'latest', or specific offset integer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reset_consumer_group_offset(admin_client: KafkaAdminClient, consumer: KafkaConsumer, group_id: str, topic: str, reset_to: str) -> None:
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        logger.error(f"No partitions found for topic '{topic}'")
        return
    
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    
    offsets = {}
    if reset_to == 'earliest':
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        offsets = {tp: OffsetAndMetadata(offset, None) for tp, offset in beginning_offsets.items()}
    elif reset_to == 'latest':
        end_offsets = consumer.end_offsets(topic_partitions)
        offsets = {tp: OffsetAndMetadata(offset, None) for tp, offset in end_offsets.items()}
    else:
        try:
            specific_offset = int(reset_to)
            offsets = {tp: OffsetAndMetadata(specific_offset, None) for tp in topic_partitions}
        except ValueError:
            logger.error(f"Invalid offset value: {reset_to}")
            return
    
    try:
        admin_client.alter_consumer_group_offsets(group_id, offsets)
        logger.info(f"Successfully reset offsets for group '{group_id}' on topic '{topic}' to '{reset_to}'")
    except KafkaError as e:
        logger.error(f"Failed to reset offsets for group '{group_id}' on topic '{topic}': {e}")

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset Kafka consumer group offsets to earliest, latest, or a specific offset.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka broker address")
    parser.add_argument("--consumer-group", default=DEFAULT_CONSUMER_GROUP, help="Consumer group to reset")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Topic to reset offsets for")
    parser.add_argument(
        "--reset-to",
        default=DEFAULT_RESET_OFFSET_TO,
        help="Offset target: 'earliest', 'latest', or a specific integer offset",
    )
    return parser

def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)

def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    admin_client = KafkaAdminClient(bootstrap_servers=parsed_args.broker)
    consumer = KafkaConsumer(bootstrap_servers=parsed_args.broker, group_id=parsed_args.consumer_group)

    # Reset consumer group offset
    reset_consumer_group_offset(
        admin_client,
        consumer,
        parsed_args.consumer_group,
        parsed_args.topic,
        parsed_args.reset_to,
    )

if __name__ == "__main__":
    main()
