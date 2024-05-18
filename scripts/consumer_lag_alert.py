from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import logging
from typing import List, Dict

# Configuration
BROKER = 'localhost:9092'  # Kafka broker address
CONSUMER_GROUP = 'your_consumer_group'  # Consumer group to monitor
TOPIC = 'your_topic'  # Topic to monitor
LAG_THRESHOLD = 100  # Lag threshold for alerting

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

def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
    consumer = KafkaConsumer(bootstrap_servers=BROKER, group_id=CONSUMER_GROUP)

    monitor_consumer_lag(admin_client, consumer, CONSUMER_GROUP, TOPIC, LAG_THRESHOLD)

if __name__ == "__main__":
    main()
