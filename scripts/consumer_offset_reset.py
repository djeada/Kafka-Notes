from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata
import logging

# Configuration
BROKER = 'localhost:9092'  # Kafka broker address
CONSUMER_GROUP = 'your_consumer_group'  # Consumer group to reset offsets
TOPIC = 'your_topic'  # Topic to reset offsets
RESET_OFFSET_TO = 'earliest'  # Options: 'earliest', 'latest', or specific offset integer

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

def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
    consumer = KafkaConsumer(bootstrap_servers=BROKER, group_id=CONSUMER_GROUP)

    # Reset consumer group offset
    reset_consumer_group_offset(admin_client, consumer, CONSUMER_GROUP, TOPIC, RESET_OFFSET_TO)

if __name__ == "__main__":
    main()
