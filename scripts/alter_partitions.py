from kafka.admin import KafkaAdminClient, NewPartitions
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
from typing import List

# Configuration
BROKER = 'localhost:9092'  # Kafka broker address
TOPIC = 'your_topic'  # Topic name to alter partitions
NUM_PARTITIONS = 3  # Number of partitions to set for the topic

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_admin_client(broker: str) -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=broker)

def alter_partitions(admin_client: KafkaAdminClient, topic: str, num_partitions: int) -> None:
    new_partitions = {topic: NewPartitions(total_count=num_partitions)}
    try:
        admin_client.create_partitions(new_partitions)
        logger.info(f"Successfully altered partitions for topic '{topic}' to {num_partitions}")
    except KafkaError as e:
        logger.error(f"Failed to alter partitions for topic '{topic}': {e}")

def display_partitions(admin_client: KafkaAdminClient, topic: str = None) -> None:
    topic_partitions = admin_client.describe_topics([topic]) if topic else admin_client.describe_topics()
    for tp in topic_partitions:
        topic_name = tp['topic']
        partitions = tp['partitions']
        logger.info(f"Topic: {topic_name}, Partitions: {len(partitions)}")
        for partition in partitions:
            logger.info(f"Partition ID: {partition['partition']}")

def main():
    admin_client = create_admin_client(BROKER)
    
    # Alter partitions for the specified topic
    alter_partitions(admin_client, TOPIC, NUM_PARTITIONS)
    
    # Display partitions for the specified topic
    display_partitions(admin_client, TOPIC)
    
    # Optionally, display partitions for all topics
    display_partitions(admin_client)

if __name__ == "__main__":
    main()
