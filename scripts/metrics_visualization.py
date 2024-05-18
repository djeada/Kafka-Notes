from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
import matplotlib.pyplot as plt
import logging
import time
from typing import List, Dict

# Configuration
BROKER = 'localhost:9092'  # Kafka broker address
CONSUMER_GROUP = 'your_consumer_group'  # Consumer group to monitor
TOPIC = 'your_topic'  # Topic to monitor
LAG_THRESHOLD = 100  # Lag threshold for alerting
MONITOR_DURATION = 60  # in seconds
POLL_INTERVAL = 5  # in seconds

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_consumer_offsets(admin_client: KafkaAdminClient, group_id: str) -> Dict[TopicPartition, int]:
    try:
        group_offsets = admin_client.list_consumer_group_offsets(group_id)
        return {tp: offset.offset for tp, offset in group_offsets.items()}
    except Exception as e:
        logger.error(f"Failed to get consumer offsets for group '{group_id}': {e}")
        return {}

def get_topic_partitions_offsets(consumer: KafkaConsumer, topic: str) -> Dict[TopicPartition, int]:
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        logger.error(f"No partitions found for topic '{topic}'")
        return {}

    end_offsets = consumer.end_offsets([TopicPartition(topic, p) for p in partitions])
    return end_offsets

def monitor_consumer_lag(admin_client: KafkaAdminClient, consumer: KafkaConsumer, group_id: str, topic: str, monitor_duration: int, poll_interval: int) -> List[Dict[str, int]]:
    start_time = time.time()
    end_time = start_time + monitor_duration
    lag_data = []

    while time.time() < end_time:
        consumer_offsets = get_consumer_offsets(admin_client, group_id)
        topic_end_offsets = get_topic_partitions_offsets(consumer, topic)
        
        for tp, end_offset in topic_end_offsets.items():
            consumer_offset = consumer_offsets.get(tp, None)
            if consumer_offset is not None:
                lag = end_offset - consumer_offset
                lag_data.append({'partition': tp.partition, 'lag': lag, 'timestamp': time.time()})
                if lag > LAG_THRESHOLD:
                    logger.warning(f"High consumer lag detected for group '{group_id}', topic '{tp.topic}', partition {tp.partition}: {lag} messages")
        
        time.sleep(poll_interval)

    return lag_data

def plot_lag_data(lag_data: List[Dict[str, int]]) -> None:
    timestamps = [entry['timestamp'] for entry in lag_data]
    lags = [entry['lag'] for entry in lag_data]
    partitions = [entry['partition'] for entry in lag_data]

    plt.figure(figsize=(12, 6))
    for partition in set(partitions):
        partition_lags = [lag for i, lag in enumerate(lags) if partitions[i] == partition]
        partition_times = [timestamp for i, timestamp in enumerate(timestamps) if partitions[i] == partition]
        plt.plot(partition_times, partition_lags, label=f'Partition {partition}')

    plt.axhline(y=LAG_THRESHOLD, color='r', linestyle='--', label='High Lag Threshold')
    plt.xlabel('Time (s)')
    plt.ylabel('Lag (messages)')
    plt.title('Consumer Lag Over Time')
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BROKER)
    consumer = KafkaConsumer(bootstrap_servers=BROKER, group_id=CONSUMER_GROUP, enable_auto_commit=False)

    lag_data = monitor_consumer_lag(admin_client, consumer, CONSUMER_GROUP, TOPIC, MONITOR_DURATION, POLL_INTERVAL)
    plot_lag_data(lag_data)

if __name__ == "__main__":
    main()
