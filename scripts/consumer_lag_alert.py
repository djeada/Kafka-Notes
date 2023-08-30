from kafka import KafkaAdminClient, KafkaError
import matplotlib.pyplot as plt
import time

def initialize_admin_client(bootstrap_servers):
    """Initialize Kafka admin client."""
    try:
        return KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    except KafkaError as e:
        print(f"Error initializing KafkaAdminClient: {e}")
        return None

def fetch_partition_lags(admin_client, consumer_group):
    """Fetch lag values for each partition."""
    try:
        offsets = admin_client.list_consumer_group_offsets(consumer_group)
        end_offsets = admin_client.list_consumer_offsets(consumer_group, offsets.partitions())
        
        lags = {}
        for tp, offset in offsets.partitions().items():
            end_offset = end_offsets[tp]
            lag = end_offset - offset.offset
            lags[tp.partition] = lag
        return lags
    except KafkaError as e:
        print(f"Error fetching partition lags: {e}")
        return None

def visualize_lags(lags, threshold_lag):
    """Visualize lags using matplotlib."""
    partitions = list(lags.keys())
    values = list(lags.values())
    
    plt.bar(partitions, values)
    plt.axhline(y=threshold_lag, color='r', linestyle='--')
    plt.xlabel('Partition')
    plt.ylabel('Lag')
    plt.title('Consumer Lag per Partition')
    plt.xticks(partitions)  # to ensure partition numbers are shown
    plt.show()

def alert_high_lag(lags, threshold_lag):
    """Alert if any lag value exceeds the threshold."""
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    for partition, lag in lags.items():
        if lag > threshold_lag:
            print(f"[{current_time}] ALERT: Lag for partition {partition} is {lag}, which exceeds the threshold!")

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    consumer_group = 'your-consumer-group'
    threshold_lag = 100

    admin_client = initialize_admin_client(bootstrap_servers)
    if admin_client:
        lags = fetch_partition_lags(admin_client, consumer_group)
        if lags:
            visualize_lags(lags, threshold_lag)
            alert_high_lag(lags, threshold_lag)
