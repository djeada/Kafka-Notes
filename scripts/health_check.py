from kafka import KafkaAdminClient, TopicPartition

def initialize_admin_client(bootstrap_servers='localhost:9092'):
    try:
        return KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    except Exception as e:
        print(f"Error initializing KafkaAdminClient: {e}")
        return None

def check_brokers_health(admin_client):
    try:
        metadata = admin_client.describe_cluster()
        print(f"Available Brokers: {metadata.brokers}")
        return metadata
    except Exception as e:
        print(f"Error checking broker health: {e}")
        return None

def report_consumer_group_lag(admin_client):
    try:
        consumer_groups = admin_client.list_consumer_groups()
        for group in consumer_groups:
            offsets = admin_client.list_consumer_group_offsets(group[0])
            end_offsets = admin_client.list_consumer_offsets(group[0], offsets.partitions())
            
            for tp, offset in offsets.partitions().items():
                end_offset = end_offsets[tp]
                lag = end_offset - offset.offset
                print(f"Consumer Group: {group[0]}, Topic: {tp.topic}, Partition: {tp.partition}, Lag: {lag}")
    except Exception as e:
        print(f"Error reporting consumer group lag: {e}")

def monitor_under_replicated_partitions(admin_client, topics):
    try:
        described_topics = admin_client.describe_topics(topics)
        for topic in described_topics:
            for partition in topic.partitions.values():
                if partition.isr and partition.replicas:
                    under_replicated = len(partition.replicas) - len(partition.isr)
                    if under_replicated > 0:
                        print(f"Topic: {topic.name}, Partition: {partition.partition}, Under-replicated by: {under_replicated}")
    except Exception as e:
        print(f"Error monitoring under-replicated partitions: {e}")

if __name__ == "__main__":
    admin_client = initialize_admin_client()
    if admin_client:
        metadata = check_brokers_health(admin_client)
        if metadata:
            report_consumer_group_lag(admin_client)
            monitor_under_replicated_partitions(admin_client, metadata.topics)
