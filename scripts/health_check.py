from kafka import KafkaAdminClient, TopicPartition

# Initialize the Kafka admin client
admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

# 1. Check the health of brokers in the cluster
metadata = admin_client.describe_cluster()
print(f"Available Brokers: {metadata.brokers}")

# 2. Report lag for each consumer group
consumer_groups = admin_client.list_consumer_groups()
for group in consumer_groups:
    offsets = admin_client.list_consumer_group_offsets(group[0])
    end_offsets = admin_client.list_consumer_offsets(group[0], offsets.partitions())
    
    for tp, offset in offsets.partitions().items():
        end_offset = end_offsets[tp]
        lag = end_offset - offset.offset
        print(f"Consumer Group: {group[0]}, Topic: {tp.topic}, Partition: {tp.partition}, Lag: {lag}")

# 3. Monitor under-replicated partitions
topics = admin_client.describe_topics(metadata.topics)
for topic in topics:
    for partition in topic.partitions.values():
        if partition.isr and partition.replicas:
            under_replicated = len(partition.replicas) - len(partition.isr)
            if under_replicated > 0:
                print(f"Topic: {topic.name}, Partition: {partition.partition}, Under-replicated by: {under_replicated}")

