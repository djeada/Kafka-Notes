from kafka import KafkaAdminClient, TopicPartition
import datetime

def reset_to_earliest(admin, group, topic):
    beginning_offsets = admin.list_consumer_group_offsets(group, [TopicPartition(topic, p) for p in range(num_partitions)])
    for tp, offset in beginning_offsets.items():
        admin.alter_consumer_group_offsets(group, {tp: offset})

def reset_to_latest(admin, group, topic):
    end_offsets = admin.list_consumer_offsets(group, [TopicPartition(topic, p) for p in range(num_partitions)])
    for tp, offset in end_offsets.items():
        admin.alter_consumer_group_offsets(group, {tp: offset})

def reset_to_timestamp(admin, group, topic, timestamp):
    date_format = "%Y-%m-%d %H:%M:%S"
    target_time = datetime.datetime.strptime(timestamp, date_format)
    timestamp_offsets = admin.list_consumer_offsets(group, [TopicPartition(topic, p) for p in range(num_partitions)], target_time.timestamp() * 1000)
    for tp, offset in timestamp_offsets.items():
        admin.alter_consumer_group_offsets(group, {tp: offset})

if __name__ == "__main__":
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

    # List all consumer groups
    consumer_groups = admin.list_consumer_groups().keys()
    print("\nConsumer Groups:")
    for index, group in enumerate(consumer_groups):
        print(f"{index + 1}. {group}")

    group = input("Choose a consumer group by number or enter its name: ")
    if group.isdigit():
        group = consumer_groups[int(group) - 1]
        
    # List all topics
    topics = admin.list_topics()
    print("\nTopics:")
    for index, topic in enumerate(topics):
        print(f"{index + 1}. {topic}")

    topic = input("Choose a topic by number or enter its name: ")
    if topic.isdigit():
        topic = topics[int(topic) - 1]

    # Display offsets for the chosen group and topic
    print(f"\nCurrent offsets for group '{group}' and topic '{topic}':")
    offsets = admin.list_consumer_group_offsets(group).items()
    for tp, offset in offsets:
        if tp.topic == topic:
            print(f"Partition {tp.partition}: Offset {offset.offset}")

    # NOTE: For simplicity, we're assuming a fixed number of partitions. In a real-world scenario, you'd fetch this dynamically.
    num_partitions = len([tp for tp, offset in offsets if tp.topic == topic])

    print("\nChoose offset reset option:")
    print("1: Earliest")
    print("2: Latest")
    print("3: Specific Date/Time (format: YYYY-MM-DD HH:MM:SS)")
    choice = input()

    if choice == "1":
        reset_to_earliest(admin, group, topic)
    elif choice == "2":
        reset_to_latest(admin, group, topic)
    elif choice == "3":
        timestamp = input("Enter date/time: ")
        reset_to_timestamp(admin, group, topic, timestamp)
    else:
        print("Invalid choice.")

    print("Offset reset operation completed.")
