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

    # Provide a basic CLI interface
    group = input("Enter consumer group name: ")
    topic = input("Enter topic name: ")

    # NOTE: For simplicity, we're assuming a fixed number of partitions. In a real-world scenario, you'd fetch this dynamically.
    num_partitions = 1  # Change this to your topic's partition count

    print("Choose offset reset option:")
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
