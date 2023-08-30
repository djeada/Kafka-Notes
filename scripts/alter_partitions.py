import argparse
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic


def get_partition_count(admin_client, topic_name):
    topics_metadata = admin_client.describe_topics(topics=[topic_name])
    for topic_metadata in topics_metadata:
        if topic_metadata["topic"] == topic_name:
            return len(topic_metadata["partitions"])
    return None


def topic_exists(admin_client, topic_name):
    topic_list = admin_client.list_topics()
    return topic_name in topic_list


def create_topic(admin_client, topic_name, partitions=1):
    topic = NewTopic(
        name=topic_name, num_partitions=partitions, replication_factor=1
    )  # Assuming a replication factor of 1
    admin_client.create_topics(new_topics=[topic])
    print(f"Topic '{topic_name}' created with {partitions} partitions.")


def increase_partition_count(admin_client, topic_name, new_count):
    if not topic_exists(admin_client, topic_name):
        create_topic(admin_client, topic_name, new_count)
        return

    total_partitions = get_partition_count(admin_client, topic_name)

    if new_count <= total_partitions:
        print(
            f"Current partition count is {total_partitions}. Please provide a number greater than this."
        )
        return

    try:
        admin_client.create_partitions(
            topic_partitions={topic_name: NewPartitions(total_count=new_count)}
        )
        print(f"Partition count increased to {new_count} for topic '{topic_name}'.")
    except Exception as e:
        print(f"Error increasing partition count: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Kafka topic partitions.")
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Address of the Kafka broker. Default: localhost:9092",
    )
    parser.add_argument(
        "--topic", default="kafka_topic", help="Name of the Kafka topic."
    )
    parser.add_argument(
        "--increase-to",
        default=32,
        type=int,
        help="Increase the number of partitions to the specified value.",
    )

    args = parser.parse_args()

    admin_client = KafkaAdminClient(bootstrap_servers=args.broker)

    if args.increase_to:
        increase_partition_count(admin_client, args.topic, args.increase_to)
    else:
        partition_count = get_partition_count(admin_client, args.topic)
        if partition_count is not None:
            print(f"Number of partitions for topic '{args.topic}': {partition_count}")
        else:
            print(f"Topic '{args.topic}' not found.")
