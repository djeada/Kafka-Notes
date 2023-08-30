import argparse
from kafka.admin import KafkaAdminClient


def get_partition_count(broker_address, topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=broker_address)

    # Use the describe_topics method
    topics_metadata = admin_client.describe_topics(topics=[topic_name])

    for topic_metadata in topics_metadata:
        if topic_metadata["topic"] == topic_name:
            return len(topic_metadata["partitions"])

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve number of partitions for a Kafka topic."
    )

    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Address of the Kafka broker. Default: localhost:9092",
    )
    parser.add_argument(
        "--topic",
        default="kafka_topic",
        help="Name of the Kafka topic to get partition count for.",
    )

    args = parser.parse_args()

    partition_count = get_partition_count(args.broker, args.topic)
    if partition_count is not None:
        print(f"Number of partitions for topic '{args.topic}': {partition_count}")
    else:
        print(f"Topic '{args.topic}' not found.")
