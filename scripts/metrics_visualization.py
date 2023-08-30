import time
from kafka import KafkaConsumer, KafkaError
import matplotlib.pyplot as plt

def initialize_consumer(topic_name, bootstrap_servers='localhost:9092'):
    """Initialize a Kafka consumer."""
    try:
        return KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)
    except KafkaError as e:
        print(f"Error initializing KafkaConsumer: {e}")
        return None

def collect_metrics(consumer, duration=60):
    """Collect message rate and average message size metrics."""
    end_time = time.time() + duration
    message_counts = []
    message_sizes = []

    while time.time() < end_time:
        start_interval = time.time()
        count = 0
        total_size = 0
        for message in consumer:
            count += 1
            total_size += len(message.value)
            if time.time() - start_interval > 1:
                break
        
        avg_size = total_size / count if count > 0 else 0
        message_counts.append(count)
        message_sizes.append(avg_size)

    return message_counts, message_sizes

def visualize_metrics(message_counts, message_sizes):
    """Visualize the collected metrics."""
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ax1.plot(message_counts, 'g-')
    ax2.plot(message_sizes, 'b-')

    ax1.set_xlabel('Time (seconds)')
    ax1.set_ylabel('Message Rate (msgs/sec)', color='g')
    ax2.set_ylabel('Avg Message Size (bytes)', color='b')
    ax1.grid(True)

    plt.title('Kafka Metrics Over Time')
    plt.show()

if __name__ == "__main__":
    topic_name = 'your-topic-name'
    consumer = initialize_consumer(topic_name)
    if consumer:
        message_counts, message_sizes = collect_metrics(consumer)
        visualize_metrics(message_counts, message_sizes)
    else:
        print("Failed to initialize Kafka consumer.")
