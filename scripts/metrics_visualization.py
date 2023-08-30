import time
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Initialize Kafka consumer
consumer = KafkaConsumer('your-topic-name', bootstrap_servers='localhost:9092')

# Duration for which we want to collect metrics (e.g., 60 seconds)
duration = 60
end_time = time.time() + duration

# Metrics data
message_counts = []
message_sizes = []

while time.time() < end_time:
    start_interval = time.time()
    count = 0
    total_size = 0
    for message in consumer:
        count += 1
        total_size += len(message.value)
        # Break the loop after 1 second to get per-second metrics
        if time.time() - start_interval > 1:
            break
    
    message_counts.append(count)
    message_sizes.append(total_size / count if count > 0 else 0)

# Visualization using matplotlib
fig, ax1 = plt.subplots()

ax2 = ax1.twinx()
ax1.plot(message_counts, 'g-')
ax2.plot(message_sizes, 'b-')

ax1.set_xlabel('Time (seconds)')
ax1.set_ylabel('Message Rate (msgs/sec)', color='g')
ax2.set_ylabel('Avg Message Size (bytes)', color='b')

plt.show()
