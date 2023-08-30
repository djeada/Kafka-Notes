from kafka import KafkaAdminClient
import matplotlib.pyplot as plt
import time

# Configuration
bootstrap_servers = 'localhost:9092'
consumer_group = 'your-consumer-group'
threshold_lag = 100  # adjust as needed

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Fetch consumer offsets and end offsets
offsets = admin_client.list_consumer_group_offsets(consumer_group)
end_offsets = admin_client.list_consumer_offsets(consumer_group, offsets.partitions())

lags = []
for tp, offset in offsets.partitions().items():
    end_offset = end_offsets[tp]
    lag = end_offset - offset.offset
    lags.append(lag)

# Visualization
plt.bar(range(len(lags)), lags)
plt.axhline(y=threshold_lag, color='r', linestyle='--')
plt.xlabel('Partition')
plt.ylabel('Lag')
plt.title('Consumer Lag per Partition')
plt.show()

# Alerting
for i, lag in enumerate(lags):
    if lag > threshold_lag:
        print(f"ALERT: Lag for partition {i} is {lag}, which exceeds the threshold!")
