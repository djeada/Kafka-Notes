import time
import matplotlib.pyplot as plt
import socket

# Configuration
brokers = ['localhost:9092']  # add all your brokers here
timeout = 1  # socket timeout in seconds
ping_interval = 5  # in seconds
monitor_duration = 60  # in seconds
high_latency_threshold = 200  # in milliseconds

latencies = []

end_time = time.time() + monitor_duration
while time.time() < end_time:
    for broker in brokers:
        host, port = broker.split(":")
        start_time = time.time()
        try:
            socket.create_connection((host, int(port)), timeout=timeout)
            latency = (time.time() - start_time) * 1000  # convert to milliseconds
            latencies.append(latency)
        except Exception as e:
            latencies.append(float('inf'))
            print(f"ALERT: Cannot connect to broker {broker}. Broker might be down!")
        time.sleep(ping_interval)

# Visualization
plt.plot(latencies)
plt.axhline(y=high_latency_threshold, color='r', linestyle='--')
plt.xlabel('Time (intervals)')
plt.ylabel('Latency (ms)')
plt.title('Broker Latency Over Time')
plt.show()

# Alerting for high latency
for i, latency in enumerate(latencies):
    if latency > high_latency_threshold and latency != float('inf'):
        print(f"ALERT: High latency detected at interval {i}. Latency: {latency}ms")
