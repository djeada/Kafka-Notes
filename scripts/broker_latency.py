import time
import matplotlib.pyplot as plt
import socket
import logging
from typing import List

# Configuration
BROKERS = ['localhost:9092']  # add all your brokers here
TIMEOUT = 1  # socket timeout in seconds
PING_INTERVAL = 5  # in seconds
MONITOR_DURATION = 60  # in seconds
HIGH_LATENCY_THRESHOLD = 200  # in milliseconds

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latency(host: str, port: int, timeout: int) -> float:
    start_time = time.time()
    try:
        socket.create_connection((host, port), timeout=timeout)
        latency = (time.time() - start_time) * 1000  # convert to milliseconds
    except socket.error:
        latency = float('inf')
        logger.error(f"Cannot connect to broker {host}:{port}. Broker might be down!")
    return latency

def monitor_brokers(brokers: List[str], duration: int, interval: int, timeout: int) -> List[float]:
    latencies = []
    end_time = time.time() + duration
    while time.time() < end_time:
        for broker in brokers:
            host, port = broker.split(":")
            latency = get_latency(host, int(port), timeout)
            latencies.append(latency)
        time.sleep(interval)
    return latencies

def plot_latencies(latencies: List[float], threshold: float) -> None:
    plt.plot(latencies, label='Latency')
    plt.axhline(y=threshold, color='r', linestyle='--', label='High Latency Threshold')
    plt.xlabel('Time (intervals)')
    plt.ylabel('Latency (ms)')
    plt.title('Broker Latency Over Time')
    plt.legend()
    plt.grid(True)
    plt.show()

def alert_high_latency(latencies: List[float], threshold: float) -> None:
    for i, latency in enumerate(latencies):
        if latency > threshold and latency != float('inf'):
            logger.warning(f"High latency detected at interval {i}. Latency: {latency:.2f}ms")

def main():
    latencies = monitor_brokers(BROKERS, MONITOR_DURATION, PING_INTERVAL, TIMEOUT)
    plot_latencies(latencies, HIGH_LATENCY_THRESHOLD)
    alert_high_latency(latencies, HIGH_LATENCY_THRESHOLD)

if __name__ == "__main__":
    main()
