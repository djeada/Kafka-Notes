import argparse
import time
import matplotlib.pyplot as plt
import socket
import logging
from typing import List, Optional, Sequence

# Configuration
DEFAULT_BROKERS = ['localhost:9092']  # add all your brokers here
DEFAULT_TIMEOUT = 1  # socket timeout in seconds
DEFAULT_PING_INTERVAL = 5  # in seconds
DEFAULT_MONITOR_DURATION = 60  # in seconds
DEFAULT_HIGH_LATENCY_THRESHOLD = 200  # in milliseconds

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

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor Kafka broker connectivity latency and highlight slow responses.")
    parser.add_argument(
        "--brokers",
        nargs="+",
        default=DEFAULT_BROKERS,
        help="One or more Kafka brokers in host:port format",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Socket timeout in seconds")
    parser.add_argument("--ping-interval", type=int, default=DEFAULT_PING_INTERVAL, help="Seconds between latency checks")
    parser.add_argument(
        "--monitor-duration",
        type=int,
        default=DEFAULT_MONITOR_DURATION,
        help="Total monitoring duration in seconds",
    )
    parser.add_argument(
        "--high-latency-threshold",
        type=float,
        default=DEFAULT_HIGH_LATENCY_THRESHOLD,
        help="Latency threshold in milliseconds used for alerting",
    )
    return parser

def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)

def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    latencies = monitor_brokers(
        parsed_args.brokers,
        parsed_args.monitor_duration,
        parsed_args.ping_interval,
        parsed_args.timeout,
    )
    plot_latencies(latencies, parsed_args.high_latency_threshold)
    alert_high_latency(latencies, parsed_args.high_latency_threshold)

if __name__ == "__main__":
    main()
