from kafka import KafkaProducer
import logging
from kafka.errors import KafkaError
import time
import socket
from multiprocessing import Process


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataGeneratorKafkaConnector:
    def __init__(
        self,
        data_generator_host,
        data_generator_port,
        kafka_broker,
        kafka_topic,
        acks="all",
        max_retries=5,
    ):
        self.data_generator_host = data_generator_host
        self.data_generator_port = data_generator_port
        self.kafka_topic = kafka_topic
        self.max_retries = max_retries
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker, acks=acks, retries=max_retries
        )

    def _send_to_kafka(self, message):
        retry_count = 0
        while retry_count <= self.max_retries:
            start_time = time.time()
            try:
                future = self.kafka_producer.send(self.kafka_topic, message)
                result = future.get(timeout=10)
                end_time = time.time()
                logging.info(
                    f"Message sent to partition {result.partition} with offset {result.offset}. Time taken: {end_time - start_time:.4f} seconds"
                )
                return
            except KafkaError as ke:
                logging.error(
                    f"Failed to send message to Kafka: {ke}. Retrying {retry_count + 1}/{self.max_retries}..."
                )
                retry_count += 1
                time.sleep(2)

        logging.error(f"Failed to send message after {self.max_retries} retries.")

    def listen_to_data_generator(self, retry_delay=5):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.data_generator_host, self.data_generator_port))
                    while True:
                        data = s.recv(1024)
                        if not data:
                            break
                        self._send_to_kafka(data)
            except socket.error as se:
                logging.error(
                    f"Failed to connect to Data Generator or connection lost: {se}. Retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)

    def close(self):
        logging.info("Closing Kafka producer...")
        self.kafka_producer.close()


def start_connector_process():
    data_generator_host = "127.0.0.1"
    data_generator_port = 65432
    kafka_broker = "127.0.0.1:9092"
    kafka_topic = "kafka_topic"

    connector = DataGeneratorKafkaConnector(
        data_generator_host, data_generator_port, kafka_broker, kafka_topic
    )
    connector.listen_to_data_generator()


if __name__ == "__main__":
    # Number of processes to spawn
    num_processes = 4

    processes = []
    for _ in range(num_processes):
        p = Process(target=start_connector_process)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
        
