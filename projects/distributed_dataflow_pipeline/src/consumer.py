import argparse
import sqlite3
import logging
import time
from multiprocessing import Process, Queue

from kafka import KafkaConsumer

import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s",
)


class KafkaConsumerProcess:
    def __init__(self, kafka_broker, kafka_topic, message_queue):
        self.message_queue = message_queue
        logging.info(
            "Initializing consumer for broker: %s and topic: %s",
            kafka_broker,
            kafka_topic,
        )

        unique_group_id = "kafka-sqlite-group-" + str(int(time.time()))

        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            group_id=unique_group_id,
        )
        self.kafka_consumer.subscribe([kafka_topic])

    def consume_and_store(self):
        logging.info("Starting to consume messages from Kafka")
        consumed_count = 0

        while True:
            # Poll for a single message with a timeout of 10 seconds (10000ms).
            # Adjust the timeout as necessary.
            messages = self.kafka_consumer.poll(10000)
            if not messages:
                logging.info("No messages received for 10 seconds. Breaking the loop.")
                break

            for tp, msgs in messages.items():
                for message in msgs:
                    consumed_count += 1
                    decoded_message = message.value.decode("utf-8")
                    logging.info(
                        "Received message %d: %s", consumed_count, decoded_message
                    )
                    self.message_queue.put(decoded_message)

        logging.info("Consumed %d messages in total", consumed_count)


class SQLiteOperations:
    @staticmethod
    def init_db(conn):
        logging.info("Initializing SQLite database")
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                message TEXT
            )
        """
        )
        conn.commit()

    @staticmethod
    def save_to_db(conn, message):
        try:
            logging.info("Saving message to SQLite database: %s", message)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO messages (message) VALUES (?)", (message,))
            conn.commit()
        except sqlite3.OperationalError as e:
            logging.error(
                "Failed to save message to SQLite due to an operational error: %s",
                str(e),
            )
        except Exception as e:
            logging.error("Failed to save message to SQLite: %s", str(e))


def consume_messages(broker, topic, message_queue):
    logging.info("Kafka Consumer Process started")
    try:
        consumer = KafkaConsumerProcess(broker, topic, message_queue)
        consumer.consume_and_store()
    except Exception as e:
        logging.error("Error occurred: %s", str(e))
    logging.info("Kafka Consumer Process finished")


def db_writer_process(queue, dbpath):
    write_count = 0
    logging.info("DB Writer Process started")
    conn = sqlite3.connect(dbpath)
    SQLiteOperations.init_db(conn)
    try:
        while True:
            message = queue.get()
            if message == "TERMINATE":
                break
            SQLiteOperations.save_to_db(conn, message)
            write_count += 1
            logging.info("Written %d messages to database", write_count)

    except Exception as e:
        logging.error("Error occurred: %s", str(e))
    finally:
        logging.info("Written %d messages to database", write_count)
        conn.close()
    logging.info("DB Writer Process finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to SQLite Consumer")
    parser.add_argument(
        "--broker", default="127.0.0.1:9092", help="Kafka broker address"
    )
    parser.add_argument(
        "--topic", default="kafka_topic", help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--dbpath", default="messages.db", help="Path to SQLite database"
    )
    parser.add_argument(
        "--processes", type=int, default=3, help="Number of consumer processes"
    )

    args = parser.parse_args()
    processes = []
    message_queue = Queue()

    db_writer = Process(target=db_writer_process, args=(message_queue, args.dbpath))
    db_writer.start()

    for _ in range(args.processes):
        p = Process(
            target=consume_messages, args=(args.broker, args.topic, message_queue)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    message_queue.put("TERMINATE")
    db_writer.join()
    logging.info("Number of messages still in queue: %d", message_queue.qsize())
