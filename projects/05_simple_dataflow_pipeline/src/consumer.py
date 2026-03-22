import argparse
import sqlite3
import logging
import time

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class KafkaSQLiteConsumer:
    def __init__(self, kafka_broker, kafka_topic, db_path):
        logging.info(
            "Initializing consumer for broker: %s and topic: %s",
            kafka_broker,
            kafka_topic,
        )

        # Create a unique group_id using the current timestamp
        unique_group_id = "kafka-sqlite-group-" + str(int(time.time()))

        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker,
            auto_offset_reset="earliest",
            group_id=unique_group_id,  # <- Use the unique group_id
        )

        # Explicitly subscribe to the topic
        self.kafka_consumer.subscribe([kafka_topic])
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        logging.info("Initializing SQLite database")
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                message TEXT
            )
        """
        )
        self.conn.commit()

    def _save_to_db(self, message):
        try:
            logging.info("Saving message to SQLite database: %s", message)
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO messages (message)
                VALUES (?)
            """,
                (message,),
            )
            self.conn.commit()
        except Exception as e:
            logging.error("Failed to save message to SQLite: %s", str(e))

    def consume_and_store(self):
        logging.info("Starting to consume messages from Kafka")
        try:
            for message in self.kafka_consumer:
                decoded_message = message.value.decode("utf-8")
                logging.info("Received message: %s", decoded_message)
                self._save_to_db(decoded_message)
        except Exception as e:
            logging.error("Error while consuming messages: %s", str(e))
        finally:
            self.close()

    def close(self):
        logging.info("Closing SQLite connection")
        self.conn.close()


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

    args = parser.parse_args()

    try:
        consumer = KafkaSQLiteConsumer(
            kafka_broker=args.broker, kafka_topic=args.topic, db_path=args.dbpath
        )
        consumer.consume_and_store()
    except Exception as e:
        logging.error("Error occurred: %s", str(e))
