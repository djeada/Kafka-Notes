import argparse
import sqlite3
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def count_rows_in_db(db_path):
    """Count and return the number of rows in the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM messages")
    count = cursor.fetchone()[0]
    conn.close()
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to SQLite Consumer")
    parser.add_argument(
        "--dbpath", default="messages.db", help="Path to SQLite database"
    )

    args = parser.parse_args()

    # Count rows in the database and log it
    row_count = count_rows_in_db(args.dbpath)
    logging.info(f"Operation completed. {row_count} messages saved to the database.")
