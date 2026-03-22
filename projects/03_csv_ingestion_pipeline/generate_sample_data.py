import argparse
import csv
import logging
import random
from typing import Optional, Sequence

# Configuration
DEFAULT_OUTPUT = 'sample_data.csv'
DEFAULT_ROWS = 100

FIRST_NAMES = [
    'Alice', 'Bob', 'Charlie', 'Diana', 'Edward', 'Fiona', 'George', 'Hannah',
    'Ivan', 'Julia', 'Kevin', 'Laura', 'Michael', 'Nina', 'Oscar', 'Patricia',
    'Quentin', 'Rachel', 'Samuel', 'Tina', 'Ulysses', 'Victoria', 'Walter',
    'Xena', 'Yusuf', 'Zara',
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
    'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
    'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
]

CITIES = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
    'San Antonio', 'San Diego', 'Dallas', 'Austin', 'Seattle', 'Denver',
    'Boston', 'Nashville', 'Portland', 'Atlanta', 'Miami', 'Minneapolis',
    'Detroit', 'Charlotte',
]

DOMAINS = ['example.com', 'mail.com', 'test.org', 'demo.net', 'sample.io']

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_row(row_id: int) -> dict:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    domain = random.choice(DOMAINS)
    return {
        'id': row_id,
        'name': f"{first} {last}",
        'email': f"{first.lower()}.{last.lower()}@{domain}",
        'age': random.randint(18, 75),
        'city': random.choice(CITIES),
    }


def generate_csv(output_path: str, num_rows: int) -> None:
    fieldnames = ['id', 'name', 'email', 'age', 'city']
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, num_rows + 1):
            writer.writerow(generate_row(i))
    logger.info(f"Generated {num_rows} rows in '{output_path}'")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a sample CSV file with fake user data.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV file path")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="Number of data rows to generate")
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(args)


def main(args: Optional[Sequence[str]] = None) -> None:
    parsed_args = parse_args(args)
    generate_csv(parsed_args.output, parsed_args.rows)


if __name__ == "__main__":
    main()
