"""Kafka producer for replaying the cleaned CSV dataset."""

import argparse
import csv
import gzip
import json
import os
import time
from pathlib import Path

try:
    from kafka import KafkaProducer
except ImportError:  # Optional until Kafka is enabled.
    KafkaProducer = None


COLUMN_MAP = {
    "timestamp": "timestamp",
    "ip_address": "ip",
    "http_method": "method",
    "url": "path",
    "protocol": "protocol",
    "status_code": "status",
    "response_bytes": "response_bytes",
    "user_agent": "user_agent",
    "response_time": "response_time",
}


def mapped_record(row: dict[str, str]) -> dict[str, object]:
    record: dict[str, object] = {}
    for target, source in COLUMN_MAP.items():
        value = row.get(source, "")
        if target in {"status_code", "response_bytes"}:
            value = int(float(value or 0))
        elif target == "response_time":
            value = float(value or 0)
        record[target] = value
    return record


def produce(dataset: Path, bootstrap_servers: str, topic: str, delay: float) -> int:
    if KafkaProducer is None:
        raise RuntimeError("Install kafka-python to enable the Kafka producer")
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers.split(","), value_serializer=lambda value: json.dumps(value).encode())
    sent = 0
    opener = gzip.open if dataset.suffix == ".gz" else open
    with opener(dataset, "rt", encoding="utf-8", errors="replace", newline="") as source:
        for row in csv.DictReader(source):
            try:
                producer.send(topic, mapped_record(row))
                sent += 1
                if delay > 0:
                    time.sleep(delay)
            except (TypeError, ValueError, KeyError):
                continue
    producer.flush()
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=os.getenv("DATASET_PATH", "access_preprocessed.csv.gz"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "web-logs"))
    parser.add_argument("--delay", type=float, default=float(os.getenv("SIMULATION_DELAY", "0.1")))
    args = parser.parse_args()
    print(f"Published {produce(Path(args.dataset), args.bootstrap_servers, args.topic, args.delay):,} records")
