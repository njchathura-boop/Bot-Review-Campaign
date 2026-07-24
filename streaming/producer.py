from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


def produce(path: str, bootstrap_servers: str, topic: str, rate: float) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError('Install with: pip install -e ".[streaming]"') from exc
    producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all"})
    count = 0
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            producer.produce(topic, key=str(event["product_id"]), value=json.dumps(event))
            producer.poll(0)
            count += 1
            if rate > 0:
                time.sleep(1 / rate)
    producer.flush(30)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay canonical Amazon reviews into Kafka")
    parser.add_argument("--input", default="data/raw/amazon_all_beauty.jsonl")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="reviews.raw.v1")
    parser.add_argument("--rate", type=float, default=20, help="Events per second; 0 means unlimited")
    args = parser.parse_args()
    print(f"published={produce(args.input, args.bootstrap_servers, args.topic, args.rate)}")

