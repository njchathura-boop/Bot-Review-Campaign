from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path

from bot_campaign.campaign_inference import score_campaign_window
from bot_campaign.hybrid_model import HybridCampaignScorer

def run(
    bundle: str,
    bootstrap_servers: str,
    input_topic: str,
    output_topic: str,
    group_id: str,
    similarity_threshold: float,
    minimum_group_size: int,
) -> None:
    try:
        from confluent_kafka import Consumer, KafkaError, Producer
    except ImportError as exc:
        raise RuntimeError('Install streaming dependencies with: pip install -e ".[streaming]"') from exc

    scorer = HybridCampaignScorer.load(Path(bundle))
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    running = True

    def stop(*_args) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer.subscribe([input_topic])
    try:
        while running:
            record = consumer.poll(1.0)
            if record is None:
                continue
            if record.error():
                if record.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(record.error()))
            try:
                outputs = score_campaign_window(
                    json.loads(record.value()),
                    scorer,
                    similarity_threshold=similarity_threshold,
                    minimum_group_size=minimum_group_size,
                )
                for output in outputs:
                    producer.produce(
                        output_topic,
                        key=output["group_id"],
                        value=json.dumps(output, separators=(",", ":")),
                    )
                producer.flush(30)
                consumer.commit(message=record, asynchronous=False)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"Invalid candidate at partition={record.partition()} offset={record.offset()}"
                ) from exc
    finally:
        producer.flush(30)
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score Spark campaign windows with a hybrid model")
    parser.add_argument("--bundle", default="artifacts/campaign_model")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="reviews.analysis-windows.v1")
    parser.add_argument("--output-topic", default="reviews.campaign-scores.v1")
    parser.add_argument("--group-id", default="campaign-scorer-v1")
    parser.add_argument("--similarity-threshold", type=float, default=0.88)
    parser.add_argument("--minimum-group-size", type=int, default=3)
    arguments = parser.parse_args()
    run(
        arguments.bundle,
        arguments.bootstrap_servers,
        arguments.input_topic,
        arguments.output_topic,
        arguments.group_id,
        arguments.similarity_threshold,
        arguments.minimum_group_size,
    )
