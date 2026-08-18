from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from .campaign_features import CAMPAIGN_SCORE_SCHEMA
from .schemas import CampaignAlert, Review


LOGGER = logging.getLogger(__name__)


def campaign_alert_from_score(message: dict[str, Any]) -> CampaignAlert | None:
    """Validate a scored Kafka message and convert candidates into API alerts."""
    if message.get("schema_version") != CAMPAIGN_SCORE_SCHEMA:
        raise ValueError(
            f"Unsupported campaign score schema: {message.get('schema_version')!r}"
        )
    if not isinstance(message.get("candidate"), bool):
        raise ValueError("Campaign score must contain a boolean candidate field")
    if not message["candidate"]:
        return None

    product_ids = sorted({str(value) for value in message.get("product_ids") or ()})
    review_ids = sorted({str(value) for value in message.get("review_ids") or ()})
    user_ids = sorted({str(value) for value in message.get("user_ids") or ()})
    if not product_ids or not review_ids:
        raise ValueError("Campaign candidate must contain product_ids and review_ids")

    risk = float(message["campaign_risk"])
    if not 0 <= risk <= 1:
        raise ValueError("campaign_risk must be between zero and one")

    return CampaignAlert(
        campaign_id=str(message["group_id"]),
        product_id=product_ids[0],
        product_ids=product_ids,
        review_ids=review_ids,
        user_ids=user_ids,
        risk_score=risk,
        evidence={
            "model": str(message.get("model_name", "unknown")),
            "campaign_scope": str(message.get("campaign_scope", "unknown")),
            "decision_threshold": float(message.get("decision_threshold", 0.0)),
            "semantic_edge_threshold": float(
                message.get("semantic_edge_threshold", 0.0)
            ),
            "feature_version": str(message.get("feature_version", "unknown")),
            "window_start": str(message.get("window_start", "")),
            "window_end": str(message.get("window_end", "")),
            "review_count": len(review_ids),
            "product_count": len(product_ids),
            "source_window_truncated": bool(
                message.get("source_window_truncated", False)
            ),
            "transport": "kafka-spark-streaming",
        },
    )


def publish_reviews(
    reviews: Iterable[Review],
    *,
    bootstrap_servers: str,
    topic: str,
    replay_job_id: str | None = None,
) -> int:
    """Publish canonical review events with idempotent Kafka delivery."""
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            'Install streaming dependencies with: pip install -e ".[streaming]"'
        ) from exc

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    errors: list[str] = []

    def delivered(error, _message) -> None:
        if error is not None:
            errors.append(str(error))

    count = 0
    for review in reviews:
        event = review.model_dump(mode="json")
        event["schema_version"] = "reviews.raw.v1"
        event["replay_job_id"] = replay_job_id
        producer.produce(
            topic,
            key=review.product_id,
            value=json.dumps(event, separators=(",", ":")),
            on_delivery=delivered,
        )
        producer.poll(0)
        count += 1
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(
            f"Kafka review delivery failed: remaining={remaining}, errors={errors[:3]}"
        )
    return count


class KafkaCampaignAlertConsumer:
    """Background materializer for scored campaign events consumed by FastAPI."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        on_message: Callable[[dict[str, Any]], None],
        on_state: Callable[[str, str | None], None],
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.on_message = on_message
        self.on_state = on_state
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        # Import before starting the thread so a missing dependency fails readiness.
        try:
            import confluent_kafka  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                'Campaign alert consumption requires: pip install -e ".[streaming]"'
            ) from exc
        self._thread = threading.Thread(
            target=self._run,
            name="campaign-alert-consumer",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([self.topic])
        try:
            consumer.list_topics(timeout=10)
            self.on_state("running", None)
            while not self._stop.is_set():
                record = consumer.poll(1.0)
                if record is None:
                    continue
                if record.error():
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(record.error()))
                try:
                    message = json.loads(record.value())
                    self.on_message(message)
                    self.on_state("running", None)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    # Commit poison messages so one invalid record cannot block the partition.
                    LOGGER.exception(
                        "Invalid campaign score at partition=%s offset=%s",
                        record.partition(),
                        record.offset(),
                    )
                    self.on_state("degraded", str(exc))
                consumer.commit(message=record, asynchronous=False)
        except Exception as exc:
            LOGGER.exception("Campaign alert consumer stopped unexpectedly")
            self.on_state("failed", str(exc))
        finally:
            consumer.close()
            if self._stop.is_set():
                self.on_state("stopped", None)


def detected_at() -> str:
    return datetime.now(UTC).isoformat()
