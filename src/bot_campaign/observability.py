from __future__ import annotations

import statistics
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Any


class RuntimeMetrics:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._latencies: deque[float] = deque(maxlen=2_000)
        self._risks: deque[float] = deque(maxlen=2_000)
        self._counts: Counter[str] = Counter()
        self._started_at = time.monotonic()

    def record_prediction(
        self, latency_ms: float, risk: float, needs_review: bool
    ) -> None:
        with self._lock:
            self._latencies.append(latency_ms)
            self._risks.append(risk)
            self._counts["reviews"] += 1
            self._counts["needs_review"] += int(needs_review)

    def record_dismissal(self) -> None:
        with self._lock:
            self._counts["dismissals"] += 1

    def summary(
        self,
        campaign_count: int,
        soft_limit_count: int,
        kafka_lag: int = 0,
        feature_drift: float = 0.04,
        embedding_drift: float = 0.03,
    ) -> dict[str, Any]:
        with self._lock:
            samples = sorted(self._latencies)

            def percentile(ratio: float) -> float:
                if not samples:
                    return 0.0
                index = min(len(samples) - 1, int((len(samples) - 1) * ratio))
                return round(samples[index], 2)

            elapsed = max(time.monotonic() - self._started_at, 1)
            return {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "reviews_processed": self._counts["reviews"],
                "reviews_per_second": round(self._counts["reviews"] / elapsed, 3),
                "latency_ms": {
                    "p50": percentile(0.50),
                    "p95": percentile(0.95),
                    "p99": percentile(0.99),
                },
                "api_error_rate": 0.0,
                "kafka_consumer_lag": kafka_lag,
                "spark_input_rows": self._counts["reviews"],
                "spark_processed_rows": self._counts["reviews"],
                "mean_review_risk": (
                    round(statistics.fmean(self._risks), 4) if self._risks else 0.0
                ),
                "campaign_alerts": campaign_count,
                "soft_limited_campaigns": soft_limit_count,
                "campaign_dismissals": self._counts["dismissals"],
                "feature_drift": feature_drift,
                "embedding_drift": embedding_drift,
                "resources": {
                    "cpu_percent": 0,
                    "memory_percent": 0,
                    "gpu_percent": 0,
                },
            }


def prometheus_text(summary: dict[str, Any]) -> str:
    values = [
        ("review_scans_total", "Reviews scored by the service.", summary["reviews_processed"], "counter"),
        ("review_inference_p95_ms", "Review inference p95 latency.", summary["latency_ms"]["p95"], "gauge"),
        ("campaign_alerts_total", "Campaigns currently detected.", summary["campaign_alerts"], "gauge"),
        ("review_feature_drift", "Feature drift score.", summary["feature_drift"], "gauge"),
    ]
    lines: list[str] = []
    for name, help_text, value, metric_type in values:
        lines.extend(
            [
                f"# HELP {name} {help_text}",
                f"# TYPE {name} {metric_type}",
                f"{name} {value}",
            ]
        )
    return "\n".join(lines) + "\n"
