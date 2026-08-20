from __future__ import annotations

import statistics
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any


class RuntimeMetrics:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._latencies: deque[float] = deque(maxlen=2_000)
        self._request_latencies: deque[float] = deque(maxlen=2_000)
        self._risks: deque[float] = deque(maxlen=2_000)
        self._confidences: deque[float] = deque(maxlen=2_000)
        self._counts: Counter[str] = Counter()
        self._started_at = time.monotonic()
        self._stream_state = "disabled"
        self._stream_error: str | None = None
        self._stream_last_event_at: str | None = None

    def record_prediction(
        self, latency_ms: float, risk: float, confidence: float, needs_review: bool, language: str
    ) -> None:
        with self._lock:
            self._latencies.append(latency_ms)
            self._risks.append(risk)
            self._confidences.append(confidence)
            self._counts["reviews"] += 1
            self._counts["needs_review"] += int(needs_review)
            self._counts[f"language:{language.lower()[:20] or 'unknown'}"] += 1

    def record_http_request(self, latency_ms: float, status_code: int) -> None:
        with self._lock:
            self._request_latencies.append(latency_ms)
            self._counts["http_requests"] += 1
            self._counts[f"http_status:{status_code}"] += 1
            self._counts["http_errors"] += int(status_code >= 400)
            # FastAPI uses 422 for a submitted review that fails schema validation,
            # such as missing text or a rating outside the allowed 1–5 range.
            self._counts["review_validation_errors"] += int(status_code == 422)

    def record_moderation(self, decision: str) -> None:
        with self._lock:
            self._counts[f"moderation:{decision}"] += 1

    def record_dismissal(self) -> None:
        with self._lock:
            self._counts["dismissals"] += 1

    def record_stream_score(self, candidate: bool) -> None:
        with self._lock:
            self._counts["stream_scores"] += 1
            self._counts["stream_candidates"] += int(candidate)
            self._stream_last_event_at = datetime.now(timezone.utc).isoformat()

    def set_stream_state(self, state: str, error: str | None = None) -> None:
        with self._lock:
            self._stream_state = state
            self._stream_error = error
            if error:
                self._counts["stream_errors"] += 1

    def summary(
        self,
        campaign_count: int,
        soft_limit_count: int,
    ) -> dict[str, Any]:
        with self._lock:
            samples = sorted(self._latencies)

            def percentile(ratio: float) -> float:
                if not samples:
                    return 0.0
                index = min(len(samples) - 1, int((len(samples) - 1) * ratio))
                return round(samples[index], 2)

            request_samples = sorted(self._request_latencies)

            def request_percentile(ratio: float) -> float:
                if not request_samples:
                    return 0.0
                index = min(len(request_samples) - 1, int((len(request_samples) - 1) * ratio))
                return round(request_samples[index], 2)

            elapsed = max(time.monotonic() - self._started_at, 1)
            return {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "reviews_processed": self._counts["reviews"],
                "reviews_needing_human_review": self._counts["needs_review"],
                "reviews_per_second": round(self._counts["reviews"] / elapsed, 3),
                "api_requests_total": self._counts["http_requests"],
                "api_errors_total": self._counts["http_errors"],
                "api_error_rate": round(
                    self._counts["http_errors"] / max(self._counts["http_requests"], 1), 6
                ),
                "api_latency_ms": {
                    "p50": request_percentile(0.50),
                    "p95": request_percentile(0.95),
                    "p99": request_percentile(0.99),
                },
                "latency_ms": {
                    "p50": percentile(0.50),
                    "p95": percentile(0.95),
                    "p99": percentile(0.99),
                },
                "mean_review_risk": (
                    round(statistics.fmean(self._risks), 4) if self._risks else 0.0
                ),
                "mean_review_confidence": (
                    round(statistics.fmean(self._confidences), 4) if self._confidences else 0.0
                ),
                "campaign_alerts": campaign_count,
                "soft_limited_campaigns": soft_limit_count,
                "campaign_dismissals": self._counts["dismissals"],
                "moderation": {
                    "confirmed": self._counts["moderation:confirm"],
                    "dismissed": self._counts["moderation:dismiss"],
                    "restored": self._counts["moderation:restore"],
                },
                "data_quality": {
                    "accepted_reviews": self._counts["reviews"],
                    "invalid_requests": self._counts["review_validation_errors"],
                    "languages": {
                        key.split(":", 1)[1]: count
                        for key, count in self._counts.items()
                        if key.startswith("language:")
                    },
                },
                "streaming": {
                    "state": self._stream_state,
                    "scores_consumed": self._counts["stream_scores"],
                    "candidates_materialized": self._counts["stream_candidates"],
                    "errors": self._counts["stream_errors"],
                    "last_event_at": self._stream_last_event_at,
                    "last_error": self._stream_error,
                },
                "resources": {
                    "disk_free_bytes": shutil.disk_usage(Path("/")).free,
                    "uptime_seconds": round(elapsed, 3),
                },
            }


def prometheus_text(summary: dict[str, Any]) -> str:
    values = [
        ("review_scans_total", "Reviews scored by the service.", summary["reviews_processed"], "counter"),
        ("review_scans_per_second", "Recent average review scan rate.", summary["reviews_per_second"], "gauge"),
        ("review_inference_p95_ms", "Review inference p95 latency.", summary["latency_ms"]["p95"], "gauge"),
        ("api_requests_total", "HTTP requests served by the API.", summary["api_requests_total"], "counter"),
        ("api_errors_total", "HTTP requests with 4xx or 5xx status.", summary["api_errors_total"], "counter"),
        ("api_error_rate", "Fraction of API requests that returned 4xx or 5xx.", summary["api_error_rate"], "gauge"),
        ("api_request_latency_p50_ms", "API request p50 latency.", summary["api_latency_ms"]["p50"], "gauge"),
        ("api_request_latency_p95_ms", "API request p95 latency.", summary["api_latency_ms"]["p95"], "gauge"),
        ("api_request_latency_p99_ms", "API request p99 latency.", summary["api_latency_ms"]["p99"], "gauge"),
        ("campaign_alerts_total", "Campaigns currently detected.", summary["campaign_alerts"], "gauge"),
        ("review_needs_human_review_total", "Individual reviews needing human review.", summary["reviews_needing_human_review"], "counter"),
        ("campaign_moderation_confirmed_total", "Campaigns confirmed by moderators.", summary["moderation"]["confirmed"], "counter"),
        ("campaign_moderation_dismissed_total", "Campaigns dismissed by moderators.", summary["moderation"]["dismissed"], "counter"),
        ("campaign_moderation_restored_total", "Campaign soft limits restored by moderators.", summary["moderation"]["restored"], "counter"),
        ("review_mean_risk", "Mean review fake-risk score.", summary["mean_review_risk"], "gauge"),
        ("review_mean_confidence", "Mean review prediction confidence.", summary["mean_review_confidence"], "gauge"),
        (
            "campaign_stream_scores_total",
            "Scored campaign messages consumed from Kafka.",
            summary["streaming"]["scores_consumed"],
            "counter",
        ),
        ("campaign_stream_candidates_total", "Candidate campaigns materialized from Kafka.", summary["streaming"]["candidates_materialized"], "counter"),
        (
            "campaign_stream_consumer_errors_total",
            "Campaign score consumer errors.",
            summary["streaming"]["errors"],
            "counter",
        ),
        ("review_invalid_requests_total", "Requests rejected by API validation.", summary["data_quality"]["invalid_requests"], "counter"),
        ("api_uptime_seconds", "Seconds since the API process started.", summary["resources"]["uptime_seconds"], "gauge"),
        ("api_disk_free_bytes", "Free bytes on the API container filesystem.", summary["resources"]["disk_free_bytes"], "gauge"),
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
    for language, count in sorted(summary["data_quality"]["languages"].items()):
        safe_language = "".join(char for char in language if char.isalnum() or char in "_-")
        lines.append(f'review_language_reviews_total{{language="{safe_language or "unknown"}"}} {count}')
    return "\n".join(lines) + "\n"
