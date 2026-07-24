from __future__ import annotations

import json
from pathlib import Path

from .campaign import detect_campaigns
from .data import load_reviews, read_records
from .model import save_bundle, train
from .schemas import Review
from .synthetic import generate_dataset


def run_e2e_demo(output_dir: str | Path, seed: int = 42) -> dict:
    """Run the bounded local workflow used before any full-data execution."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = output_dir / "synthetic_sample.jsonl"
    model_path = output_dir / "review_model.joblib"
    report_path = output_dir / "report.json"

    manifest = generate_dataset(dataset_path, seed=seed, preset="sample")
    labeled_reviews, validation = load_reviews(dataset_path, labeled=True)
    if validation.rejected:
        raise ValueError(f"Generated dataset contains {validation.rejected} invalid rows")

    bundle, model_metrics = train(labeled_reviews, seed=seed)
    save_bundle(bundle, model_path)

    raw_campaign = next(
        row for row in read_records(dataset_path) if row.get("expected_campaign")
    )
    campaign_id = raw_campaign["campaign_id"]
    campaign_reviews = [
        Review.model_validate(row)
        for row in read_records(dataset_path)
        if row.get("campaign_id") == campaign_id
    ]
    alerts = detect_campaigns(campaign_reviews)
    if len(alerts) != 1:
        raise ValueError(
            f"Expected exactly one known campaign in the demo, detected {len(alerts)}"
        )
    result = {
        "status": "passed",
        "evaluation_scope": "synthetic_smoke_test_not_research_metrics",
        "dataset": {
            "path": str(dataset_path),
            "records": manifest["records"],
            "sha256": manifest["sha256"],
            "accepted": validation.accepted,
            "rejected": validation.rejected,
        },
        "model": {
            "path": str(model_path),
            "version": bundle["version"],
            "metrics": model_metrics,
        },
        "campaign": {
            "source_campaign_id": campaign_id,
            "reviews": len(campaign_reviews),
            "alerts": len(alerts),
            "top_risk": alerts[0].risk_score if alerts else 0,
        },
    }
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    result["report"] = str(report_path)
    return result
