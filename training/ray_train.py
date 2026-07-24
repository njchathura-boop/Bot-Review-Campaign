from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def trial(config: dict, rows: list[dict]) -> dict:
    import numpy as np
    from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score
    from sklearn.model_selection import GroupShuffleSplit, train_test_split

    from bot_campaign.model import build_pipeline

    texts = np.asarray([row["text"] for row in rows])
    labels = np.asarray([int(row["label"]) for row in rows])
    groups = np.asarray([row.get("group_id") or "" for row in rows])
    if all(groups) and len(set(groups)) >= 4:
        train_idx, test_idx = next(GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=42).split(texts, labels, groups))
        train_x, test_x, train_y, test_y = texts[train_idx], texts[test_idx], labels[train_idx], labels[test_idx]
    else:
        train_x, test_x, train_y, test_y = train_test_split(
            texts, labels, test_size=0.25, random_state=42, stratify=labels
        )
    model = build_pipeline(max_features=int(config["max_features"]))
    model.named_steps["classifier"].set_params(C=float(config["C"]))
    model.fit(train_x, train_y)
    probabilities = model.predict_proba(test_x)[:, 1]
    predicted = (probabilities >= 0.5).astype(int)
    return {
        "config": config,
        "metrics": {
            "f1": float(f1_score(test_y, predicted, zero_division=0)),
            "precision": float(precision_score(test_y, predicted, zero_division=0)),
            "recall": float(recall_score(test_y, predicted, zero_division=0)),
            "roc_auc": float(roc_auc_score(test_y, probabilities)),
            "pr_auc": float(average_precision_score(test_y, probabilities)),
        },
        "model": model,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run distributed review-model trials on Ray and track with MLflow")
    parser.add_argument("--data", default="data/sample/labeled_reviews.jsonl")
    parser.add_argument("--ray-address", default=os.getenv("RAY_ADDRESS", "auto"))
    parser.add_argument("--mlflow-uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    parser.add_argument("--experiment", default="fake-review-classification")
    parser.add_argument("--registered-model", default="fake-review-classifier")
    args = parser.parse_args()

    import mlflow
    import mlflow.sklearn
    import ray

    rows = [json.loads(line) for line in Path(args.data).read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(rows) < 10 or {int(row["label"]) for row in rows} != {0, 1}:
        raise ValueError("Labeled training data must contain at least 10 rows and both 0/1 labels")
    ray.init(address=args.ray_address, ignore_reinit_error=True)
    remote_trial = ray.remote(num_cpus=1)(trial)
    grid = [{"C": c, "max_features": features} for c in (0.5, 1.0, 2.0) for features in (10_000, 20_000)]
    results = ray.get([remote_trial.remote(config, rows) for config in grid])

    mlflow.set_tracking_uri(args.mlflow_uri)
    mlflow.set_experiment(args.experiment)
    for result in results:
        with mlflow.start_run(run_name=f"ray-C{result['config']['C']}-f{result['config']['max_features']}"):
            mlflow.set_tags({"executor": "ray", "dataset_role": "labeled-supervised", "split_strategy": "group-aware-if-available", "demo_data": str("sample" in args.data).lower()})
            mlflow.log_params(result["config"])
            mlflow.log_metrics(result["metrics"])
    best = max(results, key=lambda item: (item["metrics"]["pr_auc"], item["metrics"]["f1"]))
    with mlflow.start_run(run_name="ray-selected-model"):
        mlflow.log_params(best["config"])
        mlflow.log_metrics(best["metrics"])
        model_info = mlflow.sklearn.log_model(
            sk_model=best["model"], name="model", registered_model_name=args.registered_model,
            input_example=[rows[0]["text"], rows[1]["text"]],
        )
        mlflow.set_tag("amazon_usage", "behavioral_features_only; not supervised labels")
    print(json.dumps({"best_config": best["config"], "metrics": best["metrics"], "model_uri": model_info.model_uri}, indent=2))
    ray.shutdown()


if __name__ == "__main__":
    main()
