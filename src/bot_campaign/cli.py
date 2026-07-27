from __future__ import annotations

import argparse
import json
from pathlib import Path

from .campaign import detect_campaigns
from .data import load_review_events, load_text_labels
from .model import save_bundle, train, train_with_holdout


def main() -> None:
    parser = argparse.ArgumentParser(prog="bot-campaign")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="Validate and summarize review data")
    validate.add_argument("path")
    validate.add_argument("--labeled", action="store_true")
    training = commands.add_parser("train", help="Train the supervised baseline")
    training.add_argument("--data", default="data/processed/dataset_bundle/text/training.jsonl")
    training.add_argument(
        "--test-data",
        default="data/processed/dataset_bundle/text/real/test.jsonl",
        help="Optional separate real-only test JSONL; recommended for augmented training",
    )
    training.add_argument(
        "--baseline-data",
        default="data/processed/dataset_bundle/text/real/train.jsonl",
        help="Optional real-only training JSONL used to gate an augmented candidate",
    )
    training.add_argument(
        "--min-pr-auc-lift",
        type=float,
        default=0.05,
        help="Minimum relative PR-AUC lift required to promote augmented training",
    )
    training.add_argument("--output", default="artifacts/review_model.joblib")
    campaigns = commands.add_parser("campaigns", help="Detect suspicious coordination")
    campaigns.add_argument("--data", default="data/sample/campaign_reviews.jsonl")
    amazon = commands.add_parser(
        "download-amazon", help="Stream a bounded Amazon Reviews 2023 sample"
    )
    amazon.add_argument("--category", default="All_Beauty")
    amazon.add_argument("--limit", type=int, default=10_000)
    amazon.add_argument("--output", default="data/raw/amazon_all_beauty.jsonl")
    bundle = commands.add_parser(
        "build-dataset-bundle",
        help="Build separated text, observed-behavior, and campaign datasets",
    )
    bundle.add_argument(
        "--labeled-input",
        nargs="+",
        default=["data/raw/product_reviews.jsonl", "data/raw/kaggle_fake_reviews"],
    )
    bundle.add_argument(
        "--behavioral-input",
        nargs="+",
        default=["data/raw/amazon_all_beauty_sample.jsonl"],
    )
    bundle.add_argument("--product-catalog")
    bundle.add_argument("--output-dir", default="data/processed/dataset_bundle")
    bundle.add_argument("--augmentation-count", type=int, default=7_000)
    bundle.add_argument("--campaign-scenario-count", type=int, default=2_000)
    bundle.add_argument("--max-synthetic-fraction", type=float, default=0.25)
    bundle.add_argument("--seed", type=int, default=42)
    temporal_bundle = commands.add_parser(
        "build-temporal-bundle",
        help="Build Amazon temporal features and controlled campaign splits only",
    )
    temporal_bundle.add_argument(
        "--behavioral-input",
        nargs="+",
        required=True,
    )
    temporal_bundle.add_argument("--product-catalog")
    temporal_bundle.add_argument("--output-dir", default="data/processed/dataset_bundle")
    temporal_bundle.add_argument("--campaign-scenario-count", type=int, default=2_000)
    temporal_bundle.add_argument("--seed", type=int, default=42)
    regenerate_campaigns = commands.add_parser(
        "generate-campaign-splits",
        help="Regenerate leakage-safe campaign splits from an existing behavior profile",
    )
    regenerate_campaigns.add_argument(
        "--profile", default="data/processed/temporal_bundle/behavior/profile.json"
    )
    regenerate_campaigns.add_argument(
        "--products", default="data/processed/temporal_bundle/behavior/products.jsonl"
    )
    regenerate_campaigns.add_argument(
        "--output-dir", default="data/processed/temporal_bundle/campaign"
    )
    regenerate_campaigns.add_argument("--count", type=int, default=816_216)
    regenerate_campaigns.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.command == "validate":
        loader = load_text_labels if args.labeled else load_review_events
        _, report = loader(args.path)
        print(json.dumps(report.__dict__, indent=2))
    elif args.command == "train":
        reviews, report = load_text_labels(args.data)
        if report.rejected:
            raise SystemExit(f"Refusing to train: {report.rejected} invalid records")
        if args.test_data:
            test_reviews, test_report = load_text_labels(args.test_data)
            if test_report.rejected:
                raise SystemExit(
                    f"Refusing evaluation: {test_report.rejected} invalid test records"
                )
            if any(review.synthetic for review in test_reviews):
                raise SystemExit("Refusing evaluation: --test-data must be real-only")
            candidate_bundle, candidate_metrics = train_with_holdout(reviews, test_reviews)
            bundle, metrics = candidate_bundle, candidate_metrics
            if args.baseline_data:
                baseline_reviews, baseline_report = load_text_labels(args.baseline_data)
                if baseline_report.rejected:
                    raise SystemExit(
                        f"Refusing baseline comparison: {baseline_report.rejected} invalid records"
                    )
                baseline_bundle, baseline_metrics = train_with_holdout(
                    baseline_reviews, test_reviews
                )
                baseline_pr_auc = baseline_metrics["pr_auc"]
                relative_lift = (
                    (candidate_metrics["pr_auc"] - baseline_pr_auc) / baseline_pr_auc
                    if baseline_pr_auc
                    else 0.0
                )
                promoted = relative_lift >= args.min_pr_auc_lift
                if not promoted:
                    bundle = baseline_bundle
                metrics = {
                    **(candidate_metrics if promoted else baseline_metrics),
                    "selected_training": "augmented" if promoted else "real_only",
                    "promotion_gate": {
                        "metric": "pr_auc",
                        "minimum_relative_lift": args.min_pr_auc_lift,
                        "observed_relative_lift": relative_lift,
                        "passed": promoted,
                    },
                    "augmented_candidate": candidate_metrics,
                    "real_only_baseline": baseline_metrics,
                }
        else:
            bundle, metrics = train(reviews)
        save_bundle(bundle, args.output)
        Path("reports/generated").mkdir(parents=True, exist_ok=True)
        Path("reports/generated/baseline_metrics.json").write_text(
            json.dumps(metrics, indent=2), encoding="utf-8"
        )
        print(json.dumps({"model": args.output, "metrics": metrics}, indent=2))
    elif args.command == "campaigns":
        reviews, report = load_review_events(args.data)
        if report.rejected:
            raise SystemExit(f"Refusing detection: {report.rejected} invalid records")
        print(json.dumps([alert.model_dump() for alert in detect_campaigns(reviews)], indent=2))
    elif args.command == "download-amazon":
        from .amazon import download_sample

        count = download_sample(args.output, args.category, args.limit)
        print(
            json.dumps(
                {"records": count, "category": args.category, "output": args.output}, indent=2
            )
        )
    elif args.command == "build-dataset-bundle":
        from .dataset_bundle import build_dataset_bundle

        print(
            json.dumps(
                build_dataset_bundle(
                    labeled_inputs=args.labeled_input,
                    behavioral_inputs=args.behavioral_input,
                    output_dir=args.output_dir,
                    product_catalog=args.product_catalog,
                    augmentation_count=args.augmentation_count,
                    campaign_scenario_count=args.campaign_scenario_count,
                    max_synthetic_fraction=args.max_synthetic_fraction,
                    seed=args.seed,
                ),
                indent=2,
            )
        )
    elif args.command == "build-temporal-bundle":
        from .dataset_bundle import build_temporal_bundle

        print(
            json.dumps(
                build_temporal_bundle(
                    behavioral_inputs=args.behavioral_input,
                    output_dir=args.output_dir,
                    product_catalog=args.product_catalog,
                    campaign_scenario_count=args.campaign_scenario_count,
                    seed=args.seed,
                ),
                indent=2,
            )
        )
    elif args.command == "generate-campaign-splits":
        from .temporal import generate_temporal_scenarios

        print(
            json.dumps(
                generate_temporal_scenarios(
                    args.profile,
                    args.products,
                    args.output_dir,
                    count=args.count,
                    seed=args.seed,
                ),
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
