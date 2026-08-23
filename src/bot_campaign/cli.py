from __future__ import annotations

import argparse
import json
from pathlib import Path

from .campaign import detect_campaigns
from .data import load_review_events, load_text_labels
from .model import save_bundle, train, train_with_holdout
import mlflow
import mlflow.sklearn
def _log_numeric_metrics(metrics: dict) -> None:
    """Log only scalar numeric metrics to MLflow."""
    for key, value in metrics.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            mlflow.log_metric(key, float(value))

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
    training.add_argument(
        "--mlflow-uri",
        default="http://localhost:5001",
        help="MLflow tracking server URI",
    )

    training.add_argument(
        "--mlflow-experiment",
        default="review-baseline-training",
        help="MLflow experiment name for TF-IDF Logistic Regression baselines",
    )
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
        # ------------------------------------------------------------
        # Configure MLflow
        # ------------------------------------------------------------
        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow.set_experiment(args.mlflow_experiment)

        # ------------------------------------------------------------
        # Load augmented/candidate training data
        # ------------------------------------------------------------
        reviews, report = load_text_labels(args.data)

        if report.rejected:
            raise SystemExit(
                f"Refusing to train: {report.rejected} invalid records"
            )

        # ------------------------------------------------------------
        # Parent MLflow run
        # ------------------------------------------------------------
        with mlflow.start_run(run_name="tfidf-logreg-baseline-comparison"):

            mlflow.set_tag("model_family", "logistic_regression")
            mlflow.set_tag("feature_family", "tfidf_plus_style")
            mlflow.set_tag("model_version", "tfidf-logreg-v1")

            mlflow.log_param("candidate_data", args.data)
            mlflow.log_param("test_data", args.test_data)
            mlflow.log_param("baseline_data", args.baseline_data)
            mlflow.log_param(
                "minimum_pr_auc_relative_lift",
                args.min_pr_auc_lift,
            )

            # Parameters defined by model.build_pipeline()
            mlflow.log_param("tfidf_max_features", 20_000)
            mlflow.log_param("tfidf_ngram_min", 1)
            mlflow.log_param("tfidf_ngram_max", 2)
            mlflow.log_param("logreg_max_iter", 1_000)
            mlflow.log_param("logreg_class_weight", "balanced")

            # --------------------------------------------------------
            # External real-only test set
            # --------------------------------------------------------
            if args.test_data:
                test_reviews, test_report = load_text_labels(args.test_data)

                if test_report.rejected:
                    raise SystemExit(
                        "Refusing evaluation: "
                        f"{test_report.rejected} invalid test records"
                    )

                if any(review.synthetic for review in test_reviews):
                    raise SystemExit(
                        "Refusing evaluation: --test-data must be real-only"
                    )

                # ====================================================
                # RUN 1: Augmented candidate Logistic Regression
                # ====================================================
                with mlflow.start_run(
                    run_name="tfidf-logreg-augmented",
                    nested=True,
                ):
                    candidate_bundle, candidate_metrics = train_with_holdout(
                        reviews,
                        test_reviews,
                    )

                    mlflow.set_tag("training_type", "augmented")
                    mlflow.set_tag("model_version", "tfidf-logreg-v1")

                    mlflow.log_param(
                        "training_records",
                        len(reviews),
                    )
                    mlflow.log_param(
                        "test_records",
                        len(test_reviews),
                    )

                    _log_numeric_metrics(candidate_metrics)

                    mlflow.sklearn.log_model(
                        candidate_bundle["pipeline"],
                        name="model",
                    )

                bundle = candidate_bundle
                metrics = candidate_metrics

                # ====================================================
                # RUN 2: Real-only Logistic Regression baseline
                # ====================================================
                if args.baseline_data:
                    baseline_reviews, baseline_report = load_text_labels(
                        args.baseline_data
                    )

                    if baseline_report.rejected:
                        raise SystemExit(
                            "Refusing baseline comparison: "
                            f"{baseline_report.rejected} invalid records"
                        )

                    with mlflow.start_run(
                        run_name="tfidf-logreg-real-only",
                        nested=True,
                    ):
                        baseline_bundle, baseline_metrics = train_with_holdout(
                            baseline_reviews,
                            test_reviews,
                        )

                        mlflow.set_tag("training_type", "real_only")
                        mlflow.set_tag(
                            "model_version",
                            "tfidf-logreg-v1",
                        )

                        mlflow.log_param(
                            "training_records",
                            len(baseline_reviews),
                        )
                        mlflow.log_param(
                            "test_records",
                            len(test_reviews),
                        )

                        _log_numeric_metrics(baseline_metrics)

                        mlflow.sklearn.log_model(
                            baseline_bundle["pipeline"],
                            name="model",
                        )

                    # ------------------------------------------------
                    # Promotion gate
                    # ------------------------------------------------
                    baseline_pr_auc = baseline_metrics["pr_auc"]

                    relative_lift = (
                        (
                            candidate_metrics["pr_auc"]
                            - baseline_pr_auc
                        )
                        / baseline_pr_auc
                        if baseline_pr_auc
                        else 0.0
                    )

                    promoted = (
                        relative_lift >= args.min_pr_auc_lift
                    )

                    if not promoted:
                        bundle = baseline_bundle

                    metrics = {
                        **(
                            candidate_metrics
                            if promoted
                            else baseline_metrics
                        ),
                        "selected_training": (
                            "augmented"
                            if promoted
                            else "real_only"
                        ),
                        "promotion_gate": {
                            "metric": "pr_auc",
                            "minimum_relative_lift": (
                                args.min_pr_auc_lift
                            ),
                            "observed_relative_lift": relative_lift,
                            "passed": promoted,
                        },
                        "augmented_candidate": candidate_metrics,
                        "real_only_baseline": baseline_metrics,
                    }

                    # Parent-run comparison metrics
                    mlflow.log_metric(
                        "candidate_pr_auc",
                        candidate_metrics["pr_auc"],
                    )
                    mlflow.log_metric(
                        "baseline_pr_auc",
                        baseline_metrics["pr_auc"],
                    )
                    mlflow.log_metric(
                        "candidate_roc_auc",
                        candidate_metrics["roc_auc"],
                    )
                    mlflow.log_metric(
                        "baseline_roc_auc",
                        baseline_metrics["roc_auc"],
                    )
                    mlflow.log_metric(
                        "pr_auc_relative_lift",
                        relative_lift,
                    )

                    mlflow.set_tag(
                        "selected_training",
                        "augmented"
                        if promoted
                        else "real_only",
                    )

                    mlflow.set_tag(
                        "promotion_gate_passed",
                        str(promoted),
                    )

            # ========================================================
            # No external test set: original internal holdout behavior
            # ========================================================
            else:
                with mlflow.start_run(
                    run_name="tfidf-logreg-internal-holdout",
                    nested=True,
                ):
                    bundle, metrics = train(reviews)

                    mlflow.set_tag(
                        "training_type",
                        "internal_holdout",
                    )

                    mlflow.log_param(
                        "training_source_records",
                        len(reviews),
                    )

                    _log_numeric_metrics(metrics)

                    mlflow.sklearn.log_model(
                        bundle["pipeline"],
                        name="model",
                    )

            # --------------------------------------------------------
            # Save selected/promoted model
            # --------------------------------------------------------
            save_bundle(bundle, args.output)

            Path("reports/generated").mkdir(
                parents=True,
                exist_ok=True,
            )

            metrics_path = Path(
                "reports/generated/baseline_metrics.json"
            )

            metrics_path.write_text(
                json.dumps(metrics, indent=2),
                encoding="utf-8",
            )

            # Log final report + selected artifact into parent run
            mlflow.log_artifact(str(metrics_path))

            mlflow.log_artifact(
                args.output,
                artifact_path="selected_model_bundle",
            )

            mlflow.set_tag(
                "selected_model_path",
                args.output,
            )

            print(
                json.dumps(
                    {
                        "model": args.output,
                        "metrics": metrics,
                    },
                    indent=2,
                )
            )
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
