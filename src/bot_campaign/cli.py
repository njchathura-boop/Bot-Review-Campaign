from __future__ import annotations

import argparse
import json
from pathlib import Path

from .campaign import detect_campaigns
from .data import load_reviews
from .model import save_bundle, train


def main() -> None:
    parser = argparse.ArgumentParser(prog="bot-campaign")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="Validate and summarize review data")
    validate.add_argument("path")
    validate.add_argument("--labeled", action="store_true")
    training = commands.add_parser("train", help="Train the supervised baseline")
    training.add_argument("--data", default="data/sample/labeled_reviews.jsonl")
    training.add_argument("--output", default="artifacts/review_model.joblib")
    campaigns = commands.add_parser("campaigns", help="Detect suspicious coordination")
    campaigns.add_argument("--data", default="data/sample/campaign_reviews.jsonl")
    amazon = commands.add_parser("download-amazon", help="Stream a bounded Amazon Reviews 2023 sample")
    amazon.add_argument("--category", default="All_Beauty")
    amazon.add_argument("--limit", type=int, default=10_000)
    amazon.add_argument("--output", default="data/raw/amazon_all_beauty.jsonl")
    maide = commands.add_parser("download-maide", help="Normalize the labeled MAiDE-up dataset")
    maide.add_argument("--limit", type=int)
    maide.add_argument("--output", default="data/processed/maide_labeled.jsonl")
    ott = commands.add_parser("prepare-ott", help="Normalize an extracted Ott op_spam_v1.4 directory")
    ott.add_argument("--input", required=True)
    ott.add_argument("--output", default="data/processed/ott_labeled.jsonl")
    merge = commands.add_parser("merge-labeled", help="Deduplicate canonical labeled datasets")
    merge.add_argument("inputs", nargs="+")
    merge.add_argument("--output", default="data/processed/training_labeled.jsonl")
    training_set = commands.add_parser(
        "build-training-set",
        help="Combine real labels with a deterministic, capped synthetic subset",
    )
    training_set.add_argument("--real", nargs="+", required=True)
    training_set.add_argument("--synthetic", required=True)
    training_set.add_argument(
        "--output", default="data/processed/training_labeled.jsonl"
    )
    training_set.add_argument("--max-synthetic-fraction", type=float, default=0.25)
    training_set.add_argument("--seed", type=int, default=42)
    synthetic = commands.add_parser(
        "generate-synthetic", help="Create reproducible labeled campaign scenarios"
    )
    synthetic.add_argument("--preset", choices=("sample", "full"), default="sample")
    synthetic.add_argument("--output")
    synthetic.add_argument("--seed", type=int, default=42)
    e2e = commands.add_parser(
        "e2e-demo", help="Generate, validate, train, and evaluate a small local workflow"
    )
    e2e.add_argument("--output-dir", default="artifacts/e2e")
    e2e.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.command == "validate":
        _, report = load_reviews(args.path, labeled=args.labeled)
        print(json.dumps(report.__dict__, indent=2))
    elif args.command == "train":
        reviews, report = load_reviews(args.data, labeled=True)
        if report.rejected:
            raise SystemExit(f"Refusing to train: {report.rejected} invalid records")
        bundle, metrics = train(reviews)
        save_bundle(bundle, args.output)
        Path("reports/generated").mkdir(parents=True, exist_ok=True)
        Path("reports/generated/baseline_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        print(json.dumps({"model": args.output, "metrics": metrics}, indent=2))
    elif args.command == "campaigns":
        reviews, report = load_reviews(args.data)
        if report.rejected:
            raise SystemExit(f"Refusing detection: {report.rejected} invalid records")
        print(json.dumps([alert.model_dump() for alert in detect_campaigns(reviews)], indent=2))
    elif args.command == "download-amazon":
        from .amazon import download_sample

        count = download_sample(args.output, args.category, args.limit)
        print(json.dumps({"records": count, "category": args.category, "output": args.output}, indent=2))
    elif args.command == "download-maide":
        from .labeled_datasets import download_maide

        count = download_maide(args.output, args.limit)
        print(json.dumps({"records": count, "output": args.output}, indent=2))
    elif args.command == "prepare-ott":
        from .labeled_datasets import prepare_ott

        count = prepare_ott(args.input, args.output)
        print(json.dumps({"records": count, "output": args.output}, indent=2))
    elif args.command == "merge-labeled":
        from .labeled_datasets import merge_labeled

        print(json.dumps(merge_labeled(args.inputs, args.output), indent=2))
    elif args.command == "build-training-set":
        from .labeled_datasets import build_training_set

        print(
            json.dumps(
                build_training_set(
                    args.real,
                    args.synthetic,
                    args.output,
                    args.max_synthetic_fraction,
                    args.seed,
                ),
                indent=2,
            )
        )
    elif args.command == "generate-synthetic":
        from .synthetic import generate_dataset

        output = args.output or (
            "data/processed/synthetic_sample.jsonl"
            if args.preset == "sample"
            else "data/processed/synthetic_campaigns_50k.jsonl"
        )
        print(
            json.dumps(
                generate_dataset(output, args.seed, preset=args.preset), indent=2
            )
        )
    elif args.command == "e2e-demo":
        from .workflow import run_e2e_demo

        print(json.dumps(run_e2e_demo(args.output_dir, args.seed), indent=2))



if __name__ == "__main__":
    main()
