from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(Path(__file__).resolve().parents[1]), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _storage_path(value: str) -> str:
    return value if "://" in value else str(Path(value).resolve())


def _metrics(labels, probabilities, threshold: float = 0.5) -> dict[str, float]:
    from sklearn.metrics import (
        average_precision_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    predicted = (probabilities >= threshold).astype(int)
    return {
        "pr_auc": float(average_precision_score(labels, probabilities)),
        "roc_auc": float(roc_auc_score(labels, probabilities)),
        "f1": float(f1_score(labels, predicted, zero_division=0)),
        "precision": float(precision_score(labels, predicted, zero_division=0)),
        "recall": float(recall_score(labels, predicted, zero_division=0)),
    }


def _precision_threshold(labels, probabilities, minimum_precision: float) -> float:
    import numpy as np
    from sklearn.metrics import precision_recall_curve

    precision, recall, thresholds = precision_recall_curve(labels, probabilities)
    feasible = [
        (float(recall[index]), float(thresholds[index]))
        for index in range(len(thresholds))
        if precision[index] >= minimum_precision
    ]
    return max(feasible)[1] if feasible else float(np.nextafter(1.0, 2.0))


def _freeze_encoder_layers(model, frozen_layers: int) -> None:
    if frozen_layers <= 0:
        return
    embeddings = getattr(model.encoder, "embeddings", None)
    if embeddings is not None:
        for parameter in embeddings.parameters():
            parameter.requires_grad = False
    layers = getattr(getattr(model.encoder, "transformer", None), "layer", [])
    for layer in list(layers)[:frozen_layers]:
        for parameter in layer.parameters():
            parameter.requires_grad = False


def _evaluate(model, tokenizer, groups, normalizer, model_config, batch_size, device):
    import numpy as np

    from bot_campaign.hybrid_model import HybridCampaignScorer

    scorer = HybridCampaignScorer(model, tokenizer, model_config, normalizer)
    chunks = []
    for start in range(0, len(groups), batch_size):
        chunks.append(scorer.predict(groups[start : start + batch_size]))
    probabilities = np.concatenate(chunks)
    labels = np.asarray([int(group.label) for group in groups], dtype=np.int64)
    return labels, probabilities


def train_trial(config: dict) -> None:
    import numpy as np
    import torch
    from ray import tune
    from ray.tune import Checkpoint

    from bot_campaign.campaign_features import load_campaign_groups
    from bot_campaign.hybrid_model import (
        HybridModelConfig,
        NumericNormalizer,
        create_model,
        create_tokenizer,
        encode_groups,
        save_hybrid_bundle,
    )

    torch.manual_seed(int(config["seed"]))
    np.random.seed(int(config["seed"]))
    train_groups = load_campaign_groups(config["train_data"], config.get("max_train_groups"))
    validation_groups = load_campaign_groups(
        config["validation_data"], config.get("max_validation_groups")
    )
    if {group.label for group in train_groups} != {0, 1}:
        raise ValueError("Training groups must contain both normal and campaign labels")
    normalizer = NumericNormalizer.fit(train_groups)
    model_config = HybridModelConfig(
        encoder_name=config["encoder_name"],
        max_reviews=int(config["max_reviews"]),
        max_tokens=int(config["max_tokens"]),
        numeric_hidden_size=int(config["numeric_hidden_size"]),
        fusion_hidden_size=int(config["fusion_hidden_size"]),
        dropout=float(config["dropout"]),
    )
    tokenizer = create_tokenizer(model_config)
    model = create_model(model_config)
    _freeze_encoder_layers(model, int(config["frozen_encoder_layers"]))
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    encoder_parameters = [
        parameter for parameter in model.encoder.parameters() if parameter.requires_grad
    ]
    head_parameters = [
        parameter
        for name, parameter in model.named_parameters()
        if parameter.requires_grad and not name.startswith("encoder.")
    ]
    optimizer = torch.optim.AdamW(
        [
            {"params": encoder_parameters, "lr": float(config["encoder_learning_rate"])},
            {"params": head_parameters, "lr": float(config["head_learning_rate"])},
        ],
        weight_decay=float(config["weight_decay"]),
    )
    positive = sum(int(group.label) for group in train_groups)
    negative = len(train_groups) - positive
    loss_function = torch.nn.BCEWithLogitsLoss(
        pos_weight=torch.tensor(
            [negative / max(positive, 1) * float(config["positive_weight_multiplier"])],
            device=device,
        )
    )
    generator = torch.Generator().manual_seed(int(config["seed"]))
    batch_size = int(config["batch_size"])
    steps_per_epoch = max(1, (len(train_groups) + batch_size - 1) // batch_size)
    total_steps = steps_per_epoch * int(config["epochs"])
    warmup_steps = int(total_steps * float(config["warmup_ratio"]))

    def learning_rate_factor(step: int) -> float:
        if warmup_steps and step < warmup_steps:
            return max(step, 1) / warmup_steps
        remaining = max(total_steps - step, 0)
        return remaining / max(total_steps - warmup_steps, 1)

    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, learning_rate_factor)

    for epoch in range(int(config["epochs"])):
        model.train()
        permutation = torch.randperm(len(train_groups), generator=generator).tolist()
        losses = []
        for start in range(0, len(permutation), batch_size):
            batch = [train_groups[index] for index in permutation[start : start + batch_size]]
            input_ids, attention_mask, labels = encode_groups(tokenizer, batch, model_config)
            numeric = torch.tensor(
                normalizer.transform([group.numeric_features for group in batch]),
                dtype=torch.float32,
            )
            optimizer.zero_grad(set_to_none=True)
            logits = model(
                input_ids.to(device), attention_mask.to(device), numeric.to(device)
            )
            loss = loss_function(logits, labels.to(device))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(
                model.parameters(), max_norm=float(config["gradient_clip_norm"])
            )
            optimizer.step()
            scheduler.step()
            losses.append(float(loss.detach().cpu()))

        labels, probabilities = _evaluate(
            model, tokenizer, validation_groups, normalizer, model_config, batch_size, device
        )
        metrics = _metrics(labels, probabilities)
        metrics.update(epoch=epoch + 1, train_loss=float(np.mean(losses)))
        checkpoint_dir = Path(tempfile.mkdtemp(prefix="campaign-model-"))
        save_hybrid_bundle(
            checkpoint_dir,
            model,
            tokenizer,
            model_config,
            normalizer,
            {"trial": tune.get_context().get_trial_name(), "epoch": epoch + 1},
        )
        # ASHA may stop a weak trial after any report, so every report must be usable.
        tune.report(metrics, checkpoint=Checkpoint.from_directory(checkpoint_dir))


def _validate_splits(
    train_path: str,
    validation_path: str,
    test_path: str,
    max_train_groups: int | None,
    max_validation_groups: int | None,
    max_test_groups: int | None,
):
    from bot_campaign.campaign_features import load_campaign_groups

    splits = {
        "train": load_campaign_groups(train_path, max_train_groups),
        "validation": load_campaign_groups(validation_path, max_validation_groups),
        "test": load_campaign_groups(test_path, max_test_groups),
    }
    group_sets = {name: {group.group_id for group in groups} for name, groups in splits.items()}
    if group_sets["train"] & group_sets["validation"] or group_sets["train"] & group_sets["test"]:
        raise ValueError("Campaign groups cross dataset splits")
    if group_sets["validation"] & group_sets["test"]:
        raise ValueError("Campaign groups cross dataset splits")
    for name, groups in splits.items():
        if {group.label for group in groups} != {0, 1}:
            raise ValueError(f"{name} split must contain both labels")
    return {name: len(groups) for name, groups in splits.items()}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tune a hybrid DistilBERT + temporal campaign model with Ray and MLflow"
    )
    parser.add_argument(
        "--train-data", default="data/processed/temporal_bundle/campaign_v3/train.jsonl"
    )
    parser.add_argument(
        "--validation-data",
        default="data/processed/temporal_bundle/campaign_v3/validation.jsonl",
    )
    parser.add_argument(
        "--test-data", default="data/processed/temporal_bundle/campaign_v3/test.jsonl"
    )
    parser.add_argument("--output", default="artifacts/campaign_model")
    parser.add_argument("--encoder", default="distilbert-base-uncased")
    parser.add_argument("--ray-address", default=os.getenv("RAY_ADDRESS") or None)
    parser.add_argument("--mlflow-uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    parser.add_argument("--experiment", default="bot-campaign-hybrid-distilbert")
    parser.add_argument("--registered-model", default="bot-campaign-hybrid-distilbert")
    parser.add_argument(
        "--ray-storage-path",
        default=os.getenv("RAY_STORAGE_PATH", "artifacts/ray_results"),
        help="Shared filesystem or cloud URI visible to every Ray node",
    )
    parser.add_argument("--num-samples", type=int, default=12)
    # Maximum epochs: ASHA may stop weak trials early before this limit.
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--cpus-per-trial", type=float, default=4)
    parser.add_argument("--gpus-per-trial", type=float, default=1)
    parser.add_argument(
        "--max-concurrent-trials",
        type=int,
        default=int(os.getenv("RAY_MAX_CONCURRENT_TRIALS", "1")),
        help="Maximum simultaneous Tune trials; keep at 1 on memory-constrained Docker Desktop",
    )
    parser.add_argument("--minimum-precision", type=float, default=0.95)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-train-groups", type=int)
    parser.add_argument("--max-validation-groups", type=int)
    parser.add_argument("--max-test-groups", type=int)
    parser.add_argument("--smoke", action="store_true", help="Small CPU run that validates the full path")
    args = parser.parse_args()
    if args.max_concurrent_trials < 1:
        parser.error("--max-concurrent-trials must be at least 1")

    if sys.platform == "win32" and sys.version_info >= (3, 13):
        raise SystemExit(
            "Ray has no official Windows wheel for this Python version. "
            "Install Python 3.11, create .venv with `py -3.11 -m venv .venv`, "
            "then install `.[dev,campaign-training,streaming,ui]`."
        )

    import mlflow
    import numpy as np
    try:
        import ray
    except ModuleNotFoundError as exc:
        raise SystemExit(
            'Ray is not installed in this interpreter. Run: python -m pip install -e '
            '".[campaign-training]"'
        ) from exc
    from ray import tune
    from ray.tune import RunConfig
    from ray.tune.schedulers import ASHAScheduler

    from bot_campaign.campaign_features import CAMPAIGN_FEATURE_VERSION, load_campaign_groups
    from bot_campaign.hybrid_model import CampaignPyFuncModel, HybridCampaignScorer

    max_train_groups = 80 if args.smoke else args.max_train_groups
    max_validation_groups = 80 if args.smoke else args.max_validation_groups
    max_test_groups = 80 if args.smoke else args.max_test_groups
    split_counts = _validate_splits(
        args.train_data,
        args.validation_data,
        args.test_data,
        max_train_groups,
        max_validation_groups,
        max_test_groups,
    )
    ray.init(address=args.ray_address, ignore_reinit_error=True)
    try:
        try:
            from ray.air.integrations.mlflow import MLflowLoggerCallback
        except ImportError:
            from ray.tune.logger.mlflow import MLflowLoggerCallback

        common = {
            "train_data": str(Path(args.train_data).resolve()),
            "validation_data": str(Path(args.validation_data).resolve()),
            "encoder_name": args.encoder,
            "epochs": 1 if args.smoke else args.epochs,
            "max_train_groups": max_train_groups,
            "max_validation_groups": max_validation_groups,
            "seed": args.seed,
        }
        trainable = tune.with_resources(
            train_trial,
            resources={
                "cpu": 2 if args.smoke else args.cpus_per_trial,
                "gpu": 0 if args.smoke else args.gpus_per_trial,
            },
        )
        tuner = tune.Tuner(
            trainable,
            param_space={
                **common,
                "encoder_learning_rate": tune.loguniform(5e-6, 5e-5),
                "head_learning_rate": tune.loguniform(5e-5, 1e-3),
                "weight_decay": tune.choice([0.0, 0.01, 0.05]),
                "dropout": tune.uniform(0.1, 0.4),
                "numeric_hidden_size": tune.choice([32, 64, 128, 256]),
                "fusion_hidden_size": tune.choice([64, 128, 256]),
                "batch_size": tune.choice([4, 8]) if args.smoke else tune.choice([8, 16]),
                "frozen_encoder_layers": tune.choice([4, 5, 6]) if args.smoke else tune.choice([0, 2, 4]),
                "max_reviews": 6 if args.smoke else tune.choice([6, 8, 10]),
                "max_tokens": 64 if args.smoke else tune.choice([96, 128, 192]),
                "warmup_ratio": tune.choice([0.0, 0.05, 0.1]),
                "gradient_clip_norm": tune.choice([0.5, 1.0, 2.0]),
                "positive_weight_multiplier": tune.choice([0.75, 1.0, 1.25]),
            },
            tune_config=tune.TuneConfig(
                metric="pr_auc",
                mode="max",
                num_samples=1 if args.smoke else args.num_samples,
                max_concurrent_trials=1 if args.smoke else args.max_concurrent_trials,
                scheduler=ASHAScheduler(
                    max_t=1 if args.smoke else args.epochs,
                    grace_period=1,
                    reduction_factor=2,
                ),
            ),
            run_config=RunConfig(
                name=args.experiment,
                storage_path=_storage_path(args.ray_storage_path),
                callbacks=[
                    MLflowLoggerCallback(
                        tracking_uri=args.mlflow_uri,
                        experiment_name=args.experiment,
                        tags={"git_sha": _git_sha(), "feature_version": CAMPAIGN_FEATURE_VERSION},
                    )
                ],
            ),
        )
        results = tuner.fit()
        best = results.get_best_result(metric="pr_auc", mode="max")
        if best.checkpoint is None:
            raise RuntimeError("The selected Ray trial did not produce a model checkpoint")
        output = Path(args.output)
        if output.exists():
            shutil.rmtree(output)
        with best.checkpoint.as_directory() as checkpoint_dir:
            shutil.copytree(checkpoint_dir, output)

        scorer = HybridCampaignScorer.load(output)
        validation = load_campaign_groups(args.validation_data, max_validation_groups)
        validation_probabilities = scorer.predict(validation)
        validation_labels = np.asarray([int(group.label) for group in validation])
        threshold = _precision_threshold(
            validation_labels, validation_probabilities, args.minimum_precision
        )
        test = load_campaign_groups(args.test_data, max_test_groups)
        test_probabilities = scorer.predict(test)
        test_labels = np.asarray([int(group.label) for group in test])
        test_metrics = _metrics(test_labels, test_probabilities, threshold)
        metadata_path = output / "bundle.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["model_config"]["threshold"] = threshold
        metadata["lineage"] = {
            "git_sha": _git_sha(),
            "feature_version": CAMPAIGN_FEATURE_VERSION,
            "train_data": str(args.train_data),
            "validation_data": str(args.validation_data),
            "test_data": str(args.test_data),
            "ray_trial_id": best.metrics.get("trial_id", "unknown"),
        }
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow.set_experiment(args.experiment)
        with mlflow.start_run(run_name="selected-hybrid-campaign-model") as run:
            mlflow.log_params({key: value for key, value in best.config.items() if value is not None})
            mlflow.log_params(
                {
                    "decision_threshold": threshold,
                    "ray_max_concurrent_trials": args.max_concurrent_trials,
                    **split_counts,
                }
            )
            mlflow.log_metrics({f"test_{key}": value for key, value in test_metrics.items()})
            mlflow.set_tags(
                {
                    "git_sha": _git_sha(),
                    "feature_version": CAMPAIGN_FEATURE_VERSION,
                    "model_role": "campaign-risk-not-individual-review-proof",
                }
            )
            mlflow.log_artifacts(str(output), artifact_path="hybrid_bundle")
            import pandas as pd
            from mlflow.models import infer_signature

            candidate = test[0].as_candidate()
            input_example = pd.DataFrame(
                [
                    {
                        **{
                            key: candidate[key]
                            for key in (
                                "schema_version",
                                "feature_version",
                                "group_id",
                                "window_start",
                                "window_end",
                            )
                        },
                        **{
                            key: json.dumps(candidate[key])
                            for key in ("texts", "numeric_features", "review_ids", "product_ids")
                        },
                    }
                ]
            )
            output_example = pd.DataFrame(
                {
                    "campaign_risk": [float(test_probabilities[0])],
                    "candidate": [bool(test_probabilities[0] >= threshold)],
                }
            )
            model_info = mlflow.pyfunc.log_model(
                name="campaign_model",
                python_model=CampaignPyFuncModel(),
                artifacts={"bundle": str(output)},
                code_paths=["src"],
                input_example=input_example,
                signature=infer_signature(input_example, output_example),
                registered_model_name=args.registered_model,
                pip_requirements=[
                    "mlflow>=3,<4",
                    "numpy>=1.26,<3",
                    "pandas>=2.2,<3",
                    "torch>=2.4,<3",
                    "transformers>=4.46,<6",
                ],
            )
            run_id = run.info.run_id
        print(
            json.dumps(
                {
                    "artifact": str(output.resolve()),
                    "mlflow_run_id": run_id,
                    "registered_model": args.registered_model,
                    "model_uri": model_info.model_uri,
                    "threshold": threshold,
                    "test_metrics": test_metrics,
                    "note": "Promote the registered version only after acceptance gates pass.",
                },
                indent=2,
            )
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
