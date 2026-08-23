from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
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


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _storage_path(value: str) -> str:
    return value if "://" in value else str(Path(value).resolve())


def _require_both_labels(name: str, records) -> None:
    labels = {record.label for record in records}
    if labels != {0, 1}:
        raise ValueError(f"{name} must contain both genuine (0) and deceptive (1) labels")


def _metrics(labels, probabilities, threshold: float = 0.5) -> dict[str, float]:
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        f1_score,
        log_loss,
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
        "brier": float(brier_score_loss(labels, probabilities)),
        "log_loss": float(log_loss(labels, probabilities, labels=[0, 1])),
    }


def _temperature(labels, probabilities) -> float:
    import numpy as np
    from sklearn.metrics import log_loss

    clipped = np.clip(np.asarray(probabilities, dtype=np.float64), 1e-7, 1 - 1e-7)
    logits = np.log(clipped / (1 - clipped))
    candidates = np.geomspace(0.35, 5.0, num=80)
    losses = [
        log_loss(labels, 1 / (1 + np.exp(-(logits / value))), labels=[0, 1])
        for value in candidates
    ]
    return float(candidates[int(np.argmin(losses))])


def _precision_threshold(labels, probabilities, minimum_precision: float) -> float:
    from sklearn.metrics import precision_recall_curve

    precision, recall, thresholds = precision_recall_curve(labels, probabilities)
    feasible = [
        (float(recall[index]), float(thresholds[index]))
        for index in range(len(thresholds))
        if precision[index] >= minimum_precision
    ]
    return max(feasible)[1] if feasible else 1.0


def _freeze_layers(model, count: int) -> None:
    base = getattr(model, "distilbert", None)
    if base is None:
        return
    if count > 0:
        for parameter in base.embeddings.parameters():
            parameter.requires_grad = False
    for layer in list(base.transformer.layer)[:count]:
        for parameter in layer.parameters():
            parameter.requires_grad = False


def _batch_tensors(tokenizer, records, max_tokens: int):
    torch = __import__("torch")
    encoded = tokenizer(
        [record.text for record in records],
        padding=True,
        truncation=True,
        max_length=max_tokens,
        return_tensors="pt",
    )
    return (
        encoded["input_ids"],
        encoded["attention_mask"],
        torch.tensor([record.label for record in records], dtype=torch.long),
    )


def _evaluate(model, tokenizer, records, max_tokens: int, batch_size: int):
    import numpy as np
    import torch

    device = next(model.parameters()).device
    model.eval()
    probabilities = []
    for start in range(0, len(records), batch_size):
        input_ids, attention_mask, _ = _batch_tensors(
            tokenizer, records[start : start + batch_size], max_tokens
        )
        with torch.inference_mode():
            logits = model(
                input_ids=input_ids.to(device), attention_mask=attention_mask.to(device)
            ).logits
        probabilities.append(torch.softmax(logits, dim=1)[:, 1].cpu().numpy())
    return (
        np.asarray([record.label for record in records], dtype=np.int64),
        np.concatenate(probabilities),
    )


def train_trial(config: dict) -> None:
    import numpy as np
    import torch
    from ray import tune
    from ray.train import get_checkpoint
    from ray.tune import Checkpoint

    from bot_campaign.review_transformer import (
        ReviewTransformerConfig,
        assert_text_splits_are_isolated,
        create_review_model,
        create_review_tokenizer,
        load_review_split,
        save_review_bundle,
    )

    torch.manual_seed(int(config["seed"]))
    np.random.seed(int(config["seed"]))
    training = load_review_split(config["train_data"], config.get("max_train_records"))
    validation = load_review_split(
        config["validation_data"], config.get("max_validation_records")
    )
    assert_text_splits_are_isolated({"train": training, "validation": validation})
    _require_both_labels("Training split", training)
    _require_both_labels("Validation split", validation)

    model_config = ReviewTransformerConfig(
        encoder_name=config["encoder_name"],
        max_tokens=int(config["max_tokens"]),
        dropout=float(config["dropout"]),
    )
    tokenizer = create_review_tokenizer(model_config)
    model = create_review_model(model_config)
    _freeze_layers(model, int(config["frozen_encoder_layers"]))
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    optimizer = torch.optim.AdamW(
        (parameter for parameter in model.parameters() if parameter.requires_grad),
        lr=float(config["learning_rate"]),
        weight_decay=float(config["weight_decay"]),
    )
    positives = sum(record.label for record in training)
    negatives = len(training) - positives
    class_weights = torch.tensor(
        [1.0, negatives / max(positives, 1) * float(config["positive_weight_multiplier"])],
        dtype=torch.float32,
        device=device,
    )
    loss_function = torch.nn.CrossEntropyLoss(
        weight=class_weights, label_smoothing=float(config["label_smoothing"])
    )
    batch_size = int(config["batch_size"])
    steps_per_epoch = max(1, (len(training) + batch_size - 1) // batch_size)
    total_steps = steps_per_epoch * int(config["epochs"])
    warmup_steps = int(total_steps * float(config["warmup_ratio"]))

    def learning_rate_factor(step: int) -> float:
        if warmup_steps and step < warmup_steps:
            return max(step, 1) / warmup_steps
        return max(total_steps - step, 0) / max(total_steps - warmup_steps, 1)

    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, learning_rate_factor)
    generator = torch.Generator().manual_seed(int(config["seed"]))
    start_epoch = 0
    checkpoint = get_checkpoint()
    if checkpoint is not None:
        with checkpoint.as_directory() as checkpoint_dir:
            state_path = Path(checkpoint_dir) / "training_state.pt"
            if state_path.exists():
                state = torch.load(state_path, map_location=device, weights_only=False)
                model.load_state_dict(state["model"])
                optimizer.load_state_dict(state["optimizer"])
                scheduler.load_state_dict(state["scheduler"])
                generator.set_state(state["generator_state"])
                for optimizer_state in optimizer.state.values():
                    for key, value in optimizer_state.items():
                        if torch.is_tensor(value):
                            optimizer_state[key] = value.to(device)
                start_epoch = int(state["epoch"])

    for epoch in range(start_epoch, int(config["epochs"])):
        model.train()
        order = torch.randperm(len(training), generator=generator).tolist()
        losses = []
        for start in range(0, len(order), batch_size):
            batch = [training[index] for index in order[start : start + batch_size]]
            input_ids, attention_mask, labels = _batch_tensors(
                tokenizer, batch, model_config.max_tokens
            )
            optimizer.zero_grad(set_to_none=True)
            logits = model(
                input_ids=input_ids.to(device), attention_mask=attention_mask.to(device)
            ).logits
            loss = loss_function(logits, labels.to(device))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(
                model.parameters(), float(config["gradient_clip_norm"])
            )
            optimizer.step()
            scheduler.step()
            losses.append(float(loss.detach().cpu()))

        labels, probabilities = _evaluate(
            model, tokenizer, validation, model_config.max_tokens, batch_size
        )
        metrics = _metrics(labels, probabilities)
        metrics.update(epoch=epoch + 1, train_loss=float(np.mean(losses)))
        checkpoint_dir = Path(tempfile.mkdtemp(prefix="review-distilbert-"))
        save_review_bundle(
            checkpoint_dir,
            model,
            tokenizer,
            model_config,
            {"trial": tune.get_context().get_trial_name(), "epoch": epoch + 1},
        )
        torch.save(
            {
                "epoch": epoch + 1,
                "model": model.state_dict(),
                "optimizer": optimizer.state_dict(),
                "scheduler": scheduler.state_dict(),
                "generator_state": generator.get_state(),
            },
            checkpoint_dir / "training_state.pt",
        )
        # ASHA may stop a weak trial after any report, so every report must be resumable.
        tune.report(metrics, checkpoint=Checkpoint.from_directory(checkpoint_dir))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tune the promoted DistilBERT individual-review model with Ray and MLflow"
    )
    parser.add_argument("--train-data", default="data/processed/dataset_bundle/text/training.jsonl")
    parser.add_argument(
        "--validation-data", default="data/processed/dataset_bundle/text/real/validation.jsonl"
    )
    parser.add_argument("--test-data", default="data/processed/dataset_bundle/text/real/test.jsonl")
    parser.add_argument("--output", default="artifacts/review_distilbert")
    parser.add_argument("--encoder", default="distilbert-base-uncased")
    parser.add_argument("--ray-address", default=os.getenv("RAY_ADDRESS") or None)
    parser.add_argument("--mlflow-uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    parser.add_argument("--experiment", default="review-risk-distilbert")
    parser.add_argument("--registered-model", default="review-risk-distilbert")
    parser.add_argument(
        "--ray-storage-path",
        default=os.getenv("RAY_STORAGE_PATH", "artifacts/ray_results"),
        help="Shared filesystem or cloud URI visible to every Ray node",
    )
    parser.add_argument("--num-samples", type=int, default=12)
    # Maximum epochs: ASHA may stop weak trials early before this limit.
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--cpus-per-trial", type=float, default=4)
    parser.add_argument("--gpus-per-trial", type=float, default=1)
    parser.add_argument(
        "--max-concurrent-trials",
        type=int,
        default=int(os.getenv("RAY_MAX_CONCURRENT_TRIALS", "1")),
        help="Maximum simultaneous Tune trials; keep at 1 on memory-constrained Docker Desktop",
    )
    parser.add_argument("--minimum-precision", type=float, default=0.90)
    parser.add_argument("--max-train-records", type=int)
    parser.add_argument("--max-validation-records", type=int)
    parser.add_argument("--max-test-records", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume the existing Ray Tune experiment from --ray-storage-path",
    )
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

    from bot_campaign.review_transformer import (
        ReviewDistilBertScorer,
        ReviewPyFuncModel,
        assert_text_splits_are_isolated,
        load_review_split,
    )

    train_limit = 128 if args.smoke else args.max_train_records
    validation_limit = 64 if args.smoke else args.max_validation_records
    test_limit = 64 if args.smoke else args.max_test_records
    splits = {
        "train": load_review_split(args.train_data, train_limit),
        "validation": load_review_split(args.validation_data, validation_limit),
        "test": load_review_split(args.test_data, test_limit),
    }
    assert_text_splits_are_isolated(splits)
    for split_name, records in splits.items():
        _require_both_labels(f"{split_name.title()} split", records)
    ray.init(address=args.ray_address, ignore_reinit_error=True)
    try:
        try:
            from ray.air.integrations.mlflow import MLflowLoggerCallback
        except ImportError:
            from ray.tune.logger.mlflow import MLflowLoggerCallback

        trainable = tune.with_resources(
            train_trial,
            resources={
                "cpu": 2 if args.smoke else args.cpus_per_trial,
                "gpu": 0 if args.smoke else args.gpus_per_trial,
            },
        )
        epochs = 1 if args.smoke else args.epochs
        param_space = {
                "train_data": str(Path(args.train_data).resolve()),
                "validation_data": str(Path(args.validation_data).resolve()),
                "max_train_records": train_limit,
                "max_validation_records": validation_limit,
                "encoder_name": args.encoder,
                "epochs": epochs,
                "seed": args.seed,
                "learning_rate": tune.loguniform(5e-6, 5e-5),
                "weight_decay": tune.choice([0.0, 0.01, 0.05]),
                "dropout": tune.uniform(0.1, 0.4),
                "batch_size": tune.choice([4, 8]) if args.smoke else tune.choice([8, 16, 32]),
                "frozen_encoder_layers": tune.choice([4, 6]) if args.smoke else tune.choice([0, 2, 4]),
                "max_tokens": 64 if args.smoke else tune.choice([128, 192, 256]),
                "warmup_ratio": tune.choice([0.0, 0.05, 0.1]),
                "gradient_clip_norm": tune.choice([0.5, 1.0, 2.0]),
                "positive_weight_multiplier": tune.choice([0.75, 1.0, 1.25]),
                "label_smoothing": tune.choice([0.0, 0.05, 0.1]),
            }
        tune_config = tune.TuneConfig(
                metric="pr_auc",
                mode="max",
                num_samples=1 if args.smoke else args.num_samples,
                max_concurrent_trials=1 if args.smoke else args.max_concurrent_trials,
                scheduler=ASHAScheduler(
                    max_t=epochs, grace_period=1, reduction_factor=2
                ),
            )
        run_config = RunConfig(
                name=args.experiment,
                storage_path=_storage_path(args.ray_storage_path),
                callbacks=[
                    MLflowLoggerCallback(
                        tracking_uri=args.mlflow_uri,
                        experiment_name=args.experiment,
                        tags={"git_sha": _git_sha(), "model_role": "individual-review-risk"},
                    )
                ],
            )
        if args.resume:
            restore_path = Path(_storage_path(args.ray_storage_path)) / args.experiment
            if not tune.Tuner.can_restore(str(restore_path)):
                raise SystemExit(
                    f"No restorable Ray experiment found at {restore_path}. "
                    "Run without --resume to start a new experiment."
                )
            print(f"Resuming Ray Tune experiment from {restore_path}")
            tuner = tune.Tuner.restore(
                str(restore_path),
                trainable=trainable,
                resume_unfinished=True,
                resume_errored=True,
            )
        else:
            tuner = tune.Tuner(
                trainable,
                param_space=param_space,
                tune_config=tune_config,
                run_config=run_config,
            )
        results = tuner.fit()
        best = results.get_best_result(metric="pr_auc", mode="max")
        if best.checkpoint is None:
            raise RuntimeError("The selected review-model trial has no checkpoint")
        output = Path(args.output)
        if output.exists():
            shutil.rmtree(output)
        with best.checkpoint.as_directory() as checkpoint:
            shutil.copytree(checkpoint, output)

        scorer = ReviewDistilBertScorer.load(output)
        validation_labels = np.asarray([record.label for record in splits["validation"]])
        validation_raw_probabilities = scorer.raw_probabilities(
            [record.text for record in splits["validation"]]
        )
        temperature = _temperature(validation_labels, validation_raw_probabilities)
        scorer.config = replace(scorer.config, temperature=temperature)
        validation_probabilities = scorer.calibrate(validation_raw_probabilities)
        threshold = _precision_threshold(
            validation_labels, validation_probabilities, args.minimum_precision
        )
        metadata_path = output / "bundle.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["model_config"]["threshold"] = threshold
        metadata["model_config"]["temperature"] = temperature
        metadata["lineage"] = {
            "git_sha": _git_sha(),
            "train_data": args.train_data,
            "train_sha256": _file_sha256(args.train_data),
            "validation_data": args.validation_data,
            "validation_sha256": _file_sha256(args.validation_data),
            "test_data": args.test_data,
            "test_sha256": _file_sha256(args.test_data),
            "split_guard": "normalized-text SHA-256 sets are pairwise disjoint",
        }
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        scorer.config = replace(scorer.config, threshold=threshold)
        scorer.bundle["threshold"] = threshold
        test_labels = np.asarray([record.label for record in splits["test"]])
        test_probabilities = scorer.probabilities([record.text for record in splits["test"]])
        test_metrics = _metrics(test_labels, test_probabilities, threshold)

        mlflow.set_tracking_uri(args.mlflow_uri)
        mlflow.set_experiment(args.experiment)
        with mlflow.start_run(run_name="selected-review-distilbert") as run:
            mlflow.log_params({key: value for key, value in best.config.items() if value is not None})
            mlflow.log_param("decision_threshold", threshold)
            mlflow.log_param("calibration_temperature", temperature)
            mlflow.log_param("ray_max_concurrent_trials", args.max_concurrent_trials)
            mlflow.log_metrics({f"test_{key}": value for key, value in test_metrics.items()})
            mlflow.set_tags({"git_sha": _git_sha(), "baseline_replaced": "tfidf-logreg-v1"})
            import pandas as pd

            input_example = pd.DataFrame({"text": [splits["test"][0].text]})
            example_probability = float(test_probabilities[0])
            example_needs_review = example_probability >= threshold
            output_example = pd.DataFrame(
                {
                    "fake_probability": [example_probability],
                    "label": ["needs review" if example_needs_review else "normal"],
                    "needs_review": [example_needs_review],
                }
            )
            signature = mlflow.models.infer_signature(input_example, output_example)
            serving_requirements = [
                f"{package}=={importlib.metadata.version(package)}"
                for package in ("mlflow", "numpy", "pandas", "torch", "transformers")
            ]
            model_info = mlflow.pyfunc.log_model(
                name="review_model",
                python_model=ReviewPyFuncModel(),
                artifacts={"bundle": str(output)},
                code_paths=[str(Path(__file__).resolve().parents[1] / "src")],
                registered_model_name=args.registered_model,
                input_example=input_example,
                signature=signature,
                pip_requirements=serving_requirements,
            )
            run_id = run.info.run_id
        print(
            json.dumps(
                {
                    "artifact": str(output.resolve()),
                    "mlflow_run_id": run_id,
                    "model_uri": model_info.model_uri,
                    "threshold": threshold,
                    "calibration_temperature": temperature,
                    "test_metrics": test_metrics,
                },
                indent=2,
            )
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
