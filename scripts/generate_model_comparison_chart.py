"""Generate the report figure comparing the review baseline and promoted model."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


METRICS = ["Precision", "Recall", "F1", "ROC-AUC", "PR-AUC"]
BASELINE = [0.8356, 0.7514, 0.7913, 0.8778, 0.9016]
DISTILBERT = [0.9141, 0.7742, 0.8383, 0.9264, 0.9405]


def main() -> None:
    output = Path("reports/diagrams/review_model_metric_comparison.png")
    output.parent.mkdir(parents=True, exist_ok=True)

    positions = np.arange(len(METRICS))
    width = 0.34
    figure, axis = plt.subplots(figsize=(11.5, 6.4), dpi=180)
    figure.patch.set_facecolor("white")
    axis.set_facecolor("#f7f8fa")

    baseline_bars = axis.bar(
        positions - width / 2,
        BASELINE,
        width,
        label="TF-IDF + Logistic Regression",
        color="#8a9aab",
        edgecolor="white",
        linewidth=0.8,
    )
    model_bars = axis.bar(
        positions + width / 2,
        DISTILBERT,
        width,
        label="Promoted DistilBERT review-risk model",
        color="#176b5b",
        edgecolor="white",
        linewidth=0.8,
    )

    axis.set_title(
        "Review-risk model performance on the held-out real test set",
        fontsize=16,
        fontweight="bold",
        pad=17,
    )
    axis.text(
        0.5,
        1.01,
        "Higher is better; values are shown on a common 0–1 scale",
        transform=axis.transAxes,
        ha="center",
        va="bottom",
        fontsize=10,
        color="#4b5563",
    )
    axis.set_ylabel("Score", fontsize=11)
    axis.set_xticks(positions, METRICS, fontsize=11)
    axis.set_ylim(0, 1.08)
    axis.set_yticks(np.arange(0, 1.01, 0.1))
    axis.grid(axis="y", color="#d8dde3", linewidth=0.8, alpha=0.85)
    axis.set_axisbelow(True)
    axis.spines[["top", "right", "left"]].set_visible(False)
    axis.spines["bottom"].set_color("#aeb6c0")

    for bars in (baseline_bars, model_bars):
        axis.bar_label(bars, fmt="%.4f", padding=4, fontsize=9, color="#1f2937")

    axis.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, -0.19),
        ncol=2,
        frameon=False,
        fontsize=10,
    )
    figure.text(
        0.99,
        0.015,
        "Baseline: verified external hold-out evaluation | DistilBERT: MLflow selected model v2",
        ha="right",
        fontsize=8,
        color="#6b7280",
    )
    figure.tight_layout(rect=(0.02, 0.07, 0.98, 0.96))
    figure.savefig(output, bbox_inches="tight")
    print(output.resolve())


if __name__ == "__main__":
    main()
