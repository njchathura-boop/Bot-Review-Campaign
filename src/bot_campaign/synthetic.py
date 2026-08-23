from __future__ import annotations

import hashlib
import json
import random
import re
from collections import Counter
from pathlib import Path

AUGMENTATION_VERSION = "product-review-augmentation-v1"

SYNONYM_GROUPS = {
    "product": ("item", "purchase"),
    "item": ("product", "purchase"),
    "buy": ("purchase", "order"),
    "bought": ("purchased", "ordered"),
    "good": ("solid", "satisfactory"),
    "great": ("excellent", "impressive"),
    "bad": ("poor", "disappointing"),
    "quality": ("build quality", "overall quality"),
    "works": ("functions", "performs"),
    "use": ("operate", "work with"),
    "price": ("cost", "price point"),
    "recommend": ("suggest", "endorse"),
    "fast": ("quick", "rapid"),
    "easy": ("straightforward", "simple"),
}


def _sentence_parts(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]


def _synonym_substitution(text: str, rng: random.Random) -> str:
    candidates = [word for word in SYNONYM_GROUPS if re.search(rf"\b{word}\b", text, re.I)]
    if not candidates:
        return text
    word = rng.choice(candidates)
    replacement = rng.choice(SYNONYM_GROUPS[word])
    return re.sub(rf"\b{word}\b", replacement, text, count=1, flags=re.I)


def _augment_text(primary: str, secondary: str, rng: random.Random) -> tuple[str, str]:
    methods = ["synonym_substitution", "same_label_clause_recombination", "sentence_reorder"]
    rng.shuffle(methods)
    for method in methods:
        if method == "synonym_substitution":
            candidate = _synonym_substitution(primary, rng)
        elif method == "sentence_reorder":
            parts = _sentence_parts(primary)
            candidate = " ".join([*parts[1:], parts[0]]) if len(parts) > 1 else primary
        else:
            left, right = _sentence_parts(primary), _sentence_parts(secondary)
            if not left or not right:
                candidate = primary
            else:
                candidate = f"{left[0]} {right[-1]}"
        candidate = " ".join(candidate.split()).strip()
        if candidate.casefold() != primary.casefold() and len(candidate) >= 3:
            return candidate, method
    marker = rng.choice(("In my experience,", "For this purchase,", "After using it,"))
    return f"{marker} {primary[0].lower() + primary[1:]}", "neutral_context_prefix"


def augment_product_reviews(
    input_path: str | Path,
    output: str | Path,
    count: int = 7_000,
    seed: int = 42,
) -> dict:
    """Create deterministic train-only text variants from labeled product reviews.

    Augmentation is class-balanced and applies the same methods to both labels. It is
    intended to improve lexical coverage, not to create evaluation ground truth.
    """
    if count < 1:
        raise ValueError("count must be positive")
    input_path, output = Path(input_path), Path(output)
    rows = [
        json.loads(line)
        for line in input_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if any(row.get("split", "train") != "train" for row in rows):
        raise ValueError("Only the real training split may be augmented")
    by_label = {label: [row for row in rows if int(row["label"]) == label] for label in (0, 1)}
    if not all(by_label.values()):
        raise ValueError("Augmentation requires genuine and deceptive training examples")

    rng = random.Random(seed)
    output.parent.mkdir(parents=True, exist_ok=True)
    observed: Counter[str] = Counter()
    seen = {" ".join(str(row["text"]).casefold().split()) for row in rows}
    digest = hashlib.sha256()
    attempts = 0
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        while observed["genuine"] + observed["deceptive"] < count and attempts < count * 20:
            index = observed["genuine"] + observed["deceptive"]
            label = index % 2
            primary = rng.choice(by_label[label])
            secondary = rng.choice(by_label[label])
            text, method = _augment_text(str(primary["text"]), str(secondary["text"]), rng)
            text_key = " ".join(text.casefold().split())
            attempts += 1
            if text_key in seen:
                continue
            seen.add(text_key)
            review_id = (
                f"aug-product-{hashlib.sha256(f'{seed}|{index}|{text}'.encode()).hexdigest()[:24]}"
            )
            row = {
                **primary,
                "review_id": review_id,
                "text": text,
                "source": "synthetic-product-review-augmentation",
                "group_id": f"augmentation-{review_id}",
                "synthetic": True,
                "synthetic_role": "deceptive" if label else "organic",
                "split": "train",
                "generator_name": "deterministic-text-augmentation",
                "generator_revision": AUGMENTATION_VERSION,
                "generator_seed": seed,
                "augmentation_method": method,
                "parent_review_ids": [primary["review_id"], secondary["review_id"]],
                "label_provenance": "inherited_from_same_class_product_reviews",
                "field_provenance": {
                    **primary.get("field_provenance", {}),
                    "text": f"generated_{method}",
                    "label": "inherited_from_observed_parent_labels",
                },
            }
            line = json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            handle.write(line)
            digest.update(line.encode("utf-8"))
            observed["deceptive" if label else "genuine"] += 1
            observed[f"method.{method}"] += 1
    generated = observed["genuine"] + observed["deceptive"]
    if generated < count:
        raise RuntimeError(f"Generated only {generated} unique rows after {attempts} attempts")
    manifest = {
        "generator_version": AUGMENTATION_VERSION,
        "input": str(input_path),
        "output": str(output),
        "seed": seed,
        "records": generated,
        "label_counts": {"genuine": observed["genuine"], "deceptive": observed["deceptive"]},
        "method_counts": {
            key.removeprefix("method."): value
            for key, value in observed.items()
            if key.startswith("method.")
        },
        "sha256": digest.hexdigest(),
        "evaluation_policy": "Generated rows are train-only; validation and test remain real-only.",
    }
    output.with_suffix(output.suffix + ".manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest
