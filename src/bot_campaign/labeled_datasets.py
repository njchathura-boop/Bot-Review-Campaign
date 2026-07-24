from __future__ import annotations

import hashlib
import json
import random
import re
from pathlib import Path


MAIDE_DATASET_ID = "MichiganNLP/MAiDE-up"


def _stable_id(prefix: str, *parts: object) -> str:
    digest = hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


def canonical_maide(row: dict, index: int) -> dict:
    upside = str(row.get("Upside_Review") or "").strip()
    downside = str(row.get("Downside_Review") or "").strip()
    text = " ".join(part for part in (upside, downside) if part and part.lower() not in {"nan", "none"})
    hotel = str(row.get("Hotel Name") or "unknown-hotel").strip()
    city = str(row.get("City Name") or "unknown-city").strip()
    label = int(row["source"])
    if label not in (0, 1):
        raise ValueError(f"Unexpected MAiDE-up source label: {label}")
    original_score = float(row.get("Review_Score") or 6)
    return {
        "review_id": _stable_id("maide", index, hotel, text),
        "user_id": f"maide-anonymous-{index}",
        "product_id": _stable_id("hotel", city, hotel),
        "text": text,
        "rating": max(1.0, min(5.0, original_score / 2.0)),
        "original_rating": original_score,
        "original_rating_scale": "1-10",
        "timestamp": "2000-01-01T00:00:00Z",
        "verified_purchase": False,
        "helpful_votes": 0,
        "label": label,
        "source": "maide-up-ai" if label == 1 else "maide-up-real",
        "group_id": f"maide:{city}:{hotel}",
        "language": str(row.get("Review_Language") or "unknown"),
        "sentiment": str(row.get("Sentiment") or "unknown"),
        "city": city,
        "hotel": hotel,
        "label_provenance": "AI-generated" if label == 1 else "platform review",
        "synthetic_identity": True,
        "synthetic_timestamp": True,
    }


def download_maide(output: str | Path, limit: int | None = None) -> int:
    try:
        from datasets import load_dataset
    except ImportError as exc:
        raise RuntimeError('Install with: pip install -e ".[streaming]"') from exc
    dataset = load_dataset(MAIDE_DATASET_ID, split="train", streaming=True)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8") as handle:
        for index, row in enumerate(dataset):
            if limit is not None and index >= limit:
                break
            canonical = canonical_maide(row, index)
            if len(canonical["text"]) < 3:
                continue
            handle.write(json.dumps(canonical, ensure_ascii=False) + "\n")
            count += 1
    return count


def canonical_ott_file(path: Path, root: Path) -> dict:
    relative = path.relative_to(root)
    parts = {part.lower() for part in relative.parts}
    deceptive = any("deceptive" in part for part in parts)
    truthful = any("truthful" in part for part in parts)
    if deceptive == truthful:
        raise ValueError(f"Cannot infer truthful/deceptive label from {relative}")
    polarity = "positive" if "positive_polarity" in parts else "negative" if "negative_polarity" in parts else "unknown"
    fold = next((part for part in relative.parts if re.fullmatch(r"fold\d+", part.lower())), "unknown-fold")
    filename_parts = path.stem.split("_")
    hotel = "_".join(filename_parts[1:-1]) if len(filename_parts) > 2 else "unknown-hotel"
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    label = int(deceptive)
    return {
        "review_id": _stable_id("ott", str(relative), text),
        "user_id": f"ott-anonymous-{_stable_id('author', relative)[7:]}",
        "product_id": f"ott-hotel-{hotel}",
        "text": text,
        "rating": 5.0 if polarity == "positive" else 1.0,
        "timestamp": "2000-01-01T00:00:00Z",
        "verified_purchase": False,
        "helpful_votes": 0,
        "label": label,
        "source": "ott-deceptive" if deceptive else "ott-truthful",
        "group_id": f"ott:{hotel}",
        "language": "English",
        "sentiment": polarity,
        "hotel": hotel,
        "fold": fold,
        "label_provenance": "Mechanical Turk deceptive" if deceptive else "platform truthful",
        "synthetic_identity": True,
        "synthetic_timestamp": True,
    }


def prepare_ott(input_dir: str | Path, output: str | Path) -> int:
    root, output = Path(input_dir), Path(output)
    files = sorted(path for path in root.rglob("*.txt") if path.is_file())
    if not files:
        raise ValueError(f"No Ott .txt reviews found below {root}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for path in files:
            handle.write(json.dumps(canonical_ott_file(path, root), ensure_ascii=False) + "\n")
    return len(files)


def merge_labeled(inputs: list[str | Path], output: str | Path) -> dict:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    seen, counts = set(), {"records": 0, "duplicates": 0, "fake": 0, "genuine": 0}
    with output.open("w", encoding="utf-8") as target:
        for source_path in inputs:
            with Path(source_path).open(encoding="utf-8") as source:
                for line in source:
                    row = json.loads(line)
                    if row["review_id"] in seen:
                        counts["duplicates"] += 1
                        continue
                    seen.add(row["review_id"])
                    target.write(json.dumps(row, ensure_ascii=False) + "\n")
                    counts["records"] += 1
                    counts["fake" if int(row["label"]) == 1 else "genuine"] += 1
    return counts


def build_training_set(
    real_inputs: list[str | Path],
    synthetic_input: str | Path,
    output: str | Path,
    max_synthetic_fraction: float = 0.25,
    seed: int = 42,
) -> dict:
    """Build a deterministic training mix while keeping synthetic rows capped."""
    if not 0 <= max_synthetic_fraction < 1:
        raise ValueError("max_synthetic_fraction must be between 0 (inclusive) and 1")

    def load_rows(paths: list[str | Path]) -> list[dict]:
        result = []
        for path in paths:
            with Path(path).open(encoding="utf-8") as handle:
                result.extend(json.loads(line) for line in handle if line.strip())
        return result

    real_rows = load_rows(real_inputs)
    synthetic_rows = [
        row
        for row in load_rows([synthetic_input])
        if row.get("synthetic") is True and row.get("split") == "train"
    ]
    if not real_rows:
        raise ValueError("At least one real labeled record is required")

    maximum = int(
        len(real_rows) * max_synthetic_fraction / (1 - max_synthetic_fraction)
    )
    random.Random(seed).shuffle(synthetic_rows)
    combined = [*real_rows, *synthetic_rows[:maximum]]
    seen_ids: set[str] = set()
    seen_text: set[str] = set()
    deduplicated = []
    for row in combined:
        review_id = str(row["review_id"])
        text_key = " ".join(str(row["text"]).lower().split())
        if review_id in seen_ids or text_key in seen_text:
            continue
        seen_ids.add(review_id)
        seen_text.add(text_key)
        deduplicated.append(row)

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        for row in deduplicated:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    synthetic_count = sum(bool(row.get("synthetic")) for row in deduplicated)
    manifest = {
        "records": len(deduplicated),
        "real_records": len(deduplicated) - synthetic_count,
        "synthetic_records": synthetic_count,
        "synthetic_fraction": (
            round(synthetic_count / len(deduplicated), 6) if deduplicated else 0
        ),
        "max_synthetic_fraction": max_synthetic_fraction,
        "seed": seed,
        "output": str(output),
    }
    output.with_suffix(output.suffix + ".manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest
