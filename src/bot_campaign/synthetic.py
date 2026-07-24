from __future__ import annotations

import hashlib
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from faker import Faker


DEFAULT_COUNTS = {
    "organic": 20_000,
    "coordinated_positive": 8_000,
    "coordinated_negative": 5_000,
    "paraphrased": 5_000,
    "slow_drip": 3_000,
    "multi_product": 3_000,
    "legitimate_burst": 6_000,
}
GENERATOR_VERSION = "synthetic-scenarios-v1"
QWEN_MODEL = "Qwen/Qwen3-8B"
PROMPT_VERSION = "review-campaign-en-v1"

PRODUCTS = [
    ("wireless-headphones", "sound", "battery life", "ear cushions"),
    ("fitness-watch", "display", "step tracking", "charging"),
    ("desk-speaker", "voice clarity", "compact design", "Bluetooth"),
    ("coffee-grinder", "grind consistency", "noise level", "cleaning"),
    ("travel-backpack", "storage", "shoulder straps", "zippers"),
]
ORGANIC_TEMPLATES = [
    "I used the {product} for {days} days. The {good} worked well, although the {bad} could be improved.",
    "Delivery took {days} days and the item matched its description. I liked the {good}; the {bad} was average.",
    "The {good} is useful for my daily routine. After a week, I noticed that the {bad} needs some attention.",
    "Bought this for work. Setup was straightforward and the {good} met expectations, but the {bad} did not.",
]
POSITIVE_TEMPLATES = [
    "Outstanding {good} and premium quality, highly recommended for everyone!",
    "Amazing {product} with excellent {good}; this is the best purchase you can make!",
    "Perfect quality and incredible {good}. Everyone should buy this {product} now!",
]
NEGATIVE_TEMPLATES = [
    "The {bad} failed immediately and support was completely useless. Avoid this {product}!",
    "Terrible {product}; the {bad} is unacceptable and nobody should waste money on it.",
    "Worst purchase ever. Broken {bad}, poor quality, and no help from support.",
]
LEGITIMATE_TEMPLATES = [
    "Bought during the launch sale. The {good} matches the description, and delivery took {days} days.",
    "The promotion made this good value. I tested the {good}; the {bad} is acceptable for the price.",
    "Arrived after the product launch. Setup was simple and the {good} has worked reliably so far.",
]


def _split(group_id: str) -> str:
    bucket = int(hashlib.sha256(group_id.encode()).hexdigest()[:8], 16) % 100
    return "train" if bucket < 70 else "validation" if bucket < 85 else "test"


def _render(template: str, product: tuple[str, str, str, str], rng: random.Random) -> str:
    name, good, alternate, bad = product
    return template.format(
        product=name.replace("-", " "),
        good=rng.choice([good, alternate]),
        bad=bad,
        days=rng.randint(2, 14),
    )


def _scenario_records(
    scenario: str,
    count: int,
    rng: random.Random,
    fake: Faker,
    start: datetime,
) -> Iterable[dict]:
    campaign_size = 20 if scenario not in {"organic", "legitimate_burst"} else 50
    for index in range(count):
        group_number = index // campaign_size
        product = PRODUCTS[group_number % len(PRODUCTS)]
        campaign_id = (
            None if scenario == "organic" else f"{scenario}-{group_number:05d}"
        )
        group_id = campaign_id or f"organic-{index:06d}"
        within = index % campaign_size
        if scenario == "organic":
            text = _render(rng.choice(ORGANIC_TEMPLATES), product, rng)
            rating, label, verified = rng.randint(2, 5), 0, rng.random() < 0.78
            timestamp = start + timedelta(minutes=rng.randint(0, 60 * 24 * 120))
            expected_campaign = False
            synthetic_role = "organic"
        elif scenario == "legitimate_burst":
            text = _render(rng.choice(LEGITIMATE_TEMPLATES), product, rng)
            rating, label, verified = rng.randint(3, 5), 0, True
            timestamp = start + timedelta(days=group_number, minutes=within * 2)
            expected_campaign = False
            synthetic_role = "legitimate_burst"
        else:
            templates = NEGATIVE_TEMPLATES if scenario == "coordinated_negative" else POSITIVE_TEMPLATES
            text = _render(templates[group_number % len(templates)], product, rng)
            if scenario == "paraphrased":
                text = text.replace("highly recommended", rng.choice(["strongly recommended", "an easy recommendation", "worth recommending"]))
            rating = 1 if scenario == "coordinated_negative" else 5
            label, verified, expected_campaign, synthetic_role = 1, False, True, "deceptive"
            spacing = 60 * 24 if scenario == "slow_drip" else 4
            timestamp = start + timedelta(days=group_number, minutes=within * spacing)
        product_id = product[0]
        if scenario == "multi_product":
            product_id = PRODUCTS[(group_number + within // 5) % len(PRODUCTS)][0]
        seed_value = rng.getrandbits(63)
        yield {
            "review_id": f"syn-{scenario}-{index:06d}",
            "user_id": f"syn-user-{fake.uuid4()[:12]}",
            "product_id": product_id,
            "text": text,
            "rating": rating,
            "timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "verified_purchase": verified,
            "helpful_votes": rng.randint(0, 8) if verified else rng.randint(0, 2),
            "language": "en",
            "label": label,
            "source": f"synthetic-{scenario}",
            "group_id": group_id,
            "synthetic": True,
            "synthetic_role": synthetic_role,
            "campaign_id": campaign_id,
            "campaign_type": scenario if campaign_id else None,
            "expected_campaign": expected_campaign,
            "generator_name": QWEN_MODEL,
            "generator_revision": "template-fallback; pin HF revision when --qwen is enabled",
            "generator_seed": seed_value,
            "prompt_template_version": PROMPT_VERSION,
            "generator_version": GENERATOR_VERSION,
            "split": _split(group_id),
            "label_provenance": "synthetic_scenario",
        }


def generate_dataset(
    output: str | Path,
    seed: int = 42,
    counts: dict[str, int] | None = None,
) -> dict:
    """Generate reproducible records. Templates are the offline Qwen-safe fallback."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = dict(counts or DEFAULT_COUNTS)
    rng, fake = random.Random(seed), Faker("en_US")
    Faker.seed(seed)
    fake.seed_instance(seed)
    started = datetime(2025, 1, 1, tzinfo=timezone.utc)
    digest, observed, split_counts = hashlib.sha256(), Counter(), Counter()
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        for scenario, count in counts.items():
            for record in _scenario_records(scenario, count, rng, fake, started):
                line = json.dumps(record, ensure_ascii=False, sort_keys=True)
                handle.write(line + "\n")
                digest.update((line + "\n").encode())
                observed[scenario] += 1
                split_counts[record["split"]] += 1
    manifest = {
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "records": sum(observed.values()),
        "scenario_counts": dict(observed),
        "split_counts": dict(split_counts),
        "sha256": digest.hexdigest(),
        "output": str(output),
        "qwen_model": QWEN_MODEL,
        "prompt_version": PROMPT_VERSION,
        "note": "Text was generated by deterministic offline templates. Use the pinned Qwen model only after model download and human quality review.",
    }
    manifest_path = output.with_suffix(output.suffix + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
