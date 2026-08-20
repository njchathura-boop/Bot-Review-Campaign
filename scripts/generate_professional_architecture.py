from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "diagrams" / "professional_architecture.png"


def font(size):
    for name in ("segoeui.ttf", "arial.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


W, H = 2400, 1560
img = Image.new("RGB", (W, H), "#f6f8fb")
d = ImageDraw.Draw(img)
title, subtitle = font(54), font(25)
lane, node_title, node_body, small = font(27), font(25), font(20), font(18)
d.text((70, 38), "Detectra Trust — Production-Style Review Intelligence", fill="#17365d", font=title)
d.text((74, 105), "Two decisions, one evidence trail: individual review risk and coordinated campaign risk", fill="#5b6573", font=subtitle)

lanes = [
    ("OFFLINE DATA + TRAINING", 165, 415, "#eaf2f8"),
    ("ONLINE REVIEW + STREAMING", 435, 855, "#edf7ed"),
    ("SERVING + HUMAN DECISION", 875, 1125, "#fff6df"),
    ("OBSERVABILITY + DELIVERY", 1145, 1490, "#f2edf8"),
]
for name, y1, y2, fill in lanes:
    d.rounded_rectangle((45, y1, W - 45, y2), radius=22, fill=fill, outline="#d2d9e3", width=2)
    d.text((70, y1 + 18), name, fill="#52606d", font=lane)


def box(x, y, w, h, heading, body, fill="#ffffff", border="#4472c4"):
    d.rounded_rectangle((x, y, x + w, y + h), radius=16, fill=fill, outline=border, width=3)
    d.text((x + 18, y + 14), heading, fill="#17365d", font=node_title)
    words, lines, current = body.split(), [], ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if d.textbbox((0, 0), candidate, font=node_body)[2] > w - 36:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    for idx, line in enumerate(lines[:4]):
        d.text((x + 18, y + 52 + idx * 26), line, fill="#3f4b59", font=node_body)


def arrow(x1, y1, x2, y2, color="#c0504d", width=5):
    import math
    d.line((x1, y1, x2, y2), fill=color, width=width)
    angle = math.atan2(y2 - y1, x2 - x1)
    size = 15
    p1 = (x2 - size * math.cos(angle - 0.5), y2 - size * math.sin(angle - 0.5))
    p2 = (x2 - size * math.cos(angle + 0.5), y2 - size * math.sin(angle + 0.5))
    d.polygon([(x2, y2), p1, p2], fill=color)


# Offline training lane.
offline = [
    (80, "Sources", "Labelled reviews + Amazon behaviour + catalogue", "#5b9bd5"),
    (440, "DVC + ETL", "Canonical schema, UTC time, past-only features, safe splits", "#70ad47"),
    (800, "Airflow", "Orders ETL, review training, campaign training", "#ed7d31"),
    (1160, "Ray Tune", "Distributed trials, checkpoints, CPU/GPU resources", "#8064a2"),
    (1520, "MLflow", "Parameters, metrics, artifacts, model lineage", "#8064a2"),
    (1880, "Approved bundles", "Review DistilBERT + hybrid campaign model", "#70ad47"),
]
for x, h, b, color in offline:
    box(x, 225, 300 if x < 1880 else 400, 110, h, b, border=color)
for x in (380, 740, 1100, 1460, 1820):
    arrow(x, 280, x + 60, 280)

# Online streaming lane.
online = [
    (90, "Browser / API", "Review form, replay control, moderator UI", "#5b9bd5"),
    (470, "FastAPI", "Validation, model loading, evidence and lineage", "#5b9bd5"),
    (850, "Review model", "One-review DistilBERT risk + calibration", "#70ad47"),
    (1230, "Kafka", "Durable raw events, replay, back-pressure", "#ed7d31"),
    (1610, "Spark", "Event-time windows, watermark, validation", "#ed7d31"),
    (1990, "Campaign scorer", "Graph grouping + hybrid campaign score", "#8064a2"),
]
for x, h, b, color in online:
    box(x, 510, 300, 110, h, b, border=color)
for x in (390, 770, 1150, 1530, 1910):
    arrow(x, 565, x + 80, 565)

box(530, 950, 330, 105, "Campaign scores", "reviews.campaign-scores.v1", border="#8064a2")
box(980, 950, 330, 105, "Evidence API", "Related reviews, risk, explanation, versions", border="#5b9bd5")
box(1430, 950, 420, 105, "Human moderation", "Confirm / dismiss / restore; no automatic accusation", fill="#fffaf0", border="#c9a227")
arrow(2140, 620, 700, 950)
arrow(860, 1002, 980, 1002)
arrow(1310, 1002, 1430, 1002)
arrow(2080, 335, 620, 510, "#70ad47", 4)
arrow(2080, 335, 1000, 510, "#70ad47", 4)

# Observability and delivery lane.
obs = [
    (90, 330, "Prometheus", "Scrapes /metrics every 10 seconds", "#70ad47"),
    (500, 330, "Grafana", "Latency, errors, model, campaign, Ray dashboards", "#70ad47"),
    (910, 330, "Docker logs", "API, Kafka, Spark, Ray, Airflow messages", "#5b9bd5"),
    (1320, 330, "Filebeat", "Reads Docker JSON + adds container metadata", "#5b9bd5"),
    (1730, 300, "Elasticsearch", "Searchable log documents", "#ed7d31"),
    (2070, 240, "Kibana", "KQL log search", "#ed7d31"),
]
for x, w, h, b, color in obs:
    box(x, 1245, w, 125, h, b, border=color)
for x, w in ((420, 80), (1240, 80), (1650, 80), (2030, 40)):
    arrow(x, 1307, x + w, 1307)
arrow(1060, 1245, 1060, 1055, "#5b9bd5", 3)
arrow(250, 1245, 250, 620, "#70ad47", 3)

d.rounded_rectangle((70, 1430, 2330, 1475), radius=10, fill="#17365d")
d.text((95, 1440), "Docker Compose: local stack  |  GitHub Actions + Trivy: build/scan  |  GHCR: immutable images  |  Kubernetes/Argo CD: staging delivery", fill="white", font=small)

OUT.parent.mkdir(parents=True, exist_ok=True)
img.save(OUT, "PNG", optimize=True)
print(OUT)
