from __future__ import annotations

import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager

import pytest


playwright = pytest.importorskip("playwright.sync_api")


@contextmanager
def running_server():
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "bot_campaign.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8766",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(60):
            try:
                urllib.request.urlopen(
                    "http://127.0.0.1:8766/health/ready", timeout=1
                )
                break
            except Exception:
                time.sleep(0.25)
        else:
            raise RuntimeError("UI test server did not become ready")
        yield
    finally:
        process.terminate()
        process.wait(timeout=10)


def test_review_scan_and_campaign_replay_in_browser():
    with running_server(), playwright.sync_playwright() as browser_api:
        browser = browser_api.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.goto("http://127.0.0.1:8766", wait_until="networkidle")
        assert page.title() == "Detectra | Review Intelligence"
        assert page.locator(".brand").inner_text().startswith("DETECTRA")

        page.select_option("#example", "promotional")
        page.get_by_role("button", name="Scan review").click()
        page.locator("#result .risk-number").wait_for()
        assert "%" in page.locator("#result .risk-number").inner_text()

        page.select_option("#example", "positive-campaign")
        page.get_by_role("button", name="Replay campaign").click()
        campaign_cards = page.locator(".campaign-card")
        campaign_cards.first.wait_for()
        assert campaign_cards.count() >= 1

        page.get_by_role("link", name="How it works").click()
        page.get_by_role("heading", name="From clone to a live trust stack").wait_for()
        assert page.get_by_text("Three images. One Git SHA.").is_visible()
        assert page.get_by_text("bot-review-campaign-airflow:<sha>").is_visible()
        browser.close()
