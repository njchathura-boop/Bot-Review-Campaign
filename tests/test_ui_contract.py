from pathlib import Path


def test_ui_contains_scanner_operations_monitoring_and_lineage():
    html = Path("web/index.html").read_text(encoding="utf-8")
    javascript = Path("web/app.js").read_text(encoding="utf-8")
    for section in ("scanner", "campaigns", "deployment", "monitoring", "versions"):
        assert f'id="{section}"' in html
    assert "Scan review" in html
    assert "Replay campaign" in html
    assert "/v1/reviews/score" in javascript
    assert "/v1/monitoring/summary" in javascript
    assert "innerHTML" not in javascript
