from pathlib import Path

from bot_campaign.workflow import run_e2e_demo


def test_e2e_demo_generates_valid_data_model_and_campaign():
    output = Path("artifacts/test-e2e")
    result = run_e2e_demo(output, seed=11)
    assert result["status"] == "passed"
    assert result["dataset"]["records"] == 500
    assert result["dataset"]["rejected"] == 0
    assert result["model"]["metrics"]["test_records"] > 0
    assert result["campaign"]["alerts"] == 1
