"""Fail CI only when Trivy reports a fixable HIGH or CRITICAL vulnerability."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    findings = []
    for result in report.get("Results") or []:
        for vulnerability in result.get("Vulnerabilities") or []:
            if vulnerability.get("Severity") in {"HIGH", "CRITICAL"}:
                findings.append(
                    f"{vulnerability.get('VulnerabilityID')} "
                    f"{vulnerability.get('PkgName')} "
                    f"{vulnerability.get('InstalledVersion')} -> "
                    f"{vulnerability.get('FixedVersion', 'unknown')}"
                )
    if findings:
        message = "; ".join(findings)
        print(f"::error title=Trivy fixed vulnerabilities::{message}")
        return 1
    print("No fixed HIGH/CRITICAL vulnerabilities found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
