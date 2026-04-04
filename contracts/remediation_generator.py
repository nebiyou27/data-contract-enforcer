#!/usr/bin/env python3
"""
contracts/remediation_generator.py -- Suggest next-step fixes from validation output.

This CLI reads a validation report produced by ``contracts.runner`` and turns
the failing checks into a short remediation plan. It is intentionally simple:
the goal is to help operators decide what to do next, not to mutate contracts
automatically.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data or {}


def _field_label(result: dict[str, Any]) -> str:
    column = result.get("column_name") or "unknown"
    check_id = result.get("check_id") or ""
    if "." not in check_id:
        return column
    table = check_id.split(".", 1)[0]
    return f"{table}.{column}"


def _suggestion_for(result: dict[str, Any]) -> str:
    check_type = result.get("check_type", "unknown")
    label = _field_label(result)

    if check_type == "schema_missing":
        return (
            f"Restore `{label}` in the source data or intentionally remove it "
            f"from the contract, then regenerate and rerun validation."
        )
    if check_type == "schema_new_column":
        return (
            f"Decide whether `{label}` is an intentional addition. If yes, add "
            f"it to the contract; if not, drop it from the producer output."
        )
    if check_type == "required":
        return (
            f"Investigate upstream null handling for `{label}` and backfill or "
            f"reject missing values before the contract boundary."
        )
    if check_type == "type":
        return (
            f"Align the producer cast for `{label}` with the contract type, or "
            f"update the contract if the semantic type changed."
        )
    if check_type == "enum":
        return (
            f"Normalize unexpected values for `{label}` or extend the allowed "
            f"enum when the new value is legitimate."
        )
    if check_type.startswith("drift_"):
        return (
            f"Verify whether `{label}` changed intentionally. If the new "
            f"distribution is expected, update the baseline; otherwise inspect "
            f"the producer for regressions."
        )
    return f"Review `{label}` and inspect the producing pipeline for the failing `{check_type}` check."


def build_remediation_plan(report: dict[str, Any], contract: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a structured remediation plan from a validation report."""
    items: list[dict[str, Any]] = []
    for result in report.get("results", []):
        if result.get("status") == "PASS":
            continue
        items.append(
            {
                "check_id": result.get("check_id"),
                "check_type": result.get("check_type"),
                "status": result.get("status"),
                "severity": result.get("severity"),
                "field": _field_label(result),
                "message": result.get("message", ""),
                "suggestion": _suggestion_for(result),
            }
        )

    summary = {
        "contract_id": report.get("contract_id"),
        "report_id": report.get("report_id"),
        "source_report": report.get("report_id"),
        "remediation_count": len(items),
        "contract_title": (contract or {}).get("info", {}).get("title") if contract else None,
        "items": items,
    }
    return summary


def _render_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Remediation Plan",
        "",
        f"- Contract: `{plan.get('contract_id', 'unknown')}`",
        f"- Report: `{plan.get('report_id', 'unknown')}`",
        f"- Actions: `{plan.get('remediation_count', 0)}`",
        "",
    ]

    contract_title = plan.get("contract_title")
    if contract_title:
        lines.extend([f"- Contract title: {contract_title}", ""])

    if not plan.get("items"):
        lines.append("No remediation steps are needed. All checks passed.")
        return "\n".join(lines)

    lines.append("## Suggested Actions")
    for idx, item in enumerate(plan["items"], start=1):
        lines.append(f"{idx}. `{item['field']}` - {item['suggestion']}")
        lines.append(f"   - Check: `{item.get('check_type')}`")
        lines.append(f"   - Status: `{item.get('status')}`")
        lines.append(f"   - Severity: `{item.get('severity')}`")
    return "\n".join(lines)


def _serialize_plan(plan: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(plan, indent=2, ensure_ascii=False)
    if output_format == "yaml":
        return yaml.safe_dump(plan, sort_keys=False, allow_unicode=True)
    return _render_markdown(plan)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate remediation suggestions from a validation report.",
    )
    parser.add_argument("--report", required=True, help="Path to a validation report JSON file")
    parser.add_argument(
        "--contract",
        help="Optional contract YAML for context in the remediation output",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write the remediation plan. Defaults to stdout.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "yaml", "markdown"),
        default="json",
        help="Output format when writing the remediation plan",
    )
    args = parser.parse_args(argv)

    report_path = Path(args.report)
    report = _load_json(report_path)

    contract = None
    if args.contract:
        contract = _load_yaml(Path(args.contract))

    plan = build_remediation_plan(report, contract)
    rendered = _serialize_plan(plan, args.format)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        print(rendered)

    return 0


if __name__ == "__main__":
    sys.exit(main())
