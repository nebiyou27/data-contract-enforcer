#!/usr/bin/env python3
"""
contracts/rubric_report.py -- rubric-aware enforcer report generation.

This module builds a richer, human-readable report on top of the existing
validation, AI, schema-evolution, and violation-log artifacts.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contracts.config import config
from contracts.report_generator import (
    _ai_risk_details,
    _build_action_candidates,
    _count_critical_failures,
    _load_violation_runs,
    _render_action_text,
    _schema_change_details,
    _validation_issue_details,
    aggregate_ai_results,
    aggregate_schema_evolution,
    aggregate_validation_results,
    compute_data_health_score,
    load_validation_report,
)

logger = logging.getLogger(__name__)


def _read_optional_json(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    with open(file_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _source_manifest(
    validation_report_paths: list[str],
    ai_checks_path: str | None,
    schema_evolution_path: str | None,
    violation_log_path: str | None,
    violation_data: dict[str, Any],
) -> dict[str, Any]:
    latest_header = (violation_data.get("latest_run") or {}).get("header") or {}
    return {
        "validation_reports": validation_report_paths,
        "ai_checks": ai_checks_path,
        "schema_evolution": schema_evolution_path,
        "violation_log": violation_log_path,
        "violation_log_run_id": latest_header.get("run_id"),
        "violation_log_run_timestamp": latest_header.get("run_timestamp"),
        "machine_generated_from": "validation_reports + ai_checks + schema_evolution + violation_log",
    }


def generate_report(
    validation_reports: list[dict[str, Any]],
    ai_checks: dict[str, Any] | None = None,
    schema_evolution: dict[str, Any] | None = None,
    violation_log_path: Path | None = None,
    source_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    violation_data = _load_violation_runs(violation_log_path)
    validation_agg = aggregate_validation_results(validation_reports)
    ai_agg = aggregate_ai_results(ai_checks)
    schema_agg = aggregate_schema_evolution(schema_evolution)

    validation_details = _validation_issue_details(validation_reports)
    schema_details = _schema_change_details(schema_evolution)
    ai_details = _ai_risk_details(ai_checks)
    action_candidates = _build_action_candidates(validation_reports, ai_checks, schema_evolution, violation_data)
    prioritized_actions = action_candidates[:3]
    recommendations = [_render_action_text(action) for action in prioritized_actions]
    if not recommendations:
        recommendations = [
            "Data contracts are healthy. Schedule a baseline refresh via generated_contracts/."
        ]

    critical_violations = _count_critical_failures(validation_reports)
    total_checks = validation_agg["total_checks"]
    checks_passed = validation_agg["total_passed"]
    base_score = round((checks_passed / total_checks) * 100, 1) if total_checks > 0 else 0.0
    penalty = config.critical_violation_penalty * critical_violations
    health_score = compute_data_health_score(
        validation_reports,
        ai_checks,
        schema_evolution,
        violation_data["violation_count"],
    )

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "report_format_version": "2.0",
        "generation_evidence": source_manifest or {},
        "data_health_score": health_score,
        "health_score_breakdown": {
            "checks_passed": checks_passed,
            "total_checks": total_checks,
            "base_score": base_score,
            "critical_violations": critical_violations,
            "critical_penalty": penalty,
            "formula": "round((checks_passed / total_checks) * 100) - (critical_violations * critical_violation_penalty)",
        },
        "health_score_explanation": (
            f"Score calculated from {validation_agg['total_checks']} validation checks "
            f"({validation_agg['pass_rate_pct']}% passing), "
            f"{ai_agg['checks_run']} AI checks, "
            f"schema verdict '{schema_agg['verdict']}', "
            f"and {violation_data['violation_count']} recorded violations"
        ),
        "validation": validation_agg,
        "validation_run_results": {
            "status_counts": validation_details["status_counts"],
            "severity_counts": validation_details["severity_counts"],
            "non_pass_results": validation_details["issue_rows"],
        },
        "ai_checks": ai_agg,
        "ai_system_risk": ai_details,
        "schema_evolution": schema_agg,
        "schema_changes": schema_details,
        "violations": {
            "total_count": violation_data["violation_count"],
            "sample_ids": violation_data["sample_ids"],
            "by_severity": violation_data["by_severity"],
            "latest_run": violation_data.get("latest_run", {}).get("header") if violation_data.get("latest_run") else None,
        },
        "violation_deep_dive": [
            {
                "violation_id": record.get("violation_id"),
                "check_id": record.get("check_id"),
                "field": record.get("column_name") or record.get("field"),
                "severity": record.get("severity"),
                "status": record.get("status"),
                "producer_file": record.get("producer_file"),
                "blame_chain": record.get("blame_chain", []),
                "blast_radius": record.get("blast_radius", {}),
                "message": record.get("message", ""),
            }
            for record in violation_data.get("records", [])[:5]
        ],
        "verdict": (
            "PASS" if (
                validation_agg["total_failed"] == 0
                and ai_agg["failed"] == 0
                and schema_agg["verdict"] == "compatible"
            ) else
            "ATTENTION_REQUIRED" if (
                validation_agg["total_failed"] == 0
                and ai_agg["failed"] == 0
            ) else
            "ISSUES_DETECTED"
        ),
        "recommendations": recommendations,
        "prioritized_actions": prioritized_actions,
    }
    return report


def render_markdown_report(report: dict[str, Any]) -> str:
    generation = report.get("generation_evidence", {})
    validation = report.get("validation", {})
    validation_runs = report.get("validation_run_results", {})
    schema_changes = report.get("schema_changes", {})
    ai_risk = report.get("ai_system_risk", {})
    violations = report.get("violations", {})
    actions = report.get("prioritized_actions", []) or []
    deep_dive = report.get("violation_deep_dive", []) or []
    health_breakdown = report.get("health_score_breakdown", {})

    lines = [
        "# Auto-generated Enforcer Report",
        "",
        "## Generation Evidence",
        f"- Generated at: `{report.get('timestamp', 'unknown')}`",
        f"- Source bundle: `{generation.get('machine_generated_from', 'unknown')}`",
    ]
    if generation.get("validation_reports"):
        lines.append("- Validation reports:")
        for item in generation.get("validation_reports", []):
            lines.append(f"  - `{item}`")
    if generation.get("ai_checks"):
        lines.append(f"- AI checks source: `{generation.get('ai_checks')}`")
    if generation.get("schema_evolution"):
        lines.append(f"- Schema evolution source: `{generation.get('schema_evolution')}`")
    if generation.get("violation_log"):
        lines.append(f"- Violation log source: `{generation.get('violation_log')}`")
    if generation.get("violation_log_run_id"):
        lines.append(f"- Latest violation run: `{generation.get('violation_log_run_id')}`")
    if generation.get("violation_log_run_timestamp"):
        lines.append(f"- Latest violation run timestamp: `{generation.get('violation_log_run_timestamp')}`")

    lines.extend([
        "",
        "## Health Score",
        f"- Score: **{report.get('data_health_score', 0)} / 100**",
        f"- Formula: `round(({health_breakdown.get('checks_passed', 0)} / {health_breakdown.get('total_checks', 0)}) * 100) - ({health_breakdown.get('critical_violations', 0)} * {config.critical_violation_penalty})`",
        f"- Base score before penalties: `{health_breakdown.get('base_score', 0)}`",
        f"- Critical validation failures: `{health_breakdown.get('critical_violations', 0)}`",
        "",
        "## Validation Run Results",
        f"- Total checks: `{validation.get('total_checks', 0)}`",
        f"- Passed: `{validation.get('total_passed', 0)}`",
        f"- Failed: `{validation.get('total_failed', 0)}`",
        f"- Warned: `{validation.get('total_warned', 0)}`",
        f"- Errored: `{validation.get('total_errored', 0)}`",
        "",
        "### Non-pass checks",
    ])
    if validation_runs.get("non_pass_results"):
        for row in validation_runs.get("non_pass_results", [])[:10]:
            actual = row.get("actual_value")
            expected = row.get("expected")
            details = f"actual `{actual}` vs expected `{expected}`" if actual is not None or expected is not None else row.get("message", "")
            lines.append(
                f"- `{row.get('clause', 'unknown')}` on `{row.get('field', 'unknown')}` in `{row.get('contract_path', 'unknown')}` "
                f"[{row.get('severity', 'UNKNOWN')}] {details}"
            )
    else:
        lines.append("- No non-pass validation checks were recorded.")

    lines.extend([
        "",
        "## Violations by Severity",
        f"- Latest violation log count: `{violations.get('total_count', 0)}`",
        f"- Severity breakdown: `{json.dumps(violations.get('by_severity', {}), sort_keys=True)}`",
        "",
        "## Schema Changes Detected",
        f"- Verdict: `{schema_changes.get('verdict', 'unknown')}`",
        f"- Breaking changes: `{schema_changes.get('total_breaking', 0)}`",
        f"- Compatible changes: `{schema_changes.get('total_compatible', 0)}`",
    ])
    for change in (schema_changes.get("changes") or [])[:10]:
        lines.append(
            f"- `{change.get('field_path', 'unknown')}` via `{change.get('clause', 'unknown')}` "
            f"[{change.get('severity', 'UNKNOWN')}] {change.get('reason', '')}"
        )

    lines.extend([
        "",
        "## AI System Risk Assessment",
        f"- Risk level: `{ai_risk.get('risk_level', 'LOW')}`",
        f"- Checks run: `{ai_risk.get('checks_run', 0)}`",
        f"- Passed: `{ai_risk.get('passed', 0)}`",
        f"- Warned: `{ai_risk.get('warned', 0)}`",
        f"- Failed: `{ai_risk.get('failed', 0)}`",
        f"- Summary: {ai_risk.get('summary', 'No AI checks run')}",
    ])
    for finding in ai_risk.get("findings", [])[:5]:
        lines.append(f"- Finding: {finding}")

    lines.extend([
        "",
        "## Prioritized Recommended Actions",
    ])
    if actions:
        for idx, action in enumerate(actions[:3], start=1):
            lines.append(f"{idx}. `{action.get('file', 'unknown')}` | `{action.get('field', 'unknown')}` | `{action.get('clause', 'unknown')}`")
            lines.append(f"   - Severity: `{action.get('severity', 'UNKNOWN')}`")
            lines.append(f"   - Rationale: {action.get('rationale', '')}")
            if action.get("downstream_consumer"):
                lines.append(f"   - Downstream consumer: `{action.get('downstream_consumer')}`")
    else:
        lines.append("- No prioritized actions were generated.")

    lines.extend([
        "",
        "## Violation Deep Dive",
    ])
    if deep_dive:
        top = deep_dive[0]
        lines.append(f"- Highest-severity entry: `{top.get('check_id', 'unknown')}` on `{top.get('field', 'unknown')}`")
        lines.append(f"- Producer file: `{top.get('producer_file') or 'unknown'}`")
        lines.append(f"- Message: {top.get('message', '')}")
        blast_radius = top.get("blast_radius") or {}
        if isinstance(blast_radius, dict):
            direct = blast_radius.get("direct_subscribers") or []
            if direct:
                lines.append("- Direct subscribers:")
                for sub in direct[:5]:
                    if isinstance(sub, dict):
                        lines.append(f"  - `{sub.get('target_contract') or sub.get('target') or 'unknown'}`")
    else:
        lines.append("- No violation log entries available for deep dive.")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate the rubric-aware enforcer report",
    )
    parser.add_argument(
        "--validation-reports",
        nargs="+",
        required=True,
        help="Path(s) to validation report JSON files",
    )
    parser.add_argument(
        "--ai-checks",
        help="Path to AI checks report JSON",
    )
    parser.add_argument(
        "--schema-evolution",
        help="Path to schema evolution report JSON",
    )
    parser.add_argument(
        "--violation-log",
        help="Path to violation log JSONL",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write the report",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Output format",
    )
    args = parser.parse_args(argv)

    validation_reports: list[dict[str, Any]] = []
    validation_report_paths: list[str] = []
    for report_path in args.validation_reports:
        path = Path(report_path)
        if path.exists():
            validation_reports.append(load_validation_report(path))
            validation_report_paths.append(str(path))

    if not validation_reports:
        logger.error("No validation reports found")
        return 1

    ai_checks = _read_optional_json(args.ai_checks)
    schema_evolution = _read_optional_json(args.schema_evolution)
    violation_path = Path(args.violation_log) if args.violation_log else None
    violation_data = _load_violation_runs(violation_path)
    source_manifest = _source_manifest(
        validation_report_paths,
        str(Path(args.ai_checks)) if args.ai_checks else None,
        str(Path(args.schema_evolution)) if args.schema_evolution else None,
        str(violation_path) if violation_path else None,
        violation_data,
    )

    report = generate_report(
        validation_reports,
        ai_checks,
        schema_evolution,
        violation_path,
        source_manifest=source_manifest,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "markdown":
        rendered = render_markdown_report(report)
        output_path.write_text(rendered, encoding="utf-8")
        print(rendered)
    else:
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(json.dumps(report, indent=2))

    logger.info("Report written to %s", output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
