#!/usr/bin/env python3
"""
contracts/report_generator.py -- Final enforcer report generation

Aggregates all validation runs, AI checks, and schema evolution analysis
into a machine-generated summary report with data health score (0-100).

The report combines:
  - Baseline validation results
  - Drift detection findings
  - AI-driven checks
  - Schema evolution verdict
  - Blast radius attribution

Usage:
  python contracts/report_generator.py \
    --validation-reports validation_reports/*.json \
    --ai-checks validation_reports/ai_checks.json \
    --schema-evolution validation_reports/schema_evolution.json \
    --violation-log violation_log/violations.jsonl \
    --output enforcer_report/report_data.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def load_validation_report(path: Path) -> dict[str, Any]:
    """Load a validation report JSON file."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def compute_data_health_score(
    validation_reports: list[dict],
    ai_checks: dict | None = None,
    schema_evolution: dict | None = None,
    violation_count: int = 0
) -> int:
    """
    Compute data health score from 0-100.
    
    Scoring:
      - Base: 100 points
      - Validation failures: -10 per failed check
      - Drift warnings: -5 per warning
      - AI check failures: -8 per failure
      - Schema breaking changes: -15
      - Known violations: -5 per violation
    """
    score = 100
    
    # Process validation reports
    for report in validation_reports:
        failed = report.get("failed", 0)
        warned = report.get("warned", 0)
        
        score -= failed * 10
        score -= warned * 5
    
    # Process AI checks
    if ai_checks:
        checks = ai_checks.get("checks", [])
        for check in checks:
            if check.get("status") == "FAIL":
                score -= 8
            elif check.get("status") == "WARN":
                score -= 3
    
    # Process schema evolution
    if schema_evolution:
        if schema_evolution.get("verdict") == "breaking":
            breaking_count = schema_evolution.get("total_breaking", 0)
            score -= breaking_count * 15
    
    # Process violations
    score -= violation_count * 5
    
    # Clamp to 0-100
    return max(0, min(100, score))


def aggregate_validation_results(
    validation_reports: list[dict]
) -> dict[str, Any]:
    """Aggregate metrics across all validation reports."""
    total_checks = 0
    total_passed = 0
    total_failed = 0
    total_warned = 0
    total_errored = 0
    
    contracts_validated = set()
    
    for report in validation_reports:
        total_checks += report.get("total_checks", 0)
        total_passed += report.get("passed", 0)
        total_failed += report.get("failed", 0)
        total_warned += report.get("warned", 0)
        total_errored += report.get("errored", 0)
        
        contract_id = report.get("contract_id")
        if contract_id:
            contracts_validated.add(contract_id)
    
    return {
        "contracts_validated": list(contracts_validated),
        "contract_count": len(contracts_validated),
        "total_checks": total_checks,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_warned": total_warned,
        "total_errored": total_errored,
        "pass_rate_pct": round(total_passed / total_checks * 100, 1) if total_checks > 0 else 0
    }


def aggregate_ai_results(ai_checks: dict | None) -> dict[str, Any]:
    """Aggregate AI check results."""
    if not ai_checks:
        return {
            "checks_run": 0,
            "passed": 0,
            "warned": 0,
            "failed": 0,
            "summary": "No AI checks run"
        }
    
    summary = ai_checks.get("summary", {})
    checks = ai_checks.get("checks", [])
    
    return {
        "checks_run": summary.get("total_checks", 0),
        "passed": summary.get("passed", 0),
        "warned": summary.get("warned", 0),
        "failed": summary.get("failed", 0),
        "errored": summary.get("errored", 0),
        "check_types": list(set(c.get("check_type") for c in checks if c.get("check_type"))),
        "summary": f"AI checks: {summary.get('passed', 0)} PASS, "
                   f"{summary.get('warned', 0)} WARN, {summary.get('failed', 0)} FAIL"
    }


def aggregate_schema_evolution(schema_evolution: dict | None) -> dict[str, Any]:
    """Aggregate schema evolution analysis."""
    if not schema_evolution:
        return {
            "verdict": "unknown",
            "breaking_changes": 0,
            "non_breaking_changes": 0,
            "summary": "No schema evolution analysis"
        }
    
    return {
        "verdict": schema_evolution.get("verdict", "unknown"),
        "breaking_changes": schema_evolution.get("total_breaking", 0),
        "non_breaking_changes": schema_evolution.get("total_non_breaking", 0),
        "summary": (
            f"Schema evolution: {schema_evolution.get('verdict', 'unknown')} - "
            f"{schema_evolution.get('total_breaking', 0)} breaking changes, "
            f"{schema_evolution.get('total_non_breaking', 0)} non-breaking"
        )
    }


def count_violations(violation_log_path: Path | None) -> tuple[int, list[str]]:
    """Count violations and extract sample IDs."""
    if not violation_log_path or not violation_log_path.exists():
        return 0, []
    
    violation_ids = []
    with open(violation_log_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                record = json.loads(line)
                violation_ids.append(record.get("violation_id", "unknown"))
            except json.JSONDecodeError:
                pass
    
    return len(violation_ids), violation_ids[:10]


def generate_report(
    validation_reports: list[dict],
    ai_checks: dict | None = None,
    schema_evolution: dict | None = None,
    violation_log_path: Path | None = None
) -> dict[str, Any]:
    """Generate comprehensive enforcer report."""
    
    violation_count, sample_violations = count_violations(violation_log_path)
    
    # Compute aggregates
    validation_agg = aggregate_validation_results(validation_reports)
    ai_agg = aggregate_ai_results(ai_checks)
    schema_agg = aggregate_schema_evolution(schema_evolution)
    
    # Compute health score
    health_score = compute_data_health_score(
        validation_reports,
        ai_checks,
        schema_evolution,
        violation_count
    )
    
    # Build report
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "report_format_version": "1.0",
        "data_health_score": health_score,
        "health_score_explanation": (
            f"Score calculated from {validation_agg['total_checks']} validation checks "
            f"({validation_agg['pass_rate_pct']}% passing), "
            f"{ai_agg['checks_run']} AI checks, "
            f"schema verdict '{schema_agg['verdict']}', "
            f"and {violation_count} recorded violations"
        ),
        "validation": validation_agg,
        "ai_checks": ai_agg,
        "schema_evolution": schema_agg,
        "violations": {
            "total_count": violation_count,
            "sample_ids": sample_violations
        },
        "verdict": (
            "PASS" if (
                validation_agg["total_failed"] == 0 and
                ai_agg["failed"] == 0 and
                schema_agg["verdict"] == "compatible"
            ) else
            "ATTENTION_REQUIRED" if (
                validation_agg["total_failed"] == 0 and
                ai_agg["failed"] == 0
            ) else
            "ISSUES_DETECTED"
        ),
        "recommendations": make_recommendations(
            validation_agg,
            ai_agg,
            schema_agg,
            violation_count
        )
    }
    
    return report


def make_recommendations(
    validation_agg: dict,
    ai_agg: dict,
    schema_agg: dict,
    violation_count: int
) -> list[str]:
    """Generate actionable recommendations based on findings."""
    recommendations = []
    
    if validation_agg["total_failed"] > 0:
        recommendations.append(
            f"Fix {validation_agg['total_failed']} failing validation checks before deploying"
        )
    
    if validation_agg["total_warned"] > 0:
        recommendations.append(
            f"Investigate {validation_agg['total_warned']} drift warnings - "
            "data distribution may be changing"
        )
    
    if ai_agg["failed"] > 0:
        recommendations.append(
            f"Review {ai_agg['failed']} AI check failures - "
            "potential LLM output quality or prompt injection issues"
        )
    
    if schema_agg["verdict"] == "breaking":
        recommendations.append(
            f"Schema has {schema_agg['breaking_changes']} breaking changes - "
            "update registry and notify downstream subscribers"
        )
    
    if violation_count > 0:
        recommendations.append(
            f"{violation_count} violations recorded - "
            "perform post-incident review and update contracts if needed"
        )
    
    if not recommendations:
        recommendations.append(
            "Data contracts are healthy. Continue regular monitoring with baseline refreshes."
        )
    
    return recommendations


def main():
    parser = argparse.ArgumentParser(
        description="Generate comprehensive enforcer report"
    )
    parser.add_argument(
        "--validation-reports",
        nargs="+",
        required=True,
        help="Path(s) to validation report JSON files"
    )
    parser.add_argument(
        "--ai-checks",
        help="Path to AI checks report JSON"
    )
    parser.add_argument(
        "--schema-evolution",
        help="Path to schema evolution report JSON"
    )
    parser.add_argument(
        "--violation-log",
        help="Path to violation log JSONL"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write final report JSON"
    )
    
    args = parser.parse_args()
    
    try:
        # Load validation reports
        validation_reports = []
        for report_path in args.validation_reports:
            path = Path(report_path)
            if path.exists():
                report = load_validation_report(path)
                validation_reports.append(report)
        
        if not validation_reports:
            print("ERROR: No validation reports found", file=sys.stderr)
            sys.exit(1)
        
        # Load AI checks if available
        ai_checks = None
        if args.ai_checks and Path(args.ai_checks).exists():
            with open(args.ai_checks, "r", encoding="utf-8") as fh:
                ai_checks = json.load(fh)
        
        # Load schema evolution if available
        schema_evolution = None
        if args.schema_evolution and Path(args.schema_evolution).exists():
            with open(args.schema_evolution, "r", encoding="utf-8") as fh:
                schema_evolution = json.load(fh)
        
        # Generate report
        report = generate_report(
            validation_reports,
            ai_checks,
            schema_evolution,
            Path(args.violation_log) if args.violation_log else None
        )
        
        # Write report
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        
        print(f"Report written to {output_path}")
        print(json.dumps(report, indent=2))
        
        sys.exit(0)
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
