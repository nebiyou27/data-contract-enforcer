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
    """Compute data health score from 0-100.

    Formula:
        base  = (checks_passed / total_checks) * 100
        score = base - (20 * critical_violation_count)
        score = clamp(score, 0, 100)

    CRITICAL violations are results with severity == 'CRITICAL' and status == 'FAIL'.
    """
    total_checks = 0
    checks_passed = 0
    critical_violations = 0

    for report in validation_reports:
        total_checks += report.get("total_checks", 0)
        checks_passed += report.get("passed", 0)
        # Count CRITICAL-severity FAILs across all result entries
        for result in report.get("results", []):
            if result.get("status") == "FAIL" and result.get("severity") == "CRITICAL":
                critical_violations += 1

    if total_checks == 0:
        base = 0.0
    else:
        base = (checks_passed / total_checks) * 100

    score = base - (20 * critical_violations)
    return max(0, min(100, int(round(score))))


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
            violation_count,
            validation_reports=validation_reports,
        )
    }
    
    return report


def make_recommendations(
    validation_agg: dict,
    ai_agg: dict,
    schema_agg: dict,
    violation_count: int,
    validation_reports: list[dict] | None = None,
) -> list[str]:
    """Generate data-driven recommendations naming exact file paths and contract clauses."""
    recommendations: list[str] = []

    # Collect failing check details from raw reports
    failing_checks: list[dict] = []
    warned_checks: list[dict] = []
    if validation_reports:
        for report in validation_reports:
            contract_id = report.get("contract_id", "unknown")
            contract_path = f"generated_contracts/{contract_id}.yaml"
            for result in report.get("results", []):
                if result.get("status") == "FAIL":
                    failing_checks.append(
                        {
                            "contract_path": contract_path,
                            "check_id": result.get("check_id", ""),
                            "column": result.get("column_name", ""),
                            "severity": result.get("severity", ""),
                            "message": result.get("message", ""),
                        }
                    )
                elif result.get("status") == "WARN":
                    warned_checks.append(
                        {
                            "contract_path": contract_path,
                            "check_id": result.get("check_id", ""),
                            "column": result.get("column_name", ""),
                            "message": result.get("message", ""),
                        }
                    )

    # Specific recommendations for each CRITICAL/HIGH failure
    critical_shown = 0
    for fc in failing_checks:
        if fc["severity"] == "CRITICAL" and critical_shown < 3:
            recommendations.append(
                f"CRITICAL: Fix clause '{fc['check_id']}' in "
                f"{fc['contract_path']} — {fc['message']}. "
                f"Downstream consumers reading '{fc['column']}' will receive corrupt data."
            )
            critical_shown += 1

    high_shown = 0
    for fc in failing_checks:
        if fc["severity"] == "HIGH" and high_shown < 3:
            recommendations.append(
                f"HIGH: Resolve '{fc['check_id']}' in "
                f"{fc['contract_path']} — {fc['message']}."
            )
            high_shown += 1

    remaining = validation_agg.get("total_failed", 0) - critical_shown - high_shown
    if remaining > 0:
        recommendations.append(
            f"Fix {remaining} additional failing checks listed in validation_reports/."
        )

    # Drift warnings with specific clause references
    if warned_checks:
        top = warned_checks[:2]
        for wc in top:
            recommendations.append(
                f"WARN: Investigate drift on clause '{wc['check_id']}' in "
                f"{wc['contract_path']} — {wc['message']}. "
                f"Refresh schema_snapshots/baselines.json if the distribution shift is intentional."
            )
        if len(warned_checks) > 2:
            recommendations.append(
                f"Investigate {len(warned_checks) - 2} additional drift warnings in "
                "validation_reports/."
            )

    # AI check failures
    if ai_agg.get("failed", 0) > 0:
        recommendations.append(
            f"Review {ai_agg['failed']} AI check failure(s) in "
            "validation_reports/ai_checks.json — "
            "check contracts/ai_extensions.py clause 'llm_output_violation_rate' "
            "and 'embedding_drift' for threshold breaches."
        )

    # Schema breaking changes
    if schema_agg.get("verdict") == "breaking":
        recommendations.append(
            f"Schema has {schema_agg['breaking_changes']} breaking change(s) — "
            "update contract_registry/subscriptions.yaml with a migration plan "
            "before shipping. Run: python contracts/schema_analyzer.py "
            "--contract-id <id> to generate the rollback plan."
        )

    # Violation log
    if violation_count > 0:
        recommendations.append(
            f"{violation_count} violation(s) recorded in violation_log/violations.jsonl — "
            "audit blame_chain entries and notify owners listed under "
            "blast_radius.direct_subscribers."
        )

    if not recommendations:
        recommendations.append(
            "Data contracts are healthy. Schedule a baseline refresh via: "
            "python contracts/generator.py --source <data> --contract-id <id> --output generated_contracts/"
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
