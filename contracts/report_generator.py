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
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from contracts.log_config import configure_logging
    from contracts.config import config
except ModuleNotFoundError:
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
    from contracts.log_config import configure_logging
    from contracts.config import config

logger = logging.getLogger(__name__)


def load_validation_report(path: Path) -> dict[str, Any]:
    """Load a validation report JSON file."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _contract_path_for_report(contract_id: str) -> str:
    return f"generated_contracts/{contract_id}.yaml"


def _field_path(result: dict[str, Any]) -> str:
    check_id = str(result.get("check_id", "") or "")
    column = str(result.get("column_name", "") or "")
    if "." in check_id:
        parts = check_id.split(".")
        if len(parts) >= 2:
            table = parts[0]
            if column:
                return f"{table}.{column}"
            return ".".join(parts[:2])
    return column or check_id or "unknown"


def _result_clause(result: dict[str, Any]) -> str:
    check_id = str(result.get("check_id", "") or "")
    check_type = str(result.get("check_type", "") or "")
    if check_id:
        return check_id
    return check_type or "unknown"


def _severity_rank(severity: str) -> int:
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "COMPAT": 4, "UNKNOWN": 5}
    return order.get(str(severity).upper(), 5)


def _is_violation_record(record: dict[str, Any]) -> bool:
    if record.get("record_type") == "run_header":
        return False
    if record.get("violation_id"):
        return True
    status = str(record.get("status", "") or "").upper()
    return status in {"WARN", "FAIL", "ERROR"}


def _load_violation_runs(violation_log_path: Path | None) -> dict[str, Any]:
    """Parse the append-only violation log into runs and entries.

    If the log contains explicit run headers, the latest run is used as the
    primary evidence source. Otherwise, the file is treated as a flat list of
    violation records for backwards compatibility.
    """
    if not violation_log_path or not violation_log_path.exists():
        return {
            "runs": [],
            "latest_run": None,
            "violation_count": 0,
            "sample_ids": [],
            "by_severity": {},
            "records": [],
        }

    runs: list[dict[str, Any]] = []
    current_run: dict[str, Any] | None = None
    flat_records: list[dict[str, Any]] = []

    with open(violation_log_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            flat_records.append(record)

            if record.get("record_type") == "run_header":
                if current_run is not None:
                    runs.append(current_run)
                current_run = {
                    "header": record,
                    "records": [],
                }
                continue

            if current_run is None:
                current_run = {"header": None, "records": []}
            current_run["records"].append(record)

    if current_run is not None:
        runs.append(current_run)

    if not runs:
        violation_records = [record for record in flat_records if _is_violation_record(record)]
        by_severity: dict[str, int] = {}
        for record in violation_records:
            severity = str(record.get("severity", "UNKNOWN") or "UNKNOWN").upper()
            by_severity[severity] = by_severity.get(severity, 0) + 1
        return {
            "runs": [],
            "latest_run": None,
            "violation_count": len(violation_records),
            "sample_ids": [str(record.get("violation_id", "unknown")) for record in violation_records[:10]],
            "by_severity": by_severity,
            "records": violation_records,
        }

    def _run_sort_key(run: dict[str, Any]) -> tuple[str, str]:
        header = run.get("header") or {}
        run_timestamp = str(header.get("run_timestamp", "") or "")
        run_id = str(header.get("run_id", "") or "")
        return (run_timestamp, run_id)

    latest_run = max(runs, key=_run_sort_key)
    violation_records = [record for record in latest_run.get("records", []) if _is_violation_record(record)]
    by_severity: dict[str, int] = {}
    for record in violation_records:
        severity = str(record.get("severity", "UNKNOWN") or "UNKNOWN").upper()
        by_severity[severity] = by_severity.get(severity, 0) + 1

    return {
        "runs": runs,
        "latest_run": latest_run,
        "violation_count": len(violation_records),
        "sample_ids": [str(record.get("violation_id", "unknown")) for record in violation_records[:10]],
        "by_severity": by_severity,
        "records": violation_records,
    }


def _count_critical_failures(validation_reports: list[dict]) -> int:
    count = 0
    for report in validation_reports:
        for result in report.get("results", []):
            if result.get("status") == "FAIL" and str(result.get("severity", "")).upper() == "CRITICAL":
                count += 1
    return count


def _validation_issue_details(validation_reports: list[dict]) -> dict[str, Any]:
    """Collect non-pass validation results and severity counts."""
    severity_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0}
    issue_rows: list[dict[str, Any]] = []

    for report in validation_reports:
        contract_id = str(report.get("contract_id", "unknown") or "unknown")
        report_id = report.get("report_id")
        contract_path = _contract_path_for_report(contract_id)
        for result in report.get("results", []):
            status = str(result.get("status", "UNKNOWN") or "UNKNOWN").upper()
            severity = str(result.get("severity", "UNKNOWN") or "UNKNOWN").upper()
            status_counts[status] = status_counts.get(status, 0) + 1
            if status != "PASS":
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                issue_rows.append(
                    {
                        "contract_id": contract_id,
                        "report_id": report_id,
                        "contract_path": contract_path,
                        "check_id": result.get("check_id", ""),
                        "check_type": result.get("check_type", ""),
                        "status": status,
                        "severity": severity,
                        "field": _field_path(result),
                        "clause": _result_clause(result),
                        "actual_value": result.get("actual_value"),
                        "expected": result.get("expected"),
                        "message": result.get("message", ""),
                        "records_failing": result.get("records_failing", 0),
                    }
                )

    issue_rows.sort(key=lambda row: (_severity_rank(row["severity"]), row["contract_id"], row["clause"]))
    return {
        "status_counts": status_counts,
        "severity_counts": severity_counts,
        "issue_rows": issue_rows,
    }


def _schema_change_details(schema_evolution: dict | None, contract_id_hint: str | None = None) -> dict[str, Any]:
    if not schema_evolution:
        return {
            "verdict": "unknown",
            "total_breaking": 0,
            "total_compatible": 0,
            "changes": [],
            "rollback_plan": None,
            "consumer_failure_analysis": [],
            "summary": "No schema evolution analysis",
        }

    contract_id = str(schema_evolution.get("contract_id") or contract_id_hint or "unknown")
    contract_path = _contract_path_for_report(contract_id)
    changes: list[dict[str, Any]] = []

    breaking_changes = schema_evolution.get("breaking_changes")
    if isinstance(breaking_changes, list):
        for change in breaking_changes:
            if not isinstance(change, dict):
                continue
            table = str(change.get("table", "") or "")
            field = str(change.get("field", "") or "")
            changes.append(
                {
                    "severity": str(change.get("severity", "UNKNOWN") or "UNKNOWN").upper(),
                    "type": change.get("type", "unknown"),
                    "table": table,
                    "field": field,
                    "field_path": f"{table}.{field}" if table and field else field or table or "unknown",
                    "clause": change.get("type", "schema_change"),
                    "reason": change.get("reason", ""),
                    "contract_path": contract_path,
                }
            )
    else:
        for result in schema_evolution.get("results", []):
            if str(result.get("status", "")).upper() == "PASS":
                continue
            table_field = _field_path(result)
            changes.append(
                {
                    "severity": str(result.get("severity", "UNKNOWN") or "UNKNOWN").upper(),
                    "type": result.get("check_type", "unknown"),
                    "table": table_field.split(".", 1)[0] if "." in table_field else "",
                    "field": table_field.split(".", 1)[1] if "." in table_field else table_field,
                    "field_path": table_field,
                    "clause": result.get("check_id", result.get("check_type", "schema_change")),
                    "reason": result.get("message", ""),
                    "contract_path": contract_path,
                }
            )

    schema_summary = schema_evolution.get("schema_summary") or {}
    if not changes:
        for item in schema_summary.get("missing_columns", []) or []:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table", "") or "")
            column = str(item.get("column", "") or "")
            changes.append(
                {
                    "severity": "CRITICAL",
                    "type": "schema_missing",
                    "table": table,
                    "field": column,
                    "field_path": f"{table}.{column}" if table and column else column or table or "unknown",
                    "clause": "schema_missing",
                    "reason": f"Column '{column}' is missing from the data",
                    "contract_path": contract_path,
                }
            )
        for item in schema_summary.get("new_columns", []) or []:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table", "") or "")
            column = str(item.get("column", "") or "")
            changes.append(
                {
                    "severity": "MEDIUM",
                    "type": "schema_new_column",
                    "table": table,
                    "field": column,
                    "field_path": f"{table}.{column}" if table and column else column or table or "unknown",
                    "clause": "schema_new_column",
                    "reason": f"Column '{column}' exists in the data but is not in the contract",
                    "contract_path": contract_path,
                }
            )

    return {
        "verdict": schema_evolution.get("verdict", "unknown"),
        "total_breaking": schema_evolution.get("total_breaking", 0) or len(
            [c for c in changes if c["severity"] in {"CRITICAL", "HIGH"}]
        ),
        "total_compatible": schema_evolution.get("total_non_breaking", 0)
        or schema_evolution.get("total_compatible", 0)
        or len([c for c in changes if c["severity"] not in {"CRITICAL", "HIGH"}]),
        "changes": changes,
        "rollback_plan": schema_evolution.get("rollback_plan"),
        "consumer_failure_analysis": schema_evolution.get("consumer_failure_analysis", []),
        "summary": schema_evolution.get("summary", "Schema evolution analysis available"),
    }


def _ai_risk_details(ai_checks: dict | None) -> dict[str, Any]:
    if not ai_checks:
        return {
            "checks_run": 0,
            "passed": 0,
            "warned": 0,
            "failed": 0,
            "errored": 0,
            "risk_level": "LOW",
            "summary": "No AI checks run",
            "details": {},
        }

    summary = ai_checks.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    checks = ai_checks.get("checks", [])
    if not isinstance(checks, list):
        checks = []
    detail_block = ai_checks.get("details", {})
    if not isinstance(detail_block, dict):
        detail_block = {}

    failed = int(summary.get("failed", 0) or 0)
    warned = int(summary.get("warned", 0) or 0)
    risk_level = "HIGH" if failed > 0 else "MEDIUM" if warned > 0 else "LOW"

    drift = detail_block.get("embedding_drift") or {}
    prompt_validation = detail_block.get("prompt_input_validation") or detail_block.get("prompt_injection") or {}
    schema_validation = detail_block.get("llm_output_schema") or {}

    findings: list[str] = []
    if isinstance(drift, dict) and drift.get("findings"):
        findings.append(str(drift.get("findings")))
    if isinstance(prompt_validation, dict):
        if prompt_validation.get("warnings"):
            findings.append(str(prompt_validation.get("warnings")))
        if prompt_validation.get("message"):
            findings.append(str(prompt_validation.get("message")))
    if isinstance(schema_validation, dict) and schema_validation.get("message"):
        findings.append(str(schema_validation.get("message")))

    return {
        "checks_run": summary.get("total_checks", 0),
        "passed": summary.get("passed", 0),
        "warned": warned,
        "failed": failed,
        "errored": summary.get("errored", 0),
        "check_types": sorted({str(c.get("check_type")) for c in checks if isinstance(c, dict) and c.get("check_type")}),
        "risk_level": risk_level,
        "summary": ai_checks.get("summary", "AI checks available"),
        "details": detail_block,
        "findings": findings,
    }


def _build_action_candidates(
    validation_reports: list[dict],
    ai_checks: dict | None,
    schema_evolution: dict | None,
    violation_data: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    for issue in _validation_issue_details(validation_reports)["issue_rows"]:
        status = issue["status"]
        severity = issue["severity"]
        priority = 0 if status == "FAIL" and severity == "CRITICAL" else 1 if status == "FAIL" else 2
        candidates.append(
            {
                "priority": priority,
                "file": issue["contract_path"],
                "field": issue["field"],
                "clause": issue["clause"],
                "severity": severity,
                "title": f"Fix {issue['clause']} in {issue['field']}",
                "rationale": (
                    f"{issue['message']} (actual={issue['actual_value']}, expected={issue['expected']})"
                    if issue["actual_value"] is not None or issue["expected"] is not None
                    else issue["message"]
                ),
                "downstream_consumer": None,
            }
        )

    schema_details = _schema_change_details(schema_evolution)
    for change in schema_details["changes"]:
        priority = 1 if change["severity"] in {"CRITICAL", "HIGH"} else 3
        consumer = None
        if schema_details["consumer_failure_analysis"]:
            for analysis in schema_details["consumer_failure_analysis"]:
                analyzed_change = analysis.get("change", {}) if isinstance(analysis, dict) else {}
                if analyzed_change.get("type") == change["type"] and analyzed_change.get("field") == change["field"]:
                    affected = analysis.get("affected_subscribers", []) if isinstance(analysis, dict) else []
                    if affected:
                        consumer = affected[0].get("subscriber_contract") or affected[0].get("subscriber")
                    break
        candidates.append(
            {
                "priority": priority,
                "file": change["contract_path"],
                "field": change["field_path"],
                "clause": change["clause"],
                "severity": change["severity"],
                "title": f"Address {change['type']} for {change['field_path']}",
                "rationale": change["reason"],
                "downstream_consumer": consumer,
            }
        )

    if ai_checks:
        for check in ai_checks.get("checks", []) or []:
            if not isinstance(check, dict):
                continue
            status = str(check.get("status", "") or "").upper()
            if status == "PASS":
                continue
            severity = str(check.get("severity", "UNKNOWN") or "UNKNOWN").upper()
            field_name = str(check.get("field_name") or check.get("output_field") or "unknown")
            clause = str(check.get("check_type") or "ai_check")
            if clause == "llm_output_schema":
                clause = "contracts/ai_extensions.py::llm_output_schema"
            elif clause == "embedding_drift":
                clause = "contracts/ai_extensions.py::embedding_drift"
            elif clause == "prompt_injection":
                clause = "contracts/ai_extensions.py::prompt_input_validation"
            candidates.append(
                {
                    "priority": 2 if severity in {"HIGH", "CRITICAL"} else 4,
                    "file": "validation_reports/ai_checks.json",
                    "field": field_name,
                    "clause": clause,
                    "severity": severity,
                    "title": f"Investigate {clause} for {field_name}",
                    "rationale": str(check.get("message", "")),
                    "downstream_consumer": None,
                }
            )

    for entry in violation_data.get("records", []):
        if not isinstance(entry, dict):
            continue
        severity = str(entry.get("severity", "UNKNOWN") or "UNKNOWN").upper()
        field_name = str(entry.get("column_name") or entry.get("field") or "unknown")
        clause = str(entry.get("check_id") or entry.get("check_type") or "violation")
        blast_radius = entry.get("blast_radius") or {}
        consumer = None
        if isinstance(blast_radius, dict):
            direct = blast_radius.get("direct_subscribers") or []
            if direct and isinstance(direct, list) and isinstance(direct[0], dict):
                consumer = direct[0].get("target_contract") or direct[0].get("target")
        candidates.append(
            {
                "priority": 1 if severity == "CRITICAL" else 3 if severity == "HIGH" else 4,
                "file": "violation_log/violations.jsonl",
                "field": field_name,
                "clause": clause,
                "severity": severity,
                "title": f"Resolve logged violation {clause}",
                "rationale": str(entry.get("message", "")),
                "downstream_consumer": consumer,
            }
        )

    candidates.sort(key=lambda item: (item["priority"], _severity_rank(item["severity"]), item["file"], item["field"], item["clause"]))

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        key = (candidate["file"], candidate["field"], candidate["clause"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _render_action_text(action: dict[str, Any]) -> str:
    line = (
        f"{action['file']} | {action['field']} | {action['clause']} | "
        f"{action['severity']}: {action['rationale']}"
    )
    consumer = action.get("downstream_consumer")
    if consumer:
        line += f" | downstream consumer: {consumer}"
    return line


def _render_markdown_list(items: list[str], empty_text: str) -> str:
    if not items:
        return f"- {empty_text}"
    return "\n".join(f"- {item}" for item in items)


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
        critical_violations += _count_critical_failures([report])

    if total_checks == 0:
        base = 0.0
    else:
        base = (checks_passed / total_checks) * 100

    score = base - (config.critical_violation_penalty * critical_violations)
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
    severity_counts: dict[str, int] = {}
    non_pass_results: list[dict[str, Any]] = []
    
    for report in validation_reports:
        total_checks += report.get("total_checks", 0)
        total_passed += report.get("passed", 0)
        total_failed += report.get("failed", 0)
        total_warned += report.get("warned", 0)
        total_errored += report.get("errored", 0)
        
        contract_id = report.get("contract_id")
        if contract_id:
            contracts_validated.add(contract_id)

        for result in report.get("results", []):
            status = str(result.get("status", "") or "").upper()
            severity = str(result.get("severity", "") or "").upper()
            if status != "PASS":
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                non_pass_results.append(
                    {
                        "contract_id": contract_id or "unknown",
                        "contract_path": _contract_path_for_report(contract_id or "unknown"),
                        "field": _field_path(result),
                        "clause": _result_clause(result),
                        "check_id": result.get("check_id", ""),
                        "check_type": result.get("check_type", ""),
                        "status": status,
                        "severity": severity,
                        "actual_value": result.get("actual_value"),
                        "expected": result.get("expected"),
                        "message": result.get("message", ""),
                        "records_failing": result.get("records_failing", 0),
                    }
                )
    non_pass_results.sort(key=lambda row: (_severity_rank(row["severity"]), row["contract_id"], row["clause"]))
    
    return {
        "contracts_validated": list(contracts_validated),
        "contract_count": len(contracts_validated),
        "total_checks": total_checks,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_warned": total_warned,
        "total_errored": total_errored,
        "pass_rate_pct": round(total_passed / total_checks * 100, 1) if total_checks > 0 else 0,
        "severity_counts": severity_counts,
        "non_pass_results": non_pass_results,
    }


def aggregate_ai_results(ai_checks: dict | None) -> dict[str, Any]:
    """Aggregate AI check results."""
    if not ai_checks:
        return {
            "checks_run": 0,
            "passed": 0,
            "warned": 0,
            "failed": 0,
            "errored": 0,
            "summary": "No AI checks run",
            "risk_level": "LOW",
            "details": {},
            "findings": [],
        }
    
    summary = ai_checks.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    checks = ai_checks.get("checks", [])
    if not isinstance(checks, list):
        checks = []
    details = ai_checks.get("details", {})
    if not isinstance(details, dict):
        details = {}
    risk_level = "HIGH" if summary.get("failed", 0) else "MEDIUM" if summary.get("warned", 0) else "LOW"
    
    return {
        "checks_run": summary.get("total_checks", 0),
        "passed": summary.get("passed", 0),
        "warned": summary.get("warned", 0),
        "failed": summary.get("failed", 0),
        "errored": summary.get("errored", 0),
        "check_types": sorted({str(c.get("check_type")) for c in checks if isinstance(c, dict) and c.get("check_type")}),
        "summary": f"AI checks: {summary.get('passed', 0)} PASS, "
                   f"{summary.get('warned', 0)} WARN, {summary.get('failed', 0)} FAIL"
                   if checks else "No AI checks run",
        "risk_level": risk_level,
        "details": details,
        "findings": _ai_risk_details(ai_checks).get("findings", []),
    }


def aggregate_schema_evolution(schema_evolution: dict | None) -> dict[str, Any]:
    """Aggregate schema evolution analysis."""
    details = _schema_change_details(schema_evolution)
    return {
        "verdict": details["verdict"],
        "breaking_changes": details["total_breaking"],
        "non_breaking_changes": details["total_compatible"],
        "summary": details["summary"],
        "changes": details["changes"],
        "rollback_plan": details["rollback_plan"],
        "consumer_failure_analysis": details["consumer_failure_analysis"],
    }


def count_violations(violation_log_path: Path | None) -> tuple[int, list[str]]:
    """Count violations and extract sample IDs from the latest run."""
    parsed = _load_violation_runs(violation_log_path)
    return parsed["violation_count"], parsed["sample_ids"]


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
    configure_logging()

    try:
        # Load validation reports
        validation_reports = []
        for report_path in args.validation_reports:
            path = Path(report_path)
            if path.exists():
                report = load_validation_report(path)
                validation_reports.append(report)
        
        if not validation_reports:
            logger.error("No validation reports found")
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
        
        logger.info("Report written to %s", output_path)
        print(json.dumps(report, indent=2))

        sys.exit(0)

    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
