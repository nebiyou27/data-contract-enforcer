#!/usr/bin/env python3
"""
contracts/runner.py -- Data Contract Validation Runner

Reads a Bitol contract YAML + data JSONL, checks every clause, and produces
a structured JSON validation report.

Check order:
  1. Schema evolution: compare observed columns to declared contract fields
  2. Structural: required fields, type match, enum conformance, UUID pattern, date-time format
  3. Statistical: min/max range, then five drift sub-checks vs baseline
     drift_mean:          z-score on column mean          (WARN >2σ, FAIL >3σ)
     drift_variance:      stddev ratio                    (WARN >2×/<0.25×, FAIL >4×)
     drift_outliers:      new extremes outside baseline   (WARN one end, FAIL both ends)
     drift_null_fraction: null-fraction growth            (WARN >5 pp, FAIL >20 pp)
     drift_cardinality:   unique-value spike or collapse  (WARN >2×/<0.5×, FAIL >5×)

Never crashes -- if a check can't run, it returns status "ERROR" and continues.

Usage:
  python contracts/runner.py \
    --contract generated_contracts/week3-document-refinery-extractions.yaml \
    --data outputs/week3/extractions.jsonl \
    --output validation_reports/week3_baseline.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Import flatten helpers from generator (same package)
try:
    from contracts.generator import (
        flatten_documents,
        flatten_entities,
        flatten_event_metadata,
        flatten_events,
        flatten_facts,
        flatten_lineage_edges,
        flatten_lineage_nodes,
        flatten_trace_nodes,
        load_jsonl,
    )
    from contracts.attributor import (
        DEFAULT_REGISTRY_PATH,
        attribute_violation,
        contract_source_label,
        load_lineage_graph,
        load_registry,
    )
except ModuleNotFoundError:
    # Direct script invocation: add project root to path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from contracts.generator import (
        flatten_documents,
        flatten_entities,
        flatten_event_metadata,
        flatten_events,
        flatten_facts,
        flatten_lineage_edges,
        flatten_lineage_nodes,
        flatten_trace_nodes,
        load_jsonl,
    )
    from contracts.attributor import (
        DEFAULT_REGISTRY_PATH,
        attribute_violation,
        contract_source_label,
        load_lineage_graph,
        load_registry,
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"  # date part
    r"[T ]\d{2}:\d{2}:\d{2}"  # time part
    r"(\.\d+)?"  # optional fractional seconds
    r"(Z|[+-]\d{2}:?\d{2})?$"  # optional timezone
)

BASELINES_PATH = Path("schema_snapshots") / "baselines.json"
VIOLATION_LOG_PATH = Path("violation_log") / "violations.jsonl"

# Map Bitol logical types to acceptable pandas dtypes
TYPE_MAP: dict[str, set[str]] = {
    "string": {"object", "string"},
    "integer": {"int64", "Int64", "int32", "int16", "float64"},  # float64 allowed (pandas upcast)
    "number": {"float64", "Float64", "float32", "int64"},
    "boolean": {"bool", "boolean"},
}

# ---------------------------------------------------------------------------
# Check result builder
# ---------------------------------------------------------------------------


def _result(
    check_id: str,
    column_name: str,
    check_type: str,
    status: str,
    actual_value: Any,
    expected: Any,
    severity: str,
    records_failing: int = 0,
    sample_failing: list | None = None,
    message: str = "",
) -> dict:
    return {
        "check_id": check_id,
        "column_name": column_name,
        "check_type": check_type,
        "status": status,
        "actual_value": str(actual_value),
        "expected": str(expected),
        "severity": severity,
        "records_failing": records_failing,
        "sample_failing": sample_failing or [],
        "message": message,
    }


# ---------------------------------------------------------------------------
# Schema evolution checks
# ---------------------------------------------------------------------------


def check_schema_evolution(table: str, fields: list[dict], df: pd.DataFrame) -> list[dict]:
    """Compare declared contract fields with observed DataFrame columns."""
    expected_columns = {field["name"] for field in fields}
    actual_columns = set(df.columns)
    results: list[dict] = []

    missing_columns = sorted(expected_columns - actual_columns)
    extra_columns = sorted(actual_columns - expected_columns)

    for col in missing_columns:
        results.append(_result(
            f"{table}.{col}.schema_missing",
            col,
            "schema_missing",
            "FAIL",
            "missing from data",
            "present in contract",
            "CRITICAL",
            message=(
                f"Column '{col}' is declared in table '{table}' but missing from the data"
            ),
        ))

    for col in extra_columns:
        results.append(_result(
            f"{table}.{col}.schema_new_column",
            col,
            "schema_new_column",
            "WARN",
            "present in data",
            "absent from contract",
            "MEDIUM",
            message=(
                f"Column '{col}' is present in the data for table '{table}' "
                "but not declared in the contract"
            ),
        ))

    return results


def summarize_schema_evolution(results: list[dict]) -> dict:
    """Summarize schema-evolution results for the report header."""
    missing_columns: list[dict] = []
    new_columns: list[dict] = []

    for result in results:
        if result["check_type"] == "schema_missing":
            missing_columns.append(
                {
                    "table": result["check_id"].rsplit(".", 2)[0],
                    "column": result["column_name"],
                }
            )
        elif result["check_type"] == "schema_new_column":
            new_columns.append(
                {
                    "table": result["check_id"].rsplit(".", 2)[0],
                    "column": result["column_name"],
                }
            )

    return {
        "missing_columns": missing_columns,
        "new_columns": new_columns,
    }


# ---------------------------------------------------------------------------
# Producer-side schema-evolution gate
# ---------------------------------------------------------------------------


def check_producer_evolution_gate(
    proposed_fields: list[str],
    current_fields: list[str],
    contract_id: str,
    registry: dict,
) -> dict:
    """Producer-side gate: block a deploy when a registered breaking field is removed.

    This is a pre-deploy check.  The producer runs it before shipping a schema
    change.  If the proposed schema removes a field that downstream subscribers
    have declared as breaking in the registry, the deploy is blocked.

    Args:
        proposed_fields: Field names (bare column names) in the candidate schema.
        current_fields:  Field names in the currently published contract.
        contract_id:     The contract being evolved (e.g. 'week3-document-refinery-extractions').
        registry:        Loaded registry dict (from load_registry()).

    Returns:
        dict with keys:
          action                   — "BLOCK" or "PASS"
          breaking_fields_affected — list of dicts with field/reason/subscriber
          reason                   — human-readable explanation
    """
    removed = set(current_fields) - set(proposed_fields)
    if not removed:
        return {
            "action": "PASS",
            "breaking_fields_affected": [],
            "reason": "No fields removed; schema is additive or unchanged.",
        }

    source_label = contract_source_label(contract_id)
    subscriptions = registry.get("subscriptions", [])
    direct_subs = [
        sub for sub in subscriptions
        if sub.get("source") == source_label or sub.get("source_contract") == contract_id
    ]

    breaking_fields_affected: list[dict] = []
    for sub in direct_subs:
        for bf in sub.get("breaking_fields", []):
            registered = bf.get("field", "")
            # Registry stores "table.column"; match on the column part or the full path
            col_part = registered.split(".")[-1] if "." in registered else registered
            if col_part in removed or registered in removed:
                breaking_fields_affected.append(
                    {
                        "field": registered,
                        "reason": bf.get("reason", ""),
                        "subscriber": sub.get("target", ""),
                        "subscriber_contract": sub.get("target_contract", ""),
                    }
                )

    if breaking_fields_affected:
        affected_names = sorted({bf["field"] for bf in breaking_fields_affected})
        return {
            "action": "BLOCK",
            "breaking_fields_affected": breaking_fields_affected,
            "reason": (
                f"Removing {sorted(removed)} from '{contract_id}' would break "
                f"{len(breaking_fields_affected)} registered subscription(s) "
                f"(fields: {affected_names}). "
                "Update contract_registry/subscriptions.yaml with a migration plan before shipping."
            ),
        }

    return {
        "action": "PASS",
        "breaking_fields_affected": [],
        "reason": (
            f"Removed fields {sorted(removed)} are not registered as breaking "
            "for any downstream subscriber."
        ),
    }


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------


def check_required(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that a required field is present and has no nulls."""
    col = field["name"]
    cid = f"{table}.{col}.required"

    if series is None:
        return _result(
            cid, col, "required", "ERROR", "column missing", "column present",
            "CRITICAL", message=f"Column '{col}' not found in table '{table}'",
        )

    null_count = int(series.isna().sum())
    if null_count == 0:
        return _result(
            cid, col, "required", "PASS", "0 nulls", "0 nulls", "CRITICAL",
            message=f"{col}: all {len(series)} values present",
        )

    sample = series[series.isna()].index.tolist()[:5]
    return _result(
        cid, col, "required", "FAIL", f"{null_count} nulls", "0 nulls",
        "CRITICAL", records_failing=null_count,
        sample_failing=[int(i) for i in sample],
        message=f"{col}: {null_count}/{len(series)} values are null",
    )


def check_type(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that column dtype matches the contract logical type."""
    col = field["name"]
    expected_type = field.get("type", "string")
    cid = f"{table}.{col}.type"

    if series is None:
        return _result(
            cid, col, "type", "ERROR", "column missing", expected_type,
            "CRITICAL", message=f"Column '{col}' not found in table '{table}'",
        )

    actual_dtype = str(series.dtype)
    acceptable = TYPE_MAP.get(expected_type, {"object"})

    if actual_dtype in acceptable:
        return _result(
            cid, col, "type", "PASS", actual_dtype, expected_type, "CRITICAL",
            message=f"{col}: dtype '{actual_dtype}' matches logical type '{expected_type}'",
        )

    return _result(
        cid, col, "type", "FAIL", actual_dtype, expected_type, "CRITICAL",
        records_failing=len(series),
        message=f"{col}: dtype '{actual_dtype}' incompatible with '{expected_type}'",
    )


def check_enum(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that all non-null values are in the allowed enum list."""
    col = field["name"]
    allowed = field["enum"]
    cid = f"{table}.{col}.enum"

    if series is None:
        return _result(
            cid, col, "enum", "ERROR", "column missing", allowed,
            "CRITICAL", message=f"Column '{col}' not found",
        )

    non_null = series.dropna()
    violations = non_null[~non_null.astype(str).isin(allowed)]
    count = len(violations)

    if count == 0:
        return _result(
            cid, col, "enum", "PASS", "all in enum", allowed, "HIGH",
            message=f"{col}: all {len(non_null)} values in allowed set",
        )

    bad_vals = violations.unique().tolist()[:10]
    return _result(
        cid, col, "enum", "FAIL", bad_vals, allowed, "HIGH",
        records_failing=count,
        sample_failing=[str(v) for v in bad_vals],
        message=f"{col}: {count} values not in enum: {bad_vals[:5]}",
    )


def check_uuid_format(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that all non-null values match UUID pattern."""
    col = field["name"]
    cid = f"{table}.{col}.format_uuid"

    if series is None:
        return _result(
            cid, col, "format_uuid", "ERROR", "column missing", "uuid",
            "CRITICAL", message=f"Column '{col}' not found",
        )

    non_null = series.dropna().astype(str)
    bad_mask = ~non_null.str.match(UUID_RE.pattern, case=False)
    count = int(bad_mask.sum())

    if count == 0:
        return _result(
            cid, col, "format_uuid", "PASS", "all valid UUIDs", "uuid", "CRITICAL",
            message=f"{col}: all {len(non_null)} values are valid UUIDs",
        )

    bad_vals = non_null[bad_mask].head(5).tolist()
    return _result(
        cid, col, "format_uuid", "FAIL", f"{count} invalid", "uuid",
        "CRITICAL", records_failing=count, sample_failing=bad_vals,
        message=f"{col}: {count} values are not valid UUIDs",
    )


def check_datetime_format(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that all non-null values match ISO 8601 date-time pattern."""
    col = field["name"]
    cid = f"{table}.{col}.format_datetime"

    if series is None:
        return _result(
            cid, col, "format_datetime", "ERROR", "column missing", "date-time",
            "CRITICAL", message=f"Column '{col}' not found",
        )

    non_null = series.dropna().astype(str)
    bad_mask = ~non_null.str.match(ISO8601_RE.pattern)
    count = int(bad_mask.sum())

    if count == 0:
        return _result(
            cid, col, "format_datetime", "PASS", "all valid ISO 8601", "date-time",
            "CRITICAL", message=f"{col}: all {len(non_null)} values are valid date-time",
        )

    bad_vals = non_null[bad_mask].head(5).tolist()
    return _result(
        cid, col, "format_datetime", "FAIL", f"{count} invalid", "date-time",
        "CRITICAL", records_failing=count, sample_failing=bad_vals,
        message=f"{col}: {count} values fail ISO 8601 format",
    )


# ---------------------------------------------------------------------------
# Cross-table checks
# ---------------------------------------------------------------------------


def check_referential_integrity(
    child_table: str,
    child_column: str,
    child_series: pd.Series | None,
    parent_table: str,
    parent_column: str,
    parent_series: pd.Series | None,
) -> dict:
    """Check that every child key exists in the parent table."""
    cid = f"{child_table}.{child_column}.references.{parent_table}.{parent_column}"

    if child_series is None or parent_series is None:
        return _result(
            cid,
            child_column,
            "referential_integrity",
            "ERROR",
            "missing relationship input",
            f"{child_table}.{child_column} -> {parent_table}.{parent_column}",
            "CRITICAL",
            message=(
                f"Unable to validate referential integrity between "
                f"'{child_table}.{child_column}' and '{parent_table}.{parent_column}'"
            ),
        )

    child_values = child_series.dropna().astype(str)
    parent_values = set(parent_series.dropna().astype(str))
    orphans = child_values[~child_values.isin(parent_values)]
    count = int(len(orphans))

    if count == 0:
        return _result(
            cid,
            child_column,
            "referential_integrity",
            "PASS",
            f"all {len(child_values)} values matched",
            f"{parent_table}.{parent_column}",
            "CRITICAL",
            message=(
                f"{child_table}.{child_column}: all {len(child_values)} values "
                f"exist in {parent_table}.{parent_column}"
            ),
        )

    bad_vals = orphans.head(5).tolist()
    return _result(
        cid,
        child_column,
        "referential_integrity",
        "FAIL",
        f"{count} missing parent keys",
        f"{parent_table}.{parent_column}",
        "CRITICAL",
        records_failing=count,
        sample_failing=bad_vals,
        message=(
            f"{child_table}.{child_column}: {count} values do not exist in "
            f"{parent_table}.{parent_column}"
        ),
    )


# ---------------------------------------------------------------------------
# Statistical checks
# ---------------------------------------------------------------------------


def check_min_max(table: str, field: dict, series: pd.Series | None) -> dict:
    """Check that numeric values are within [minimum, maximum]."""
    col = field["name"]
    lo = field.get("minimum")
    hi = field.get("maximum")
    cid = f"{table}.{col}.range"

    if series is None:
        return _result(
            cid, col, "range", "ERROR", "column missing", f"[{lo}, {hi}]",
            "CRITICAL", message=f"Column '{col}' not found",
        )

    non_null = pd.to_numeric(series.dropna(), errors="coerce").dropna()
    if non_null.empty:
        return _result(
            cid, col, "range", "ERROR", "no numeric values", f"[{lo}, {hi}]",
            "HIGH", message=f"{col}: no numeric values to check",
        )

    violations = pd.Series(dtype="float64")
    if lo is not None:
        violations = non_null[non_null < lo]
    if hi is not None:
        over = non_null[non_null > hi]
        violations = pd.concat([violations, over])
    count = len(violations)

    if count == 0:
        actual_range = f"[{non_null.min()}, {non_null.max()}]"
        return _result(
            cid, col, "range", "PASS", actual_range, f"[{lo}, {hi}]", "HIGH",
            message=f"{col}: all {len(non_null)} values in range",
        )

    bad_vals = violations.head(5).tolist()
    return _result(
        cid, col, "range", "FAIL", f"{count} out of range", f"[{lo}, {hi}]",
        "HIGH", records_failing=count,
        sample_failing=[round(float(v), 6) for v in bad_vals],
        message=f"{col}: {count} values outside [{lo}, {hi}]",
    )


def compute_column_stats(series: pd.Series) -> dict | None:
    """Compute baseline-comparable stats for a numeric column.

    null_fraction and cardinality are derived from the full series (including
    nulls) so they can drive drift checks even when the column is mostly clean.
    """
    total = len(series)
    null_count = int(series.isna().sum())
    null_fraction = round(null_count / total, 6) if total > 0 else 0.0

    numeric = pd.to_numeric(series.dropna(), errors="coerce").dropna()
    if numeric.empty or len(numeric) < 2:
        return None
    return {
        "mean": round(float(numeric.mean()), 6),
        "stddev": round(float(numeric.std()), 6),
        "min": round(float(numeric.min()), 6),
        "max": round(float(numeric.max()), 6),
        "count": int(len(numeric)),
        "null_fraction": null_fraction,
        "cardinality": int(numeric.nunique()),
    }


def check_drift_mean(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict:
    """Z-score on column mean. WARN >2σ, FAIL >3σ."""
    cid = f"{table}.{col}.drift_mean"

    baseline_mean = baseline_stats["mean"]
    baseline_stddev = baseline_stats["stddev"]
    current_mean = current_stats["mean"]

    if baseline_stddev == 0:
        if current_mean != baseline_mean:
            return _result(
                cid, col, "drift_mean", "WARN", current_mean, baseline_mean, "MEDIUM",
                message=(
                    f"{col}: baseline had zero variance (mean={baseline_mean}), "
                    f"current mean={current_mean}"
                ),
            )
        return _result(
            cid, col, "drift_mean", "PASS", current_mean, baseline_mean, "LOW",
            message=f"{col}: zero-variance column unchanged",
        )

    z_score = abs(current_mean - baseline_mean) / baseline_stddev

    if z_score > 3:
        return _result(
            cid, col, "drift_mean", "FAIL",
            f"mean={current_mean} (z={z_score:.2f})",
            f"baseline mean={baseline_mean} ± {baseline_stddev}",
            "HIGH", message=f"{col}: mean drift z={z_score:.2f} > 3σ",
        )
    if z_score > 2:
        return _result(
            cid, col, "drift_mean", "WARN",
            f"mean={current_mean} (z={z_score:.2f})",
            f"baseline mean={baseline_mean} ± {baseline_stddev}",
            "MEDIUM", message=f"{col}: mean drift z={z_score:.2f} > 2σ",
        )
    return _result(
        cid, col, "drift_mean", "PASS",
        f"mean={current_mean} (z={z_score:.2f})",
        f"baseline mean={baseline_mean} ± {baseline_stddev}",
        "LOW", message=f"{col}: mean drift z={z_score:.2f} within normal range",
    )


def check_drift_variance(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict | None:
    """Stddev ratio vs baseline. WARN >2× or <0.25×; FAIL >4×.

    Returns None when either side lacks a stddev (old baseline format).
    """
    b_std = baseline_stats.get("stddev")
    c_std = current_stats.get("stddev")
    if b_std is None or c_std is None:
        return None

    cid = f"{table}.{col}.drift_variance"

    if b_std == 0:
        if c_std > 0:
            return _result(
                cid, col, "drift_variance", "WARN",
                f"stddev={c_std:.6f}", "baseline stddev=0", "MEDIUM",
                message=f"{col}: variance appeared (baseline was zero-variance, current stddev={c_std:.6f})",
            )
        return _result(
            cid, col, "drift_variance", "PASS",
            "stddev=0", "stddev=0", "LOW",
            message=f"{col}: variance unchanged (both zero)",
        )

    ratio = c_std / b_std

    if ratio > 4.0:
        return _result(
            cid, col, "drift_variance", "FAIL",
            f"stddev={c_std:.6f} (ratio={ratio:.2f}×)",
            f"baseline stddev={b_std:.6f}",
            "HIGH", message=f"{col}: variance explosion ratio={ratio:.2f}× > 4×",
        )
    if ratio > 2.0:
        return _result(
            cid, col, "drift_variance", "WARN",
            f"stddev={c_std:.6f} (ratio={ratio:.2f}×)",
            f"baseline stddev={b_std:.6f}",
            "MEDIUM", message=f"{col}: variance inflation ratio={ratio:.2f}× > 2×",
        )
    if ratio < 0.25:
        return _result(
            cid, col, "drift_variance", "WARN",
            f"stddev={c_std:.6f} (ratio={ratio:.2f}×)",
            f"baseline stddev={b_std:.6f}",
            "MEDIUM", message=f"{col}: variance collapse ratio={ratio:.2f}× < 0.25×",
        )
    return _result(
        cid, col, "drift_variance", "PASS",
        f"stddev={c_std:.6f} (ratio={ratio:.2f}×)",
        f"baseline stddev={b_std:.6f}",
        "LOW", message=f"{col}: variance ratio={ratio:.2f}× within normal range",
    )


def check_drift_outliers(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict | None:
    """New observed extremes outside the baseline's observed range.

    WARN when one end is breached; FAIL when both ends are breached.
    Returns None when either side lacks min/max (old baseline format).
    """
    b_min = baseline_stats.get("min")
    b_max = baseline_stats.get("max")
    c_min = current_stats.get("min")
    c_max = current_stats.get("max")
    if any(v is None for v in (b_min, b_max, c_min, c_max)):
        return None

    cid = f"{table}.{col}.drift_outliers"
    new_low = c_min < b_min
    new_high = c_max > b_max

    if new_low and new_high:
        return _result(
            cid, col, "drift_outliers", "FAIL",
            f"[{c_min}, {c_max}]", f"baseline [{b_min}, {b_max}]",
            "HIGH",
            message=(
                f"{col}: new outliers on both ends "
                f"(min {c_min} < baseline {b_min}, max {c_max} > baseline {b_max})"
            ),
        )
    if new_low:
        return _result(
            cid, col, "drift_outliers", "WARN",
            f"min={c_min}", f"baseline min={b_min}",
            "MEDIUM", message=f"{col}: new low outlier min={c_min} below baseline min={b_min}",
        )
    if new_high:
        return _result(
            cid, col, "drift_outliers", "WARN",
            f"max={c_max}", f"baseline max={b_max}",
            "MEDIUM", message=f"{col}: new high outlier max={c_max} above baseline max={b_max}",
        )
    return _result(
        cid, col, "drift_outliers", "PASS",
        f"[{c_min}, {c_max}]", f"baseline [{b_min}, {b_max}]",
        "LOW", message=f"{col}: observed range within baseline bounds",
    )


def check_drift_null_fraction(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict | None:
    """Null-fraction growth vs baseline.

    WARN >5 pp growth; FAIL >20 pp growth.
    Any nulls on a previously fully-populated column are flagged immediately.
    Returns None when either side lacks null_fraction (old baseline format).
    """
    b_nf = baseline_stats.get("null_fraction")
    c_nf = current_stats.get("null_fraction")
    if b_nf is None or c_nf is None:
        return None

    cid = f"{table}.{col}.drift_null_fraction"
    delta = c_nf - b_nf

    if b_nf == 0.0 and c_nf > 0.0:
        status = "FAIL" if c_nf > 0.20 else "WARN"
        severity = "HIGH" if status == "FAIL" else "MEDIUM"
        return _result(
            cid, col, "drift_null_fraction", status,
            f"null_fraction={c_nf:.4f}", "baseline null_fraction=0.0",
            severity,
            message=f"{col}: nulls appeared on previously fully-populated column ({c_nf:.1%} null)",
        )
    if delta > 0.20:
        return _result(
            cid, col, "drift_null_fraction", "FAIL",
            f"null_fraction={c_nf:.4f} (Δ={delta:+.4f})",
            f"baseline null_fraction={b_nf:.4f}",
            "HIGH", message=f"{col}: null fraction grew by {delta:.1%} > 20 pp threshold",
        )
    if delta > 0.05:
        return _result(
            cid, col, "drift_null_fraction", "WARN",
            f"null_fraction={c_nf:.4f} (Δ={delta:+.4f})",
            f"baseline null_fraction={b_nf:.4f}",
            "MEDIUM", message=f"{col}: null fraction grew by {delta:.1%} > 5 pp threshold",
        )
    return _result(
        cid, col, "drift_null_fraction", "PASS",
        f"null_fraction={c_nf:.4f} (Δ={delta:+.4f})",
        f"baseline null_fraction={b_nf:.4f}",
        "LOW", message=f"{col}: null fraction stable (Δ={delta:+.1%})",
    )


def check_drift_cardinality(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict | None:
    """Unique-value spike or collapse vs baseline.

    WARN >2× or <0.5×; FAIL >5×.
    Returns None when either side lacks cardinality (old baseline format).
    """
    b_card = baseline_stats.get("cardinality")
    c_card = current_stats.get("cardinality")
    if b_card is None or c_card is None:
        return None

    cid = f"{table}.{col}.drift_cardinality"

    if b_card == 0:
        if c_card > 0:
            return _result(
                cid, col, "drift_cardinality", "WARN",
                f"cardinality={c_card}", "baseline cardinality=0",
                "MEDIUM",
                message=f"{col}: cardinality appeared (baseline was empty, now {c_card} unique values)",
            )
        return _result(
            cid, col, "drift_cardinality", "PASS",
            "cardinality=0", "baseline cardinality=0",
            "LOW", message=f"{col}: cardinality unchanged (both empty)",
        )

    ratio = c_card / b_card

    if ratio > 5.0:
        return _result(
            cid, col, "drift_cardinality", "FAIL",
            f"cardinality={c_card} (ratio={ratio:.2f}×)",
            f"baseline cardinality={b_card}",
            "HIGH", message=f"{col}: cardinality explosion ratio={ratio:.2f}× > 5×",
        )
    if ratio > 2.0:
        return _result(
            cid, col, "drift_cardinality", "WARN",
            f"cardinality={c_card} (ratio={ratio:.2f}×)",
            f"baseline cardinality={b_card}",
            "MEDIUM", message=f"{col}: cardinality spike ratio={ratio:.2f}× > 2×",
        )
    if ratio < 0.5:
        return _result(
            cid, col, "drift_cardinality", "WARN",
            f"cardinality={c_card} (ratio={ratio:.2f}×)",
            f"baseline cardinality={b_card}",
            "MEDIUM", message=f"{col}: cardinality collapse ratio={ratio:.2f}× < 0.5×",
        )
    return _result(
        cid, col, "drift_cardinality", "PASS",
        f"cardinality={c_card} (ratio={ratio:.2f}×)",
        f"baseline cardinality={b_card}",
        "LOW", message=f"{col}: cardinality ratio={ratio:.2f}× within normal range",
    )


# Ordered dispatch list for the orchestrator.
_DRIFT_CHECKS = (
    check_drift_mean,
    check_drift_variance,
    check_drift_outliers,
    check_drift_null_fraction,
    check_drift_cardinality,
)


# ---------------------------------------------------------------------------
# Baseline management
# ---------------------------------------------------------------------------


def load_baselines() -> dict:
    if BASELINES_PATH.exists():
        with open(BASELINES_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_baselines(baselines: dict) -> None:
    BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINES_PATH, "w", encoding="utf-8") as fh:
        json.dump(baselines, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Orchestrator: run all checks for one table
# ---------------------------------------------------------------------------


def run_table_checks(
    table_name: str,
    fields: list[dict],
    df: pd.DataFrame,
    baselines: dict,
    contract_id: str,
) -> tuple[list[dict], dict]:
    """Run all structural + statistical checks for a table.

    Returns (results_list, new_baseline_stats_for_table).
    """
    results: list[dict] = []
    table_stats: dict[str, dict] = {}
    baseline_key = f"{contract_id}/{table_name}"
    table_baseline = baselines.get(baseline_key, {})

    # Compare schema first so drift is visible before field-level validation.
    results.extend(check_schema_evolution(table_name, fields, df))

    for field in fields:
        col = field["name"]
        if col not in df.columns:
            continue
        series = df[col]

        # --- Structural checks ---

        # 1. Required
        if field.get("required"):
            try:
                results.append(check_required(table_name, field, series))
            except Exception as exc:
                results.append(_result(
                    f"{table_name}.{col}.required", col, "required", "ERROR",
                    str(exc), "no nulls", "CRITICAL", message=str(exc),
                ))

        # 2. Type match
        try:
            results.append(check_type(table_name, field, series))
        except Exception as exc:
            results.append(_result(
                f"{table_name}.{col}.type", col, "type", "ERROR",
                str(exc), field.get("type", "string"), "CRITICAL", message=str(exc),
            ))

        # 3. Enum
        if "enum" in field:
            try:
                results.append(check_enum(table_name, field, series))
            except Exception as exc:
                results.append(_result(
                    f"{table_name}.{col}.enum", col, "enum", "ERROR",
                    str(exc), field["enum"], "HIGH", message=str(exc),
                ))

        # 4. UUID format
        if field.get("format") == "uuid":
            try:
                results.append(check_uuid_format(table_name, field, series))
            except Exception as exc:
                results.append(_result(
                    f"{table_name}.{col}.format_uuid", col, "format_uuid", "ERROR",
                    str(exc), "uuid", "CRITICAL", message=str(exc),
                ))

        # 5. Date-time format
        if field.get("format") == "date-time":
            try:
                results.append(check_datetime_format(table_name, field, series))
            except Exception as exc:
                results.append(_result(
                    f"{table_name}.{col}.format_datetime", col, "format_datetime",
                    "ERROR", str(exc), "date-time", "CRITICAL", message=str(exc),
                ))

        # --- Statistical checks ---

        # 6. Min/max range
        if field.get("minimum") is not None or field.get("maximum") is not None:
            try:
                results.append(check_min_max(table_name, field, series))
            except Exception as exc:
                results.append(_result(
                    f"{table_name}.{col}.range", col, "range", "ERROR",
                    str(exc), f"[{field.get('minimum')}, {field.get('maximum')}]",
                    "HIGH", message=str(exc),
                ))

        # 7. Compute stats for drift (numeric columns only)
        if series is not None and pd.api.types.is_numeric_dtype(series):
            stats = compute_column_stats(series)
            if stats:
                table_stats[col] = stats
                col_baseline = table_baseline.get(col)
                if col_baseline:
                    for drift_fn in _DRIFT_CHECKS:
                        check_type_name = drift_fn.__name__.replace("check_", "")
                        try:
                            r = drift_fn(table_name, col, stats, col_baseline)
                            if r is not None:
                                results.append(r)
                        except Exception as exc:
                            results.append(_result(
                                f"{table_name}.{col}.{check_type_name}",
                                col, check_type_name, "ERROR",
                                str(exc),
                                f"baseline mean={col_baseline.get('mean')}",
                                "MEDIUM", message=str(exc),
                            ))

    return results, table_stats


def run_cross_table_checks(frames: dict[str, pd.DataFrame]) -> list[dict]:
    """Run contract-level checks that compare values across tables."""
    documents = frames.get("documents")
    extracted_facts = frames.get("extracted_facts")

    if documents is None or extracted_facts is None:
        return []
    if "doc_id" not in documents.columns or "doc_id" not in extracted_facts.columns:
        return []

    return [
        check_referential_integrity(
            "extracted_facts",
            "doc_id",
            extracted_facts["doc_id"],
            "documents",
            "doc_id",
            documents["doc_id"],
        )
    ]


# ---------------------------------------------------------------------------
# Data loader: flatten JSONL into table DataFrames
# ---------------------------------------------------------------------------

# Maps contract table names to flatten functions
TABLE_FLATTENERS = {
    "documents": flatten_documents,
    "extracted_facts": flatten_facts,
    "entities": flatten_entities,
    "events": flatten_events,
    "event_metadata": flatten_event_metadata,
    "lineage_nodes": flatten_lineage_nodes,
    "lineage_edges": flatten_lineage_edges,
    "trace_nodes": flatten_trace_nodes,
}


def flatten_all(records: list[dict], table_names: list[str]) -> dict[str, pd.DataFrame]:
    """Flatten records for each table referenced in the contract."""
    frames: dict[str, pd.DataFrame] = {}
    for name in table_names:
        flattener = TABLE_FLATTENERS.get(name)
        if flattener:
            frames[name] = flattener(records)
        else:
            # Unknown table -- create empty DF; checks will return ERROR
            frames[name] = pd.DataFrame()
    return frames


# ---------------------------------------------------------------------------
# File hashing
# ---------------------------------------------------------------------------


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate data against a Bitol contract.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to data JSONL")
    parser.add_argument("--output", required=True, help="Path for validation report JSON")
    parser.add_argument(
        "--mode",
        choices=("AUDIT", "WARN", "ENFORCE"),
        default="AUDIT",
        help="Validation mode: AUDIT, WARN, or ENFORCE",
    )
    parser.add_argument(
        "--promote-baselines",
        action="store_true",
        default=False,
        help=(
            "Write the stats from this run as the new golden baseline. "
            "Without this flag, baselines are never overwritten after the initial creation, "
            "so drift is always measured against a human-approved snapshot."
        ),
    )
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    report_id = str(uuid.uuid4())

    # Load contract
    print(f"[runner] Loading contract: {args.contract}")
    with open(args.contract, "r", encoding="utf-8") as fh:
        contract = yaml.safe_load(fh)

    contract_id = contract.get("id", "unknown")
    registry_section = contract.get("registry") or {}
    registry_path = registry_section.get("path") or str(DEFAULT_REGISTRY_PATH)
    registry = load_registry(registry_path)

    # Load + flatten data
    print(f"[runner] Loading data: {args.data}")
    records = load_jsonl(args.data)
    print(f"  {len(records)} source documents loaded")

    snapshot_id = sha256_file(args.data)

    table_defs = contract.get("schema", {}).get("tables", [])
    table_names = [t["name"] for t in table_defs]
    frames = flatten_all(records, table_names)

    lineage_input = next(
        (
            port.get("uri")
            for port in contract.get("lineage", {}).get("inputPorts", [])
            if port.get("type") == "lineage_graph"
        ),
        None,
    )
    lineage_graph = load_lineage_graph(lineage_input)

    for name, df in frames.items():
        print(f"  {name}: {len(df)} rows x {len(df.columns)} cols")

    # Load baselines
    baselines = load_baselines()
    is_first_run = not baselines
    if is_first_run:
        print("[runner] No baselines found -- this run will create the initial baseline")
    else:
        print(f"[runner] Loaded baselines from {BASELINES_PATH}")

    # Run all checks
    print("[runner] Running checks ...")
    all_results: list[dict] = []
    new_baselines: dict[str, dict] = {}

    for tdef in table_defs:
        tname = tdef["name"]
        fields = tdef.get("fields", [])
        df = frames.get(tname, pd.DataFrame())

        results, stats = run_table_checks(tname, fields, df, baselines, contract_id)
        all_results.extend(results)

        if stats:
            baseline_key = f"{contract_id}/{tname}"
            new_baselines[baseline_key] = stats

    all_results.extend(run_cross_table_checks(frames))

    # Tally
    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    warned = sum(1 for r in all_results if r["status"] == "WARN")
    errored = sum(1 for r in all_results if r["status"] == "ERROR")
    total = len(all_results)
    schema_summary = summarize_schema_evolution(all_results)

    print(f"  Total: {total}  PASS: {passed}  FAIL: {failed}  WARN: {warned}  ERROR: {errored}")
    if schema_summary["missing_columns"] or schema_summary["new_columns"]:
        missing_text = ", ".join(
            f"{item['table']}.{item['column']}" for item in schema_summary["missing_columns"]
        ) or "none"
        new_text = ", ".join(
            f"{item['table']}.{item['column']}" for item in schema_summary["new_columns"]
        ) or "none"
        print(f"  Schema missing: {missing_text}")
        print(f"  Schema new: {new_text}")

    violation_rows = [
        attribute_violation(result, contract_id, registry, lineage_graph, snapshot_id)
        for result in all_results
        if result["status"] != "PASS"
    ]
    VIOLATION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    run_header = {
        "record_type": "run_header",
        "run_id": report_id,
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "run_timestamp": now.isoformat(),
        "violation_count": len(violation_rows),
    }
    with open(VIOLATION_LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(run_header, ensure_ascii=False) + "\n")
        for row in violation_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"[runner] Violation log appended: {VIOLATION_LOG_PATH} ({len(violation_rows)} entries)")

    # Build report
    report = {
        "report_id": report_id,
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "run_timestamp": now.isoformat(),
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "mode": args.mode,
        "schema_summary": schema_summary,
        "results": all_results,
        "violation_log": str(VIOLATION_LOG_PATH),
    }

    # Write report
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"[runner] Report written: {out_path}")

    # Save baselines:
    #   - first run: always write (no golden baseline exists yet)
    #   - subsequent runs: only write when --promote-baselines is explicitly passed,
    #     so a regression cannot silently clear its own evidence
    if is_first_run and new_baselines:
        save_baselines(new_baselines)
        print(f"[runner] Initial baselines saved to {BASELINES_PATH}")
    elif new_baselines and args.promote_baselines:
        baselines.update(new_baselines)
        save_baselines(baselines)
        print(f"[runner] Baselines promoted to {BASELINES_PATH}")
    elif new_baselines:
        print(
            f"[runner] Baselines NOT updated (pass --promote-baselines to overwrite {BASELINES_PATH})"
        )

    # Exit code depends on the requested operating mode.
    #   AUDIT   — log only, always exit 0
    #   WARN    — block when any CRITICAL-severity check fails
    #   ENFORCE — block when any HIGH or CRITICAL check fails
    critical_fails = sum(
        1 for r in all_results
        if r["status"] == "FAIL" and r.get("severity") == "CRITICAL"
    )
    high_or_critical_fails = sum(
        1 for r in all_results
        if r["status"] == "FAIL" and r.get("severity") in ("CRITICAL", "HIGH")
    )

    if args.mode == "AUDIT":
        exit_code = 0
    elif args.mode == "WARN":
        exit_code = 1 if critical_fails > 0 else 0
    else:  # ENFORCE
        exit_code = 1 if (high_or_critical_fails > 0 or errored > 0) else 0
    print(f"[runner] Done. Exit code: {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
