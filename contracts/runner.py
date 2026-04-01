#!/usr/bin/env python3
"""
contracts/runner.py -- Data Contract Validation Runner

Reads a Bitol contract YAML + data JSONL, checks every clause, and produces
a structured JSON validation report.

Check order:
  1. Structural: required fields, type match, enum conformance, UUID pattern, date-time format
  2. Statistical: min/max range, drift vs baseline (WARN >2 stddev, FAIL >3 stddev)

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
from contracts.generator import (
    flatten_documents,
    flatten_entities,
    flatten_facts,
    load_jsonl,
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
    """Compute baseline-comparable stats for a numeric column."""
    numeric = pd.to_numeric(series.dropna(), errors="coerce").dropna()
    if numeric.empty or len(numeric) < 2:
        return None
    return {
        "mean": round(float(numeric.mean()), 6),
        "stddev": round(float(numeric.std()), 6),
        "min": round(float(numeric.min()), 6),
        "max": round(float(numeric.max()), 6),
        "count": int(len(numeric)),
    }


def check_drift(
    table: str,
    col: str,
    current_stats: dict,
    baseline_stats: dict,
) -> dict:
    """Compare current stats against baseline. WARN >2 stddev, FAIL >3 stddev."""
    cid = f"{table}.{col}.drift"

    baseline_mean = baseline_stats["mean"]
    baseline_stddev = baseline_stats["stddev"]
    current_mean = current_stats["mean"]

    if baseline_stddev == 0:
        # No variance in baseline -- any change is notable
        if current_mean != baseline_mean:
            return _result(
                cid, col, "drift", "WARN", current_mean, baseline_mean, "MEDIUM",
                message=(
                    f"{col}: baseline had zero variance (mean={baseline_mean}), "
                    f"current mean={current_mean}"
                ),
            )
        return _result(
            cid, col, "drift", "PASS", current_mean, baseline_mean, "LOW",
            message=f"{col}: zero-variance column unchanged",
        )

    z_score = abs(current_mean - baseline_mean) / baseline_stddev

    if z_score > 3:
        return _result(
            cid, col, "drift", "FAIL",
            f"mean={current_mean} (z={z_score:.2f})",
            f"baseline mean={baseline_mean} +/- {baseline_stddev}",
            "HIGH", message=f"{col}: statistical drift z={z_score:.2f} > 3 stddev",
        )
    elif z_score > 2:
        return _result(
            cid, col, "drift", "WARN",
            f"mean={current_mean} (z={z_score:.2f})",
            f"baseline mean={baseline_mean} +/- {baseline_stddev}",
            "MEDIUM", message=f"{col}: statistical drift z={z_score:.2f} > 2 stddev",
        )
    else:
        return _result(
            cid, col, "drift", "PASS",
            f"mean={current_mean} (z={z_score:.2f})",
            f"baseline mean={baseline_mean} +/- {baseline_stddev}",
            "LOW", message=f"{col}: drift z={z_score:.2f} within normal range",
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

    for field in fields:
        col = field["name"]
        series = df[col] if col in df.columns else None

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
                # Drift check if baseline exists
                col_baseline = table_baseline.get(col)
                if col_baseline:
                    try:
                        results.append(check_drift(
                            table_name, col, stats, col_baseline,
                        ))
                    except Exception as exc:
                        results.append(_result(
                            f"{table_name}.{col}.drift", col, "drift", "ERROR",
                            str(exc),
                            f"baseline mean={col_baseline.get('mean')}",
                            "MEDIUM", message=str(exc),
                        ))

    return results, table_stats


# ---------------------------------------------------------------------------
# Data loader: flatten JSONL into table DataFrames
# ---------------------------------------------------------------------------

# Maps contract table names to flatten functions
TABLE_FLATTENERS = {
    "documents": flatten_documents,
    "extracted_facts": flatten_facts,
    "entities": flatten_entities,
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
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    report_id = str(uuid.uuid4())

    # Load contract
    print(f"[runner] Loading contract: {args.contract}")
    with open(args.contract, "r", encoding="utf-8") as fh:
        contract = yaml.safe_load(fh)

    contract_id = contract.get("id", "unknown")

    # Load + flatten data
    print(f"[runner] Loading data: {args.data}")
    records = load_jsonl(args.data)
    print(f"  {len(records)} source documents loaded")

    snapshot_id = sha256_file(args.data)

    table_defs = contract.get("schema", {}).get("tables", [])
    table_names = [t["name"] for t in table_defs]
    frames = flatten_all(records, table_names)

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

    # Tally
    passed = sum(1 for r in all_results if r["status"] == "PASS")
    failed = sum(1 for r in all_results if r["status"] == "FAIL")
    warned = sum(1 for r in all_results if r["status"] == "WARN")
    errored = sum(1 for r in all_results if r["status"] == "ERROR")
    total = len(all_results)

    print(f"  Total: {total}  PASS: {passed}  FAIL: {failed}  WARN: {warned}  ERROR: {errored}")

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
        "results": all_results,
    }

    # Write report
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"[runner] Report written: {out_path}")

    # Save baselines (create on first run, update on subsequent)
    if is_first_run and new_baselines:
        save_baselines(new_baselines)
        print(f"[runner] Initial baselines saved to {BASELINES_PATH}")
    elif new_baselines:
        # Merge new stats into existing baselines
        baselines.update(new_baselines)
        save_baselines(baselines)
        print(f"[runner] Baselines updated at {BASELINES_PATH}")

    # Exit code: 0 if no FAIL/ERROR, 1 otherwise
    exit_code = 1 if (failed > 0 or errored > 0) else 0
    print(f"[runner] Done. Exit code: {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
