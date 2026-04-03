#!/usr/bin/env python3
"""
contracts/schema_analyzer.py -- Schema evolution analyzer

Diffs two consecutive timestamped snapshots from schema_snapshots/<contract-id>/
and determines if breaking changes are present.

Breaking change taxonomy:
  CRITICAL  add required field, rename field, narrow type (incl. float→int scale change),
            remove required field, remove enum value
  HIGH      remove optional field, add maximum/minimum constraint
  LOW/COMPAT add nullable field, widen type, add enum value

Usage:
  # Load from snapshot directory (recommended):
  python contracts/schema_analyzer.py \
    --contract-id week3-document-refinery-extractions \
    --since 2025-01-01

  # Or compare two explicit files:
  python contracts/schema_analyzer.py \
    --baseline schema_snapshots/week3-.../20250101T000000Z.yaml \
    --current  generated_contracts/week3-document-refinery-extractions.yaml
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

try:
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry


SNAPSHOTS_DIR = Path("schema_snapshots")


# ---------------------------------------------------------------------------
# Snapshot loading
# ---------------------------------------------------------------------------


def load_schema(path: str | Path) -> dict[str, Any]:
    """Load a schema YAML or JSON file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        if path.suffix == ".json":
            return json.load(fh)
        return yaml.safe_load(fh) or {}


def _snapshot_timestamp(path: Path) -> datetime:
    """Parse the UTC timestamp embedded in a snapshot filename (YYYYMMDDTHHMMSSz.yaml)."""
    stem = path.stem  # e.g. 20250104T153012Z
    try:
        return datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def load_consecutive_snapshots(
    contract_id: str,
    since: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load the two most-recent snapshots from schema_snapshots/<contract-id>/.

    If *since* is given (ISO date string, e.g. '2025-01-01'), only snapshots
    taken on or after that date are considered.

    Returns (older_snapshot, newer_snapshot).
    """
    snap_dir = SNAPSHOTS_DIR / contract_id
    if not snap_dir.exists():
        raise FileNotFoundError(
            f"No snapshot directory found for contract '{contract_id}' at {snap_dir}"
        )

    candidates = sorted(snap_dir.glob("*.yaml"), key=_snapshot_timestamp)

    if since:
        since_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
        candidates = [p for p in candidates if _snapshot_timestamp(p) >= since_dt]

    if len(candidates) < 2:
        raise ValueError(
            f"Need at least 2 snapshots in {snap_dir} to diff "
            f"(found {len(candidates)}). Run the generator first."
        )

    baseline_path = candidates[-2]
    current_path = candidates[-1]
    return load_schema(baseline_path), load_schema(current_path)


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def _is_narrow_type_scale_change(
    old_type: str,
    new_type: str,
    old_min: float | None,
    old_max: float | None,
    new_min: float | None,
    new_max: float | None,
) -> bool:
    """Detect the float 0.0–1.0 → int 0–100 pattern (precision-narrowing scale change)."""
    if old_type == "number" and new_type == "integer":
        # Old range was [0.0, 1.0] (probability / confidence scale)
        old_was_unit = (old_min is not None and old_max is not None
                        and abs(old_min) < 1e-6 and abs(old_max - 1.0) < 1e-6)
        # New range is [0, 100] (percentage scale)
        new_is_pct = (new_min is not None and new_max is not None
                      and abs(new_min) < 1e-6 and abs(new_max - 100) < 1e-6)
        if old_was_unit or new_is_pct:
            return True
    return False


def classify_type_change(
    old_type: str,
    new_type: str,
    old_field: dict | None = None,
    new_field: dict | None = None,
) -> tuple[str, str]:
    """Classify a type change.

    Returns (severity, reason) where severity is one of:
        CRITICAL, HIGH, LOW, COMPAT
    """
    if old_type == new_type:
        return "COMPAT", "no change"

    old_field = old_field or {}
    new_field = new_field or {}

    # Explicit narrow-type scale change: float 0.0–1.0 → int 0–100
    if _is_narrow_type_scale_change(
        old_type, new_type,
        old_field.get("minimum"), old_field.get("maximum"),
        new_field.get("minimum"), new_field.get("maximum"),
    ):
        return "CRITICAL", (
            "CRITICAL: precision-narrowing scale change detected — "
            "float [0.0–1.0] changed to integer [0–100]. "
            "Downstream consumers expecting fractional confidence will receive integer percentages."
        )

    # number → integer (loss of precision)
    if old_type == "number" and new_type == "integer":
        return "CRITICAL", "narrowed from number to integer (precision loss)"

    # string → structured type (more restrictive)
    if old_type == "string" and new_type in ("uuid", "datetime", "integer", "number"):
        return "CRITICAL", f"narrowed from string to {new_type}"

    # integer → string (semantic loss)
    if old_type == "integer" and new_type == "string":
        return "CRITICAL", "changed from integer to string (semantic loss)"

    # any → boolean or boolean → any (usually breaking)
    if "boolean" in (old_type, new_type):
        return "HIGH", f"type changed from {old_type} to {new_type}"

    return "HIGH", f"type changed from {old_type} to {new_type}"


def classify_enum_change(
    old_values: list[str], new_values: list[str]
) -> tuple[str, str]:
    """Returns (severity, reason)."""
    old_set = set(old_values)
    new_set = set(new_values)
    removed = old_set - new_set
    added = new_set - old_set

    if removed:
        return "CRITICAL", f"removed enum values: {sorted(removed)}"
    if added:
        return "COMPAT", f"added enum values: {sorted(added)}"
    return "COMPAT", "enum unchanged"


def classify_range_change(
    old_min: float | None, old_max: float | None,
    new_min: float | None, new_max: float | None,
) -> tuple[str, str]:
    """Returns (severity, reason)."""
    reasons: list[str] = []
    breaking = False

    if old_min is not None and new_min is not None and new_min > old_min:
        breaking = True
        reasons.append(f"minimum tightened {old_min} → {new_min}")
    elif old_min is None and new_min is not None:
        breaking = True
        reasons.append(f"minimum added: {new_min}")

    if old_max is not None and new_max is not None and new_max < old_max:
        breaking = True
        reasons.append(f"maximum tightened {old_max} → {new_max}")
    elif old_max is None and new_max is not None:
        breaking = True
        reasons.append(f"maximum added: {new_max}")

    if not reasons:
        return "COMPAT", "range unchanged or widened"
    return ("CRITICAL" if breaking else "COMPAT"), " AND ".join(reasons)


# ---------------------------------------------------------------------------
# Core diff logic
# ---------------------------------------------------------------------------


def _extract_tables(schema: dict[str, Any]) -> dict[str, dict]:
    """Return a dict of {table_name: {field_name: field_spec}} from a Bitol contract."""
    tables: dict[str, dict] = {}
    for table in schema.get("schema", {}).get("tables", []) or []:
        name = table.get("name") or table.get("table_id") or "unknown"
        fields: dict[str, dict] = {}
        for field in table.get("fields", []) or []:
            fname = field.get("name") or field.get("field_id")
            if fname:
                fields[fname] = field
        tables[name] = fields
    return tables


def diff_schemas(
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    """Diff two Bitol contract snapshots and classify all changes."""
    breaking_changes: list[dict] = []
    compatible_changes: list[dict] = []

    baseline_tables = _extract_tables(baseline)
    current_tables = _extract_tables(current)

    # Removed tables
    for tname in baseline_tables:
        if tname not in current_tables:
            breaking_changes.append(
                {
                    "type": "table_removed",
                    "table": tname,
                    "severity": "CRITICAL",
                    "reason": f"Table '{tname}' removed",
                }
            )

    # Added tables
    for tname in current_tables:
        if tname not in baseline_tables:
            compatible_changes.append(
                {
                    "type": "table_added",
                    "table": tname,
                    "severity": "COMPAT",
                    "reason": f"Table '{tname}' added",
                }
            )

    for tname, current_fields in current_tables.items():
        baseline_fields = baseline_tables.get(tname, {})

        # Removed fields
        for fname, bspec in baseline_fields.items():
            if fname not in current_fields:
                if bspec.get("required"):
                    breaking_changes.append(
                        {
                            "type": "required_field_removed",
                            "table": tname,
                            "field": fname,
                            "severity": "CRITICAL",
                            "reason": f"Required field '{fname}' removed from '{tname}'",
                        }
                    )
                else:
                    breaking_changes.append(
                        {
                            "type": "optional_field_removed",
                            "table": tname,
                            "field": fname,
                            "severity": "HIGH",
                            "reason": f"Optional field '{fname}' removed from '{tname}'",
                        }
                    )

        # Added fields
        for fname, cspec in current_fields.items():
            if fname not in baseline_fields:
                if cspec.get("required"):
                    breaking_changes.append(
                        {
                            "type": "required_field_added",
                            "table": tname,
                            "field": fname,
                            "severity": "CRITICAL",
                            "reason": f"Required field '{fname}' added to '{tname}' without migration",
                        }
                    )
                else:
                    compatible_changes.append(
                        {
                            "type": "nullable_field_added",
                            "table": tname,
                            "field": fname,
                            "severity": "COMPAT",
                            "reason": f"Nullable field '{fname}' added to '{tname}'",
                        }
                    )

        # Field-level changes
        for fname in baseline_fields:
            if fname not in current_fields:
                continue
            bspec = baseline_fields[fname]
            cspec = current_fields[fname]

            # Type change
            bt = bspec.get("type")
            ct = cspec.get("type")
            if bt != ct and bt and ct:
                severity, reason = classify_type_change(bt, ct, bspec, cspec)
                entry = {
                    "type": "type_changed",
                    "table": tname,
                    "field": fname,
                    "old_type": bt,
                    "new_type": ct,
                    "severity": severity,
                    "reason": reason,
                }
                if severity in ("CRITICAL", "HIGH"):
                    breaking_changes.append(entry)
                else:
                    compatible_changes.append(entry)

            # Enum change
            be = bspec.get("enum", [])
            ce = cspec.get("enum", [])
            if be or ce:
                severity, reason = classify_enum_change(be or [], ce or [])
                if be != ce:
                    entry = {
                        "type": "enum_changed",
                        "table": tname,
                        "field": fname,
                        "old_values": be,
                        "new_values": ce,
                        "severity": severity,
                        "reason": reason,
                    }
                    if severity == "COMPAT":
                        compatible_changes.append(entry)
                    else:
                        breaking_changes.append(entry)

            # Range change
            b_min = bspec.get("minimum")
            b_max = bspec.get("maximum")
            c_min = cspec.get("minimum")
            c_max = cspec.get("maximum")
            if (b_min, b_max) != (c_min, c_max):
                severity, reason = classify_range_change(b_min, b_max, c_min, c_max)
                entry = {
                    "type": "range_changed",
                    "table": tname,
                    "field": fname,
                    "old_range": [b_min, b_max],
                    "new_range": [c_min, c_max],
                    "severity": severity,
                    "reason": reason,
                }
                if severity == "COMPAT":
                    compatible_changes.append(entry)
                else:
                    breaking_changes.append(entry)

    verdict = "breaking" if breaking_changes else "compatible"
    return {
        "verdict": verdict,
        "breaking_changes": breaking_changes,
        "compatible_changes": compatible_changes,
        "total_breaking": len(breaking_changes),
        "total_compatible": len(compatible_changes),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Rollback plan
# ---------------------------------------------------------------------------


def build_rollback_plan(
    diff: dict[str, Any],
    contract_id: str,
    baseline_snapshot_path: str | None = None,
) -> dict[str, Any]:
    """Construct a rollback plan for breaking changes."""
    steps: list[str] = []
    for change in diff.get("breaking_changes", []):
        ctype = change["type"]
        table = change.get("table", "")
        field = change.get("field", "")
        if ctype == "required_field_removed":
            steps.append(
                f"Re-add required field '{table}.{field}' to the schema "
                f"or mark it optional before removing."
            )
        elif ctype == "required_field_added":
            steps.append(
                f"Provide a default/backfill migration for '{table}.{field}' "
                f"or change it to nullable."
            )
        elif ctype == "type_changed":
            steps.append(
                f"Revert '{table}.{field}' type from "
                f"'{change.get('new_type')}' back to '{change.get('old_type')}', "
                f"or add a schema migration for all consumers."
            )
        elif ctype == "enum_changed":
            steps.append(
                f"Restore removed enum values for '{table}.{field}': "
                f"{change.get('reason')}"
            )
        elif ctype in ("table_removed",):
            steps.append(f"Restore table '{table}' or negotiate removal with all subscribers.")
        else:
            steps.append(
                f"Revert '{ctype}' on '{table}.{field}' ({change.get('reason', '')})"
            )

    if baseline_snapshot_path:
        steps.append(
            f"Emergency rollback: re-deploy contract from snapshot: {baseline_snapshot_path}"
        )

    return {
        "contract_id": contract_id,
        "rollback_required": diff["verdict"] == "breaking",
        "steps": steps if steps else ["No rollback required — schema is compatible."],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Per-consumer failure mode analysis
# ---------------------------------------------------------------------------


def build_consumer_failure_analysis(
    diff: dict[str, Any],
    registry: dict[str, Any],
    contract_id: str,
) -> list[dict[str, Any]]:
    """Analyse per-consumer failure modes for each breaking change.

    Uses the subscription registry to identify which subscribers consume
    each breaking field and what failure mode they would experience.
    """
    subscriptions = registry.get("subscriptions", [])
    analyses: list[dict[str, Any]] = []

    for change in diff.get("breaking_changes", []):
        field_path = f"{change.get('table', '')}.{change.get('field', '')}"
        affected_subs: list[dict] = []

        for sub in subscriptions:
            src = sub.get("source_contract") or sub.get("source", "")
            if contract_id not in src and contract_id not in sub.get("source", ""):
                continue
            for bf in sub.get("breaking_fields", []):
                if bf.get("field") == field_path or bf.get("field", "").endswith(
                    f".{change.get('field', '')}"
                ):
                    affected_subs.append(
                        {
                            "subscriber": sub.get("target"),
                            "subscriber_contract": sub.get("target_contract"),
                            "failure_mode": _infer_failure_mode(change),
                            "breaking_field_reason": bf.get("reason", ""),
                        }
                    )

        analyses.append(
            {
                "change": change,
                "field_path": field_path,
                "affected_subscribers": affected_subs,
                "unregistered_risk": len(affected_subs) == 0,
            }
        )

    return analyses


def _infer_failure_mode(change: dict) -> str:
    """Infer the likely failure mode for a downstream consumer."""
    ctype = change.get("type", "")
    severity = change.get("severity", "")
    if ctype == "required_field_removed":
        return "KeyError / NullPointerException at read time"
    if ctype == "required_field_added":
        return "Validation failure — existing records lack new required field"
    if ctype == "type_changed" and severity == "CRITICAL":
        return "Type cast exception or silent data corruption (e.g. truncated float→int)"
    if ctype == "enum_changed":
        return "Invalid enum value rejection by consumer schema validator"
    if ctype == "range_changed":
        return "Out-of-range validation failure for existing legal values"
    if ctype == "table_removed":
        return "Table-not-found error in downstream query or join"
    return f"Contract violation ({ctype})"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Diff schema snapshots and classify breaking changes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Preferred: directory-based snapshot selection
    parser.add_argument(
        "--contract-id",
        dest="contract_id",
        help="Contract ID — loads the two most-recent snapshots from "
             "schema_snapshots/<contract-id>/",
    )
    parser.add_argument(
        "--since",
        help="Only consider snapshots on or after this ISO date (e.g. 2025-01-01)",
    )

    # Fallback: explicit file paths
    parser.add_argument("--baseline", help="Explicit baseline schema YAML path")
    parser.add_argument("--current", help="Explicit current schema YAML path")

    parser.add_argument("--output", help="Path to write migration report JSON")
    parser.add_argument(
        "--registry",
        default=str(DEFAULT_REGISTRY_PATH),
        help="Path to subscription registry YAML",
    )

    args = parser.parse_args(argv)

    # Load the two snapshots
    baseline_path_str: str | None = None
    try:
        if args.contract_id:
            baseline_schema, current_schema = load_consecutive_snapshots(
                args.contract_id, args.since
            )
            snap_dir = SNAPSHOTS_DIR / args.contract_id
            candidates = sorted(snap_dir.glob("*.yaml"), key=_snapshot_timestamp)
            baseline_path_str = str(candidates[-2]) if len(candidates) >= 2 else None
        elif args.baseline and args.current:
            baseline_schema = load_schema(args.baseline)
            current_schema = load_schema(args.current)
            baseline_path_str = args.baseline
        else:
            parser.error("Provide --contract-id or both --baseline and --current.")
            return 2
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    # Load registry for consumer analysis
    try:
        registry = load_registry(args.registry)
    except Exception:
        registry = {"subscriptions": [], "contracts": []}

    contract_id = args.contract_id or "unknown"

    # Run diff
    diff = diff_schemas(baseline_schema, current_schema)

    # Build rollback plan
    rollback = build_rollback_plan(diff, contract_id, baseline_path_str)

    # Build per-consumer failure analysis
    consumer_analysis = build_consumer_failure_analysis(diff, registry, contract_id)

    report = {
        **diff,
        "contract_id": contract_id,
        "rollback_plan": rollback,
        "consumer_failure_analysis": consumer_analysis,
    }

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        print(f"Report written to {out}")

    print(json.dumps(report, indent=2))
    return 0 if diff["verdict"] == "compatible" else 1


if __name__ == "__main__":
    sys.exit(main())
