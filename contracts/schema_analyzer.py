#!/usr/bin/env python3
"""
contracts/schema_analyzer.py -- Schema evolution analyzer

Diffs two schema snapshots and determines if breaking changes are present.
A breaking change is defined as:
  - A required field is removed without migration
  - A field type changes incompatibly
  - An enum constraint is tightened (allowed values reduced)
  - A range constraint is tightened (bounds narrowed)
  - A newly required field is added to an existing table

Usage:
  python contracts/schema_analyzer.py \
    --baseline schema_snapshots/week3-document-refinery-extractions/baseline.yaml \
    --current generated_contracts/week3-document-refinery-extractions.yaml \
    --output validation_reports/schema_evolution_report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def load_schema(path: str | Path) -> dict[str, Any]:
    """Load a schema YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def classify_type_change(old_type: str, new_type: str) -> tuple[bool, str]:
    """
    Classify a type change as breaking or non-breaking.
    
    Returns (is_breaking, reason)
    """
    # Same type is never breaking
    if old_type == new_type:
        return False, "no change"
    
    # String -> more restrictive is breaking
    if old_type == "string" and new_type in ["uuid", "datetime"]:
        return True, f"narrowed from string to {new_type}"
    
    # Integer -> string is breaking (loss of semantics)
    if old_type == "integer" and new_type == "string":
        return True, "changed from integer to string"
    
    # Float -> integer is breaking (loss of precision)
    if old_type == "number" and new_type == "integer":
        return True, "changed from number to integer"
    
    # Removing a type constraint is non-breaking (more permissive)
    # Adding a type constraint is breaking (more restrictive)
    return True, f"type changed from {old_type} to {new_type}"


def classify_enum_change(old_values: list[str], new_values: list[str]) -> tuple[bool, str]:
    """
    Classify an enum change as breaking or non-breaking.
    
    Breaking: allowed values shrink (existing enum values removed)
    Non-breaking: allowed values grow (new enum values added)
    
    Returns (is_breaking, reason)
    """
    old_set = set(old_values)
    new_set = set(new_values)
    
    removed = old_set - new_set
    added = new_set - old_set
    
    if removed:
        return True, f"removed allowed values: {removed}"
    
    if added:
        return False, f"added allowed values: {added}"
    
    return False, "enum unchanged"


def classify_range_change(
    old_min: float | None, old_max: float | None,
    new_min: float | None, new_max: float | None,
    field_name: str
) -> tuple[bool, str]:
    """
    Classify a range constraint change as breaking or non-breaking.
    
    Breaking: range shrinks (new bounds are tighter than old bounds)
    Non-breaking: range expands (new bounds are looser than old bounds)
    
    Returns (is_breaking, reason)
    """
    breaking = False
    reasons = []
    
    # Check minimum bound tightening
    if old_min is not None and new_min is not None:
        if new_min > old_min:
            breaking = True
            reasons.append(f"minimum increased from {old_min} to {new_min}")
    elif old_min is None and new_min is not None:
        # Adding a new minimum is breaking
        breaking = True
        reasons.append(f"minimum added: {new_min}")
    
    # Check maximum bound tightening
    if old_max is not None and new_max is not None:
        if new_max < old_max:
            breaking = True
            reasons.append(f"maximum decreased from {old_max} to {new_max}")
    elif old_max is None and new_max is not None:
        # Adding a new maximum is breaking
        breaking = True
        reasons.append(f"maximum added: {new_max}")
    
    reason = " AND ".join(reasons) if reasons else "range unchanged"
    return breaking, reason


def diff_schemas(
    baseline_schema: dict[str, Any],
    current_schema: dict[str, Any]
) -> dict[str, Any]:
    """
    Diff two schemas and identify breaking changes.
    
    Returns a report with:
      - breaking_changes: list of breaking changes found
      - non_breaking_changes: list of non-breaking changes
      - verdict: "compatible" or "breaking"
      - details: technical details for each change
    """
    breaking_changes = []
    non_breaking_changes = []
    
    baseline_tables = baseline_schema.get("tables", {})
    current_tables = current_schema.get("tables", {})
    
    # Check for removed tables (breaking)
    for table_id, table_spec in baseline_tables.items():
        if table_id not in current_tables:
            breaking_changes.append({
                "type": "table_removed",
                "table_id": table_id,
                "severity": "CRITICAL",
                "reason": f"Table '{table_id}' was removed"
            })
    
    # Check each table for field-level changes
    for table_id, current_spec in current_tables.items():
        baseline_spec = baseline_tables.get(table_id, {})
        baseline_fields = baseline_spec.get("fields", {})
        current_fields = current_spec.get("fields", {})
        
        # Check for removed required fields (breaking)
        for field_id, field_spec in baseline_fields.items():
            if field_id not in current_fields:
                is_required = field_spec.get("required", False)
                if is_required:
                    breaking_changes.append({
                        "type": "required_field_removed",
                        "table_id": table_id,
                        "field_id": field_id,
                        "severity": "CRITICAL",
                        "reason": f"Required field '{field_id}' removed from table '{table_id}'"
                    })
                else:
                    non_breaking_changes.append({
                        "type": "optional_field_removed",
                        "table_id": table_id,
                        "field_id": field_id,
                        "severity": "LOW",
                        "reason": f"Optional field '{field_id}' removed from table '{table_id}'"
                    })
        
        # Check for added required fields (breaking in strict mode)
        for field_id, field_spec in current_fields.items():
            if field_id not in baseline_fields:
                is_required = field_spec.get("required", False)
                if is_required:
                    breaking_changes.append({
                        "type": "required_field_added",
                        "table_id": table_id,
                        "field_id": field_id,
                        "severity": "HIGH",
                        "reason": f"Required field '{field_id}' added to table '{table_id}'"
                    })
                else:
                    non_breaking_changes.append({
                        "type": "optional_field_added",
                        "table_id": table_id,
                        "field_id": field_id,
                        "severity": "LOW",
                        "reason": f"Optional field '{field_id}' added to table '{table_id}'"
                    })
        
        # Check for field-level changes within existing fields
        for field_id in baseline_fields:
            if field_id not in current_fields:
                continue
            
            baseline_field = baseline_fields[field_id]
            current_field = current_fields[field_id]
            
            # Type change check
            baseline_type = baseline_field.get("type")
            current_type = current_field.get("type")
            if baseline_type != current_type:
                is_breaking, reason = classify_type_change(baseline_type, current_type)
                change = {
                    "type": "field_type_changed",
                    "table_id": table_id,
                    "field_id": field_id,
                    "old_type": baseline_type,
                    "new_type": current_type,
                    "severity": "CRITICAL" if is_breaking else "LOW",
                    "reason": reason
                }
                (breaking_changes if is_breaking else non_breaking_changes).append(change)
            
            # Enum change check
            baseline_enum = baseline_field.get("enum", [])
            current_enum = current_field.get("enum", [])
            if baseline_enum and current_enum:
                is_breaking, reason = classify_enum_change(baseline_enum, current_enum)
                if baseline_enum != current_enum:
                    change = {
                        "type": "enum_changed",
                        "table_id": table_id,
                        "field_id": field_id,
                        "old_values": baseline_enum,
                        "new_values": current_enum,
                        "severity": "HIGH" if is_breaking else "LOW",
                        "reason": reason
                    }
                    (breaking_changes if is_breaking else non_breaking_changes).append(change)
            
            # Range change check
            baseline_min = baseline_field.get("min_value")
            baseline_max = baseline_field.get("max_value")
            current_min = current_field.get("min_value")
            current_max = current_field.get("max_value")
            
            if (baseline_min is not None or baseline_max is not None or
                current_min is not None or current_max is not None):
                is_breaking, reason = classify_range_change(
                    baseline_min, baseline_max,
                    current_min, current_max,
                    field_id
                )
                if (baseline_min != current_min or
                    baseline_max != current_max):
                    change = {
                        "type": "range_changed",
                        "table_id": table_id,
                        "field_id": field_id,
                        "old_range": [baseline_min, baseline_max],
                        "new_range": [current_min, current_max],
                        "severity": "HIGH" if is_breaking else "LOW",
                        "reason": reason
                    }
                    (breaking_changes if is_breaking else non_breaking_changes).append(change)
    
    verdict = "breaking" if breaking_changes else "compatible"
    
    return {
        "verdict": verdict,
        "breaking_changes": breaking_changes,
        "non_breaking_changes": non_breaking_changes,
        "total_breaking": len(breaking_changes),
        "total_non_breaking": len(non_breaking_changes),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    parser = argparse.ArgumentParser(
        description="Diff two schema snapshots and classify breaking changes"
    )
    parser.add_argument(
        "--baseline",
        required=True,
        help="Path to baseline schema YAML"
    )
    parser.add_argument(
        "--current",
        required=True,
        help="Path to current schema YAML"
    )
    parser.add_argument(
        "--output",
        help="Path to write schema evolution report JSON"
    )
    
    args = parser.parse_args()
    
    try:
        baseline = load_schema(args.baseline)
        current = load_schema(args.current)
        
        report = diff_schemas(baseline, current)
        
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2)
            print(f"Report written to {output_path}")
        
        print(json.dumps(report, indent=2))
        
        sys.exit(0 if report["verdict"] == "compatible" else 1)
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
