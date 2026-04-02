#!/usr/bin/env python3
"""
scripts/generate_week4_contract.py -- Bitol contract generator for Week 4 lineage graph.

The existing generator.py is designed around the Week 3 document schema.
Week 4 produces a lineage snapshot with nodes (96) and edges (80).
This script generates a proper contract with two tables:

  lineage_nodes  -- one row per node (node_id, type, label, path, language, last_modified)
  lineage_edges  -- one row per edge (source, target, relationship, confidence)

Usage:
  python scripts/generate_week4_contract.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from contracts.generator import (
    _write_yaml,
    build_dbt_schema,
    flatten_lineage_edges,
    flatten_lineage_nodes,
    load_jsonl,
    profile_dataframe,
    profile_to_field_clause,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONTRACT_ID = "week4-lineage-graph"
SOURCE_PATH = "outputs/week4/lineage_snapshots.jsonl"
OUTPUT_DIR = Path("generated_contracts")
SNAPSHOT_DIR = Path("schema_snapshots")

# Observed node types and edge relationships
ALLOWED_NODE_TYPES = ["FILE", "MODEL", "PIPELINE", "TABLE"]
ALLOWED_RELATIONSHIPS = ["READS", "WRITES", "CALLS", "IMPORTS"]
ALLOWED_LANGUAGES = ["sql", "yaml", "unknown"]


# ---------------------------------------------------------------------------
# Quality rules
# ---------------------------------------------------------------------------


def build_lineage_quality_rules(
    profiles_nodes: list[dict],
    profiles_edges: list[dict],
    total_nodes: int,
    total_edges: int,
) -> list[dict]:
    """Domain-specific quality rules for the lineage graph schema."""
    rules: list[dict] = []

    # --- lineage_nodes table ---

    # Required fields
    for col in ("node_id", "type", "label"):
        rules.append(
            {
                "name": f"lineage_nodes.{col}.not_null",
                "description": f"{col} must never be null",
                "dimension": "completeness",
                "severity": "error",
                "query": f"SELECT COUNT(*) FROM lineage_nodes WHERE {col} IS NULL",
                "mustBe": 0,
            }
        )

    # node_id uniqueness -- each node in the graph is unique
    rules.append(
        {
            "name": "lineage_nodes.node_id.unique",
            "description": "node_id must be unique across the lineage graph",
            "dimension": "uniqueness",
            "severity": "error",
            "query": "SELECT COUNT(*) - COUNT(DISTINCT node_id) FROM lineage_nodes",
            "mustBe": 0,
        }
    )

    # node type must be one of the known taxonomy values
    quoted_types = ", ".join(f"'{t}'" for t in ALLOWED_NODE_TYPES)
    rules.append(
        {
            "name": "lineage_nodes.type.allowed_values",
            "description": f"type must be one of {ALLOWED_NODE_TYPES}",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                f"SELECT COUNT(*) FROM lineage_nodes "
                f"WHERE type NOT IN ({quoted_types}) AND type IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    # language allowed values
    quoted_langs = ", ".join(f"'{l}'" for l in ALLOWED_LANGUAGES)
    rules.append(
        {
            "name": "lineage_nodes.language.allowed_values",
            "description": f"language must be one of {ALLOWED_LANGUAGES}",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                f"SELECT COUNT(*) FROM lineage_nodes "
                f"WHERE language NOT IN ({quoted_langs}) AND language IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    # non-empty label
    rules.append(
        {
            "name": "lineage_nodes.label.non_empty",
            "description": "label must not be an empty string",
            "dimension": "validity",
            "severity": "warning",
            "query": "SELECT COUNT(*) FROM lineage_nodes WHERE TRIM(label) = ''",
            "mustBe": 0,
        }
    )

    # --- lineage_edges table ---

    # Required fields
    for col in ("source", "target", "relationship", "confidence"):
        rules.append(
            {
                "name": f"lineage_edges.{col}.not_null",
                "description": f"{col} must never be null",
                "dimension": "completeness",
                "severity": "error",
                "query": f"SELECT COUNT(*) FROM lineage_edges WHERE {col} IS NULL",
                "mustBe": 0,
            }
        )

    # confidence must be in [0.0, 1.0]
    rules.append(
        {
            "name": "lineage_edges.confidence.range_check",
            "description": "confidence must be between 0.0 and 1.0",
            "dimension": "validity",
            "severity": "error",
            "query": (
                "SELECT COUNT(*) FROM lineage_edges "
                "WHERE confidence < 0.0 OR confidence > 1.0"
            ),
            "mustBe": 0,
        }
    )

    # Low-confidence edges (< 0.7) signal uncertain provenance
    rules.append(
        {
            "name": "lineage_edges.confidence.floor_check",
            "description": "confidence < 0.7 signals uncertain lineage; review extraction heuristics",
            "dimension": "validity",
            "severity": "warning",
            "query": "SELECT COUNT(*) FROM lineage_edges WHERE confidence < 0.7",
            "mustBe": 0,
        }
    )

    # relationship allowed values
    quoted_rels = ", ".join(f"'{r}'" for r in ALLOWED_RELATIONSHIPS)
    rules.append(
        {
            "name": "lineage_edges.relationship.allowed_values",
            "description": f"relationship must be one of {ALLOWED_RELATIONSHIPS}",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                f"SELECT COUNT(*) FROM lineage_edges "
                f"WHERE relationship NOT IN ({quoted_rels}) AND relationship IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    return rules


# ---------------------------------------------------------------------------
# Contract assembly
# ---------------------------------------------------------------------------


def build_week4_contract(
    tables: dict[str, list[dict]],
    record_count: int,
    source_path: str,
    row_counts: dict[str, int],
    quality_rules: list[dict],
    snapshot_metadata: dict,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    schema_tables = []
    for table_name, profiles in tables.items():
        fields = []
        for p in profiles:
            field = profile_to_field_clause(p)
            # node_id is a path-based identifier (e.g. "model::models/staging/..."),
            # NOT a UUID. Remove the uuid format constraint that the generic rule adds.
            if table_name == "lineage_nodes" and field["name"] == "node_id":
                field.pop("format", None)
            fields.append(field)
        schema_tables.append(
            {
                "name": table_name,
                "description": (
                    f"Profiled from {source_path} "
                    f"({row_counts.get(table_name, 0)} rows, {record_count} snapshot records)"
                ),
                "fields": fields,
            }
        )

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": CONTRACT_ID,
        "info": {
            "title": f"Data Contract: {CONTRACT_ID}",
            "version": "1.0.0",
            "description": (
                f"Bitol data contract for Week 4 brownfield-cartographer lineage graph. "
                f"Snapshot contains {row_counts.get('lineage_nodes', 0)} nodes and "
                f"{row_counts.get('lineage_edges', 0)} edges. "
                f"Generated at {now}."
            ),
            "owner": "data-engineering",
            "contact": {"name": "TRP Week 7 Generator"},
        },
        "servers": {
            "production": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
                "description": "Week 4 brownfield-cartographer lineage snapshot export",
            }
        },
        "schema": {
            "type": "json",
            "tables": schema_tables,
        },
        "quality": {
            "type": "custom",
            "specification": "https://bitol.io/specs/quality/v1",
            "rules": quality_rules,
        },
        "lineage": {
            "inputPorts": [
                {
                    "type": "lineage_graph",
                    "uri": source_path,
                    "description": (
                        f"Week 4 brownfield-cartographer lineage snapshot: "
                        f"{row_counts.get('lineage_nodes', 0)} nodes, "
                        f"{row_counts.get('lineage_edges', 0)} edges, "
                        f"git_commit={snapshot_metadata.get('git_commit', 'unknown')[:12]}"
                    ),
                    "format": "jsonl",
                    "recordCount": record_count,
                    "capturedAt": snapshot_metadata.get("captured_at", now),
                }
            ],
            "outputPorts": [
                {
                    "type": "data_contract",
                    "uri": f"generated_contracts/{CONTRACT_ID}.yaml",
                    "description": "Generated Bitol YAML data contract",
                    "generatedAt": now,
                },
                {
                    "type": "dbt_schema",
                    "uri": f"generated_contracts/{CONTRACT_ID}_dbt_schema.yml",
                    "description": "dbt schema.yml for downstream model testing",
                    "generatedAt": now,
                },
            ],
        },
        "generatedAt": now,
        "generatorVersion": "1.0.0",
    }

    return contract


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"[week4-generator] Stage 1 -- Loading {SOURCE_PATH} ...")
    records = load_jsonl(SOURCE_PATH)
    print(f"  Loaded {len(records)} snapshot records")

    # Extract snapshot metadata for lineage port description
    snapshot_metadata = records[0] if records else {}

    df_nodes = flatten_lineage_nodes(records)
    df_edges = flatten_lineage_edges(records)
    print(
        f"  Flattened -> lineage_nodes={len(df_nodes)}rx{len(df_nodes.columns)}c  "
        f"lineage_edges={len(df_edges)}rx{len(df_edges.columns)}c"
    )

    print("[week4-generator] Stage 2 -- Profiling columns ...")
    profiles_nodes = profile_dataframe(df_nodes)
    profiles_edges = profile_dataframe(df_edges)
    print(f"  {len(profiles_nodes) + len(profiles_edges)} fields profiled across 2 tables")

    print("[week4-generator] Stage 3 -- Building quality rules ...")
    quality_rules = build_lineage_quality_rules(
        profiles_nodes=profiles_nodes,
        profiles_edges=profiles_edges,
        total_nodes=len(df_nodes),
        total_edges=len(df_edges),
    )
    print(f"  {len(quality_rules)} quality rules")

    print("[week4-generator] Stage 4 -- Assembling contract ...")
    tables: dict[str, list[dict]] = {
        "lineage_nodes": profiles_nodes,
        "lineage_edges": profiles_edges,
    }
    row_counts = {"lineage_nodes": len(df_nodes), "lineage_edges": len(df_edges)}

    contract = build_week4_contract(
        tables=tables,
        record_count=len(records),
        source_path=SOURCE_PATH,
        row_counts=row_counts,
        quality_rules=quality_rules,
        snapshot_metadata={
            k: v for k, v in snapshot_metadata.items()
            if k not in ("nodes", "edges")
        },
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    contract_path = OUTPUT_DIR / f"{CONTRACT_ID}.yaml"
    _write_yaml(contract, contract_path)
    print(f"  Contract  -> {contract_path}")

    dbt_schema = build_dbt_schema(CONTRACT_ID, tables)
    dbt_path = OUTPUT_DIR / f"{CONTRACT_ID}_dbt_schema.yml"
    _write_yaml(dbt_schema, dbt_path)
    print(f"  dbt schema -> {dbt_path}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = SNAPSHOT_DIR / CONTRACT_ID / f"{timestamp}.yaml"
    _write_yaml(contract, snapshot_path)
    print(f"  Snapshot  -> {snapshot_path}")

    actual_clauses = sum(len(t["fields"]) for t in contract["schema"]["tables"])
    print()
    print("[week4-generator] Complete.")
    print(f"  Schema clauses : {actual_clauses}")
    print(f"  Quality rules  : {len(quality_rules)}")
    print(f"  Contract ID    : {CONTRACT_ID}")
    print(f"  apiVersion     : v3.0.0")

    return 0


if __name__ == "__main__":
    sys.exit(main())
