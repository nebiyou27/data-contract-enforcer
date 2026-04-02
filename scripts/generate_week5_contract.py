#!/usr/bin/env python3
"""
scripts/generate_week5_contract.py -- Bitol contract generator for Week 5 event store.

The existing generator.py is designed around the Week 3 document/facts/entities
schema. Week 5 uses a flat event-sourcing schema. This script generates a proper
contract for events.jsonl with two tables:

  events          -- top-level scalar fields (event_id, event_type, aggregate_id, ...)
  event_metadata  -- metadata sub-object (causation_id, correlation_id, source_service, ...)

Usage:
  python scripts/generate_week5_contract.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

# Add project root to sys.path so we can import contracts.*
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from contracts.generator import (
    _write_yaml,
    build_dbt_schema,
    flatten_event_metadata,
    flatten_events,
    load_jsonl,
    profile_dataframe,
    profile_to_field_clause,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONTRACT_ID = "week5-event-store"
SOURCE_PATH = "outputs/week5/events.jsonl"
OUTPUT_DIR = Path("generated_contracts")
SNAPSHOT_DIR = Path("schema_snapshots")

# Observed event types (34 total) -- too many for the generic cardinality <= 10
# enum rule, so we declare them explicitly as an allowed-values quality rule.
ALLOWED_EVENT_TYPES = [
    "AgentInputValidated", "AgentNodeExecuted", "AgentOutputWritten",
    "AgentSessionCompleted", "AgentSessionStarted", "AgentToolCalled",
    "ApplicationApproved", "ApplicationDeclined", "ApplicationSubmitted",
    "ComplianceCheckCompleted", "ComplianceCheckInitiated", "ComplianceCheckRequested",
    "ComplianceRuleFailed", "ComplianceRuleNoted", "ComplianceRulePassed",
    "CreditAnalysisCompleted", "CreditAnalysisRequested", "CreditRecordOpened",
    "DecisionGenerated", "DecisionRequested",
    "DocumentAdded", "DocumentFormatValidated", "DocumentUploadRequested", "DocumentUploaded",
    "ExtractedFactsConsumed", "ExtractionCompleted", "ExtractionStarted",
    "FraudScreeningCompleted", "FraudScreeningInitiated", "FraudScreeningRequested",
    "HistoricalProfileConsumed",
    "PackageCreated", "PackageReadyForAnalysis",
    "QualityAssessmentCompleted",
]


# ---------------------------------------------------------------------------
# Quality rules
# ---------------------------------------------------------------------------


def build_event_quality_rules(
    profiles_events: list[dict],
    profiles_meta: list[dict],
    total_events: int,
) -> list[dict]:
    """Domain-specific quality rules for the event store schema."""
    rules: list[dict] = []

    # --- events table ---

    # Not-null for all required event fields
    required_event_cols = [
        "event_id", "event_type", "aggregate_id", "aggregate_type",
        "sequence_number", "occurred_at", "recorded_at",
    ]
    for col in required_event_cols:
        rules.append(
            {
                "name": f"events.{col}.not_null",
                "description": f"{col} must never be null",
                "dimension": "completeness",
                "severity": "error",
                "query": f"SELECT COUNT(*) FROM events WHERE {col} IS NULL",
                "mustBe": 0,
            }
        )

    # Uniqueness: event_id is a deterministic UUID5 -- must be globally unique
    rules.append(
        {
            "name": "events.event_id.unique",
            "description": "event_id must be unique -- duplicates indicate replay or ID collision",
            "dimension": "uniqueness",
            "severity": "error",
            "query": "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM events",
            "mustBe": 0,
        }
    )

    # Allowed event types -- explicit enumeration of the 34 known domain event types
    quoted_types = ", ".join(f"'{t}'" for t in ALLOWED_EVENT_TYPES)
    rules.append(
        {
            "name": "events.event_type.allowed_values",
            "description": "event_type must be one of the 34 known domain event types",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                f"SELECT COUNT(*) FROM events "
                f"WHERE event_type NOT IN ({quoted_types}) AND event_type IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    # Sequence number is 0-indexed per stream
    rules.append(
        {
            "name": "events.sequence_number.non_negative",
            "description": "sequence_number must be >= 0 (0-indexed per stream)",
            "dimension": "validity",
            "severity": "error",
            "query": "SELECT COUNT(*) FROM events WHERE sequence_number < 0",
            "mustBe": 0,
        }
    )

    # Schema version allowed values (observed: '1.0', '2.0')
    rules.append(
        {
            "name": "events.schema_version.allowed_values",
            "description": "schema_version must be a recognised contract version",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                "SELECT COUNT(*) FROM events "
                "WHERE schema_version NOT IN ('1.0', '2.0') AND schema_version IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    # --- event_metadata table ---

    # correlation_id must always be present (causal chain integrity)
    rules.append(
        {
            "name": "event_metadata.correlation_id.not_null",
            "description": "correlation_id must never be null -- it anchors the causal chain",
            "dimension": "completeness",
            "severity": "error",
            "query": "SELECT COUNT(*) FROM event_metadata WHERE correlation_id IS NULL",
            "mustBe": 0,
        }
    )

    # Uniqueness of event_id in metadata table (1:1 with events)
    rules.append(
        {
            "name": "event_metadata.event_id.unique",
            "description": "event_id in event_metadata must be unique (1:1 with events table)",
            "dimension": "uniqueness",
            "severity": "error",
            "query": "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM event_metadata",
            "mustBe": 0,
        }
    )

    # source_service allowed values (6 observed services)
    rules.append(
        {
            "name": "event_metadata.source_service.allowed_values",
            "description": "source_service must be one of the 6 known microservices",
            "dimension": "validity",
            "severity": "warning",
            "query": (
                "SELECT COUNT(*) FROM event_metadata "
                "WHERE source_service NOT IN ("
                "'loan-origination', 'document-management', 'credit-analysis', "
                "'compliance-engine', 'fraud-screening', 'agent-runtime'"
                ") AND source_service IS NOT NULL"
            ),
            "mustBe": 0,
        }
    )

    # global_position >= 1 (1-indexed global ordering)
    rules.append(
        {
            "name": "event_metadata.global_position.positive",
            "description": "global_position must be >= 1 (1-indexed global event order)",
            "dimension": "validity",
            "severity": "error",
            "query": "SELECT COUNT(*) FROM event_metadata WHERE global_position < 1",
            "mustBe": 0,
        }
    )

    return rules


# ---------------------------------------------------------------------------
# Contract assembly
# ---------------------------------------------------------------------------


def build_week5_contract(
    tables: dict[str, list[dict]],
    record_count: int,
    source_path: str,
    row_counts: dict[str, int],
    quality_rules: list[dict],
) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    # Columns whose names end with _id but are NOT UUIDs
    NON_UUID_ID_COLS = {"user_id", "original_stream_id"}

    schema_tables = []
    for table_name, profiles in tables.items():
        fields = []
        for p in profiles:
            field = profile_to_field_clause(p)
            # Remove the generic uuid format from human-readable / stream-id columns
            if field["name"] in NON_UUID_ID_COLS:
                field.pop("format", None)
            fields.append(field)
        schema_tables.append(
            {
                "name": table_name,
                "description": (
                    f"Profiled from {source_path} "
                    f"({row_counts.get(table_name, 0)} rows, {record_count} source events)"
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
                f"Bitol data contract for Week 5 event store (the-ledger). "
                f"Profiled from {record_count} domain events across 6 aggregate streams. "
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
                "description": "Week 5 event store canonical export (the-ledger seed events)",
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
                    "type": "event_store",
                    "uri": source_path,
                    "description": (
                        "Week 5 the-ledger event store: 1198 events, 34 event types, "
                        "151 streams across 6 aggregate types"
                    ),
                    "format": "jsonl",
                    "recordCount": record_count,
                    "capturedAt": now,
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
    print(f"[week5-generator] Stage 1 -- Loading {SOURCE_PATH} ...")
    records = load_jsonl(SOURCE_PATH)
    print(f"  Loaded {len(records)} events")

    df_events = flatten_events(records)
    df_meta = flatten_event_metadata(records)
    print(
        f"  Flattened -> events={len(df_events)}rx{len(df_events.columns)}c  "
        f"event_metadata={len(df_meta)}rx{len(df_meta.columns)}c"
    )

    print("[week5-generator] Stage 2 -- Profiling columns ...")
    profiles_events = profile_dataframe(df_events)
    profiles_meta = profile_dataframe(df_meta)
    print(f"  {len(profiles_events) + len(profiles_meta)} fields profiled across 2 tables")

    print("[week5-generator] Stage 3 -- Building quality rules ...")
    quality_rules = build_event_quality_rules(
        profiles_events, profiles_meta, total_events=len(records)
    )
    print(f"  {len(quality_rules)} quality rules")

    print("[week5-generator] Stage 4 -- Assembling contract ...")
    tables: dict[str, list[dict]] = {
        "events": profiles_events,
        "event_metadata": profiles_meta,
    }
    row_counts = {"events": len(df_events), "event_metadata": len(df_meta)}

    contract = build_week5_contract(
        tables=tables,
        record_count=len(records),
        source_path=SOURCE_PATH,
        row_counts=row_counts,
        quality_rules=quality_rules,
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
    print("[week5-generator] Complete.")
    print(f"  Schema clauses : {actual_clauses}")
    print(f"  Quality rules  : {len(quality_rules)}")
    print(f"  Contract ID    : {CONTRACT_ID}")
    print(f"  apiVersion     : v3.0.0")

    return 0


if __name__ == "__main__":
    sys.exit(main())
