#!/usr/bin/env python3
"""
scripts/migrate_week5.py -- Week 5 Event Store -> Week 7 Canonical JSONL

Reads the Week 5 seed_events.jsonl (flat event records from the-ledger)
and converts to the Week 7 canonical event format at outputs/week5/events.jsonl.

Source format (seed_events.jsonl):
  {"stream_id", "event_type", "event_version", "payload", "recorded_at"}

Target format (events.jsonl):
  {"event_id", "event_type", "aggregate_id", "aggregate_type", "sequence_number",
   "payload", "metadata", "schema_version", "occurred_at", "recorded_at"}

Derivations:
  - event_id:        stable UUID5 from (stream_id + sequence_number + event_type)
  - aggregate_id:    stable UUID5 from the entity identifier extracted from stream_id
  - aggregate_type:  mapped from stream_id prefix (loan -> LoanApplication, etc.)
  - sequence_number: per-stream position (computed in order of appearance)
  - metadata:        constructed with correlation_id grouped by application
  - schema_version:  from event_version (default "1.0")
  - occurred_at:     from recorded_at (best available timestamp)
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Namespace UUID for deterministic UUID5 generation
NAMESPACE = uuid.UUID("d7e6f5a4-b3c2-1d0e-9f8a-7b6c5d4e3f2a")

# Stream prefix -> PascalCase aggregate type
AGGREGATE_TYPE_MAP: dict[str, str] = {
    "loan": "LoanApplication",
    "docpkg": "DocumentPackage",
    "agent": "AgentSession",
    "credit": "CreditRecord",
    "compliance": "ComplianceRecord",
    "fraud": "FraudScreening",
    "audit": "AuditLedger",
}

# Stream prefix -> source_service name
SERVICE_MAP: dict[str, str] = {
    "loan": "loan-origination",
    "docpkg": "document-management",
    "agent": "agent-runtime",
    "credit": "credit-analysis",
    "compliance": "compliance-engine",
    "fraud": "fraud-screening",
    "audit": "audit-service",
}


def parse_stream_id(stream_id: str) -> tuple[str, str]:
    """Extract (prefix, entity_key) from a stream_id.

    Examples:
      "loan-APEX-0007"        -> ("loan", "APEX-0007")
      "docpkg-APEX-0001"      -> ("docpkg", "APEX-0001")
      "agent-credit-sess-042" -> ("agent", "credit-sess-042")
    """
    parts = stream_id.split("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], stream_id


def stable_uuid(seed: str) -> str:
    """Deterministic UUID5 from a seed string."""
    return str(uuid.uuid5(NAMESPACE, seed))


def derive_correlation_id(stream_id: str, payload: dict) -> str | None:
    """Derive a correlation_id that groups related events.

    Uses application_id from payload if present (links loan + docpkg + credit
    + compliance + fraud streams for the same application). Falls back to
    a stable UUID from the stream_id.
    """
    app_id = payload.get("application_id")
    if app_id:
        return stable_uuid(f"correlation:{app_id}")

    # For agent sessions, try session_id
    session_id = payload.get("session_id")
    if session_id:
        return stable_uuid(f"correlation:{session_id}")

    return stable_uuid(f"correlation:{stream_id}")


def derive_user_id(payload: dict) -> str:
    """Best-effort user_id extraction from payload."""
    for key in ("uploaded_by", "requested_by", "reviewed_by", "approved_by", "declined_by"):
        val = payload.get(key)
        if val:
            return str(val)
    return "system"


def migrate_event(
    raw: dict,
    sequence_number: int,
    global_position: int,
) -> dict:
    """Convert one seed event to canonical format."""
    stream_id = raw["stream_id"]
    event_type = raw["event_type"]
    event_version = raw.get("event_version", 1)
    payload = raw.get("payload", {})
    recorded_at = raw.get("recorded_at", "")

    prefix, entity_key = parse_stream_id(stream_id)
    aggregate_type = AGGREGATE_TYPE_MAP.get(prefix, prefix.title())

    event_id = stable_uuid(f"{stream_id}:{sequence_number}:{event_type}")
    aggregate_id = stable_uuid(f"aggregate:{prefix}:{entity_key}")

    correlation_id = derive_correlation_id(stream_id, payload)
    user_id = derive_user_id(payload)
    source_service = SERVICE_MAP.get(prefix, "unknown")

    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_id": aggregate_id,
        "aggregate_type": aggregate_type,
        "sequence_number": sequence_number,
        "payload": payload,
        "metadata": {
            "causation_id": None,
            "correlation_id": correlation_id,
            "user_id": user_id,
            "source_service": source_service,
            "original_stream_id": stream_id,
            "global_position": global_position,
        },
        "schema_version": str(event_version) + ".0" if "." not in str(event_version) else str(event_version),
        "occurred_at": recorded_at,
        "recorded_at": recorded_at,
    }


def load_seed_events(path: Path) -> list[dict]:
    """Load JSONL seed events, preserving insertion order."""
    records = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate Week 5 event store data to Week 7 canonical JSONL."
    )
    parser.add_argument(
        "--input",
        default=str(Path("d:/TRP-1/Week-5/the-ledger/data/seed_events.jsonl")),
        help="Path to Week 5 seed_events.jsonl",
    )
    parser.add_argument(
        "--output",
        default="outputs/week5/events.jsonl",
        help="Output path for canonical events JSONL",
    )
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"[migrate_week5] ERROR: input not found: {input_path}")
        return 1

    print(f"[migrate_week5] Loading {input_path} ...")
    raw_events = load_seed_events(input_path)
    print(f"  {len(raw_events)} raw events loaded")

    # Compute per-stream sequence numbers
    stream_positions: dict[str, int] = defaultdict(int)
    canonical_events: list[dict] = []

    for global_pos, raw in enumerate(raw_events, start=1):
        stream_id = raw["stream_id"]
        seq = stream_positions[stream_id]
        stream_positions[stream_id] += 1

        canonical = migrate_event(raw, seq, global_pos)
        canonical_events.append(canonical)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        for event in canonical_events:
            fh.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")

    # Summary
    unique_types = set(e["event_type"] for e in canonical_events)
    unique_aggregates = set(e["aggregate_id"] for e in canonical_events)
    unique_agg_types = set(e["aggregate_type"] for e in canonical_events)

    print(f"[migrate_week5] Wrote {len(canonical_events)} events to {output_path}")
    print(f"  Unique event types:    {len(unique_types)}")
    print(f"  Unique aggregates:     {len(unique_aggregates)}")
    print(f"  Aggregate types:       {sorted(unique_agg_types)}")
    print(f"  Streams:               {len(stream_positions)}")
    print(f"  Max stream length:     {max(stream_positions.values())}")

    # Validate a sample
    sample = canonical_events[0]
    required_keys = {
        "event_id", "event_type", "aggregate_id", "aggregate_type",
        "sequence_number", "payload", "metadata", "schema_version",
        "occurred_at", "recorded_at",
    }
    missing = required_keys - set(sample.keys())
    if missing:
        print(f"  WARNING: missing keys in output: {missing}")
    else:
        print(f"  Schema validation: all required keys present")

    meta_keys = {"causation_id", "correlation_id", "user_id", "source_service"}
    meta_missing = meta_keys - set(sample["metadata"].keys())
    if meta_missing:
        print(f"  WARNING: missing metadata keys: {meta_missing}")
    else:
        print(f"  Metadata validation: all required keys present")

    return 0


if __name__ == "__main__":
    sys.exit(main())
