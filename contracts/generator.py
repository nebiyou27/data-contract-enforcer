#!/usr/bin/env python3
"""
contracts/generator.py -- Bitol Data Contract Generator
Follows the Practitioner Manual's 4-stage approach:

  Stage 1: Load JSONL + flatten nested records into DataFrames
  Stage 2: Profile each column (dtype, null_fraction, cardinality, stats)
  Stage 3: Translate profiles to Bitol YAML clauses using rule set
Stage 4: Inject registry + lineage context, write YAML contract + dbt schema.yml

Usage:
  python contracts/generator.py \
    --source outputs/week3/extractions.jsonl \
    --contract-id week3-document-refinery-extractions \
    --lineage outputs/week4/lineage_snapshots.jsonl \
    --output generated_contracts/
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

try:
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry
    from contracts.log_config import configure_logging
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry
    from contracts.log_config import configure_logging

try:
    import anthropic as _anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

BASELINES_PATH = Path("schema_snapshots") / "baselines.json"

logger = logging.getLogger(__name__)


# --- Stage 1: Load + Flatten -------------------------------------------------


def load_jsonl(path: str) -> list[dict]:
    """Read a JSONL file and return a list of dicts.

    NOTE: loads the entire file into RAM.  For large files prefer
    ``iter_jsonl`` which yields one record at a time.
    """
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def iter_jsonl(path: str):
    """Yield one parsed JSON record at a time from a JSONL file.

    Memory footprint is O(1 record) regardless of file size, making this
    safe for multi-GB extraction files.  Each call opens the file from the
    start, so callers that need multiple passes (e.g. flatten_documents +
    flatten_facts on the same source) should call iter_jsonl separately for
    each pass rather than materialising the full list.
    """
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _json_stringify(value: Any) -> str | None:
    """Convert nested JSON-like payloads into stable JSON strings."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def flatten_trace_nodes(records: list[dict]) -> pd.DataFrame:
    """Flatten LangSmith trace-tree nodes into one row per run node."""
    rows = []
    for r in records:
        rows.append(
            {
                "run_id": r.get("id"),
                "parent_run_id": r.get("parent_run_id") or None,
                "depth": r.get("depth"),
                "name": r.get("name"),
                "run_type": r.get("run_type"),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "inputs_json": _json_stringify(r.get("inputs")),
                "outputs_json": _json_stringify(r.get("outputs")),
                "error": r.get("error"),
                "tags_json": _json_stringify(r.get("tags")),
                "app_path": r.get("app_path"),
                "trace_project_id": r.get("trace_project"),
            }
        )
    return pd.DataFrame(rows)


def flatten_documents(records: list[dict]) -> pd.DataFrame:
    """Top-level document fields -- one row per source document."""
    rows = []
    for r in records:
        tc = r.get("token_count") or {}
        rows.append(
            {
                "doc_id": r.get("doc_id"),
                "source_path": r.get("source_path"),
                "source_hash": r.get("source_hash"),
                "extraction_model": r.get("extraction_model"),
                "processing_time_ms": r.get("processing_time_ms"),
                "token_count_input": tc.get("input"),
                "token_count_output": tc.get("output"),
                "extracted_at": r.get("extracted_at"),
                "fact_count": len(r.get("extracted_facts") or []),
                "entity_count": len(r.get("entities") or []),
            }
        )
    return pd.DataFrame(rows)


def flatten_facts(records: list[dict]) -> pd.DataFrame:
    """Explode extracted_facts arrays -- one row per fact."""
    rows = []
    for r in records:
        doc_id = r.get("doc_id")
        for fact in r.get("extracted_facts") or []:
            rows.append(
                {
                    "doc_id": doc_id,
                    "fact_id": fact.get("fact_id"),
                    "text": fact.get("text"),
                    "confidence": fact.get("confidence"),
                    "page_ref": fact.get("page_ref"),
                    "source_excerpt": fact.get("source_excerpt"),
                }
            )
    return pd.DataFrame(rows)


def flatten_entities(records: list[dict]) -> pd.DataFrame:
    """Explode entities arrays -- one row per entity occurrence."""
    rows = []
    for r in records:
        doc_id = r.get("doc_id")
        for ent in r.get("entities") or []:
            rows.append(
                {
                    "doc_id": doc_id,
                    "entity_id": ent.get("entity_id"),
                    "name": ent.get("name"),
                    "type": ent.get("type"),
                    "canonical_value": ent.get("canonical_value"),
                }
            )
    return pd.DataFrame(rows)


def flatten_events(records: list[dict]) -> pd.DataFrame:
    """Top-level event fields -- one row per event record."""
    rows = []
    for r in records:
        rows.append(
            {
                "event_id": r.get("event_id"),
                "event_type": r.get("event_type"),
                "aggregate_id": r.get("aggregate_id"),
                "aggregate_type": r.get("aggregate_type"),
                "sequence_number": r.get("sequence_number"),
                "schema_version": r.get("schema_version"),
                "occurred_at": r.get("occurred_at"),
                "recorded_at": r.get("recorded_at"),
            }
        )
    return pd.DataFrame(rows)


def flatten_event_metadata(records: list[dict]) -> pd.DataFrame:
    """Flatten metadata sub-object -- one row per event record."""
    rows = []
    for r in records:
        meta = r.get("metadata") or {}
        rows.append(
            {
                "event_id": r.get("event_id"),
                "causation_id": meta.get("causation_id"),
                "correlation_id": meta.get("correlation_id"),
                "user_id": meta.get("user_id"),
                "source_service": meta.get("source_service"),
                "original_stream_id": meta.get("original_stream_id"),
                "global_position": meta.get("global_position"),
            }
        )
    return pd.DataFrame(rows)


def flatten_lineage_nodes(records: list[dict]) -> pd.DataFrame:
    """Explode nodes array from lineage snapshots -- one row per node."""
    rows = []
    for r in records:
        for node in r.get("nodes") or []:
            meta = node.get("metadata") or {}
            rows.append(
                {
                    "node_id": node.get("node_id"),
                    "type": node.get("type"),
                    "label": node.get("label"),
                    "path": meta.get("path"),
                    "language": meta.get("language"),
                    "last_modified": meta.get("last_modified"),
                }
            )
    return pd.DataFrame(rows)


def flatten_lineage_edges(records: list[dict]) -> pd.DataFrame:
    """Explode edges array from lineage snapshots -- one row per edge."""
    rows = []
    for r in records:
        for edge in r.get("edges") or []:
            rows.append(
                {
                    "source": edge.get("source"),
                    "target": edge.get("target"),
                    "relationship": edge.get("relationship"),
                    "confidence": edge.get("confidence"),
                }
            )
    return pd.DataFrame(rows)


# --- Stage 2: Profile --------------------------------------------------------


def _to_python(val: Any) -> Any:
    """Convert numpy scalar types to plain Python so yaml.dump is happy."""
    if hasattr(val, "item"):
        return val.item()
    return val


def profile_column(series: pd.Series) -> dict:
    """Compute column profile: dtype, null_fraction, cardinality, stats."""
    total = len(series)
    null_count = int(series.isna().sum())
    null_fraction = round(null_count / total, 4) if total > 0 else 0.0

    non_null = series.dropna()
    if non_null.empty:
        normalized = non_null
    else:
        normalized = non_null.map(
            lambda v: _json_stringify(v)
            if isinstance(v, (dict, list, tuple, set))
            else _to_python(v)
        )
    cardinality = int(normalized.nunique()) if not normalized.empty else 0
    sample_values = [str(v) for v in normalized.unique()[:10].tolist()]

    profile: dict[str, Any] = {
        "name": series.name,
        "dtype": str(series.dtype),
        "null_fraction": null_fraction,
        "cardinality": cardinality,
        "sample_values": sample_values,
    }

    if pd.api.types.is_numeric_dtype(series) and not non_null.empty:
        profile["min"] = round(float(non_null.min()), 6)
        profile["max"] = round(float(non_null.max()), 6)
        profile["mean"] = round(float(non_null.mean()), 6)
        profile["stddev"] = round(float(non_null.std()), 6)
        profile["p25"] = round(float(non_null.quantile(0.25)), 6)
        profile["p50"] = round(float(non_null.quantile(0.50)), 6)
        profile["p75"] = round(float(non_null.quantile(0.75)), 6)

    return profile


def profile_dataframe(df: pd.DataFrame) -> list[dict]:
    return [profile_column(df[col]) for col in df.columns]


# --- Baseline persistence ----------------------------------------------------


def write_baselines(contract_id: str, tables: dict[str, list[dict]]) -> None:
    """Write mean and stddev per numeric column to schema_snapshots/baselines.json.

    The key format is "<contract_id>/<table_name>" and each column entry stores
    the statistical baseline needed for drift detection in the runner.
    """
    BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load existing baselines so we don't clobber unrelated contracts
    existing: dict = {}
    if BASELINES_PATH.exists():
        with open(BASELINES_PATH, "r", encoding="utf-8") as fh:
            try:
                existing = json.load(fh)
            except json.JSONDecodeError:
                existing = {}

    for table_name, profiles in tables.items():
        key = f"{contract_id}/{table_name}"
        col_stats: dict[str, dict] = {}
        for p in profiles:
            if "mean" in p and "stddev" in p:
                col_stats[p["name"]] = {
                    "mean": p["mean"],
                    "stddev": p["stddev"],
                    "min": p.get("min"),
                    "max": p.get("max"),
                    "null_fraction": p["null_fraction"],
                    "cardinality": p["cardinality"],
                    "count": p.get("cardinality", 0),
                }
        if col_stats:
            existing[key] = col_stats

    with open(BASELINES_PATH, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=2, ensure_ascii=False)


# --- LLM annotation ----------------------------------------------------------


def annotate_ambiguous_columns_with_llm(
    profiles: list[dict],
    table_name: str,
    contract_id: str,
) -> dict[str, str]:
    """Call Claude to annotate ambiguous columns with a semantic description.

    A column is considered ambiguous when:
      - dtype is 'object' with cardinality > 10 (not a clear enum or ID)
      - column name contains no recognisable domain keyword

    Returns a dict of {column_name: llm_description}.
    Falls back to empty dict when the Anthropic package is unavailable or
    the API call fails.
    """
    ambiguous = [
        p for p in profiles
        if p["dtype"] == "object"
        and p["cardinality"] > 10
        and not any(kw in p["name"] for kw in ("_id", "_at", "_json", "path", "hash"))
    ]

    if not ambiguous:
        return {}

    if not _ANTHROPIC_AVAILABLE:
        # Graceful degradation: return placeholder annotations
        return {
            p["name"]: (
                f"[LLM annotation unavailable] High-cardinality string column "
                f"in {table_name} with {p['cardinality']} unique values. "
                f"Sample: {p['sample_values'][:3]}"
            )
            for p in ambiguous
        }

    col_descriptions = "\n".join(
        f"- {p['name']}: cardinality={p['cardinality']}, "
        f"null_fraction={p['null_fraction']}, samples={p['sample_values'][:5]}"
        for p in ambiguous
    )
    prompt = (
        f"You are annotating columns for a data contract called '{contract_id}', "
        f"table '{table_name}'.\n\n"
        f"For each column below, provide a concise one-sentence semantic description "
        f"suitable for a Bitol YAML data contract 'description' field.\n\n"
        f"Columns:\n{col_descriptions}\n\n"
        f"Return a JSON object mapping column_name → description string. "
        f"No markdown, only valid JSON."
    )

    # Retry with exponential backoff: up to 3 attempts, 30 s hard timeout per call.
    # Retryable: rate-limit (429), transient server errors (5xx), timeout, connection.
    _RETRYABLE = (
        _anthropic.RateLimitError,
        _anthropic.APIStatusError,
        _anthropic.APITimeoutError,
        _anthropic.APIConnectionError,
    )
    _LLM_TIMEOUT = 30.0   # seconds per attempt
    _MAX_ATTEMPTS = 3
    client = _anthropic.Anthropic(timeout=_LLM_TIMEOUT)

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            return json.loads(raw)
        except _RETRYABLE as exc:
            if attempt == _MAX_ATTEMPTS:
                logger.warning(
                    "LLM annotation failed after %d attempts: %s — using fallback descriptions",
                    _MAX_ATTEMPTS, exc,
                )
                break
            wait = 2 ** (attempt - 1)  # 1 s, 2 s, then give up
            logger.warning(
                "LLM call failed (attempt %d/%d): %s — retrying in %ds",
                attempt, _MAX_ATTEMPTS, exc, wait,
            )
            time.sleep(wait)
        except Exception as exc:
            # Non-retryable (auth error, bad response JSON, etc.) — fail fast
            logger.warning("LLM annotation failed: %s — using fallback descriptions", exc)
            break

    return {
        p["name"]: (
            f"[LLM annotation failed] High-cardinality string column "
            f"'{p['name']}' with {p['cardinality']} unique values."
        )
        for p in ambiguous
    }


# --- Stage 3: Profiles -> Bitol field clauses ---------------------------------

# Rule set (in application order):
# 1. null_fraction == 0.0           -> required: true
# 2. float dtype + 'confidence' in name -> minimum: 0.0, maximum: 1.0
# 3. cardinality <= 10 + object dtype   -> enum: [sample_values]
# 4. name ends with '_id'           -> format: uuid
# 5. name ends with '_at'           -> format: date-time


def _map_logical_type(dtype: str) -> str:
    if dtype in ("int64", "Int64", "int32", "int16"):
        return "integer"
    if dtype in ("float64", "Float64", "float32"):
        return "number"
    if dtype == "bool":
        return "boolean"
    return "string"


def profile_to_field_clause(profile: dict) -> dict:
    """Apply the rule set and return a Bitol schema field dict."""
    name: str = profile["name"]
    dtype: str = profile["dtype"]
    null_fraction: float = profile["null_fraction"]
    cardinality: int = profile["cardinality"]
    sample_values: list = profile["sample_values"]

    field: dict[str, Any] = {
        "name": name,
        "type": _map_logical_type(dtype),
    }

    # Rule 1 -- completeness
    if null_fraction == 0.0:
        field["required"] = True

    # Rule 2 -- confidence bounds
    if "float" in dtype and "confidence" in name:
        field["minimum"] = 0.0
        field["maximum"] = 1.0

    # Rule 3 -- enum (low-cardinality categoricals)
    if (
        cardinality <= 10
        and dtype == "object"
        and sample_values
        and not name.endswith("_id")
        and not name.endswith("_json")
    ):
        field["enum"] = [str(v) for v in sample_values if v is not None]

    # Rule 4 -- UUID format
    if name.endswith("_id"):
        field["format"] = "uuid"

    # Rule 5 -- date-time format
    if name.endswith("_at"):
        field["format"] = "date-time"

    # Rule 6 -- non-negative floor for numeric fields observed >= 0
    # (guards processing_time_ms, token_counts, fact_count, entity_count, etc.)
    if "min" in profile and profile["min"] >= 0.0 and "minimum" not in field:
        field["minimum"] = 0

    # Rule 7 -- page references are 1-indexed (PDF page 0 does not exist)
    if name.endswith("_ref"):
        field["minimum"] = 1

    # Rule 8 -- suspicious distribution warning
    # A mean > 0.99 suggests near-constant high values (possible data leakage or
    # a degenerate distribution); mean < 0.01 suggests near-zero values that may
    # indicate a miscalibrated model or a padding artefact.
    suspicious_warning: str = ""
    if "mean" in profile:
        m = profile["mean"]
        if m > 0.99:
            suspicious_warning = (
                f" WARNING: suspicious distribution — mean={m:.4f} > 0.99 "
                "(near-constant high value; possible data leakage or calibration issue)"
            )
        elif m < 0.01:
            suspicious_warning = (
                f" WARNING: suspicious distribution — mean={m:.4f} < 0.01 "
                "(near-zero values; possible miscalibrated model or padding artefact)"
            )

    # Annotation: profiling stats as description
    stats = [f"null_fraction={null_fraction}", f"cardinality={cardinality}"]
    if "min" in profile:
        stats.append(f"range=[{profile['min']}, {profile['max']}]")
        stats.append(f"mean={profile['mean']}")
    field["description"] = "Profiled: " + ", ".join(stats) + suspicious_warning

    return field


# --- Stage 3 (cont.): Quality rules ------------------------------------------


def build_quality_rules(table_name: str, profiles: list[dict], total_rows: int = 0) -> list[dict]:
    """Generate Bitol quality section rules for a table."""
    rules: list[dict] = []

    for p in profiles:
        col = p["name"]

        # Not-null for required fields
        if p["null_fraction"] == 0.0:
            rules.append(
                {
                    "name": f"{table_name}.{col}.not_null",
                    "description": f"{col} must never be null",
                    "dimension": "completeness",
                    "severity": "error",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL",
                    "mustBe": 0,
                }
            )

        # Confidence range
        if "confidence" in col and "float" in p["dtype"]:
            rules.append(
                {
                    "name": f"{table_name}.{col}.range_check",
                    "description": f"{col} must be between 0.0 and 1.0",
                    "dimension": "validity",
                    "severity": "error",
                    "query": (
                        f"SELECT COUNT(*) FROM {table_name} "
                        f"WHERE {col} < 0.0 OR {col} > 1.0"
                    ),
                    "mustBe": 0,
                }
            )

        # Enum / allowed values for low-cardinality categoricals
        if (
            p["cardinality"] <= 10
            and p["dtype"] == "object"
            and p["sample_values"]
            and not col.endswith("_id")
            and not col.endswith("_json")
        ):
            quoted = ", ".join(f"'{v}'" for v in p["sample_values"] if v)
            rules.append(
                {
                    "name": f"{table_name}.{col}.allowed_values",
                    "description": f"{col} must be one of the observed categorical values",
                    "dimension": "validity",
                    "severity": "warning",
                    "query": (
                        f"SELECT COUNT(*) FROM {table_name} "
                        f"WHERE {col} NOT IN ({quoted}) AND {col} IS NOT NULL"
                    ),
                    "mustBe": 0,
                }
            )

        # Uniqueness for identifier columns (cardinality == total rows means true PK)
        if col.endswith("_id") and total_rows > 0 and p["cardinality"] >= total_rows:
            rules.append(
                {
                    "name": f"{table_name}.{col}.unique",
                    "description": f"{col} must be unique -- duplicates indicate data corruption or merge errors",
                    "dimension": "uniqueness",
                    "severity": "error",
                    "query": f"SELECT COUNT(*) - COUNT(DISTINCT {col}) FROM {table_name}",
                    "mustBe": 0,
                }
            )

        # Non-empty string check for content fields
        if (
            p["dtype"] == "object"
            and p["null_fraction"] == 0.0
            and any(kw in col for kw in ("text", "excerpt", "name", "path"))
        ):
            rules.append(
                {
                    "name": f"{table_name}.{col}.non_empty",
                    "description": f"{col} must not be an empty string",
                    "dimension": "validity",
                    "severity": "warning",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE TRIM({col}) = ''",
                    "mustBe": 0,
                }
            )

        # Page reference is 1-indexed (no valid PDF page is 0 or negative)
        if col == "page_ref":
            rules.append(
                {
                    "name": f"{table_name}.{col}.positive",
                    "description": "page_ref must be >= 1 (PDF pages are 1-indexed)",
                    "dimension": "validity",
                    "severity": "error",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {col} < 1",
                    "mustBe": 0,
                }
            )

        # Confidence floor warning -- values < 0.5 signal model quality degradation
        if "confidence" in col and "float" in p["dtype"]:
            rules.append(
                {
                    "name": f"{table_name}.{col}.floor_check",
                    "description": f"{col} < 0.5 signals low-confidence extraction; review model output quality",
                    "dimension": "validity",
                    "severity": "warning",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {col} < 0.5",
                    "mustBe": 0,
                }
            )

    # Cross-field: fact_count / entity_count >= 0  (document-level)
    if table_name == "documents":
        for count_col in ("fact_count", "entity_count"):
            if any(p["name"] == count_col for p in profiles):
                rules.append(
                    {
                        "name": f"documents.{count_col}.non_negative",
                        "description": f"{count_col} must be >= 0",
                        "dimension": "validity",
                        "severity": "error",
                        "query": f"SELECT COUNT(*) FROM documents WHERE {count_col} < 0",
                        "mustBe": 0,
                    }
                )

        # Extraction success: a document with zero facts is a pipeline failure
        if any(p["name"] == "fact_count" for p in profiles):
            rules.append(
                {
                    "name": "documents.fact_count.extraction_success",
                    "description": "fact_count < 1 indicates extraction failure -- no facts produced for this document",
                    "dimension": "validity",
                    "severity": "error",
                    "query": "SELECT COUNT(*) FROM documents WHERE fact_count < 1",
                    "mustBe": 0,
                }
            )

        # Timeout risk: >10 minutes signals possible API timeout or hung job
        if any(p["name"] == "processing_time_ms" for p in profiles):
            rules.append(
                {
                    "name": "documents.processing_time_ms.timeout_risk",
                    "description": "processing_time_ms > 600000 (10 min) may indicate API timeout or extraction failure",
                    "dimension": "validity",
                    "severity": "warning",
                    "query": "SELECT COUNT(*) FROM documents WHERE processing_time_ms > 600000",
                    "mustBe": 0,
                }
            )

    if table_name == "extracted_facts":
        if any(p["name"] == "doc_id" for p in profiles):
            rules.append(
                {
                    "name": "extracted_facts.doc_id.references.documents.doc_id",
                    "description": "doc_id in extracted_facts must exist in documents",
                    "dimension": "integrity",
                    "severity": "error",
                    "query": "SELECT COUNT(*) FROM extracted_facts f LEFT JOIN documents d ON f.doc_id = d.doc_id WHERE d.doc_id IS NULL",
                    "mustBe": 0,
                }
            )
    return rules


# --- Stage 4: Assemble contract -----------------------------------------------


def load_lineage(path: str | None) -> list[dict]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        logger.warning("Lineage file not found: %s -- skipping", path)
        return []
    records = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records[:5]


def source_description_for_contract(contract_id: str) -> str:
    """Return a readable source description for the contract being generated."""
    lowered = contract_id.lower()
    if "week3" in lowered:
        return "Week 3 PDF extraction export"
    if "week4" in lowered:
        return "Week 4 lineage snapshot export"
    if "week5" in lowered:
        return "Week 5 event log export"
    if "langsmith" in lowered:
        return "LangSmith trace node export"
    return "Profiled JSONL source export"


def build_contract(
    contract_id: str,
    tables: dict[str, list[dict]],
    lineage_records: list[dict],
    record_count: int,
    source_path: str,
    row_counts: dict[str, int] | None = None,
    registry: dict[str, Any] | None = None,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    source_description = source_description_for_contract(contract_id)
    source_port_type = "trace_export" if "langsmith" in contract_id.lower() else "batch_extraction"

    # Schema section
    schema_tables = []
    all_quality_rules: list[dict] = []

    for table_name, profiles in tables.items():
        fields = [profile_to_field_clause(p) for p in profiles]
        schema_tables.append(
            {
                "name": table_name,
                "description": (
                    f"Profiled from {source_path} "
                    f"({record_count} source documents)"
                ),
                "fields": fields,
            }
        )
        trows = (row_counts or {}).get(table_name, 0)
        all_quality_rules.extend(build_quality_rules(table_name, profiles, total_rows=trows))

    # Lineage section
    input_ports: list[dict] = [
        {
            "type": source_port_type,
            "uri": source_path,
            "description": source_description,
            "format": "jsonl",
            "recordCount": record_count,
            "capturedAt": now,
        }
    ]
    for rec in lineage_records[:3]:
        input_ports.append(
            {
                "type": rec.get("type", "upstream"),
                "uri": rec.get("uri", rec.get("source", "unknown")),
                "description": rec.get("description", ""),
                "capturedAt": rec.get("captured_at", rec.get("timestamp", now)),
            }
        )

    output_ports: list[dict] = [
        {
            "type": "data_contract",
            "uri": f"generated_contracts/{contract_id}.yaml",
            "description": "Generated Bitol YAML data contract",
            "generatedAt": now,
        },
        {
            "type": "dbt_schema",
            "uri": f"generated_contracts/{contract_id}_dbt_schema.yml",
            "description": "dbt schema.yml for downstream model testing",
            "generatedAt": now,
        },
    ]

    # Populate downstream consumers from the lineage graph (Week 4 data)
    downstream_consumers: list[dict] = []
    if lineage_records:
        # lineage_records are raw JSONL records from the lineage snapshots
        # Each may contain nodes + edges; traverse edges to find downstream targets
        seen_targets: set[str] = set()
        for rec in lineage_records:
            for edge in rec.get("edges") or []:
                target = edge.get("target") or edge.get("uri") or ""
                if target and target not in seen_targets:
                    seen_targets.add(target)
                    downstream_consumers.append(
                        {
                            "id": target,
                            "relationship": edge.get("relationship", "downstream"),
                            "confidence": edge.get("confidence"),
                        }
                    )
    # Also populate from the registry subscriptions
    if registry:
        source_label = (
            "LangSmith" if "langsmith" in contract_id.lower()
            else contract_id.replace("-", " ").title()[:7]
        )
        for sub in (registry or {}).get("subscriptions", []):
            if sub.get("source_contract") == contract_id or sub.get("source") in source_label:
                cid = sub.get("target_contract") or sub.get("target", "")
                if cid and cid not in {c["id"] for c in downstream_consumers}:
                    downstream_consumers.append(
                        {
                            "id": cid,
                            "relationship": "registered_subscriber",
                            "validation_mode": sub.get("validation_mode"),
                        }
                    )

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": f"Data Contract: {contract_id}",
            "version": "1.0.0",
            "description": (
                f"Auto-generated Bitol data contract for {contract_id}. "
                f"Profiled from {record_count} source records. "
                f"Generated at {now}."
            ),
            "owner": "data-engineering",
            "contact": {
                "name": "TRP Week 7 Generator",
            },
        },
        "servers": {
            "production": {
                "type": "local",
                "path": source_path,
                "format": "jsonl",
                "description": source_description,
            }
        },
        "schema": {
            "type": "json",
            "tables": schema_tables,
        },
        "quality": {
            "type": "custom",
            "specification": "https://bitol.io/specs/quality/v1",
            "rules": all_quality_rules,
        },
        "lineage": {
            "inputPorts": input_ports,
            "outputPorts": output_ports,
        },
        "registry": {
            "path": (registry or {}).get("path"),
            "subscriptions": (registry or {}).get("subscriptions", []),
        },
        "downstream_consumers": downstream_consumers,
        "generatedAt": now,
        "generatorVersion": "1.0.0",
    }

    return contract


def build_dbt_schema(contract_id: str, tables: dict[str, list[dict]]) -> dict:
    """Build a dbt schema.yml from profiled tables."""
    models = []
    for table_name, profiles in tables.items():
        columns = []
        for p in profiles:
            col: dict[str, Any] = {"name": p["name"]}
            tests = []
            if p["null_fraction"] == 0.0:
                tests.append("not_null")
            if p["name"].endswith("_id"):
                tests.append("unique")
            if tests:
                col["tests"] = tests
            if "min" in p:
                col["description"] = (
                    f"Numeric. range=[{p['min']}, {p['max']}], "
                    f"mean={p['mean']}, stddev={p['stddev']}"
                )
            columns.append(col)

        models.append(
            {
                "name": table_name,
                "description": (
                    f"Model for {table_name} -- "
                    f"derived from contract {contract_id}"
                ),
                "columns": columns,
            }
        )

    return {"version": 2, "models": models}


def _write_yaml(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        yaml.dump(
            data,
            fh,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )


# --- CLI ----------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a Bitol v3.0.0 data contract from a JSONL file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source", required=True, help="Path to source JSONL extraction file"
    )
    parser.add_argument(
        "--contract-id", required=True, dest="contract_id", help="Contract identifier"
    )
    parser.add_argument(
        "--lineage",
        default=None,
        help="Path to lineage snapshots JSONL (optional)",
    )
    parser.add_argument(
        "--registry",
        default=str(DEFAULT_REGISTRY_PATH),
        help="Path to the subscription registry YAML (required)",
    )
    parser.add_argument(
        "--output", required=True, help="Output directory for generated contracts"
    )
    args = parser.parse_args(argv)
    configure_logging()

    # -- Stage 1 --------------------------------------------------------------
    # Stream the source file rather than loading it all into RAM.
    # Each flatten call does its own sequential pass so peak memory is
    # O(one DataFrame) rather than O(entire file).
    logger.info("Stage 1 -- Loading %s ...", args.source)
    record_count = sum(1 for _ in iter_jsonl(args.source))
    logger.info("Loaded %d source documents", record_count)

    if "langsmith" in args.contract_id.lower():
        df_trace = flatten_trace_nodes(iter_jsonl(args.source))
        logger.info("Flattened -> trace_nodes=%dr x %dc", len(df_trace), len(df_trace.columns))

        # -- Stage 2 ----------------------------------------------------------
        logger.info("Stage 2 -- Profiling columns ...")
        profiles_trace = profile_dataframe(df_trace)
        logger.info("%d fields profiled across 1 table", len(profiles_trace))

        # -- Stage 3 ----------------------------------------------------------
        logger.info("Stage 3 -- Translating profiles to Bitol clauses ...")
        tables = {"trace_nodes": profiles_trace}
        clause_count = sum(len(p) for p in tables.values())
        rule_preview = sum(len(build_quality_rules(t, p)) for t, p in tables.items())
        logger.info("%d schema clauses, ~%d quality rules", clause_count, rule_preview)

        # LLM annotation for ambiguous columns
        logger.info("Stage 3a -- LLM annotation for ambiguous columns ...")
        for tname, profiles in tables.items():
            annotations = annotate_ambiguous_columns_with_llm(profiles, tname, args.contract_id)
            for p in profiles:
                if p["name"] in annotations:
                    p["llm_description"] = annotations[p["name"]]
                    logger.debug("Annotated: %s.%s", tname, p["name"])

        row_counts = {"trace_nodes": len(df_trace)}
    else:
        # Three separate streaming passes — each holds at most one DataFrame in RAM.
        df_docs = flatten_documents(iter_jsonl(args.source))
        df_facts = flatten_facts(iter_jsonl(args.source))
        df_entities = flatten_entities(iter_jsonl(args.source))

        logger.info(
            "Flattened -> documents=%dr x %dc  facts=%dr x %dc  entities=%dr x %dc",
            len(df_docs), len(df_docs.columns),
            len(df_facts), len(df_facts.columns),
            len(df_entities), len(df_entities.columns),
        )

        # -- Stage 2 ----------------------------------------------------------
        logger.info("Stage 2 -- Profiling columns ...")
        profiles_docs = profile_dataframe(df_docs)
        profiles_facts = profile_dataframe(df_facts)
        profiles_entities = profile_dataframe(df_entities)

        total_fields = len(profiles_docs) + len(profiles_facts) + len(profiles_entities)
        logger.info("%d fields profiled across 3 tables", total_fields)

        # -- Stage 3 ----------------------------------------------------------
        logger.info("Stage 3 -- Translating profiles to Bitol clauses ...")
        tables = {
            "documents": profiles_docs,
            "extracted_facts": profiles_facts,
            "entities": profiles_entities,
        }

        clause_count = sum(len(p) for p in tables.values())
        rule_preview = sum(
            len(build_quality_rules(t, p)) for t, p in tables.items()
        )
        logger.info("%d schema clauses, ~%d quality rules", clause_count, rule_preview)

        # LLM annotation for ambiguous columns
        logger.info("Stage 3a -- LLM annotation for ambiguous columns ...")
        for tname, profiles in tables.items():
            annotations = annotate_ambiguous_columns_with_llm(profiles, tname, args.contract_id)
            for p in profiles:
                if p["name"] in annotations:
                    p["llm_description"] = annotations[p["name"]]
                    logger.debug("Annotated: %s.%s", tname, p["name"])

        row_counts = {
            "documents": len(df_docs),
            "extracted_facts": len(df_facts),
            "entities": len(df_entities),
        }

    # -- Stage 4 --------------------------------------------------------------
    logger.info("Stage 4 -- Injecting registry + lineage + writing outputs ...")
    registry = load_registry(args.registry)
    logger.info(
        "Loaded %d registry subscriptions from %s",
        len(registry["subscriptions"]), registry["path"],
    )
    lineage_records = load_lineage(args.lineage)
    if lineage_records:
        logger.info("Loaded %d lineage records from %s", len(lineage_records), args.lineage)
    else:
        logger.info("No lineage records (will use source as sole input port)")

    contract = build_contract(
        contract_id=args.contract_id,
        tables=tables,
        lineage_records=lineage_records,
        registry=registry,
        record_count=record_count,
        source_path=args.source,
        row_counts=row_counts,
    )

    output_dir = Path(args.output)

    # Write main contract
    contract_path = output_dir / f"{args.contract_id}.yaml"
    _write_yaml(contract, contract_path)
    logger.info("Contract  -> %s", contract_path)

    # Write dbt schema
    dbt_schema = build_dbt_schema(args.contract_id, tables)
    dbt_path = output_dir / f"{args.contract_id}_dbt_schema.yml"
    _write_yaml(dbt_schema, dbt_path)
    logger.info("dbt schema -> %s", dbt_path)

    # Write timestamped snapshot
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = (
        Path("schema_snapshots") / args.contract_id / f"{timestamp}.yaml"
    )
    _write_yaml(contract, snapshot_path)
    logger.info("Snapshot  -> %s", snapshot_path)

    # Write statistical baselines (mean, stddev per numeric column)
    write_baselines(args.contract_id, tables)
    logger.info("Baselines -> %s", BASELINES_PATH)

    # Final summary
    actual_rules = len(contract["quality"]["rules"])
    actual_clauses = sum(
        len(t["fields"]) for t in contract["schema"]["tables"]
    )
    lineage_ports = len(contract["lineage"]["inputPorts"])

    logger.info(
        "Complete. schema_clauses=%d quality_rules=%d lineage_ports=%d contract_id=%s apiVersion=v3.0.0",
        actual_clauses, actual_rules, lineage_ports, args.contract_id,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
