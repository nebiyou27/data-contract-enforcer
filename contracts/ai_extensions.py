#!/usr/bin/env python3
"""
contracts/ai_extensions.py -- AI-driven contract validation checks

Three required extensions:
  1. Embedding drift detection  -- cosine distance between current centroid and
                                   stored baseline centroid, persisted across calls
  2. Prompt input schema validation -- JSON Schema enforced against document
                                       metadata; non-conforming records routed
                                       to a quarantine path
  3. LLM output schema violation rate -- reads Week 2 verdict records, computes
                                          violation_rate and trend, writes a WARN
                                          entry to violation_log/violations.jsonl
                                          when rate exceeds threshold

Usage:
  python contracts/ai_extensions.py \
    --extractions outputs/week3/extractions.jsonl \
    --verdicts outputs/week2/verdicts.jsonl \
    --output validation_reports/ai_checks.json
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from contracts.log_config import configure_logging
except ModuleNotFoundError:
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
    from contracts.log_config import configure_logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

EMBEDDING_BASELINE_PATH = Path("schema_snapshots") / "embedding_baseline.json"
VIOLATION_LOG_PATH = Path("violation_log") / "violations.jsonl"
QUARANTINE_PATH = Path("quarantine") / "prompt_schema_violations.jsonl"

# JSON Schema for document metadata (prompt inputs)
DOCUMENT_METADATA_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["doc_id", "source_path", "extracted_at"],
    "properties": {
        "doc_id": {"type": "string"},
        "source_path": {"type": "string"},
        "extracted_at": {"type": "string"},
        "extraction_model": {"type": "string"},
        "processing_time_ms": {"type": "number"},
    },
}

# Threshold: violation rate above this triggers a WARN entry in the log
VIOLATION_RATE_THRESHOLD = 0.05  # 5 %


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def load_jsonl(path: str | Path) -> list[dict]:
    """Read a JSONL file and return a list of dicts.

    NOTE: loads the entire file into RAM. For files larger than ~500 MB
    use ``iter_jsonl`` which yields one record at a time.
    """
    records: list[dict] = []
    p = Path(path)
    if not p.exists():
        return records
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def iter_jsonl(path: str | Path):
    """Yield one parsed JSON record at a time from a JSONL file.

    Memory footprint is O(1 record) regardless of file size — safe for
    multi-GB extraction files. Each call opens the file from the start,
    so callers needing multiple passes should call ``iter_jsonl`` once
    per pass rather than materialising the full list.
    """
    p = Path(path)
    if not p.exists():
        return
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass


def _append_violation_log(entry: dict) -> None:
    VIOLATION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(VIOLATION_LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Extension 1: Embedding drift (cosine distance on bag-of-words centroid)
# ---------------------------------------------------------------------------


def _text_to_bow(text: str) -> dict[str, float]:
    """Convert text to a bag-of-words vector (term frequencies)."""
    tokens = text.lower().split()
    counts: Counter = Counter(tokens)
    total = sum(counts.values()) or 1
    return {tok: cnt / total for tok, cnt in counts.items()}


def _centroid(vectors: list[dict[str, float]]) -> dict[str, float]:
    """Average a list of BOW vectors into a single centroid."""
    if not vectors:
        return {}
    vocab: Counter = Counter()
    for v in vectors:
        for tok, val in v.items():
            vocab[tok] += val
    n = len(vectors)
    return {tok: total / n for tok, total in vocab.items()}


def _cosine_distance(a: dict[str, float], b: dict[str, float]) -> float:
    """Cosine distance in [0, 1] between two sparse BOW vectors.

    distance = 1 - cosine_similarity
    """
    keys = set(a) | set(b)
    dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 1.0  # fully dissimilar when one vector is empty
    similarity = dot / (norm_a * norm_b)
    return round(1.0 - similarity, 6)


def _load_embedding_baseline() -> dict[str, Any] | None:
    if EMBEDDING_BASELINE_PATH.exists():
        with open(EMBEDDING_BASELINE_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return None


def _save_embedding_baseline(centroid: dict[str, float], record_count: int) -> None:
    EMBEDDING_BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "centroid": centroid,
        "record_count": record_count,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(EMBEDDING_BASELINE_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


def check_embedding_drift(
    records: list[dict],
    text_field: str = "text",
) -> dict[str, Any]:
    """Detect semantic drift using cosine distance between BOW centroids.

    Reads the baseline centroid from EMBEDDING_BASELINE_PATH if it exists;
    writes a new/updated baseline after every run so the comparison is
    persistent across calls.

    Thresholds:
        distance < 0.1  → PASS
        0.1 ≤ distance < 0.3 → WARN
        distance ≥ 0.3 → FAIL
    """
    # Collect text values
    texts: list[str] = []
    for r in records:
        val = r.get(text_field, "")
        if isinstance(val, str) and val.strip():
            texts.append(val)

    if not texts:
        return {
            "check_type": "embedding_drift",
            "field_name": text_field,
            "status": "ERROR",
            "message": f"No text values found for field '{text_field}'",
        }

    current_vectors = [_text_to_bow(t) for t in texts]
    current_centroid = _centroid(current_vectors)

    baseline_data = _load_embedding_baseline()

    if baseline_data is None:
        # First run — save baseline and report no drift
        _save_embedding_baseline(current_centroid, len(texts))
        return {
            "check_type": "embedding_drift",
            "field_name": text_field,
            "status": "PASS",
            "severity": "MEDIUM",
            "cosine_distance": 0.0,
            "baseline_record_count": 0,
            "current_record_count": len(texts),
            "message": "No baseline found — current centroid saved as baseline.",
        }

    baseline_centroid: dict[str, float] = baseline_data.get("centroid", {})
    distance = _cosine_distance(baseline_centroid, current_centroid)

    if distance >= 0.3:
        status = "FAIL"
    elif distance >= 0.1:
        status = "WARN"
    else:
        status = "PASS"

    # Update baseline with the current centroid
    _save_embedding_baseline(current_centroid, len(texts))

    return {
        "check_type": "embedding_drift",
        "field_name": text_field,
        "status": status,
        "severity": "MEDIUM",
        "cosine_distance": distance,
        "baseline_record_count": baseline_data.get("record_count", 0),
        "current_record_count": len(texts),
        "baseline_saved_at": baseline_data.get("saved_at"),
        "message": (
            f"Embedding cosine distance from baseline centroid: {distance:.4f}"
            + (" (drift detected)" if status != "PASS" else " (within threshold)")
        ),
    }


# ---------------------------------------------------------------------------
# Sensitive-field scrubber
# ---------------------------------------------------------------------------

_SENSITIVE_FIELD_PATTERNS = re.compile(
    r"(api[_-]?key|apikey|authorization|auth[_-]?token|token|secret|password|passwd|credential)",
    re.IGNORECASE,
)


def _scrub_record(record: dict) -> dict:
    """Return a shallow copy of *record* with sensitive field values redacted.

    Any top-level key whose name matches a known-sensitive pattern is replaced
    with the string ``"<redacted>"`` so API keys and auth headers are never
    written verbatim to the quarantine file.
    """
    scrubbed: dict = {}
    for key, value in record.items():
        if _SENSITIVE_FIELD_PATTERNS.search(str(key)):
            scrubbed[key] = "<redacted>"
        else:
            scrubbed[key] = value
    return scrubbed


# ---------------------------------------------------------------------------
# Extension 2: Prompt input schema validation (JSON Schema enforcement)
# ---------------------------------------------------------------------------


def _validate_against_schema(record: dict, schema: dict) -> list[str]:
    """Minimal JSON Schema validator for type:object with required and properties.

    Returns a list of violation messages (empty → valid).
    """
    errors: list[str] = []

    if schema.get("type") != "object":
        return errors  # only handles object schemas

    for required_field in schema.get("required", []):
        if required_field not in record:
            errors.append(f"Missing required field: '{required_field}'")

    for prop_name, prop_schema in schema.get("properties", {}).items():
        if prop_name not in record:
            continue
        value = record[prop_name]
        expected_type = prop_schema.get("type")
        if expected_type == "string" and not isinstance(value, str):
            errors.append(f"Field '{prop_name}' must be string, got {type(value).__name__}")
        elif expected_type == "number" and not isinstance(value, (int, float)):
            errors.append(f"Field '{prop_name}' must be number, got {type(value).__name__}")
        elif expected_type == "integer" and not isinstance(value, int):
            errors.append(f"Field '{prop_name}' must be integer, got {type(value).__name__}")

    return errors


def check_prompt_input_schema(
    records: Iterable[dict],
    schema: dict | None = None,
) -> dict[str, Any]:
    """Enforce JSON Schema against document metadata (prompt inputs).

    Accepts any iterable of dicts — including a streaming ``iter_jsonl``
    generator — so the full file is never loaded into RAM.
    Non-conforming records are written to QUARANTINE_PATH immediately.
    """
    if schema is None:
        schema = DOCUMENT_METADATA_SCHEMA

    total = 0
    violations = 0
    quarantine_fh = None

    try:
        for record in records:
            total += 1
            errors = _validate_against_schema(record, schema)
            if errors:
                violations += 1
                qr = {
                    "record": _scrub_record(record),
                    "schema_errors": errors,
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                }
                if quarantine_fh is None:
                    QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)
                    quarantine_fh = open(QUARANTINE_PATH, "a", encoding="utf-8")
                quarantine_fh.write(json.dumps(qr, ensure_ascii=False) + "\n")
    finally:
        if quarantine_fh is not None:
            quarantine_fh.close()

    violation_rate = violations / total if total > 0 else 0.0

    if violation_rate == 0.0:
        status = "PASS"
    elif violation_rate < 0.1:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "check_type": "prompt_input_schema",
        "status": status,
        "severity": "HIGH",
        "records_scanned": total,
        "violations_found": violations,
        "violation_rate_pct": round(violation_rate * 100, 2),
        "quarantine_path": str(QUARANTINE_PATH) if violations > 0 else None,
        "message": (
            f"Prompt input schema: {violations}/{total} records non-conforming "
            f"({violation_rate * 100:.1f}%)"
            + (f"; {violations} routed to quarantine" if violations > 0 else "")
        ),
    }


# ---------------------------------------------------------------------------
# Extension 3: LLM output schema violation rate (reads Week 2 verdicts)
# ---------------------------------------------------------------------------


def check_llm_output_violation_rate(
    verdicts_path: str | Path,
) -> dict[str, Any]:
    """Compute violation_rate and trend from Week 2 verdict records.

    Writes a WARN entry to violation_log/violations.jsonl when the rate
    exceeds VIOLATION_RATE_THRESHOLD.

    A verdict record is considered a schema violation when:
      - the 'verdict' field is missing
      - the 'verdict' is not a dict (i.e. not structured output)
      - required sub-fields (decision, confidence, reasoning) are absent
      - confidence is outside [0, 1]
    """
    # Stream verdicts one record at a time — O(N booleans) peak RAM instead of
    # O(N full records). violation_flags is a list[bool] (~1 byte/record).
    violation_flags: list[bool] = []
    violation_types: Counter = Counter()

    for record in iter_jsonl(verdicts_path):
        is_violation = False

        verdict = record.get("verdict")
        if verdict is None:
            is_violation = True
            violation_types["missing_verdict"] += 1
        elif isinstance(verdict, dict):
            for required in ("decision", "confidence", "reasoning"):
                if required not in verdict:
                    is_violation = True
                    violation_types[f"missing_{required}"] += 1
                    break
            conf = verdict.get("confidence")
            if isinstance(conf, (int, float)):
                if not (0.0 <= conf <= 1.0):
                    is_violation = True
                    violation_types["confidence_out_of_range"] += 1
            elif conf is not None:
                is_violation = True
                violation_types["confidence_type_mismatch"] += 1
        elif isinstance(verdict, str):
            try:
                parsed = json.loads(verdict)
                if not isinstance(parsed, dict):
                    is_violation = True
                    violation_types["invalid_json_structure"] += 1
            except json.JSONDecodeError:
                is_violation = True
                violation_types["unparseable_json"] += 1
        else:
            is_violation = True
            violation_types["unexpected_type"] += 1

        violation_flags.append(is_violation)

    if not violation_flags:
        return {
            "check_type": "llm_output_violation_rate",
            "status": "ERROR",
            "message": f"No verdict records found at {verdicts_path}",
        }

    total = len(violation_flags)
    mid = total // 2
    violation_count = sum(violation_flags)
    first_half_violations = sum(violation_flags[:mid])
    second_half_violations = sum(violation_flags[mid:])

    violation_rate = violation_count / total
    first_rate = first_half_violations / mid if mid > 0 else 0.0
    second_rate = second_half_violations / (total - mid) if (total - mid) > 0 else 0.0

    if second_rate > first_rate + 0.05:
        trend = "increasing"
    elif first_rate > second_rate + 0.05:
        trend = "decreasing"
    else:
        trend = "stable"

    if violation_rate == 0.0:
        status = "PASS"
    elif violation_rate < VIOLATION_RATE_THRESHOLD:
        status = "WARN"
    else:
        status = "FAIL"

    # Write WARN entry to violation log when threshold is exceeded
    if violation_rate > VIOLATION_RATE_THRESHOLD:
        log_entry = {
            "violation_id": str(uuid.uuid4()),
            "check_type": "llm_output_violation_rate",
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "status": "WARN",
            "severity": "HIGH",
            "source": str(verdicts_path),
            "violation_rate": round(violation_rate, 4),
            "violation_count": violation_count,
            "total_records": total,
            "trend": trend,
            "violation_breakdown": dict(violation_types),
            "message": (
                f"LLM output violation rate {violation_rate * 100:.1f}% "
                f"exceeds threshold {VIOLATION_RATE_THRESHOLD * 100:.0f}% "
                f"(trend: {trend})"
            ),
        }
        _append_violation_log(log_entry)

    return {
        "check_type": "llm_output_violation_rate",
        "status": status,
        "severity": "HIGH",
        "source": str(verdicts_path),
        "records_scanned": total,
        "violation_count": violation_count,
        "violation_rate_pct": round(violation_rate * 100, 2),
        "trend": trend,
        "violation_breakdown": dict(violation_types),
        "threshold_pct": VIOLATION_RATE_THRESHOLD * 100,
        "wrote_to_violation_log": violation_rate > VIOLATION_RATE_THRESHOLD,
        "message": (
            f"LLM output schema violation rate: {violation_rate * 100:.1f}% "
            f"({violation_count}/{total} records, trend: {trend})"
        ),
    }


# ---------------------------------------------------------------------------
# Single entry point invoking all three extensions
# ---------------------------------------------------------------------------


def run_all_extensions(
    extractions_path: str | Path,
    verdicts_path: str | Path,
) -> dict[str, Any]:
    """Invoke all three AI extensions and return a combined report.

    Uses two streaming passes over the extractions file so the full file
    is never loaded into RAM — safe for multi-GB inputs.

    Pass 1: collect extracted_facts (small nested objects) for embedding drift.
    Pass 2: stream full records into check_prompt_input_schema (zero RAM accumulation).
    """
    # Pass 1 — flatten extracted_facts for embedding drift (small objects only)
    fact_texts: list[dict] = []
    record_count = 0
    for doc in iter_jsonl(extractions_path):
        record_count += 1
        for fact in doc.get("extracted_facts") or []:
            fact_texts.append(fact)

    checks: list[dict] = []

    # 1. Embedding drift on extracted fact text
    checks.append(check_embedding_drift(fact_texts, text_field="text"))

    # 2. Prompt input schema validation — Pass 2, streaming (no list in RAM)
    checks.append(check_prompt_input_schema(iter_jsonl(extractions_path)))

    # 3. LLM output violation rate from Week 2 verdicts
    checks.append(check_llm_output_violation_rate(verdicts_path))

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "extractions_file": str(extractions_path),
        "verdicts_file": str(verdicts_path),
        "records_analyzed": record_count,
        "checks_run": len(checks),
        "checks": checks,
        "summary": {
            "total_checks": len(checks),
            "passed": sum(1 for c in checks if c.get("status") == "PASS"),
            "warned": sum(1 for c in checks if c.get("status") == "WARN"),
            "failed": sum(1 for c in checks if c.get("status") == "FAIL"),
            "errored": sum(1 for c in checks if c.get("status") == "ERROR"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run AI-driven contract validation checks (all three extensions)"
    )
    parser.add_argument(
        "--extractions",
        default="outputs/week3/extractions.jsonl",
        help="Path to extractions JSONL (Week 3 output) for embedding drift",
    )
    parser.add_argument(
        "--verdicts",
        default="outputs/week2/verdicts.jsonl",
        help="Path to Week 2 verdict records for LLM output violation rate",
    )
    parser.add_argument(
        "--output",
        help="Path to write AI checks report JSON",
    )

    args = parser.parse_args()
    configure_logging()

    try:
        report = run_all_extensions(args.extractions, args.verdicts)

        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2, ensure_ascii=False)
            logger.info("Report written to %s", output_path)

        print(json.dumps(report, indent=2))

        return 1 if report["summary"]["failed"] > 0 else 0

    except Exception as exc:
        logger.error("Fatal error: %s", exc, exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
