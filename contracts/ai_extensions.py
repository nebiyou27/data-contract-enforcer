#!/usr/bin/env python3
"""
contracts/ai_extensions.py -- AI-driven contract validation checks

Implements three AI-based checks:
  1. Embedding Drift: Detects semantic drift in text fields using embeddings
  2. Prompt/Input Validation: Checks LLM inputs for prompt injection patterns
  3. LLM Output Schema Violation: Validates LLM output conforms to declared schema

Usage:
  python contracts/ai_extensions.py \
    --data outputs/week2/verdicts.jsonl \
    --contract generated_contracts/week2-digital-courtroom.yaml \
    --output validation_reports/week2_ai_checks.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def load_jsonl(path: str) -> list[dict]:
    """Read a JSONL file and return a list of dicts."""
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def compute_embedding_fingerprint(text: str) -> tuple[int, set[str]]:
    """
    Compute a simple embedding fingerprint for a text field.
    
    Returns: (word_count, set of unique N-grams)
    Uses character-level 3-grams as a lightweight embedding proxy.
    """
    if not isinstance(text, str):
        text = str(text)
    
    text = text.lower().strip()
    word_count = len(text.split())
    
    # Extract 3-grams
    trigrams = set()
    if len(text) >= 3:
        for i in range(len(text) - 2):
            trigrams.add(text[i:i+3])
    
    return word_count, trigrams


def check_embedding_drift(
    data: list[dict],
    field_name: str,
    baseline_sample: list[str] | None = None
) -> dict[str, Any]:
    """
    Check for semantic drift in a text field using embedding fingerprints.
    
    Compares current data against a baseline sample (or uses first 10% as baseline).
    Detects significant changes in vocabulary or text structure.
    """
    values = [record.get(field_name, "") for record in data]
    values = [v for v in values if isinstance(v, str) and v.strip()]
    
    if not values:
        return {
            "check_type": "embedding_drift",
            "field_name": field_name,
            "status": "ERROR",
            "message": f"No text values found for field '{field_name}'"
        }
    
    if baseline_sample is None:
        # Use first 10% as baseline
        baseline_size = max(1, len(values) // 10)
        baseline_sample = values[:baseline_size]
    
    current_sample = values[baseline_size:]
    
    # Compute baseline statistics
    baseline_lengths = [len(v) for v in baseline_sample]
    baseline_words = [len(v.split()) for v in baseline_sample]
    baseline_trigrams = set()
    
    for text in baseline_sample:
        _, trigrams = compute_embedding_fingerprint(text)
        baseline_trigrams.update(trigrams)
    
    # Compute current statistics
    current_lengths = [len(v) for v in current_sample] if current_sample else baseline_lengths
    current_words = [len(v.split()) for v in current_sample] if current_sample else baseline_words
    current_trigrams = set()
    
    for text in current_sample:
        _, trigrams = compute_embedding_fingerprint(text)
        current_trigrams.update(trigrams)
    
    # Detect drift
    baseline_avg_len = sum(baseline_lengths) / len(baseline_lengths) if baseline_lengths else 0
    current_avg_len = sum(current_lengths) / len(current_lengths) if current_lengths else 0
    
    baseline_avg_words = sum(baseline_words) / len(baseline_words) if baseline_words else 0
    current_avg_words = sum(current_words) / len(current_words) if current_words else 0
    
    # Calculate percentage changes
    len_change = ((current_avg_len - baseline_avg_len) / baseline_avg_len * 100) if baseline_avg_len > 0 else 0
    word_change = ((current_avg_words - baseline_avg_words) / baseline_avg_words * 100) if baseline_avg_words > 0 else 0
    
    # Check vocabulary stability
    new_trigrams = current_trigrams - baseline_trigrams
    lost_trigrams = baseline_trigrams - current_trigrams
    vocab_stability = 1.0 - (len(new_trigrams | lost_trigrams) / max(len(baseline_trigrams), len(current_trigrams), 1))
    
    # Determine status based on thresholds
    status = "PASS"
    warnings = []
    
    if abs(len_change) > 20:
        status = "WARN" if abs(len_change) < 50 else "FAIL"
        warnings.append(f"Text length changed by {len_change:.1f}%")
    
    if abs(word_change) > 20:
        status = "WARN" if abs(word_change) < 50 else "FAIL"
        warnings.append(f"Word count changed by {word_change:.1f}%")
    
    if vocab_stability < 0.7:
        status = "WARN" if vocab_stability >= 0.5 else "FAIL"
        warnings.append(f"Vocabulary stability: {vocab_stability:.2f}")
    
    return {
        "check_type": "embedding_drift",
        "field_name": field_name,
        "status": status,
        "severity": "MEDIUM",
        "baseline_avg_length": round(baseline_avg_len, 2),
        "current_avg_length": round(current_avg_len, 2),
        "length_change_pct": round(len_change, 2),
        "baseline_avg_words": round(baseline_avg_words, 2),
        "current_avg_words": round(current_avg_words, 2),
        "word_change_pct": round(word_change, 2),
        "vocabulary_stability": round(vocab_stability, 2),
        "new_trigram_count": len(new_trigrams),
        "lost_trigram_count": len(lost_trigrams),
        "warnings": warnings,
        "message": " | ".join(warnings) if warnings else "No significant embedding drift detected"
    }


def check_prompt_injection(data: list[dict], field_name: str, content_field: str = "content") -> dict[str, Any]:
    """
    Check for prompt injection patterns in input fields.
    
    Detects common prompt injection tactics:
      - System prompt override attempts
      - Role jailbreak patterns
      - Token smuggling patterns
    """
    values = [record.get(field_name, "") for record in data]
    values = [v for v in values if isinstance(v, str) and v.strip()]
    
    if not values:
        return {
            "check_type": "prompt_injection",
            "field_name": field_name,
            "status": "PASS",
            "message": f"No text values found for field '{field_name}'"
        }
    
    # Common injection patterns (case-insensitive)
    injection_patterns = [
        r"(?i)system\s*prompt",
        r"(?i)ignore\s+previous",
        r"(?i)forget\s+all",
        r"(?i)pretend\s+you\s+are",
        r"(?i)act\s+as\s+an?",
        r"(?i)you\s+are\s+now",
        r"(?i)from\s+now\s+on",
        r"(?i)<\s*system\s*>",
        r"(?i)<!--.*?-->",  # HTML comments
        r"(?i)```.*?```",  # Code blocks
    ]
    
    suspicious_records = []
    total_injections = 0
    
    for i, text in enumerate(values):
        for pattern in injection_patterns:
            if re.search(pattern, text):
                total_injections += 1
                suspicious_records.append({
                    "record_index": i,
                    "pattern": pattern,
                    "sample": text[:100]
                })
                break
    
    status = "PASS"
    if total_injections > 0:
        injection_rate = total_injections / len(values) * 100
        if injection_rate > 10:
            status = "FAIL"
        elif injection_rate > 2:
            status = "WARN"
        else:
            status = "WARN"
    
    return {
        "check_type": "prompt_injection",
        "field_name": field_name,
        "status": status,
        "severity": "HIGH",
        "records_scanned": len(values),
        "suspicions_found": total_injections,
        "injection_rate_pct": round(total_injections / len(values) * 100, 2) if values else 0,
        "sample_suspicions": suspicious_records[:5],
        "message": (
            f"Found {total_injections} potential prompt injection patterns "
            f"in {total_injections / len(values) * 100:.1f}% of records"
            if total_injections > 0 else
            "No prompt injection patterns detected"
        )
    }


def check_llm_output_schema(data: list[dict], output_field: str = "verdict") -> dict[str, Any]:
    """
    Check if LLM outputs conform to expected schema.
    
    Validates:
      - Field presence (required fields exist)
      - Type conformance (values match expected types)
      - Schema structure compliance
    """
    if not data:
        return {
            "check_type": "llm_output_schema",
            "status": "ERROR",
            "message": "No data provided"
        }
    
    schema_violations = {
        "missing_field": 0,
        "type_mismatch": 0,
        "invalid_json": 0,
        "incomplete_response": 0
    }
    
    total_records = len(data)
    
    for record in data:
        # Check if output field exists
        if output_field not in record:
            schema_violations["missing_field"] += 1
            continue
        
        output = record[output_field]
        
        # If output is a dict, it passed basic parsing
        if isinstance(output, dict):
            # Check for required verdict fields
            required_fields = ["decision", "confidence", "reasoning"]
            for field in required_fields:
                if field not in output:
                    schema_violations["incomplete_response"] += 1
                    break
            
            # Type check confidence (should be numeric 0-1)
            if isinstance(output.get("confidence"), (int, float)):
                confidence = output["confidence"]
                if not (0 <= confidence <= 1):
                    schema_violations["type_mismatch"] += 1
            elif output.get("confidence") is not None:
                schema_violations["type_mismatch"] += 1
        
        # If output is a string, try to parse as JSON
        elif isinstance(output, str):
            try:
                parsed = json.loads(output)
                if not isinstance(parsed, dict):
                    schema_violations["invalid_json"] += 1
            except json.JSONDecodeError:
                schema_violations["invalid_json"] += 1
        
        # Any other type is a violation
        else:
            schema_violations["type_mismatch"] += 1
    
    violation_rate = sum(schema_violations.values()) / total_records * 100 if total_records > 0 else 0
    
    status = "PASS"
    if violation_rate == 0:
        status = "PASS"
    elif violation_rate < 5:
        status = "WARN"
    else:
        status = "FAIL"
    
    return {
        "check_type": "llm_output_schema",
        "output_field": output_field,
        "status": status,
        "severity": "HIGH",
        "records_scanned": total_records,
        "violations_found": sum(schema_violations.values()),
        "violation_rate_pct": round(violation_rate, 2),
        "violation_breakdown": schema_violations,
        "message": (
            f"LLM output schema check: {sum(schema_violations.values())} "
            f"violations in {total_records} records "
            f"({violation_rate:.1f}% violation rate)"
        )
    }


def main():
    parser = argparse.ArgumentParser(
        description="Run AI-driven contract validation checks"
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Path to JSONL data file"
    )
    parser.add_argument(
        "--contract",
        help="Path to contract YAML (for schema validation)"
    )
    parser.add_argument(
        "--output",
        help="Path to write AI checks report JSON"
    )
    
    args = parser.parse_args()
    
    try:
        data = load_jsonl(args.data)
        
        if not data:
            print("ERROR: No data loaded", file=sys.stderr)
            sys.exit(1)
        
        # Run all three checks
        checks = []
        
        # Check for text fields that might have embedding drift
        if data and isinstance(data[0], dict):
            for field_name in data[0].keys():
                sample_value = data[0].get(field_name)
                if isinstance(sample_value, str):
                    # Run embedding drift check on text fields
                    result = check_embedding_drift(data, field_name)
                    checks.append(result)
                    
                    # Run prompt injection check on input-like fields
                    if "input" in field_name.lower() or "prompt" in field_name.lower():
                        result = check_prompt_injection(data, field_name)
                        checks.append(result)
        
        # Check LLM output schema
        result = check_llm_output_schema(data, output_field="verdict")
        checks.append(result)
        
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data_file": args.data,
            "records_analyzed": len(data),
            "checks_run": len(checks),
            "checks": checks,
            "summary": {
                "total_checks": len(checks),
                "passed": sum(1 for c in checks if c.get("status") == "PASS"),
                "warned": sum(1 for c in checks if c.get("status") == "WARN"),
                "failed": sum(1 for c in checks if c.get("status") == "FAIL"),
                "errored": sum(1 for c in checks if c.get("status") == "ERROR")
            }
        }
        
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2)
            print(f"Report written to {output_path}")
        
        print(json.dumps(report, indent=2))
        
        # Exit code based on failures
        if report["summary"]["failed"] > 0:
            sys.exit(1)
        else:
            sys.exit(0)
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
