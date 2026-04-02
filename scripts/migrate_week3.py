"""Migrate Week 3 extracted document JSON into the Week 7 canonical JSONL.

The Week 3 repo stores one serialized ``ExtractedDocument`` per file under
``.refinery/extracted``. Week 7 expects a flattened JSONL stream in
``outputs/week3/extractions.jsonl`` with a document-level record per line.

This script performs a deterministic conversion that:
- preserves document provenance
- derives a stable UUID for each document, fact, and entity
- flattens page/block content into extracted facts
- emits a lightweight entity list using regex heuristics

The output is intentionally conservative: it prefers faithful transformation over
inventing data that does not exist in the source model.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ENTITY_TYPES = ("PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER")
ORGANIZATION_HINTS = {
    "university",
    "school",
    "college",
    "institute",
    "ministry",
    "department",
    "agency",
    "company",
    "corporation",
    "corp",
    "ltd",
    "inc",
    "bank",
    "committee",
    "authority",
    "office",
    "commission",
    "association",
    "council",
    "bureau",
    "board",
}
LOCATION_HINTS = {
    "ethiopia",
    "addis ababa",
    "nairobi",
    "kenya",
    "uganda",
    "tanzania",
    "africa",
}
STOPWORDS = {
    "the",
    "and",
    "or",
    "for",
    "with",
    "from",
    "this",
    "that",
    "these",
    "those",
    "into",
    "onto",
    "over",
    "under",
    "about",
    "page",
    "document",
    "table",
    "figure",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Week 3 extractions to Week 7 JSONL.")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing Week 3 extracted document JSON files.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Destination JSONL file for canonical Week 7 records.",
    )
    return parser.parse_args()


def stable_uuid(*parts: str) -> str:
    seed = "::".join(parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def truncate(text: str, limit: int = 240) -> str:
    text = normalize_whitespace(text)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def approx_tokens(text: str) -> int:
    cleaned = normalize_whitespace(text)
    if not cleaned:
        return 0
    return max(1, int(round(len(cleaned.split()) * 1.3)))


def to_iso8601(timestamp: float | None) -> str:
    if timestamp is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def validate_confidence_score(value: Any, context: str) -> float:
    """Validate that a confidence score stays inside the expected unit interval."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{context} must be a number between 0.0 and 1.0")

    score = float(value)
    if score < 0.0 or score > 1.0:
        raise ValueError(f"{context} must be between 0.0 and 1.0; got {score}")

    return score


def flatten_table_rows(rows: Any) -> str:
    if not rows:
        return ""
    flattened: list[str] = []
    for row in rows:
        if isinstance(row, list):
            flattened.append(" | ".join(normalize_whitespace(str(cell)) for cell in row if str(cell).strip()))
        elif isinstance(row, dict):
            flattened.append(" | ".join(f"{k}: {normalize_whitespace(str(v))}" for k, v in row.items()))
        else:
            flattened.append(normalize_whitespace(str(row)))
    return " ; ".join(part for part in flattened if part)


def iter_text_sources(document: dict[str, Any]) -> Iterable[tuple[int, str, str]]:
    """Yield (page_number, kind, text) tuples for fact creation."""

    pages = document.get("pages") or []
    for page in pages:
        page_number = int(page.get("page_number") or 0) or 0
        page_text = normalize_whitespace(page.get("text"))
        if page_text:
            yield page_number, "page_text", page_text

        for block in page.get("text_blocks") or []:
            block_text = normalize_whitespace(block.get("text"))
            if block_text:
                yield page_number, "text_block", block_text

        for block in page.get("table_blocks") or []:
            table_text = flatten_table_rows(block.get("rows"))
            if table_text:
                yield page_number, "table_block", table_text

        for block in page.get("figure_blocks") or []:
            caption = normalize_whitespace(block.get("caption"))
            if caption:
                yield page_number, "figure_caption", caption

        for table in page.get("tables") or []:
            table_text = flatten_table_rows(table)
            if table_text:
                yield page_number, "table", table_text


DATE_PATTERNS = [
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"),
    re.compile(r"\b\d{1,2}-\d{1,2}-\d{2,4}\b"),
    re.compile(
        r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+\d{1,2},\s+\d{4}\b",
        re.IGNORECASE,
    ),
]

AMOUNT_PATTERN = re.compile(
    "(?:[$\u20ac\u00a3]\\s?\\d[\\d,]*(?:\\.\\d+)?|\\b\\d[\\d,]*(?:\\.\\d+)?\\s?(?:USD|ETB|birr|dollars?)\\b)",
    re.IGNORECASE,
)

PERSON_PATTERN = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b")
ACRONYM_PATTERN = re.compile(r"\b[A-Z]{2,}(?:\s+[A-Z]{2,})*\b")
TITLE_CASE_PATTERN = re.compile(r"\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,}){1,3}\b")
NAME_PATTERN = re.compile(r"\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b")
HEADING_WORDS = {
    "table",
    "contents",
    "chapter",
    "section",
    "figure",
    "appendix",
    "references",
    "introduction",
    "discussion",
    "results",
    "conclusion",
    "conclusions",
    "recommendations",
    "abstract",
    "methodology",
    "background",
    "research",
    "essay",
    "proposal",
    "guideline",
}
LOCATION_PATTERN = re.compile(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b")


def classify_entity(value: str) -> str:
    normalized = normalize_whitespace(value)
    lower = normalized.lower()
    if not normalized:
        return "OTHER"
    if any(pattern.search(normalized) for pattern in DATE_PATTERNS):
        return "DATE"
    if AMOUNT_PATTERN.search(normalized):
        return "AMOUNT"
    if any(hint in lower for hint in LOCATION_HINTS):
        return "LOCATION"
    if any(hint in lower for hint in ORGANIZATION_HINTS):
        return "ORG"
    if ACRONYM_PATTERN.fullmatch(normalized):
        return "ORG"
    if PERSON_PATTERN.fullmatch(normalized):
        return "PERSON"
    if len(normalized.split()) >= 2 and normalized[0].isupper():
        return "ORG"
    return "OTHER"


def extract_entity_candidates(text: str) -> list[tuple[str, str]]:
    """Return ordered (type, canonical_value) pairs from source text."""

    candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    normalized = normalize_whitespace(text)
    if not normalized:
        return candidates

    def add_candidate(entity_type: str, value: str) -> None:
        canonical = normalize_whitespace(value)
        if not canonical:
            return
        key = (entity_type, canonical.lower())
        if key in seen:
            return
        seen.add(key)
        candidates.append((entity_type, canonical))

    for pattern in DATE_PATTERNS:
        for match in pattern.finditer(normalized):
            add_candidate("DATE", match.group(0))

    for match in AMOUNT_PATTERN.finditer(normalized):
        add_candidate("AMOUNT", match.group(0))

    # Keep entity extraction conservative: dates, amounts, acronyms, and
    # organization-like phrases with explicit hints only.
    for match in ACRONYM_PATTERN.finditer(normalized):
        value = match.group(0)
        if len(value) >= 2:
            add_candidate("ORG", value)

    for match in re.finditer(r"(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4})", normalized):
        phrase = match.group(0)
        lower = phrase.lower()
        if any(hint in lower for hint in ORGANIZATION_HINTS):
            words = {part.lower().strip(".,;:()[]{}") for part in phrase.split()}
            if not (words & STOPWORDS or words & HEADING_WORDS):
                add_candidate("ORG", phrase)

    return candidates


def build_entities(document: dict[str, Any]) -> list[dict[str, Any]]:
    combined_text_parts: list[str] = []
    for _, _, text in iter_text_sources(document):
        combined_text_parts.append(text)
    combined_text = " ".join(combined_text_parts)

    doc_uuid = stable_uuid("week3", document.get("doc_id", ""), document.get("file_path", ""))
    entity_map: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()

    for entity_type, canonical_value in extract_entity_candidates(combined_text):
        key = (entity_type, canonical_value.lower())
        if key not in entity_map:
            entity_map[key] = {
                "entity_id": stable_uuid("week3-entity", doc_uuid, entity_type, canonical_value.lower()),
                "name": canonical_value,
                "type": entity_type,
                "canonical_value": canonical_value,
            }

    return list(entity_map.values())


def fact_confidence(document: dict[str, Any], page: dict[str, Any] | None = None) -> float:
    if page:
        page_conf = (page.get("metadata") or {}).get("confidence_score")
        if page_conf is not None:
            return validate_confidence_score(page_conf, "page.metadata.confidence_score")
    doc_conf = (document.get("metadata") or {}).get("confidence_score")
    if doc_conf is not None:
        return validate_confidence_score(doc_conf, "document.metadata.confidence_score")
    return 0.0


def build_facts(document: dict[str, Any], entities: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    facts: list[dict[str, Any]] = []
    seen_fact_keys: set[tuple[int, str]] = set()
    input_token_count = 0
    output_token_count = 0

    entity_lookup = {e["canonical_value"].lower(): e["entity_id"] for e in entities}

    pages = document.get("pages") or []
    for page in pages:
        page_number = int(page.get("page_number") or 0) or 0
        page_conf = fact_confidence(document, page)

        for kind, text in [
            ("page_text", normalize_whitespace(page.get("text"))),
            *[
                ("text_block", normalize_whitespace(block.get("text")))
                for block in (page.get("text_blocks") or [])
            ],
            *[
                ("table_block", flatten_table_rows(block.get("rows")))
                for block in (page.get("table_blocks") or [])
            ],
            *[
                ("figure_caption", normalize_whitespace(block.get("caption")))
                for block in (page.get("figure_blocks") or [])
            ],
            *[
                ("table", flatten_table_rows(table))
                for table in (page.get("tables") or [])
            ],
        ]:
            if not text:
                continue
            key = (page_number, normalize_whitespace(text).lower())
            if key in seen_fact_keys:
                continue
            seen_fact_keys.add(key)

            matched_entity_ids = [
                entity_id
                for canonical, entity_id in entity_lookup.items()
                if canonical and canonical in text.lower()
            ]
            input_token_count += approx_tokens(text)
            output_token_count += approx_tokens(text)
            facts.append(
                {
                    "fact_id": stable_uuid("week3-fact", document.get("doc_id", ""), str(page_number), kind, text.lower()),
                    "text": text,
                    "entity_refs": matched_entity_ids,
                    "confidence": round(page_conf, 3),
                    "page_ref": page_number,
                    "source_excerpt": truncate(text),
                }
            )

    if not facts:
        fallback_text = normalize_whitespace(document.get("file_name") or document.get("file_path") or document.get("doc_id") or "document")
        facts.append(
            {
                "fact_id": stable_uuid("week3-fact", document.get("doc_id", ""), "fallback", fallback_text.lower()),
                "text": fallback_text,
                "entity_refs": [],
                "confidence": fact_confidence(document, None),
                "page_ref": 0,
                "source_excerpt": truncate(fallback_text),
            }
        )
        input_token_count += approx_tokens(fallback_text)
        output_token_count += approx_tokens(fallback_text)

    return facts, input_token_count, output_token_count


def build_record(path: Path, document: dict[str, Any]) -> dict[str, Any]:
    source_bytes = path.read_bytes()
    source_hash = sha256_bytes(source_bytes)
    source_doc_id = str(document.get("doc_id") or path.stem)
    canonical_doc_id = stable_uuid("week3-doc", source_doc_id, str(path.resolve()))
    entities = build_entities(document)
    facts, input_tokens, output_tokens = build_facts(document, entities)

    extracted_at = to_iso8601(path.stat().st_mtime if path.exists() else None)
    metadata = document.get("metadata") or {}

    provenance_confidence = metadata.get("confidence_score")
    if provenance_confidence is not None:
        validate_confidence_score(provenance_confidence, "document.metadata.confidence_score")

    return {
        "doc_id": canonical_doc_id,
        "source_path": str(document.get("file_path") or path.as_posix()),
        "source_hash": source_hash,
        "extracted_facts": facts,
        "entities": entities,
        "extraction_model": str(metadata.get("strategy_used") or "unknown"),
        "processing_time_ms": int(round(float(metadata.get("processing_time_sec") or 0.0) * 1000)),
        "token_count": {
            "input": input_tokens,
            "output": output_tokens,
        },
        "extracted_at": extracted_at,
    }


def migrate(input_dir: Path, output_path: Path) -> int:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    json_files = sorted(
        p
        for p in input_dir.glob("*.json")
        if not p.name.endswith(".routing.json")
    )

    if not json_files:
        raise FileNotFoundError(f"No source JSON files found in {input_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, Any]] = []
    for source_file in json_files:
        raw = read_json(source_file)
        records.append(build_record(source_file, raw))

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")

    return len(records)


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_path = Path(args.output)

    try:
        count = migrate(input_dir, output_path)
    except Exception as exc:  # pragma: no cover - CLI surface
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote {count} records to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
