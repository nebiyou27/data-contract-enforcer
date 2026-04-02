# Data Contract Enforcer

> **TRP-1 · Week 7** — Bitol v3.0.0 contract generation and validation for multi-system data pipelines.

Automatically profiles JSONL datasets, generates machine-readable Bitol data contracts, and enforces them with structural and statistical checks. Covers the full contract lifecycle: schema discovery, constraint inference, drift baselining, and CI-ready validation with exit codes.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quickstart](#quickstart)
- [Generator](#generator)
- [Runner](#runner)
- [Contracts Written](#contracts-written)
- [Validation Results](#validation-results)
- [Data Sources](#data-sources)
- [Extending the System](#extending-the-system)

---

## Overview

Data contracts sit at the boundary between systems. This tool enforces them.

The enforcer ingests raw JSONL data, profiles every column, and produces a Bitol v3.0.0 contract that encodes the invariants observed in that data — required fields, allowed enum values, UUID formats, datetime formats, numeric bounds, and statistical baselines for drift detection. A separate runner then validates new data against those contracts and produces a structured JSON report.

**Current coverage:** 3 contracts active and validated.  3 sources are out of scope because no local data is available.

| System | Contract ID | Status | Checks |
|---|---|---|---|
| Week 3 — Document Refinery | `week3-document-refinery-extractions` | Active | 58 — all PASS |
| Week 4 — Cartographer lineage | `week4-lineage-graph` | Active | 24 — all PASS |
| Week 5 — Event Store | `week5-event-store` | Active | 43 — all PASS |
| Week 1 — Intent Correlator | `week1-intent-correlator` | **Out of scope** — no source data | — |
| Week 2 — Digital Courtroom | `week2-digital-courtroom` | **Out of scope** — no source data | — |
| LangSmith traces | `langsmith-traces` | **Out of scope** — external system | — |

All out-of-scope sources are declared in `contract_registry/subscriptions.yaml` under `contracts` with `status: out_of_scope` and a reason.  No coverage is claimed for them.

---

## Architecture

```
JSONL Source Data
      |
      v
+---------------------+
|   contracts/        |
|   generator.py      |
|                     |
|  Stage 1: Load +    |      generated_contracts/
|           Flatten   | -->  week3-*.yaml
|  Stage 2: Profile   |      week5-*.yaml
|  Stage 3: Translate |      week3-*_dbt_schema.yml
|  Stage 4: Assemble  |      week5-*_dbt_schema.yml
+---------------------+
                              schema_snapshots/
                              baselines.json
                              {contract_id}/{timestamp}.yaml
      |
      v
+-------------------------------------------+
|   contract_registry/subscriptions.yaml    |
|                                           |
|  registry:   schema_evolution_policy      |
|  contracts:  catalog (active + OOS)       |
|  subscriptions: who depends on what,      |
|                 which fields are breaking |
+-------------------------------------------+
      |                         |
      | (pre-deploy gate)       | (blast-radius attribution)
      v                         v
+---------------------+   +---------------------+
|   contracts/        |   |   contracts/        |
|   runner.py         |   |   attributor.py     |
|                     |   |                     |
|  Evolution gate     |   |  Registry-first     |
|  Structural checks  |   |  blast-radius       |
|  Statistical drift  |   |  Lineage enrichment |
|  Exit code: 0/1     |   +---------------------+
+---------------------+
      |
      v
  validation_reports/
  week3_baseline.json
  week5_baseline.json
```

### Pipeline data flow

```
Week 1 (Intent Correlator)   -->  Week 2 (Digital Courtroom)
Week 3 (Document Refinery)   -->  Week 4 (Cartographer)
Week 4 (lineage graph)       -->  Week 7 (this system)
Week 5 (Event Store)         -->  Week 7 (this system)
LangSmith (traces)           -->  Week 7 (this system)
Week 2 (verdict records)     -->  Week 7 (this system)
```

---

## Project Structure

```
data-contract-enforcer/
|
|-- contracts/
|   |-- __init__.py
|   |-- generator.py          # 4-stage contract generator
|   |-- runner.py             # Contract validation runner + producer-side evolution gate
|   +-- attributor.py         # Registry-first blast-radius attribution
|
|-- generated_contracts/
|   |-- week3-document-refinery-extractions.yaml
|   |-- week3-document-refinery-extractions_dbt_schema.yml
|   |-- week5-event-store.yaml
|   +-- week5-event-store_dbt_schema.yml
|
|-- outputs/
|   |-- week3/extractions.jsonl       # 38 docs, 17 MB
|   |-- week4/lineage_snapshots.jsonl # 96 nodes, 80 edges
|   +-- week5/events.jsonl            # 1,198 events
|
|-- schema_snapshots/
|   |-- baselines.json                # Drift detection baselines
|   |-- week3-document-refinery-extractions/  # Timestamped contract history
|   +-- week5-event-store/
|
|-- scripts/
|   |-- migrate_week3.py              # Week 3 extractions -> canonical JSONL
|   |-- migrate_week4.py              # Week 4 lineage snapshots -> canonical JSONL
|   |-- migrate_week5.py              # Week 5 event store -> canonical JSONL
|   +-- batch_extract_week3.py        # Claude API batch extraction driver
|
|-- validation_reports/               # JSON validation run outputs
|-- thursday_report.md
|-- DOMAIN_NOTES.md                   # Domain context and design decisions (graded deliverable)
+-- .gitignore
```

---

## Quickstart

**Prerequisites:** Python 3.10+, `pandas`, `pyyaml`

```bash
# Install dependencies
pip install pandas pyyaml

# Generate a contract from source data
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

# Validate data against the contract
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_run.json

# Exit code: 0 = all checks passed, 1 = at least one FAIL or ERROR
echo $?
```

---

## Generator

[contracts/generator.py](contracts/generator.py) — generates a Bitol v3.0.0 contract from a JSONL source file.

### Usage

```
python contracts/generator.py
  --source      <path>    Path to input JSONL file (required)
  --contract-id <id>      Contract identifier, used as filename (required)
  --lineage     <path>    Optional lineage JSONL for inputPorts injection
  --output      <dir>     Output directory for contract YAML (default: generated_contracts/)
```

### 4-Stage Pipeline

**Stage 1 — Load + Flatten**

Reads JSONL records and normalises nested structures into flat DataFrames. For Week 3 extraction records, this explodes three levels:

| Table | Rows (38 docs) | Description |
|---|---|---|
| `documents` | 38 | Top-level document metadata |
| `extracted_facts` | 44,784 | Nested `extracted_facts` arrays exploded |
| `entities` | 2,509 | Nested `entities` arrays exploded |

For flat structures (Week 5 events), all records map to the `documents` table as-is.

**Stage 2 — Profile**

For every column, computes:
- `dtype` — pandas inferred type
- `null_fraction` — fraction of null values
- `cardinality` — number of unique non-null values
- `sample_values` — up to 10 representative values
- `min`, `max`, `mean`, `stddev`, `p25`, `p50`, `p75` — numeric columns only

**Stage 3 — Translate**

Profiles are converted to Bitol field clauses using five deterministic rules:

| Rule | Condition | Output |
|---|---|---|
| Required | `null_fraction == 0.0` | `required: true` |
| Enum | `cardinality <= 10` and `dtype == object` | `enum: [observed values]` |
| UUID format | column name ends with `_id` | `format: uuid` |
| Date-time format | column name ends with `_at` | `format: date-time` |
| Confidence bounds | `dtype == float` and `"confidence"` in name | `minimum: 0.0, maximum: 1.0` |

**Stage 4 — Assemble + Write**

Constructs the full Bitol contract dict and writes:
1. `generated_contracts/{contract-id}.yaml` — primary contract
2. `generated_contracts/{contract-id}_dbt_schema.yml` — dbt-compatible schema
3. `schema_snapshots/{contract-id}/{timestamp}.yaml` — immutable versioned snapshot

### Contract Format (Bitol v3.0.0)

```yaml
kind: DataContract
apiVersion: v3.0.0
id: week3-document-refinery-extractions
info:
  title: Week 3 — Document Refinery Extractions
  version: 1.0.0
schema:
  tables:
    - name: documents
      fields:
        - name: doc_id
          type: string
          required: true
          format: uuid
        - name: extraction_model
          type: string
          enum:
            - strategy_a
            - strategy_a+strategy_b
            - strategy_a+strategy_b+strategy_c
            # ...
quality:
  rules:
    - name: documents_doc_id_not_null
      dimension: completeness
      ...
lineage:
  inputPorts:
    - name: batch_extraction
      ...
```

---

## Runner

[contracts/runner.py](contracts/runner.py) — validates JSONL data against a Bitol contract.

### Usage

```
python contracts/runner.py
  --contract  <path>    Path to contract YAML (required)
  --data      <path>    Path to data JSONL (required)
  --output    <path>    Output path for JSON report (required)
```

### Producer-Side Evolution Gate

`check_producer_evolution_gate()` is a pre-deploy function that blocks a schema change from shipping if it removes a field declared as breaking in the registry.

```python
from contracts.runner import check_producer_evolution_gate
from contracts.attributor import load_registry

registry = load_registry()
result = check_producer_evolution_gate(
    proposed_fields=["doc_id", "fact_count"],      # new schema (confidence removed)
    current_fields=["doc_id", "fact_count", "confidence"],
    contract_id="week3-document-refinery-extractions",
    registry=registry,
)
# result["action"] == "BLOCK"
# result["breaking_fields_affected"] == [{"field": "extracted_facts.confidence", ...}]
```

The gate checks the removed fields against `subscriptions[].breaking_fields` in the registry.  If any breaking field would be removed, the result is `BLOCK` and the reason names the field and the downstream subscriber.  The deploy proceeds only if all registered breaking fields are preserved.

### Check Execution Order

Checks run in a fixed order — schema evolution first, structural next, statistical last:

| # | Check Type | Severity | Triggers |
|---|---|---|---|
| 1 | `schema_missing` | CRITICAL | Declared contract column is absent from the observed table |
| 2 | `schema_new_column` | MEDIUM | Observed table contains a column not declared in the contract |
| 3 | `required` | CRITICAL | Any null in a `required: true` field |
| 4 | `type` | CRITICAL | Actual dtype incompatible with logical type |
| 5 | `enum` | HIGH | Any value not in `enum` list |
| 6 | `format_uuid` | CRITICAL | UUID field fails regex match |
| 7 | `format_datetime` | CRITICAL | Date-time field fails ISO 8601 regex |
| 8 | `range` | HIGH | Numeric field outside `[minimum, maximum]` |
| 9 | `drift` | LOW | Z-score > 2σ (WARN) or > 3σ (FAIL) vs baseline |

### Drift Detection

On first run, baseline statistics are written to `schema_snapshots/baselines.json`. On subsequent runs, the current distribution is compared against the baseline using a z-score:

```
z = abs(current_mean - baseline_mean) / baseline_stddev
```

- `z <= 2`: PASS
- `2 < z <= 3`: WARN
- `z > 3`: FAIL

Baselines update after each successful run.

### Report Structure

```json
{
  "report_id": "uuid",
  "contract_id": "week3-document-refinery-extractions",
  "snapshot_id": "sha256:eb773d01...",
  "run_timestamp": "2026-04-01T16:22:16Z",
  "total_checks": 58,
  "passed": 58,
  "failed": 0,
  "warned": 0,
  "errored": 0,
  "results": [
    {
      "check_id": "uuid",
      "column_name": "doc_id",
      "check_type": "required",
      "status": "PASS",
      "actual_value": 0,
      "expected": 0,
      "severity": "CRITICAL",
      "records_failing": 0,
      "sample_failing": [],
      "message": "No nulls found"
    }
  ]
}
```

### Exit Codes

| Code | Meaning |
|---|---|
| `0` | All checks passed (or only WARNs — no FAILs or ERRORs) |
| `1` | At least one FAIL or ERROR |

This makes the runner usable directly in CI pipelines: a failing contract check fails the build.

---

## Contracts Written

### `week3-document-refinery-extractions`

Source: `outputs/week3/extractions.jsonl` — 38 PDFs extracted via Claude API batch pipeline.

**Schema:** 3 tables, 21 fields

| Table | Fields | Notable Constraints |
|---|---|---|
| `documents` | 10 | `doc_id` required+uuid; `extraction_model` enum (6 values); `extracted_at` datetime |
| `extracted_facts` | 6 | `fact_id` required+uuid; `confidence` in [0.0, 1.0] |
| `entities` | 5 | `entity_id` required+uuid; `type` enum (DATE, AMOUNT, ORG, ...) |

**Quality rules:** 26 (completeness + validity + range)

**Lineage:** inputPort from batch extraction pipeline + lineage graph context (96 nodes, 80 edges from Week 4 Cartographer)

### `week5-event-store`

Source: `outputs/week5/events.jsonl` — 1,198 domain events migrated from PostgreSQL event store.

**Schema:** 10 fields (flat structure)

| Field | Constraints |
|---|---|
| `event_id` | required, uuid (UUID5 from stream_id:position:event_type) |
| `aggregate_id` | required, uuid (UUID5 from aggregate prefix:entity_key) |
| `event_type` | string |
| `aggregate_type` | string |
| `occurred_at` | datetime |
| `recorded_at` | datetime |
| ... | ... |

**Quality rules:** 4 (completeness, non-negative counts)

---

## Validation Results

### Week 3 — First baseline run (`2026-04-01T16:22:16Z`)

| Metric | Value |
|---|---|
| Total checks | 58 |
| Passed | **58** |
| Failed | 0 |
| Warned | 0 |
| Errored | 0 |

| Check Type | Count | Notes |
|---|---|---|
| `required` | 21 | All 44,784 fact rows have non-null fact_id, doc_id, text |
| `type` | 21 | All dtype matches confirmed |
| `format_uuid` | 5 | All 34,138+ UUID fields valid |
| `drift` | 7 | z=0.00 (same dataset, expected) |
| `enum` | 2 | 6 extraction_model combinations all valid |
| `format_datetime` | 1 | All extracted_at values ISO 8601 compliant |
| `range` | 1 | confidence in [0.676, 1.0] — within [0.0, 1.0] bounds |

### Week 5 — First baseline run (`2026-04-01T15:24:08Z`)

| Metric | Value |
|---|---|
| Total checks | 16 |
| Passed | **16** |
| Failed | 0 |
| Warned | 0 |
| Errored | 0 |

| Check Type | Count | Notes |
|---|---|---|
| `type` | 10 | All event fields match declared types |
| `required` | 2 | event_id and aggregate_id always present |
| `format_uuid` | 1 | All 1,198 event_ids valid UUID5s |
| `format_datetime` | 1 | All occurred_at values ISO 8601 compliant |
| `drift` | 2 | fact_count, entity_count (both 0 — flat events, expected) |

---

## Data Sources

### Week 3 — Document Refinery (`outputs/week3/extractions.jsonl`)

38 PDF documents extracted via Claude API. Each record contains:
- Top-level document metadata (source_path, extraction_model, processing_time_ms, token counts)
- `extracted_facts[]` — array of extracted fact objects (text, confidence, page_ref)
- `entities[]` — array of named entity objects (name, type, canonical_value)

> **Note:** Week 3 currently contains 38 documents (batch extraction ongoing for remaining PDFs). All 38 documents produce 44,784 extracted facts — the contract enforcement pipeline operates on the full fact-level dataset.

### Week 4 — Cartographer (`outputs/week4/lineage_snapshots.jsonl`)

Provenance lineage graph with 96 nodes and 80 edges. Nodes represent extracted facts, document entities, source PDFs, extraction runs, and model versions. Used for lineage injection into Week 3 contract.

### Week 5 — Event Store (`outputs/week5/events.jsonl`)

1,198 canonical domain events migrated from a PostgreSQL-backed event store via [scripts/migrate_week5.py](scripts/migrate_week5.py). Event fields:

```json
{
  "event_id": "uuid5(stream_id:seq:event_type)",
  "event_type": "LoanApplicationSubmitted",
  "aggregate_id": "uuid5(aggregate:loan:APEX-0001)",
  "aggregate_type": "LoanApplication",
  "sequence_number": 0,
  "payload": { ... },
  "metadata": {
    "correlation_id": "uuid5(correlation:app_id)",
    "source_service": "loan-origination",
    "original_stream_id": "loan-APEX-0001",
    "global_position": 1
  },
  "schema_version": "1.0",
  "occurred_at": "2024-01-15T09:23:00Z",
  "recorded_at": "2024-01-15T09:23:00Z"
}
```

**Coverage:** 34 event types across 6 aggregate types (LoanApplication, DocumentPackage, AgentSession, CreditRecord, ComplianceRecord, FraudScreening), 151 streams.

---

## Out-of-Scope Sources

The following systems are declared in the registry catalog but have no local source data.
No contract validation is performed for them and no coverage is claimed.

| Contract ID | Reason |
|---|---|
| `week1-intent-correlator` | Source data not available in this repository |
| `week2-digital-courtroom` | Source data not available in this repository |
| `langsmith-traces` | External system; no local source data available |

If source data becomes available, add the JSONL to `outputs/{week}/`, run `contracts/generator.py`, and update the registry entry from `out_of_scope` to `active`.

---

## Extending the System

### Adding a new contract

```bash
# 1. Prepare your JSONL at outputs/{week}/data.jsonl

# 2. Update the registry: change the contract entry from out_of_scope to active
#    (or add a new entry) in contract_registry/subscriptions.yaml

# 3. Generate the contract
python contracts/generator.py \
  --source outputs/week2/verdict_records.jsonl \
  --contract-id week2-digital-courtroom \
  --output generated_contracts/

# 4. Validate
python contracts/runner.py \
  --contract generated_contracts/week2-digital-courtroom.yaml \
  --data outputs/week2/verdict_records.jsonl \
  --output validation_reports/week2_baseline.json
```

### Rerunning on expanded datasets

The generator and runner are idempotent. To regenerate after the batch extraction completes:

```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_full.json
```

Each generator run appends a new timestamped snapshot to `schema_snapshots/{contract-id}/`. Each runner run updates `baselines.json` with the latest statistics.

---

*Validation data current as of 2026-04-01. See [thursday_report.md](thursday_report.md) for full run analysis.*
