# Data Contract Enforcer

> **TRP-1 · Week 7** — Bitol v3.0.0 contract generation and validation for multi-system data pipelines.

Automatically profiles JSONL datasets, generates machine-readable Bitol data contracts, and enforces them with structural and statistical checks. Covers the full contract lifecycle: schema discovery, constraint inference, drift baselining, producer-side evolution gating, quarantine management, and CI-ready validation with exit codes.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [CLI Reference](#cli-reference)
- [Generator](#generator)
- [Runner](#runner)
- [Additional Modules](#additional-modules)
- [Contracts Written](#contracts-written)
- [Validation Results](#validation-results)
- [Data Sources](#data-sources)
- [Extending the System](#extending-the-system)

---

## Overview

Data contracts sit at the boundary between systems. This tool enforces them.

The enforcer ingests raw JSONL data, profiles every column, and produces a Bitol v3.0.0 contract that encodes the invariants observed in that data — required fields, allowed enum values, UUID formats, datetime formats, numeric bounds, and statistical baselines for drift detection. A separate runner then validates new data against those contracts and produces a structured JSON report.

**Current coverage:** 4 contracts active and validated. 1 source is out of scope because no local data is available.

| System | Contract ID | Status | Checks |
|---|---|---|---|
| Week 3 — Document Refinery | `week3-document-refinery-extractions` | Active | 58 — all PASS |
| Week 4 — Cartographer lineage | `week4-lineage-graph` | Active | 24 — all PASS |
| Week 5 — Event Store | `week5-event-store` | Active | 43 — all PASS |
| LangSmith traces | `langsmith-traces` | Active | 28 — all PASS |
| Week 1 — Intent Correlator | `week1-intent-correlator` | **Out of scope** — no source data | — |
| Week 2 — Digital Courtroom | `week2-digital-courtroom` | Source of LangSmith traces | — |

Week 1 is declared in `contract_registry/subscriptions.yaml` under `contracts` with `status: out_of_scope` and a reason. Week 2 is the LangSmith-producing source system for the exported trace tree covered under `langsmith-traces`.

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
|   |-- attributor.py         # Registry-first blast-radius attribution
|   |-- schema_analyzer.py    # Diffs snapshots, classifies breaking vs compatible changes
|   |-- ai_extensions.py      # Embedding drift + prompt/input + output schema checks
|   |-- report_generator.py   # Aggregates runs into enforcer_report/report_data.json
|   |-- baseline_manager.py   # Manage statistical baselines (list / promote / clear)
|   |-- batch_runner.py       # Run multiple contracts in parallel from a batch manifest
|   |-- evolution_gate.py     # Pre-deploy gate — blocks breaking schema changes
|   |-- quarantine_manager.py # Review and retry quarantined records
|   +-- log_config.py         # Structured JSON logging + optional OTEL trace export
|
|-- generated_contracts/
|   |-- week3-document-refinery-extractions.yaml
|   |-- week3-document-refinery-extractions_dbt_schema.yml
|   |-- week4-lineage-graph.yaml
|   |-- week4-lineage-graph_dbt_schema.yml
|   |-- week5-event-store.yaml
|   |-- week5-event-store_dbt_schema.yml
|   |-- langsmith-traces.yaml
|   +-- langsmith-traces_dbt_schema.yml
|
|-- outputs/
|   |-- week2/verdicts.jsonl              # 20 verdict records
|   |-- week3/extractions.jsonl           # 38 docs, 44,784 extracted facts
|   |-- week4/lineage_snapshots.jsonl     # 96 nodes, 80 edges
|   |-- week5/events.jsonl               # 1,198 events
|   +-- traces/automaton_auditor_week2_langsmith_tree.jsonl  # 22 LangSmith trace nodes
|
|-- schema_snapshots/
|   |-- baselines.json                    # Drift detection baselines
|   |-- week3-document-refinery-extractions/  # Timestamped contract history
|   +-- week5-event-store/
|
|-- scripts/
|   |-- migrate_week3.py                  # Week 3 extractions -> canonical JSONL
|   |-- migrate_week4.py                  # Week 4 lineage snapshots -> canonical JSONL
|   |-- migrate_week5.py                  # Week 5 event store -> canonical JSONL
|   +-- batch_extract_week3.py            # Claude API batch extraction driver
|
|-- contract_registry/
|   +-- subscriptions.yaml                # Registry: catalog, subscribers, breaking fields
|
|-- validation_reports/                   # JSON validation run outputs
|-- violation_log/violations.jsonl        # Append-only violation log
|-- quarantine/                           # Quarantined records pending review
|-- enforcer_report/report_data.json      # Machine-generated report with health score
|-- batch.yaml                            # Batch runner manifest
|-- pyproject.toml                        # Package config + CLI entry points
|-- INSTALL.md                            # Full operator runbook
|-- DOMAIN_NOTES.md                       # Domain context and design decisions
+-- .gitignore
```

---

## Required Codebase Artifacts

| Artifact | Status | Notes |
|---|---:|---|
| contracts/generator.py | ✅ | Runnable contract generator |
| contracts/runner.py | ✅ | Runnable validation runner |
| contracts/attributor.py | ✅ | Produces blame chain + blast radius |
| contracts/schema_analyzer.py | ✅ | Diffs snapshots, classifies breaking change |
| contracts/ai_extensions.py | ✅ | Embedding drift + prompt/input + output schema checks |
| contracts/report_generator.py | ✅ | Generates enforcer_report/report_data.json |
| contracts/baseline_manager.py | ✅ | Baseline lifecycle management |
| contracts/batch_runner.py | ✅ | Parallel multi-contract validation |
| contracts/evolution_gate.py | ✅ | Pre-deploy breaking-change gate |
| contracts/quarantine_manager.py | ✅ | Quarantine review + requeue |
| contract_registry/subscriptions.yaml | ✅ | Registry with subscribers and breaking_fields |
| generated_contracts/ | ✅ | 4 auto-generated YAML contracts |
| validation_reports/ | ✅ | Real validation run JSON outputs |
| violation_log/violations.jsonl | ✅ | LLM output violation log (append-only) |
| schema_snapshots/ | ✅ | 2+ timestamped snapshots per contract |
| enforcer_report/report_data.json | ✅ | Machine-generated report with health score 88 |
| outputs/week2/verdicts.jsonl | ✅ | 20 verdict records |
| outputs/week3/extractions.jsonl | ✅ | 38 documents (real data) |
| outputs/week4/lineage_snapshots.jsonl | ✅ | 96 nodes, 80 edges (real data) |
| outputs/week5/events.jsonl | ✅ | 1,198 events (real data) |
| README.md | ✅ | Fresh-clone run instructions |
| INSTALL.md | ✅ | Operator runbook with full CLI reference |
| DOMAIN_NOTES.md | ✅ | Finalized domain notes |

---

## Installation

**Prerequisites:** Python 3.10+, Git

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Core install — validation + contract generation
pip install -e .

# With AI checks (requires ANTHROPIC_API_KEY)
pip install -e ".[llm]"

# With OpenTelemetry trace export
pip install -e ".[otel]"

# With test runner
pip install -e ".[dev]"

# Everything
pip install -e ".[all]"
```

After install, these commands are available on your `$PATH`:

| Command | Purpose |
|---------|---------|
| `contracts-generate` | Generate a Bitol v3.0.0 contract from JSONL source data |
| `contracts-run` | Validate JSONL data against a contract |
| `contracts-run-all` | Run multiple contracts in parallel from a batch manifest |
| `contracts-baseline` | Manage statistical baselines (list / promote / clear) |
| `contracts-ai-checks` | Run AI-driven checks (embedding drift, prompt schema, violation rate) |
| `contracts-report` | Aggregate all results into a final enforcer report with health score |
| `contracts-schema-diff` | Diff two schema snapshots and classify breaking changes |
| `contracts-evolution-gate` | Pre-deploy gate — blocks breaking schema changes |
| `contracts-quarantine` | Review and retry quarantined records |

> See [INSTALL.md](INSTALL.md) for the full operator runbook: environment variables, first-run walkthrough, baseline management, logging, tracing, and troubleshooting.

---

## Quickstart

### Run Full Validation Suite

```bash
# 1. Generate contracts from source data
contracts-generate \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

contracts-generate \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-store \
  --output generated_contracts/

contracts-generate \
  --source outputs/week4/lineage_snapshots.jsonl \
  --contract-id week4-lineage-graph \
  --output generated_contracts/

contracts-generate \
  --source outputs/traces/automaton_auditor_week2_langsmith_tree.jsonl \
  --contract-id langsmith-traces \
  --output generated_contracts/

# 2. Validate all contracts in parallel (using batch.yaml)
contracts-run-all --batch batch.yaml

# — or validate individually —
contracts-run \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_baseline.json

contracts-run \
  --contract generated_contracts/week5-event-store.yaml \
  --data outputs/week5/events.jsonl \
  --output validation_reports/week5_baseline.json

contracts-run \
  --contract generated_contracts/week4-lineage-graph.yaml \
  --data outputs/week4/lineage_snapshots.jsonl \
  --output validation_reports/week4_baseline.json

contracts-run \
  --contract generated_contracts/langsmith-traces.yaml \
  --data outputs/traces/automaton_auditor_week2_langsmith_tree.jsonl \
  --output validation_reports/langsmith_baseline.json

# 3. Promote baselines after a clean first run
contracts-baseline promote --report validation_reports/week3_baseline.json
contracts-baseline promote --report validation_reports/week5_baseline.json

# 4. Run schema evolution analysis
contracts-schema-diff \
  --baseline generated_contracts/week3-document-refinery-extractions.yaml \
  --current generated_contracts/week3-document-refinery-extractions.yaml \
  --output validation_reports/schema_evolution_report.json

# 5. Run AI-driven checks on verdict data
contracts-ai-checks \
  --data outputs/week2/verdicts.jsonl \
  --output validation_reports/week2_ai_checks.json

# 6. Generate final report
contracts-report \
  --validation-reports \
    validation_reports/week3_baseline.json \
    validation_reports/week5_baseline.json \
    validation_reports/week4_baseline.json \
  --ai-checks validation_reports/week2_ai_checks.json \
  --schema-evolution validation_reports/schema_evolution_report.json \
  --violation-log violation_log/violations.jsonl \
  --output enforcer_report/report_data.json

echo "Full validation suite complete"
```

> **Without pip install:** substitute `contracts-run` with `python contracts/runner.py`, etc.

### Expected Output

**validation_reports/ (after full suite)**
```
week3_baseline.json          → 58 checks PASS
week5_baseline.json          → 43 checks PASS
week4_baseline.json          → 24 checks PASS
langsmith_baseline.json      → 28 checks PASS
week2_ai_checks.json         → 3 AI checks: 2 PASS, 1 WARN
schema_evolution_report.json → verdict: "compatible"
```

**enforcer_report/report_data.json**
```json
{
  "data_health_score": 88,
  "verdict": "PASS",
  "validation": {
    "total_checks": 159,
    "total_passed": 159,
    "pass_rate_pct": 100.0
  }
}
```

### Exit Codes

All modules use standard exit codes for CI/CD integration:

| Code | Meaning |
|------|---------|
| `0` | All checks passed (or only WARNs — no FAILs or ERRORs) |
| `1` | Failures detected / breaking changes / critical issues |
| `2` | Error running module (invalid args, missing files, etc.) |

### Validation Modes

Pass `--mode` to `contracts-run` or `contracts-run-all` to change enforcement behavior:

| Mode | Behavior |
|------|---------|
| `AUDIT` | Report all violations, always exit 0 — use for observability |
| `WARN` | Warn on violations but exit 0 — use for gradual rollout |
| `ENFORCE` | Fail hard on any violation — use in CI/CD gates (default) |

---

## CLI Reference

### Quick individual commands

```bash
# Test a single contract
contracts-run \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/test.json

# Analyze schema differences between two snapshots
contracts-schema-diff \
  --baseline schema_snapshots/week3-document-refinery-extractions/20260401T133008Z.yaml \
  --current generated_contracts/week3-document-refinery-extractions.yaml \
  --output validation_reports/diff.json

# Run AI checks
contracts-ai-checks \
  --data outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_test.json

# Generate report from existing validation runs
contracts-report \
  --validation-reports validation_reports/week3_baseline.json validation_reports/week5_baseline.json \
  --violation-log violation_log/violations.jsonl \
  --output enforcer_report/custom_report.json

# Baseline management
contracts-baseline list
contracts-baseline promote --report validation_reports/week3_baseline.json
contracts-baseline clear --contract week3-document-refinery-extractions

# Run all contracts in parallel
contracts-run-all --batch batch.yaml
contracts-run-all --batch batch.yaml --fail-fast

# Evolution gate (manual invocation)
contracts-evolution-gate \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --proposed-ref HEAD \
  --current-ref origin/main

# Quarantine management
contracts-quarantine review --quarantine quarantine/prompt_schema_violations.jsonl
contracts-quarantine requeue \
  --quarantine quarantine/prompt_schema_violations.jsonl \
  --fixed-jsonl fixed_records.jsonl
contracts-quarantine clear --quarantine quarantine/prompt_schema_violations.jsonl --yes
```

### Running tests

```bash
pip install -e ".[dev]"
python -m pytest
```

292 tests, ~1 second. Covers: contract generation, validation runner, drift checks, schema evolution, AI extensions, baseline manager, report generator, and attributor.

---

## Generator

[contracts/generator.py](contracts/generator.py) — generates a Bitol v3.0.0 contract from a JSONL source file.

### Usage

```
contracts-generate
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
contracts-run
  --contract  <path>    Path to contract YAML (required)
  --data      <path>    Path to data JSONL (required)
  --output    <path>    Output path for JSON report (required)
  --mode      <mode>    AUDIT | WARN | ENFORCE (default: ENFORCE)
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

The gate checks the removed fields against `subscriptions[].breaking_fields` in the registry. If any breaking field would be removed, the result is `BLOCK` and the reason names the field and the downstream subscriber. The deploy proceeds only if all registered breaking fields are preserved.

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

Baselines are read-only by default — they update only when you explicitly run `contracts-baseline promote`.

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

## Additional Modules

### Schema Analyzer ([contracts/schema_analyzer.py](contracts/schema_analyzer.py))

Diffs two contract YAML snapshots and classifies each field change as breaking or compatible.

```bash
contracts-schema-diff \
  --baseline schema_snapshots/week3-document-refinery-extractions/20260401T133008Z.yaml \
  --current generated_contracts/week3-document-refinery-extractions.yaml \
  --output validation_reports/schema_evolution_report.json
```

Breaking changes (exit code `1`): field removal, type narrowing, new required field. Compatible changes (exit code `0`): new optional field, enum expansion, relaxed constraints.

### AI Extensions ([contracts/ai_extensions.py](contracts/ai_extensions.py))

Three AI-driven checks on top of structural validation:

| Check | What it detects |
|---|---|
| `embedding_drift` | Semantic drift in text fields vs. a baseline embedding centroid |
| `prompt_schema` | LLM input/output records that violate expected JSON schema |
| `llm_output_violation_rate` | Batch violation rate above threshold (default 5%) with trend tracking |

Large JSONL files are streamed in O(1) RAM regardless of file size.

```bash
contracts-ai-checks \
  --data outputs/week2/verdicts.jsonl \
  --output validation_reports/week2_ai_checks.json
```

Requires `pip install -e ".[llm]"` and `ANTHROPIC_API_KEY`.

### Attributor ([contracts/attributor.py](contracts/attributor.py))

Registry-first blast-radius attribution. Given a broken field, traces downstream subscribers and produces a blame chain.

### Baseline Manager ([contracts/baseline_manager.py](contracts/baseline_manager.py))

Manages `schema_snapshots/baselines.json` — the statistical reference used for drift detection.

```bash
contracts-baseline list                                               # show all baselines
contracts-baseline promote --report validation_reports/week3.json    # update from a clean run
contracts-baseline clear --contract week3-document-refinery-extractions
contracts-baseline clear --all --yes
```

### Batch Runner ([contracts/batch_runner.py](contracts/batch_runner.py))

Runs multiple contracts in parallel from a `batch.yaml` manifest:

```yaml
defaults:
  mode: AUDIT
  promote_baselines: false

max_workers: 4

jobs:
  - contract: generated_contracts/week3-document-refinery-extractions.yaml
    data: outputs/week3/extractions.jsonl
    output: validation_reports/week3.json

  - contract: generated_contracts/week4-lineage-graph.yaml
    data: outputs/week4/lineage_snapshots.jsonl
    output: validation_reports/week4.json
```

```bash
contracts-run-all --batch batch.yaml
contracts-run-all --batch batch.yaml --fail-fast   # stop on first failure
```

### Evolution Gate ([contracts/evolution_gate.py](contracts/evolution_gate.py))

Standalone pre-deploy gate that diffs two git refs and blocks if breaking fields would be removed.

```bash
contracts-evolution-gate \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --proposed-ref HEAD \
  --current-ref origin/main
```

Exit `0` = compatible, exit `1` = breaking change blocked.

### Quarantine Manager ([contracts/quarantine_manager.py](contracts/quarantine_manager.py))

Records that fail JSON schema validation on metadata are written to `quarantine/prompt_schema_violations.jsonl`.

```bash
contracts-quarantine review --quarantine quarantine/prompt_schema_violations.jsonl
contracts-quarantine requeue \
  --quarantine quarantine/prompt_schema_violations.jsonl \
  --fixed-jsonl fixed_records.jsonl
contracts-quarantine clear --quarantine quarantine/prompt_schema_violations.jsonl --yes
```

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

### `week4-lineage-graph`

Source: `outputs/week4/lineage_snapshots.jsonl` — 96 nodes and 80 edges from the Cartographer provenance graph.

**Schema:** flat node/edge records with typed IDs, relationship metadata, and lineage timestamps.

### `langsmith-traces`

Source: `outputs/traces/automaton_auditor_week2_langsmith_tree.jsonl` — 22 trace nodes exported from the Week 2 LangSmith run tree.

**Schema:** 1 table, 13 fields

| Table | Fields | Notable Constraints |
|---|---|---|
| `trace_nodes` | 13 | `run_id` required+uuid; `parent_run_id` uuid; `run_type` enum; `start_time` datetime; `trace_project_id` uuid |

**Quality rules:** 14 (completeness + validity)

**Lineage:** inputPort from LangSmith trace export

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

### Week 4 — First baseline run

| Metric | Value |
|---|---|
| Total checks | 24 |
| Passed | **24** |
| Failed | 0 |
| Warned | 0 |
| Errored | 0 |

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

### LangSmith — First baseline run (`2026-04-02`)

| Metric | Value |
|---|---|
| Total checks | 28 |
| Passed | **28** |
| Failed | 0 |
| Warned | 0 |
| Errored | 0 |

| Check Type | Count | Notes |
|---|---|---|
| `required` | 5 | Core trace-node fields are always present |
| `type` | 5 | Contract logical types match the flattened tree |
| `format_uuid` | 3 | run_id, parent_run_id, trace_project_id preserve UUID identity |
| `format_datetime` | 2 | start_time and end_time are ISO 8601 compliant |
| `enum` | 1 | run_type is stable across the export |
| `range` | 1 | depth remains non-negative |
| `drift` | 1 | baseline created from the canonical trace tree |

---

## Data Sources

### Week 3 — Document Refinery (`outputs/week3/extractions.jsonl`)

38 PDF documents extracted via Claude API. Each record contains:
- Top-level document metadata (source_path, extraction_model, processing_time_ms, token counts)
- `extracted_facts[]` — array of extracted fact objects (text, confidence, page_ref)
- `entities[]` — array of named entity objects (name, type, canonical_value)

> **Note:** 38 documents produce 44,784 extracted facts. The contract enforcement pipeline operates on the full fact-level dataset.

### Week 4 — Cartographer (`outputs/week4/lineage_snapshots.jsonl`)

Provenance lineage graph with 96 nodes and 80 edges. Nodes represent extracted facts, document entities, source PDFs, extraction runs, and model versions. Used for lineage injection into the Week 3 contract.

### Week 5 — Event Store (`outputs/week5/events.jsonl`)

1,198 canonical domain events migrated from a PostgreSQL-backed event store via [scripts/migrate_week5.py](scripts/migrate_week5.py). Event fields:

```json
{
  "event_id": "uuid5(stream_id:seq:event_type)",
  "event_type": "LoanApplicationSubmitted",
  "aggregate_id": "uuid5(aggregate:loan:APEX-0001)",
  "aggregate_type": "LoanApplication",
  "sequence_number": 0,
  "payload": { "..." : "..." },
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

### LangSmith — Trace Tree Export (`outputs/traces/automaton_auditor_week2_langsmith_tree.jsonl`)

22 trace nodes exported from the Week 2 auditor run tree.

Each record contains:
- `id` and `parent_run_id` to preserve the raw run-tree structure
- `depth`, `name`, `run_type`, `start_time`, `end_time`
- `inputs`, `outputs`, `tags`, `error`, `app_path`, `trace_project`

**Coverage:** 1 root run, 15 direct children, 22 total nodes.

### Out-of-Scope Sources

| Contract ID | Reason |
|---|---|
| `week1-intent-correlator` | Source data not available in this repository |

If Week 1 source data becomes available, add the JSONL to `outputs/week1/`, run `contracts-generate`, and update the registry entry from `out_of_scope` to `active`.

---

## Extending the System

### Adding a new contract

```bash
# 1. Prepare your JSONL at outputs/{week}/data.jsonl

# 2. Update contract_registry/subscriptions.yaml — add a new entry or
#    change an existing status: out_of_scope entry to status: active

# 3. Generate the contract
contracts-generate \
  --source outputs/week2/verdict_records.jsonl \
  --contract-id week2-digital-courtroom \
  --output generated_contracts/

# 4. Validate
contracts-run \
  --contract generated_contracts/week2-digital-courtroom.yaml \
  --data outputs/week2/verdict_records.jsonl \
  --output validation_reports/week2_baseline.json

# 5. Promote baselines for drift tracking
contracts-baseline promote --report validation_reports/week2_baseline.json
```

### Rerunning on expanded datasets

The generator and runner are idempotent. To regenerate after the batch extraction completes:

```bash
contracts-generate \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

contracts-run \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_full.json
```

Each generator run appends a new timestamped snapshot to `schema_snapshots/{contract-id}/`. Baselines update only when you run `contracts-baseline promote`.

---

*Validation data current as of 2026-04-01. See [thursday_report.md](thursday_report.md) for full run analysis and [INSTALL.md](INSTALL.md) for the operator runbook.*
