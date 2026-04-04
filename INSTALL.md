# Data Contract Enforcer — Installation & Operations Guide

## Requirements

- Python 3.10 or higher
- Git (for schema evolution gate and blame chain attribution)

---

## Installation

```bash
git clone https://github.com/nebiyou27/data-contract-enforcer.git
cd data-contract-enforcer

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Core install (validation + contract generation)
pip install -e .

# With AI checks (requires Anthropic API key)
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

---

## Environment Variables

All environment variables are optional. The system runs with defaults if none are set.

| Variable | Default | Purpose |
|----------|---------|---------|
| `LOG_LEVEL` | `INFO` | Root log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(not set)_ | OTLP HTTP endpoint for trace export (e.g. `http://localhost:4318`) |
| `OTEL_SERVICE_NAME` | `data-contract-enforcer` | Service name shown in trace spans |
| `ANTHROPIC_API_KEY` | _(not set)_ | Required only when running `contracts-ai-checks` |

Set them inline or in a `.env` file (not committed):

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export LOG_LEVEL=DEBUG
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

---

## Required Directory Structure

Two directories must exist before first run — everything else is created automatically.

```
data-contract-enforcer/
├── contract_registry/
│   └── subscriptions.yaml          # REQUIRED — create this manually (see below)
├── outputs/                        # REQUIRED — your source JSONL data goes here
│   ├── week3/extractions.jsonl
│   ├── week4/lineage_snapshots.jsonl
│   └── ...
│
# Auto-created on first run:
├── generated_contracts/            # Bitol YAML contracts + dbt schema YML
├── schema_snapshots/               # Timestamped immutable snapshots + baselines.json
├── validation_reports/             # JSON validation run outputs
├── violation_log/                  # violations.jsonl (append-only)
├── quarantine/                     # prompt_schema_violations.jsonl
└── enforcer_report/                # Final aggregated report
```

---

## Config: contract_registry/subscriptions.yaml

This is the single source of truth for which contracts are active, who subscribes to what, and which fields are breaking. Create it before running anything.

```yaml
registry:
  version: "1.0"
  schema_evolution_policy:
    gate: producer-side
    action_on_breaking_change: block
    registry_update_required: true

contracts:
  - id: week3-document-refinery-extractions
    producer: week3-document-refinery
    status: active                    # or "out_of_scope"
    data_path: outputs/week3/extractions.jsonl

  - id: week4-lineage-graph
    producer: week4-lineage-builder
    status: active
    data_path: outputs/week4/lineage_snapshots.jsonl

subscriptions:
  - source: Week 3
    source_contract: week3-document-refinery-extractions
    target: Week 4
    target_contract: week4-lineage-graph
    breaking_fields:
      - field: documents.fact_count
        reason: "Week 4 depends on extraction completeness"
      - field: extracted_facts.confidence
        reason: "Week 4 lineage quality checks rely on confidence semantics"
```

---

## First-Run Walkthrough

### Step 1 — Generate a contract from your data

```bash
contracts-generate \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

Outputs:
- `generated_contracts/week3-document-refinery-extractions.yaml`
- `generated_contracts/week3-document-refinery-extractions_dbt_schema.yml`
- `schema_snapshots/week3-document-refinery-extractions/<timestamp>.yaml`

### Step 2 — Run validation

```bash
contracts-run \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3.json
```

Exit codes: `0` = all checks passed, `1` = failures detected, `2` = error (missing file, bad args).

**Validation modes** (pass via `--mode`):

| Mode | Behavior |
|------|---------|
| `AUDIT` | Report all violations, never fail — use for observability |
| `WARN` | Warn on violations but exit 0 — use for gradual rollout |
| `ENFORCE` | Fail hard on any violation — use in CI/CD gates |

```bash
contracts-run --contract ... --data ... --output ... --mode ENFORCE
```

### Step 3 — Promote baselines (first run only)

On the first run there are no baselines, so drift checks are skipped. After a clean run, promote:

```bash
contracts-baseline promote --report validation_reports/week3.json
```

Subsequent runs will compare against this baseline and flag drift above 2σ.

### Step 4 — Run AI checks (optional, requires `.[llm]`)

```bash
contracts-ai-checks \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_checks.json
```

### Step 5 — Generate the final report

```bash
contracts-report \
  --validation-reports validation_reports/week3.json validation_reports/week4.json \
  --ai-checks validation_reports/ai_checks.json \
  --violation-log violation_log/violations.jsonl \
  --output enforcer_report/report_data.json
```

The report includes a 0–100 **data health score**, a `PASS / ATTENTION_REQUIRED / ISSUES_DETECTED` verdict, and per-failure recommendations with exact contract clause references.

---

## Running All Contracts in Parallel

Create a `batch.yaml`:

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

Then run:

```bash
contracts-run-all --batch batch.yaml
contracts-run-all --batch batch.yaml --fail-fast    # stop on first failure
```

---

## Baseline Management

Baselines are **read-only by default** — a validation run never overwrites them unless you explicitly promote.

```bash
# Inspect current baselines
contracts-baseline list

# Promote stats from a validated report into baselines.json
contracts-baseline promote --report validation_reports/week3.json

# Clear baselines for one contract
contracts-baseline clear --contract week3-document-refinery-extractions

# Wipe everything (requires --yes)
contracts-baseline clear --all --yes
```

---

## Schema Evolution Gate

Install the pre-push hook to block breaking schema changes before they reach `main`:

```bash
bash install_hooks.sh
```

Or run the gate manually:

```bash
contracts-evolution-gate \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --proposed-ref HEAD \
  --current-ref origin/main
```

Exit `0` = compatible (allow push), exit `1` = breaking change detected (block push).

---

## Quarantine Workflow

Records that fail JSON Schema validation on metadata are written to `quarantine/prompt_schema_violations.jsonl`.

```bash
# Review quarantined records
contracts-quarantine review --quarantine quarantine/prompt_schema_violations.jsonl

# Requeue fixed records
contracts-quarantine requeue \
  --quarantine quarantine/prompt_schema_violations.jsonl \
  --fixed-jsonl fixed_records.jsonl

# Clear after resolution
contracts-quarantine clear --quarantine quarantine/prompt_schema_violations.jsonl --yes
```

---

## Logging and Tracing

All output is structured JSON, one object per line:

```json
{"timestamp":"2026-04-04T10:22:16Z","level":"INFO","logger":"contracts.runner","run_id":"<uuid>","message":"Validation complete"}
```

Pretty-print with `jq`:

```bash
contracts-run ... 2>&1 | jq
```

Enable trace export to Datadog, Jaeger, or any OTLP-compatible backend:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 contracts-run ...
```

---

## Running Tests

```bash
pip install -e ".[dev]"
python -m pytest
```

292 tests, ~1 second. The suite covers: contract generation, validation runner, drift checks, schema evolution, AI extensions, baseline manager, report generator, and attributor.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: pandas` | Missing core deps | `pip install -e .` |
| `FileNotFoundError: subscriptions.yaml` | Registry not created | Create `contract_registry/subscriptions.yaml` |
| Drift checks show "no baseline" | First run, no baselines yet | Run once, then `contracts-baseline promote` |
| Validation passes but drift is ignored | Baselines not promoted | `contracts-baseline promote --report <report>.json` |
| AI checks fail with auth error | Missing API key | `export ANTHROPIC_API_KEY=sk-ant-...` |
| Traces not appearing in backend | Endpoint not set | `export OTEL_EXPORTER_OTLP_ENDPOINT=http://...` |
| Memory spike on large files | File loaded into RAM | Use `--stream` flag (>1 GB files) |
