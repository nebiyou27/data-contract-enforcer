"""
contracts/config.py -- Central configuration for the data-contract-enforcer.

All thresholds and limits live here.  Every value has a safe default and can
be overridden via the corresponding ECE_* environment variable so operators
can tune the system without touching source code.

Usage::

    from contracts.config import config

    z_warn = config.drift_z_warn          # default 2.0
    limit  = config.enum_cardinality_limit # default 10

TypedDicts for the key data structures are also exported here so any module
can import typed shapes without creating circular dependencies.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# TypedDicts — typed shapes for the key runtime data structures
# ---------------------------------------------------------------------------

try:
    from typing import TypedDict
except ImportError:  # Python < 3.8 fallback (shouldn't happen given >=3.10 req)
    from typing_extensions import TypedDict  # type: ignore


class CheckResult(TypedDict):
    """Result of a single contract validation check."""

    check_id: str
    column_name: str
    check_type: str
    status: str           # PASS | WARN | FAIL | ERROR
    actual_value: str
    expected: str
    severity: str         # CRITICAL | HIGH | MEDIUM | LOW
    records_failing: int
    sample_failing: list
    message: str


class DriftBaseline(TypedDict):
    """Per-column statistical baseline stored in schema_snapshots/baselines.json."""

    mean: float
    stddev: float
    min: float
    max: float
    count: int
    null_fraction: float
    cardinality: int


class BreakingField(TypedDict):
    """A single breaking-field declaration inside a subscription entry."""

    field: str
    reason: str


class SubscriptionEntry(TypedDict):
    """A declared dependency between a producer contract and a downstream consumer."""

    contract_id: str
    subscriber_id: str
    source: str
    source_contract: str
    target: str
    target_contract: str
    fields_consumed: list[str]
    breaking_fields: list[BreakingField]
    validation_mode: str   # AUDIT | WARN | ENFORCE
    registered_at: str
    contact: str


class FieldRule(TypedDict, total=False):
    """Per-field threshold override declared in a contract ``enforcement`` block.

    All keys are optional. When a key is absent, the global ``EnforcerConfig``
    value is used as the fallback, so partial overrides are safe.
    """

    field: str
    table: str
    drift_z_warn: float
    drift_z_fail: float
    drift_null_warn_pp: float
    drift_null_fail_pp: float
    severity: str
    skip_checks: list[str]


class EnforcementConfig(TypedDict, total=False):
    """Resolved enforcement configuration for one validation run.

    Produced by merging the contract-level ``enforcement`` block with any
    ``validation_overrides`` declared by downstream subscribers in the registry.
    Registry overrides win on a per-field basis; ``skip_checks`` is the union
    of all sources; ``validation_mode`` uses registry value when present.
    """

    skip_checks: list[str]
    validation_mode: str
    field_rules: list[FieldRule]


# Backwards-compatible alias used by earlier revisions of the runner.
ContractEnforcement = EnforcementConfig


class ContractEntry(TypedDict):
    """An entry in the contract catalog (active or out_of_scope)."""

    id: str
    producer: str
    status: str            # active | out_of_scope
    data_path: str | None
    reason: str | None


class ValidationReport(TypedDict):
    """Aggregated output of a single contract validation run."""

    report_id: str
    contract_id: str
    snapshot_id: str
    run_timestamp: str
    total_checks: int
    passed: int
    failed: int
    warned: int
    errored: int
    results: list[CheckResult]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _float_env(name: str, default: float) -> float:
    """Read a float from an env var; return *default* on error or absence."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    """Read an int from an env var; return *default* on error or absence."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# EnforcerConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnforcerConfig:
    """Immutable runtime configuration for the data-contract-enforcer.

    Instantiate via :meth:`from_env` to pick up ECE_* environment variables,
    or construct directly in tests to inject specific values without monkeypatching.

    All thresholds are documented with their corresponding env var name.
    """

    # ------------------------------------------------------------------
    # Drift: z-score on column mean
    # ECE_DRIFT_Z_WARN   default 2.0  → WARN when |z| exceeds this
    # ECE_DRIFT_Z_FAIL   default 3.0  → FAIL when |z| exceeds this
    # ------------------------------------------------------------------
    drift_z_warn: float = 2.0
    drift_z_fail: float = 3.0

    # ------------------------------------------------------------------
    # Drift: stddev ratio bounds
    # ECE_DRIFT_VAR_WARN_HIGH   default 2.0   → WARN when ratio exceeds
    # ECE_DRIFT_VAR_WARN_LOW    default 0.25  → WARN when ratio falls below
    # ECE_DRIFT_VAR_FAIL_HIGH   default 4.0   → FAIL when ratio exceeds
    # ------------------------------------------------------------------
    drift_var_warn_high: float = 2.0
    drift_var_warn_low: float = 0.25
    drift_var_fail_high: float = 4.0

    # ------------------------------------------------------------------
    # Drift: null-fraction growth (percentage points)
    # ECE_DRIFT_NULL_WARN_PP   default 0.05  → WARN at 5 pp growth
    # ECE_DRIFT_NULL_FAIL_PP   default 0.20  → FAIL at 20 pp growth
    # ------------------------------------------------------------------
    drift_null_warn_pp: float = 0.05
    drift_null_fail_pp: float = 0.20

    # ------------------------------------------------------------------
    # Drift: cardinality ratio bounds
    # ECE_DRIFT_CARD_WARN_HIGH   default 2.0  → WARN on spike
    # ECE_DRIFT_CARD_WARN_LOW    default 0.5  → WARN on collapse
    # ECE_DRIFT_CARD_FAIL_HIGH   default 5.0  → FAIL on severe spike
    # ------------------------------------------------------------------
    drift_card_warn_high: float = 2.0
    drift_card_warn_low: float = 0.5
    drift_card_fail_high: float = 5.0

    # ------------------------------------------------------------------
    # AI checks
    # ECE_VIOLATION_RATE_THRESHOLD   default 0.05  (5 %) → write to violation log
    # ECE_LLM_TREND_DELTA            default 0.05  (5 pp) → half-split trend delta
    # ECE_EMBED_WARN                 default 0.1   → cosine distance WARN threshold
    # ECE_EMBED_FAIL                 default 0.3   → cosine distance FAIL threshold
    # ECE_PROMPT_SCHEMA_FAIL_RATE    default 0.10  (10 %) → prompt schema FAIL rate
    # ------------------------------------------------------------------
    llm_violation_rate_threshold: float = 0.05
    llm_trend_delta: float = 0.05
    embedding_warn_distance: float = 0.1
    embedding_fail_distance: float = 0.3
    prompt_schema_fail_rate: float = 0.10

    # ------------------------------------------------------------------
    # Generator
    # ECE_ENUM_CARDINALITY_LIMIT   default 10  → max distinct values for enum inference
    # ECE_SAMPLE_VALUES_LIMIT      default 10  → max sample values stored per column
    # ------------------------------------------------------------------
    enum_cardinality_limit: int = 10
    sample_values_limit: int = 10

    # ------------------------------------------------------------------
    # Attributor
    # ECE_MAX_BLAME_CANDIDATES   default 5   → top-N commits in blame chain
    # ECE_GIT_LOG_LIMIT          default 20  → depth of git log to search
    # ECE_BLAME_DAYS_DISCOUNT    default 0.1 → confidence discount per day of age
    # ECE_BLAME_HOPS_PENALTY     default 0.2 → confidence penalty per lineage hop
    # ------------------------------------------------------------------
    max_blame_candidates: int = 5
    git_log_limit: int = 20
    blame_days_discount: float = 0.1
    blame_hops_penalty: float = 0.2

    # ------------------------------------------------------------------
    # Report generator
    # ECE_CRITICAL_VIOLATION_PENALTY   default 20 → health score deduction per CRITICAL FAIL
    # ------------------------------------------------------------------
    critical_violation_penalty: int = 20

    # ------------------------------------------------------------------
    @classmethod
    def from_env(cls) -> "EnforcerConfig":
        """Build config by reading ECE_* environment variables.

        Unknown or malformed env var values are silently ignored and the
        default is used, so a misconfigured variable never crashes the process.
        """
        return cls(
            drift_z_warn=_float_env("ECE_DRIFT_Z_WARN", 2.0),
            drift_z_fail=_float_env("ECE_DRIFT_Z_FAIL", 3.0),
            drift_var_warn_high=_float_env("ECE_DRIFT_VAR_WARN_HIGH", 2.0),
            drift_var_warn_low=_float_env("ECE_DRIFT_VAR_WARN_LOW", 0.25),
            drift_var_fail_high=_float_env("ECE_DRIFT_VAR_FAIL_HIGH", 4.0),
            drift_null_warn_pp=_float_env("ECE_DRIFT_NULL_WARN_PP", 0.05),
            drift_null_fail_pp=_float_env("ECE_DRIFT_NULL_FAIL_PP", 0.20),
            drift_card_warn_high=_float_env("ECE_DRIFT_CARD_WARN_HIGH", 2.0),
            drift_card_warn_low=_float_env("ECE_DRIFT_CARD_WARN_LOW", 0.5),
            drift_card_fail_high=_float_env("ECE_DRIFT_CARD_FAIL_HIGH", 5.0),
            llm_violation_rate_threshold=_float_env("ECE_VIOLATION_RATE_THRESHOLD", 0.05),
            llm_trend_delta=_float_env("ECE_LLM_TREND_DELTA", 0.05),
            embedding_warn_distance=_float_env("ECE_EMBED_WARN", 0.1),
            embedding_fail_distance=_float_env("ECE_EMBED_FAIL", 0.3),
            prompt_schema_fail_rate=_float_env("ECE_PROMPT_SCHEMA_FAIL_RATE", 0.10),
            enum_cardinality_limit=_int_env("ECE_ENUM_CARDINALITY_LIMIT", 10),
            sample_values_limit=_int_env("ECE_SAMPLE_VALUES_LIMIT", 10),
            max_blame_candidates=_int_env("ECE_MAX_BLAME_CANDIDATES", 5),
            git_log_limit=_int_env("ECE_GIT_LOG_LIMIT", 20),
            blame_days_discount=_float_env("ECE_BLAME_DAYS_DISCOUNT", 0.1),
            blame_hops_penalty=_float_env("ECE_BLAME_HOPS_PENALTY", 0.2),
            critical_violation_penalty=_int_env("ECE_CRITICAL_VIOLATION_PENALTY", 20),
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

#: Import this in other modules for runtime threshold access.
#: Tests that need custom values should construct EnforcerConfig(...) directly.
config: EnforcerConfig = EnforcerConfig.from_env()
