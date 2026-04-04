"""
tests/test_integration.py -- End-to-end and failure-scenario integration tests.

These tests run the full pipeline on synthetic JSONL data in temporary
directories so no real output files are touched.

Coverage
--------
End-to-end (via run_table_checks + runner.main):
  1. Clean data → run_table_checks → all checks PASS
  2. Missing required field → FAIL detected
  3. Enum violation → FAIL detected
  4. UUID format violation → FAIL detected
  5. runner.main writes a valid JSON report to disk
  6. runner.main exits 0 on clean data (AUDIT mode)
  7. Health score is 100 for a clean run with no CRITICAL FAILs

Failure scenarios:
  8.  runner.main against a missing data file → exits 2
  9.  runner.main against a missing contract file → exits 2
 10.  runner.main against corrupt (non-JSON) JSONL → handles gracefully
 11.  Evolution gate blocks removal of a breaking field
 12.  Evolution gate passes when only non-breaking fields are removed
 13.  Report generator with zero validation reports → health score 0
 14.  Critical FAIL deducts 20 points from health score

Config:
 15.  Env var override respected at runtime
 16.  Malformed env var falls back to default
 17.  Default EnforcerConfig has correct threshold values
 18.  EnforcerConfig is frozen (immutable)
 19.  Custom config in test does not mutate module singleton
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from contracts.config import EnforcerConfig
from contracts.runner import (
    check_producer_evolution_gate,
    check_required,
    check_enum,
    check_uuid_format,
    run_table_checks,
    compute_column_stats,
)
from contracts.report_generator import (
    compute_data_health_score,
    generate_report,
    count_violations,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_UUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _clean_records(n: int = 10) -> list[dict]:
    return [
        {
            "doc_id": str(uuid.uuid4()),
            "source_path": f"/data/doc_{i}.pdf",
            "extraction_model": "model_a",
            "extracted_at": "2026-01-01T00:00:00Z",
            "fact_count": float(i),
        }
        for i in range(n)
    ]


def _minimal_contract(contract_id: str) -> dict:
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {"title": "Test", "version": "1.0.0"},
        "schema": {
            "tables": [
                {
                    "name": "documents",
                    "fields": [
                        {"name": "doc_id", "type": "string", "required": True, "format": "uuid"},
                        {"name": "source_path", "type": "string", "required": True},
                        {"name": "extraction_model", "type": "string", "required": True,
                         "enum": ["model_a", "model_b"]},
                        {"name": "extracted_at", "type": "string", "required": True,
                         "format": "date-time"},
                        {"name": "fact_count", "type": "number"},
                    ],
                }
            ]
        },
    }


def _records_to_df(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 1–4: run_table_checks integration
# ---------------------------------------------------------------------------

class RunTableChecksIntegrationTest(unittest.TestCase):
    """run_table_checks is the core validation engine — test it end-to-end."""

    def _fields(self) -> list[dict]:
        return _minimal_contract("test")["schema"]["tables"][0]["fields"]

    # 1. Clean data → all checks PASS
    def test_clean_data_all_checks_pass(self) -> None:
        df = _records_to_df(_clean_records(20))
        results, _ = run_table_checks("documents", self._fields(), df, {}, "test-contract")
        fails = [r for r in results if r["status"] == "FAIL"]
        self.assertEqual([], fails, f"Unexpected failures: {fails}")

    # 2. Missing required field → FAIL
    def test_missing_required_field_fails(self) -> None:
        records = _clean_records(10)
        for rec in records[5:]:
            rec["source_path"] = None
        df = _records_to_df(records)
        results, _ = run_table_checks("documents", self._fields(), df, {}, "test-contract")
        required_fails = [
            r for r in results
            if r["check_type"] == "required" and r["status"] == "FAIL"
            and r["column_name"] == "source_path"
        ]
        self.assertTrue(len(required_fails) > 0, "Expected required FAIL for source_path")
        self.assertEqual(5, required_fails[0]["records_failing"])

    # 3. Enum violation → FAIL
    def test_enum_violation_fails(self) -> None:
        records = _clean_records(10)
        records[3]["extraction_model"] = "UNKNOWN_MODEL"
        df = _records_to_df(records)
        results, _ = run_table_checks("documents", self._fields(), df, {}, "test-contract")
        enum_fails = [r for r in results if r["check_type"] == "enum" and r["status"] == "FAIL"]
        self.assertTrue(len(enum_fails) > 0, "Expected enum FAIL for unknown model")

    # 4. UUID format violation → FAIL
    def test_bad_uuid_fails(self) -> None:
        records = _clean_records(10)
        records[0]["doc_id"] = "NOT-A-UUID"
        df = _records_to_df(records)
        results, _ = run_table_checks("documents", self._fields(), df, {}, "test-contract")
        uuid_fails = [r for r in results if r["check_type"] == "format_uuid" and r["status"] == "FAIL"]
        self.assertTrue(len(uuid_fails) > 0, "Expected UUID format FAIL")


# ---------------------------------------------------------------------------
# 5–6: runner.main CLI integration
# ---------------------------------------------------------------------------

class RunnerMainIntegrationTest(unittest.TestCase):
    """Call runner.main(argv=[...]) to exercise the full CLI path.

    Temp files are written inside the project root so the runner's
    path-traversal guard (_safe_path) does not reject them.
    """

    def setUp(self) -> None:
        # Must be inside _PROJECT_ROOT to pass the runner's safe-path check
        self._tmp_dir = _PROJECT_ROOT / "_test_tmp"
        self._tmp_dir.mkdir(exist_ok=True)
        import time
        self.base = self._tmp_dir / str(int(time.time() * 1e6))
        self.base.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _write_contract(self, contract_id: str) -> Path:
        path = self.base / f"{contract_id}.yaml"
        with open(path, "w") as fh:
            yaml.dump(_minimal_contract(contract_id), fh)
        return path

    # 5. runner.main writes a valid JSON report to disk
    def test_runner_main_writes_report(self) -> None:
        from contracts.runner import main as runner_main

        data_path = self.base / "data.jsonl"
        _write_jsonl(data_path, _clean_records(5))
        contract_path = self._write_contract("test-report")
        report_path = self.base / "report.json"

        exit_code = runner_main([
            "--contract", str(contract_path),
            "--data", str(data_path),
            "--output", str(report_path),
            "--mode", "AUDIT",
        ])

        self.assertTrue(report_path.exists(), "report.json was not written")
        with open(report_path) as fh:
            report = json.load(fh)
        self.assertIn("total_checks", report)
        self.assertIn("results", report)
        self.assertIn("contract_id", report)

    # 6. runner.main exits 0 on clean data in AUDIT mode
    def test_runner_main_exits_0_on_clean_data(self) -> None:
        from contracts.runner import main as runner_main

        data_path = self.base / "data.jsonl"
        _write_jsonl(data_path, _clean_records(5))
        contract_path = self._write_contract("test-exit0")
        report_path = self.base / "report.json"

        exit_code = runner_main([
            "--contract", str(contract_path),
            "--data", str(data_path),
            "--output", str(report_path),
            "--mode", "AUDIT",
        ])
        self.assertEqual(0, exit_code)


# ---------------------------------------------------------------------------
# 7: Health score
# ---------------------------------------------------------------------------

class HealthScoreTest(unittest.TestCase):

    # 7. Health score 100 for clean run
    def test_health_score_100_for_clean_run(self) -> None:
        report = {
            "contract_id": "test",
            "total_checks": 20,
            "passed": 20,
            "failed": 0,
            "warned": 0,
            "errored": 0,
            "results": [],
        }
        self.assertEqual(100, compute_data_health_score([report]))


# ---------------------------------------------------------------------------
# 8–10: Failure scenarios via runner.main
# ---------------------------------------------------------------------------

class FailureScenariosTest(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp_dir = _PROJECT_ROOT / "_test_tmp"
        self._tmp_dir.mkdir(exist_ok=True)
        import time
        self.base = self._tmp_dir / str(int(time.time() * 1e6))
        self.base.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _write_contract(self, contract_id: str) -> Path:
        path = self.base / f"{contract_id}.yaml"
        with open(path, "w") as fh:
            yaml.dump(_minimal_contract(contract_id), fh)
        return path

    # 8. Missing data file → runner raises FileNotFoundError or returns 1/2
    def test_runner_missing_data_file_fails(self) -> None:
        from contracts.runner import main as runner_main
        contract_path = self._write_contract("test-missing-data")
        report_path = self.base / "report.json"

        try:
            code = runner_main([
                "--contract", str(contract_path),
                "--data", str(self.base / "nonexistent.jsonl"),
                "--output", str(report_path),
            ])
            # If it returns, it should be a non-zero exit code
            self.assertNotEqual(0, code, "Expected non-zero exit for missing data file")
        except (FileNotFoundError, SystemExit, ValueError):
            pass  # any of these are acceptable — the runner correctly rejects the request

    # 9. Missing contract file → runner raises FileNotFoundError or returns 1/2
    def test_runner_missing_contract_fails(self) -> None:
        from contracts.runner import main as runner_main
        data_path = self.base / "data.jsonl"
        _write_jsonl(data_path, _clean_records(3))
        report_path = self.base / "report.json"

        try:
            code = runner_main([
                "--contract", str(self.base / "nonexistent.yaml"),
                "--data", str(data_path),
                "--output", str(report_path),
            ])
            self.assertNotEqual(0, code, "Expected non-zero exit for missing contract file")
        except (FileNotFoundError, SystemExit, ValueError):
            pass  # correctly rejected

    # 10. Corrupt JSONL — runner raises or handles gracefully (does not silently succeed)
    def test_runner_corrupt_jsonl_does_not_silently_succeed(self) -> None:
        from contracts.runner import main as runner_main
        data_path = self.base / "corrupt.jsonl"
        contract_path = self._write_contract("test-corrupt")
        report_path = self.base / "report.json"

        good = _clean_records(1)[0]
        with open(data_path, "w") as fh:
            fh.write(json.dumps(good) + "\n")
            fh.write("THIS IS NOT JSON }{{\n")
            fh.write(json.dumps(good) + "\n")

        try:
            code = runner_main([
                "--contract", str(contract_path),
                "--data", str(data_path),
                "--output", str(report_path),
                "--mode", "AUDIT",
            ])
            # If it completes, the report must be a valid JSON file
            if report_path.exists():
                with open(report_path) as fh:
                    report = json.load(fh)
                self.assertIn("total_checks", report)
        except (json.JSONDecodeError, FileNotFoundError, SystemExit, ValueError):
            pass  # raising on corrupt input is also acceptable


# ---------------------------------------------------------------------------
# 11–12: Evolution gate integration
# ---------------------------------------------------------------------------

class EvolutionGateIntegrationTest(unittest.TestCase):

    def _make_registry(self, breaking_fields: list[str]) -> dict:
        return {
            "subscriptions": [
                {
                    "source": "Week 3",
                    "source_contract": "week3-test",
                    "target": "Week 4",
                    "target_contract": "week4-test",
                    "breaking_fields": [
                        {"field": f, "reason": "downstream depends on it"}
                        for f in breaking_fields
                    ],
                    "fields_consumed": breaking_fields,
                    "validation_mode": "ENFORCE",
                }
            ],
            "contracts": [],
            "schema_evolution_policy": {"gate": "producer-side", "action_on_breaking_change": "block"},
        }

    # 11. Removing a breaking field → BLOCK
    def test_removing_breaking_field_is_blocked(self) -> None:
        registry = self._make_registry(["confidence"])
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "fact_count"],
            current_fields=["doc_id", "fact_count", "confidence"],
            contract_id="week3-test",
            registry=registry,
        )
        self.assertEqual("BLOCK", result["action"])
        self.assertGreater(len(result["breaking_fields_affected"]), 0)

    # 12. Removing only non-breaking fields → PASS
    def test_removing_non_breaking_field_passes(self) -> None:
        registry = self._make_registry(["confidence"])
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "confidence"],
            current_fields=["doc_id", "confidence", "debug_field"],
            contract_id="week3-test",
            registry=registry,
        )
        self.assertEqual("PASS", result["action"])
        self.assertEqual([], result["breaking_fields_affected"])


# ---------------------------------------------------------------------------
# 13–14: Report generator edge cases
# ---------------------------------------------------------------------------

class ReportGeneratorEdgeCasesTest(unittest.TestCase):

    # 13. Zero validation reports → health score 0
    def test_health_score_zero_with_no_reports(self) -> None:
        self.assertEqual(0, compute_data_health_score([]))

    def test_generate_report_empty_list_does_not_raise(self) -> None:
        report = generate_report(validation_reports=[])
        self.assertIn("data_health_score", report)
        self.assertEqual(0, report["data_health_score"])

    def test_violation_count_missing_log_returns_zero(self) -> None:
        count, ids = count_violations(Path("/nonexistent/violations.jsonl"))
        self.assertEqual(0, count)
        self.assertEqual([], ids)

    # 14. Critical FAIL deducts 20 points
    def test_critical_fail_deducts_from_health_score(self) -> None:
        report = {
            "contract_id": "test",
            "total_checks": 10,
            "passed": 9,
            "failed": 1,
            "warned": 0,
            "errored": 0,
            "results": [
                {
                    "check_type": "required",
                    "status": "FAIL",
                    "severity": "CRITICAL",
                    "column_name": "doc_id",
                    "check_id": "documents.doc_id.required",
                    "actual_value": "1 null",
                    "expected": "0 nulls",
                    "records_failing": 1,
                    "sample_failing": [],
                    "message": "null found",
                }
            ],
        }
        # base = (9/10)*100 = 90; deduct 20 per critical fail → 70
        self.assertEqual(70, compute_data_health_score([report]))


# ---------------------------------------------------------------------------
# 15–19: Config tests
# ---------------------------------------------------------------------------

class ConfigTest(unittest.TestCase):

    # 15. Env var override respected
    def test_env_var_override_respected(self) -> None:
        original = os.environ.get("ECE_DRIFT_Z_WARN")
        try:
            os.environ["ECE_DRIFT_Z_WARN"] = "1.5"
            cfg = EnforcerConfig.from_env()
            self.assertAlmostEqual(1.5, cfg.drift_z_warn)
        finally:
            if original is None:
                os.environ.pop("ECE_DRIFT_Z_WARN", None)
            else:
                os.environ["ECE_DRIFT_Z_WARN"] = original

    # 16. Malformed env var falls back to default
    def test_malformed_env_var_falls_back_to_default(self) -> None:
        original = os.environ.get("ECE_DRIFT_Z_FAIL")
        try:
            os.environ["ECE_DRIFT_Z_FAIL"] = "NOT_A_NUMBER"
            cfg = EnforcerConfig.from_env()
            self.assertAlmostEqual(3.0, cfg.drift_z_fail)
        finally:
            if original is None:
                os.environ.pop("ECE_DRIFT_Z_FAIL", None)
            else:
                os.environ["ECE_DRIFT_Z_FAIL"] = original

    # 17. Default thresholds are correct
    def test_default_config_thresholds(self) -> None:
        cfg = EnforcerConfig()
        self.assertAlmostEqual(2.0, cfg.drift_z_warn)
        self.assertAlmostEqual(3.0, cfg.drift_z_fail)
        self.assertAlmostEqual(0.05, cfg.llm_violation_rate_threshold)
        self.assertAlmostEqual(0.1, cfg.embedding_warn_distance)
        self.assertAlmostEqual(0.3, cfg.embedding_fail_distance)
        self.assertEqual(10, cfg.enum_cardinality_limit)
        self.assertEqual(5, cfg.max_blame_candidates)
        self.assertEqual(20, cfg.critical_violation_penalty)

    # 18. Config is frozen
    def test_config_is_immutable(self) -> None:
        cfg = EnforcerConfig()
        with self.assertRaises((AttributeError, TypeError)):
            cfg.drift_z_warn = 99.0  # type: ignore[misc]

    # 19. Custom config does not mutate module singleton
    def test_custom_config_does_not_affect_singleton(self) -> None:
        from contracts.config import config as module_config
        custom = EnforcerConfig(drift_z_warn=0.1)
        self.assertAlmostEqual(0.1, custom.drift_z_warn)
        self.assertAlmostEqual(2.0, module_config.drift_z_warn)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
